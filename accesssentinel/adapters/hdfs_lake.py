"""Historical store on HDFS (D3 Step 3): partitioned Parquet written by the HDFS sink, read by history
queries, periodic sweeps (Phase 6) and training-set export (Phases 5, 7, 9).

Layout (UTC dates):
    <base>/events/date=YYYY-MM-DD/app=<application>/part-*.parquet     access events e_t
    <base>/<dataset>/date=YYYY-MM-DD/part-*.parquet                    risk-scores, group-scores,
                                                                       pdp-decisions, analyst-feedback, ...
Partition pruning: reads list only the date (and app) directories inside the requested range.
Duplicates: the sink avoids writing a record twice; reads and compaction additionally keep only the first
row per event_id / record_id, so a crash between "file written" and "offset committed" cannot surface twice.

`client` is an `hdfs.InsecureClient` (WebHDFS) in deployment. `LocalFsClient` implements the same subset
on a local directory and exists ONLY as a development fallback; it is not HDFS and is logged as such.
"""
import contextlib
import io
import json
import logging
import os
import shutil
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote, unquote

import pyarrow as pa
import pyarrow.parquet as pq

from .parquet_io import read_parquet_bytes

from ..events import AccessEvent

log = logging.getLogger("accesssentinel.history")

EVENTS = "events"
_EVENT_FIELDS = list(AccessEvent.__dataclass_fields__)


def utc_date(t: float) -> str:
    return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d")


def _date_bounds(d: str) -> Tuple[float, float]:
    start = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    return start, start + 86400.0


class LocalFsClient:
    """DEVELOPMENT-ONLY stand-in for the WebHDFS client (same method subset) backed by a local directory."""

    def __init__(self, root: str):
        self.root = root
        os.makedirs(root, exist_ok=True)
        log.warning("history backend is a LOCAL DIRECTORY (%s): development fallback, not HDFS", root)

    def _p(self, path: str) -> str:
        return os.path.join(self.root, path.lstrip("/"))

    def makedirs(self, path: str) -> None:
        os.makedirs(self._p(path), exist_ok=True)

    def write(self, path: str, data=None, overwrite: bool = False) -> None:
        p = self._p(path)
        if os.path.exists(p) and not overwrite:
            raise IOError(f"exists: {path}")
        os.makedirs(os.path.dirname(p), exist_ok=True)
        tmp = f"{p}.tmp-{uuid.uuid4().hex[:6]}"
        with open(tmp, "wb") as f:
            f.write(data if isinstance(data, bytes) else data.encode())
        os.replace(tmp, p)

    @contextlib.contextmanager
    def read(self, path: str):
        with open(self._p(path), "rb") as f:
            yield f

    def list(self, path: str) -> List[str]:
        return sorted(n for n in os.listdir(self._p(path)) if ".tmp-" not in n)

    def status(self, path: str, strict: bool = True):
        p = self._p(path)
        if os.path.isdir(p):
            return {"type": "DIRECTORY"}
        if os.path.isfile(p):
            return {"type": "FILE", "length": os.path.getsize(p)}
        if strict:
            raise IOError(f"missing: {path}")
        return None

    def delete(self, path: str, recursive: bool = False) -> bool:
        p = self._p(path)
        if os.path.isdir(p):
            shutil.rmtree(p) if recursive else os.rmdir(p)
        elif os.path.exists(p):
            os.remove(p)
        else:
            return False
        return True


def _event_row(ev: AccessEvent, ingest: Optional[dict] = None) -> dict:
    row = {k: getattr(ev, k) for k in _EVENT_FIELDS}
    row["c"] = json.dumps(row["c"], sort_keys=True)
    row["meta"] = json.dumps(row["meta"], sort_keys=True)
    ing = ingest or {}
    row["_kafka_partition"] = int(ing.get("partition", -1))
    row["_kafka_offset"] = int(ing.get("offset", -1))
    row["_ingested_at"] = float(ing.get("ingested_at", time.time()))
    return row


def _row_event(row: dict) -> AccessEvent:
    d = {k: row[k] for k in _EVENT_FIELDS if k in row}
    d["c"] = json.loads(d["c"]) if isinstance(d.get("c"), str) else (d.get("c") or {})
    d["meta"] = json.loads(d["meta"]) if isinstance(d.get("meta"), str) else (d.get("meta") or {})
    return AccessEvent(**d)


def _dedup_key(row: dict, id_field: str) -> Optional[str]:
    v = row.get(id_field)
    return v if v else None


class HistoryLake:
    def __init__(self, client, base: str = "/accesssentinel"):
        self.client, self.base = client, base.rstrip("/")
        self._buf: List[AccessEvent] = []       # for the HistoricalStore-compatible append()/flush()
        self._n = 0

    # ------------------------------------------------------------------ paths
    def _dataset_dir(self, dataset: str) -> str:
        return f"{self.base}/{dataset}"

    def _part_dir(self, dataset: str, date: str, app: Optional[str] = None) -> str:
        d = f"{self._dataset_dir(dataset)}/date={date}"
        return f"{d}/app={quote(app, safe='-_.')}" if app is not None else d

    def _ls(self, path: str) -> List[str]:
        if self.client.status(path, strict=False) is None:
            return []
        return self.client.list(path)

    def _dates(self, dataset: str, t0: float, t1: float) -> List[str]:
        lo, hi = utc_date(max(t0, 0.0)), utc_date(max(t1, 0.0))
        return sorted(n.split("=", 1)[1] for n in self._ls(self._dataset_dir(dataset))
                      if n.startswith("date=") and lo <= n.split("=", 1)[1] <= hi)

    def _write_table(self, directory: str, rows: List[dict]) -> str:
        sink = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(rows), sink, compression="snappy")
        self.client.makedirs(directory)
        path = f"{directory}/part-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}.parquet"
        self.client.write(path, data=sink.getvalue(), overwrite=False)
        return path

    def _read_dir(self, directory: str) -> List[dict]:
        rows = []
        for name in self._ls(directory):
            if name.endswith(".parquet"):
                with self.client.read(f"{directory}/{name}") as r:
                    rows.extend(read_parquet_bytes(r.read()).to_pylist())
        return rows

    # ------------------------------------------------------------------ writes
    def write_events(self, items: Sequence[Tuple[AccessEvent, dict]]) -> List[str]:
        """items: (event, ingest metadata {partition, offset, ingested_at}). One file per (date, app)."""
        groups: Dict[Tuple[str, str], List[dict]] = {}
        for ev, ing in items:
            groups.setdefault((utc_date(ev.t), ev.a), []).append(_event_row(ev, ing))
        return [self._write_table(self._part_dir(EVENTS, d, a), rows) for (d, a), rows in sorted(groups.items())]

    def write_records(self, dataset: str, rows: Sequence[dict]) -> List[str]:
        """Generic records with at least {record_id, ts}; `payload` is stored as a JSON string."""
        if dataset == EVENTS:
            raise ValueError("use write_events for access events")
        groups: Dict[str, List[dict]] = {}
        for r in rows:
            row = dict(r)
            if not isinstance(row.get("payload"), str):
                row["payload"] = json.dumps(row.get("payload"), sort_keys=True)
            groups.setdefault(utc_date(float(row["ts"])), []).append(row)
        return [self._write_table(self._part_dir(dataset, d), rs) for d, rs in sorted(groups.items())]

    # ------------------------------------------------------------------ reads
    def read_events(self, t0: float, t1: float, apps: Optional[Iterable[str]] = None,
                    users: Optional[Iterable[str]] = None, dedup: bool = True) -> List[AccessEvent]:
        """Events with t0 < t <= t1 (same semantics as HistoricalStore.read_range), sorted by t."""
        app_set = set(apps) if apps is not None else None
        user_set = set(users) if users is not None else None
        seen, out = set(), []
        for d in self._dates(EVENTS, t0, t1):
            day_dir = self._part_dir(EVENTS, d)
            for name in self._ls(day_dir):
                if not name.startswith("app="):
                    continue
                if app_set is not None and unquote(name.split("=", 1)[1]) not in app_set:
                    continue
                for row in self._read_dir(f"{day_dir}/{name}"):
                    if not (t0 < row["t"] <= t1) or (user_set is not None and row["u"] not in user_set):
                        continue
                    k = _dedup_key(row, "event_id")
                    if dedup and k is not None:
                        if k in seen:
                            continue
                        seen.add(k)
                    out.append(_row_event(row))
        out.sort(key=lambda e: (e.t, e.event_id))
        return out

    def user_history(self, user: str, t0: float, t1: float) -> List[AccessEvent]:
        return self.read_events(t0, t1, users=[user])

    def read_records(self, dataset: str, t0: float, t1: float, dedup: bool = True) -> List[dict]:
        seen, out = set(), []
        for d in self._dates(dataset, t0, t1):
            for row in self._read_dir(self._part_dir(dataset, d)):
                if not (t0 < float(row["ts"]) <= t1):
                    continue
                k = _dedup_key(row, "record_id")
                if dedup and k is not None:
                    if k in seen:
                        continue
                    seen.add(k)
                row["payload"] = json.loads(row["payload"]) if isinstance(row.get("payload"), str) else row.get("payload")
                out.append(row)
        out.sort(key=lambda r: float(r["ts"]))
        return out

    def partitions(self, dataset: str = EVENTS) -> List[dict]:
        out = []
        for n in self._ls(self._dataset_dir(dataset)):
            if not n.startswith("date="):
                continue
            d = n.split("=", 1)[1]
            day_dir = self._part_dir(dataset, d)
            if dataset == EVENTS:
                for a in self._ls(day_dir):
                    if a.startswith("app="):
                        files = [f for f in self._ls(f"{day_dir}/{a}") if f.endswith(".parquet")]
                        out.append({"date": d, "app": unquote(a.split("=", 1)[1]), "files": len(files)})
            else:
                out.append({"date": d, "app": None, "files": len([f for f in self._ls(day_dir) if f.endswith(".parquet")])})
        return out

    # ------------------------------------------------------------------ maintenance
    def compact(self, dataset: str, date: str, app: Optional[str] = None) -> dict:
        """Merge the part files of one partition into one file, dropping duplicate ids. The merged file is
        written before the inputs are deleted, so a crash leaves duplicates (removed on read), never loss."""
        directory = self._part_dir(dataset, date, app if dataset == EVENTS else None)
        names = [n for n in self._ls(directory) if n.endswith(".parquet")]
        if len(names) <= 1:
            return {"files_before": len(names), "rows": None}
        tables = []
        for n in names:
            with self.client.read(f"{directory}/{n}") as r:
                tables.append(read_parquet_bytes(r.read()))
        rows = pa.concat_tables(tables, promote_options="default").to_pylist()
        id_field = "event_id" if dataset == EVENTS else "record_id"
        seen, keep = set(), []
        for row in rows:
            k = _dedup_key(row, id_field)
            if k is not None:
                if k in seen:
                    continue
                seen.add(k)
            keep.append(row)
        self._write_table(directory, keep)
        for n in names:
            self.client.delete(f"{directory}/{n}")
        return {"files_before": len(names), "rows_before": len(rows), "rows": len(keep)}

    # ------------------------------------------------------------------ HistoricalStore-compatible interface
    def append(self, ev: AccessEvent) -> None:
        self._buf.append(ev)
        self._n += 1

    def flush(self) -> None:
        if self._buf:
            self.write_events([(e, {}) for e in self._buf])
            self._buf.clear()

    def read_range(self, t0: float, t1: float) -> List[AccessEvent]:
        self.flush()
        return self.read_events(t0, t1)

    def write_feedback(self, rows: Iterable[dict]) -> None:
        now = time.time()
        recs = [{"record_id": r.get("record_id") or uuid.uuid4().hex, "ts": float(r.get("t", now)), "key": "",
                 "payload": r} for r in rows]
        if recs:
            self.write_records("feedback-records", recs)

    def __len__(self) -> int:
        return self._n


def make_lake(settings) -> HistoryLake:
    """HDFS by default; the local directory backend only when AS_HISTORY_BACKEND=local (development)."""
    if settings.history_backend == "local":
        return HistoryLake(LocalFsClient(settings.history_local_dir), base=settings.hdfs_base)
    from .hdfs_store import make_client
    return HistoryLake(make_client(settings.hdfs_url, user=settings.hdfs_user), base=settings.hdfs_base)
