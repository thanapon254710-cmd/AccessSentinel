"""Real HDFS backend over WebHDFS (pure-Python `hdfs` package) with Parquet files.
Same interface as storage.HistoricalStore. Layout: <base>/events/hour=NNNNNN/part-xxxxxxxx.parquet"""
import io
import json
import uuid
from collections import defaultdict
from typing import Iterable, List
import pyarrow as pa
import pyarrow.parquet as pq
from ..events import AccessEvent


def make_client(url: str, user: str = "root"):
    from hdfs import InsecureClient
    return InsecureClient(url, user=user)


class WebHdfsStore:
    def __init__(self, client, base: str = "/accesssentinel", flush_every: int = 5000):
        self.client, self.base, self.flush_every = client, base.rstrip("/"), flush_every
        self._buf = defaultdict(list)
        self._pending = 0
        self._n = 0
        self.client.makedirs(f"{self.base}/events"); self.client.makedirs(f"{self.base}/feedback")

    def _dir(self, hour: int) -> str:
        return f"{self.base}/events/hour={hour:06d}"

    def append(self, ev: AccessEvent):
        self._buf[int(ev.t // 3600)].append(ev); self._n += 1; self._pending += 1
        if self._pending >= self.flush_every: self.flush()

    def flush(self):
        for hour, evs in self._buf.items():
            if not evs: continue
            rows = [{**e.__dict__, "c": json.dumps(e.c)} for e in evs]
            sink = io.BytesIO(); pq.write_table(pa.Table.from_pylist(rows), sink, compression="snappy")
            self.client.makedirs(self._dir(hour))
            self.client.write(f"{self._dir(hour)}/part-{uuid.uuid4().hex[:8]}.parquet", data=sink.getvalue(), overwrite=True)
        self._buf.clear(); self._pending = 0

    def read_range(self, t0: float, t1: float) -> List[AccessEvent]:
        self.flush()
        out = []
        for hour in range(int(t0 // 3600), int(t1 // 3600) + 1):
            d = self._dir(hour)
            if self.client.status(d, strict=False) is None: continue
            for name in sorted(self.client.list(d)):
                with self.client.read(f"{d}/{name}") as r:
                    tbl = pq.read_table(io.BytesIO(r.read()))
                for row in tbl.to_pylist():
                    row["c"] = json.loads(row["c"]); e = AccessEvent(**row)
                    if t0 < e.t <= t1: out.append(e)
        out.sort(key=lambda e: e.t)
        return out

    def compact(self, hour: int) -> int:
        """Merge small part files of one hour into one (small-file / NameNode metadata pressure)."""
        self.flush(); d = self._dir(hour)
        if self.client.status(d, strict=False) is None: return 0
        names = sorted(self.client.list(d))
        if len(names) <= 1: return len(names)
        tables = []
        for n in names:
            with self.client.read(f"{d}/{n}") as r: tables.append(pq.read_table(io.BytesIO(r.read())))
        sink = io.BytesIO(); pq.write_table(pa.concat_tables(tables), sink, compression="snappy")
        merged = f"part-compact-{uuid.uuid4().hex[:8]}.parquet"
        self.client.write(f"{d}/{merged}", data=sink.getvalue(), overwrite=True)
        for n in names: self.client.delete(f"{d}/{n}")
        return 1

    def write_feedback(self, rows: Iterable[dict]):
        data = "".join(json.dumps(r) + "\n" for r in rows).encode()
        self.client.write(f"{self.base}/feedback/part-{uuid.uuid4().hex[:8]}.jsonl", data=data, overwrite=True)

    def __len__(self):
        return self._n
