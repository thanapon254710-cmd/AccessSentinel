"""Historical log store (HDFS stand-in). Hour-partitioned JSONL on the local filesystem.
Swap for pyarrow.fs.HadoopFileSystem + Parquet when a cluster is available."""
import json
import os
from collections import defaultdict
from typing import Iterable, List
from .events import AccessEvent


class HistoricalStore:
    def __init__(self, base_dir: str):
        self.base = base_dir
        os.makedirs(os.path.join(base_dir, "events"), exist_ok=True)
        os.makedirs(os.path.join(base_dir, "feedback"), exist_ok=True)
        self._buf = defaultdict(list)
        self._n = 0

    def _path(self, hour: int) -> str:
        return os.path.join(self.base, "events", f"hour={hour:06d}.jsonl")

    def append(self, ev: AccessEvent):
        self._buf[int(ev.t // 3600)].append(ev)
        self._n += 1

    def flush(self):
        for hour, evs in self._buf.items():
            with open(self._path(hour), "a") as f:
                for e in evs:
                    f.write(json.dumps(e.__dict__) + "\n")
        self._buf.clear()

    def read_range(self, t0: float, t1: float) -> List[AccessEvent]:
        self.flush()
        out = []
        for hour in range(int(t0 // 3600), int(t1 // 3600) + 1):
            p = self._path(hour)
            if not os.path.exists(p):
                continue
            with open(p) as f:
                for line in f:
                    e = AccessEvent(**json.loads(line))
                    if t0 < e.t <= t1:
                        out.append(e)
        out.sort(key=lambda e: e.t)
        return out

    def write_feedback(self, rows: Iterable[dict]):
        with open(os.path.join(self.base, "feedback", "records.jsonl"), "a") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")

    def __len__(self):
        return self._n
