"""Fakes for confluent_kafka Producer/Consumer and the `hdfs` client, so adapters can be tested without services."""
import contextlib, os, shutil
from collections import defaultdict


class FakeCluster:
    def __init__(self, n_partitions=4):
        self.n = n_partitions
        self.topics = defaultdict(lambda: [[] for _ in range(self.n)])
        self.offsets = defaultdict(int)                       # (group, topic, partition) -> next offset


class _Msg:
    def __init__(self, p, o, v): self._p, self._o, self._v = p, o, v
    def error(self): return None
    def partition(self): return self._p
    def offset(self): return self._o
    def value(self): return self._v


class FakeProducer:
    def __init__(self, cluster): self.c, self.sent = cluster, []
    def __call__(self, conf): return self
    def produce(self, topic, key, value):
        p = hash(key) % self.c.n                              # stands in for murmur2: same key -> same partition
        self.c.topics[topic][p].append(value); self.sent.append((topic, key, value, p))
    def poll(self, t): pass
    def flush(self, t=None): pass


class FakeConsumer:
    def __init__(self, cluster): self.c = cluster
    def __call__(self, conf):
        c = FakeConsumer(self.c); c.group = conf["group.id"]; c.topic = None; return c
    def subscribe(self, topics): self.topic = topics[0]
    def consume(self, num, timeout):
        out = []
        for p in range(self.c.n):
            log = self.c.topics[self.topic][p]
            while self.c.offsets[(self.group, self.topic, p)] < len(log) and len(out) < num:
                o = self.c.offsets[(self.group, self.topic, p)]
                out.append(_Msg(p, o, log[o])); self.c.offsets[(self.group, self.topic, p)] += 1
        return out
    def close(self): pass


class FakeHdfs:
    """Implements the subset of hdfs.InsecureClient used by WebHdfsStore, backed by a local directory."""
    def __init__(self, root): self.root = root
    def _p(self, path): return os.path.join(self.root, path.lstrip("/"))
    def makedirs(self, path): os.makedirs(self._p(path), exist_ok=True)
    def write(self, path, data=None, overwrite=False):
        os.makedirs(os.path.dirname(self._p(path)), exist_ok=True)
        open(self._p(path), "wb").write(data if isinstance(data, bytes) else data.encode())
    @contextlib.contextmanager
    def read(self, path):
        with open(self._p(path), "rb") as f: yield f
    def list(self, path): return sorted(os.listdir(self._p(path)))
    def status(self, path, strict=True):
        if os.path.exists(self._p(path)): return {"type": "DIRECTORY"}
        if strict: raise IOError("missing")
        return None
    def delete(self, path, recursive=False):
        p = self._p(path); shutil.rmtree(p) if os.path.isdir(p) else os.remove(p)
