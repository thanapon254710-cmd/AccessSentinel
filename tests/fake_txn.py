"""In-memory transactional Kafka for unit tests of TransactionalStage: records produced inside a transaction
become visible only on commit; abort discards them; consumer offsets are committed atomically with them."""
from confluent_kafka import TopicPartition


class Msg:
    def __init__(self, topic, partition, offset, key, value, ts):
        self._t, self._p, self._o, self._k, self._v, self._ts = topic, partition, offset, key, value, ts

    def error(self): return None
    def topic(self): return self._t
    def partition(self): return self._p
    def offset(self): return self._o
    def key(self): return self._k
    def value(self): return self._v
    def timestamp(self): return (2, self._ts)          # 2 = LogAppendTime


class Cluster:
    def __init__(self, partitions=4):
        self.n = partitions
        self.logs = {}                                    # topic -> [list per partition of (key, value, ts)]
        self.committed = {}                               # (group, topic, partition) -> offset
        self.clock_ms = 1_000_000

    def log(self, topic):
        return self.logs.setdefault(topic, [[] for _ in range(self.n)])

    def append(self, topic, key, value, partition=None):
        p = partition if partition is not None else (hash(key) % self.n if key is not None else 0)
        self.clock_ms += 1
        self.log(topic)[p].append((key, value, self.clock_ms))
        return p

    def records(self, topic):
        return [(p, k, v) for p, part in enumerate(self.log(topic)) for (k, v, _) in part]


class Producer:
    def __init__(self, cluster, fail_commits=0):
        self.c, self.fail_commits = cluster, fail_commits
        self.staged, self.offsets, self.in_txn = [], None, False
        self.commits = self.aborts = 0

    def init_transactions(self, timeout=None): pass
    def begin_transaction(self):
        assert not self.in_txn
        self.in_txn, self.staged, self.offsets = True, [], None
    def produce(self, topic, key=None, value=None, headers=None, partition=None):
        assert self.in_txn
        self.staged.append((topic, key, value, partition))
    def send_offsets_to_transaction(self, positions, meta, timeout=None):
        self.offsets = (meta, positions)
    def commit_transaction(self, timeout=None):
        if self.fail_commits:
            self.fail_commits -= 1
            raise RuntimeError("injected commit failure")
        for topic, key, value, part in self.staged:
            self.c.append(topic, key, value, part)
        if self.offsets:
            group, positions = self.offsets
            for tp in positions:
                self.c.committed[(group, tp.topic, tp.partition)] = tp.offset
        self.in_txn, self.staged = False, []
        self.commits += 1
    def abort_transaction(self, timeout=None):
        self.in_txn, self.staged = False, []
        self.aborts += 1
    def poll(self, t=0): return 0


class Consumer:
    def __init__(self, cluster, group):
        self.c, self.group = cluster, group
        self.pos, self.assigned, self.cb = {}, [], None

    def subscribe(self, topics, on_assign=None, on_revoke=None, on_lost=None):
        self.topics, self.cb = topics, on_assign
    def _ensure_assigned(self):
        if self.assigned or not self.cb and not getattr(self, "topics", None):
            return
        tps = [TopicPartition(t, p) for t in self.topics for p in range(self.c.n)]
        self.assigned = tps
        for tp in tps:
            self.pos[(tp.topic, tp.partition)] = max(0, self.c.committed.get((self.group, tp.topic, tp.partition), 0))
        if self.cb:
            self.cb(self, tps)
    def consume(self, n, timeout):
        self._ensure_assigned()
        out = []
        for tp in self.assigned:
            part = self.c.log(tp.topic)[tp.partition]
            k = (tp.topic, tp.partition)
            while self.pos[k] < len(part) and len(out) < n:
                key, value, ts = part[self.pos[k]]
                out.append(Msg(tp.topic, tp.partition, self.pos[k], key, value, ts))
                self.pos[k] += 1
        return out
    def assignment(self): return list(self.assigned)
    def position(self, tps):
        return [TopicPartition(tp.topic, tp.partition, self.pos[(tp.topic, tp.partition)]) for tp in tps]
    def committed(self, tps, timeout=None):
        return [TopicPartition(tp.topic, tp.partition, self.c.committed.get((self.group, tp.topic, tp.partition), -1001))
                for tp in tps]
    def seek(self, tp):
        self.pos[(tp.topic, tp.partition)] = 0 if tp.offset < 0 else tp.offset
    def consumer_group_metadata(self): return self.group
    def close(self): pass


class RestoreConsumer:
    """Reads changelog partitions (assign + consume) like a group-less consumer."""
    def __init__(self, cluster):
        self.c, self.pos = cluster, {}
    def get_watermark_offsets(self, tp, timeout=None):
        return 0, len(self.c.log(tp.topic)[tp.partition])
    def assign(self, tps):
        self.tps = tps
        for tp in tps:
            self.pos[(tp.topic, tp.partition)] = 0
    def consume(self, n, timeout):
        out = []
        for tp in self.tps:
            part = self.c.log(tp.topic)[tp.partition]
            k = (tp.topic, tp.partition)
            while self.pos[k] < len(part) and len(out) < n:
                key, value, ts = part[self.pos[k]]
                out.append(Msg(tp.topic, tp.partition, self.pos[k], key, value, ts))
                self.pos[k] += 1
        return out
    def position(self, tps):
        return [TopicPartition(tp.topic, tp.partition, self.pos[(tp.topic, tp.partition)]) for tp in tps]
    def close(self): pass
