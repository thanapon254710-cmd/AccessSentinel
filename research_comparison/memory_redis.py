"""Deterministic, local Redis subset for single-process offline evaluation.

The production Escalator is imported unchanged. This backend supports only its
commands. It provides no interprocess atomicity, persistence or Redis TTL clocks;
therefore offline tests here must NOT be represented as distributed/production
Kafka/Redis/HDFS end-to-end benchmarks.
"""
from __future__ import annotations


class _Pipeline:
    def __init__(self, store):
        self.store, self.ops = store, []

    def __getattr__(self, operation):
        if operation.startswith('_') or not hasattr(self.store, operation):
            raise AttributeError(operation)
        def queue(*args, **kw):
            self.ops.append((operation, args, kw))
            return self
        return queue

    def execute(self):
        ops, self.ops = self.ops, []
        return [getattr(self.store, name)(*a, **kw) for name, a, kw in ops]


class MemoryRedis:
    def __init__(self):
        self.data: dict = {}

    def pipeline(self, transaction=False):
        return _Pipeline(self)

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, ex=None, nx=False):
        if nx and key in self.data:
            return False
        self.data[key] = value
        return True

    def getset(self, key, value):
        old = self.get(key)
        self.set(key, value)
        return old

    def delete(self, *keys):
        n = 0
        for key in keys:
            if key in self.data:
                del self.data[key]
                n += 1
        return n

    def expire(self, key, seconds):
        # Offline replay controls logical expiration via the Escalator's clock,
        # not the process wall clock. Redis TTL eviction is intentionally omitted.
        return key in self.data

    def hset(self, key, field=None, value=None, mapping=None):
        h = self.data.setdefault(key, {})
        if mapping is not None:
            h.update(mapping)
            return len(mapping)
        h[field] = value
        return 1

    def hget(self, key, field):
        return self.data.get(key, {}).get(field)

    def hgetall(self, key):
        return dict(self.data.get(key, {}))

    def hdel(self, key, *fields):
        h = self.data.get(key, {})
        n = 0
        for field in fields:
            if field in h:
                del h[field]
                n += 1
        return n

    def hmget(self, key, fields):
        h = self.data.get(key, {})
        return [h.get(field) for field in fields]

    def hincrby(self, key, field, amount=1):
        h = self.data.setdefault(key, {})
        h[field] = int(h.get(field, 0)) + int(amount)
        return h[field]

    def zadd(self, key, mapping, gt=False):
        z = self.data.setdefault(key, {})
        n = 0
        for member, score in mapping.items():
            if member not in z:
                z[member] = float(score)
                n += 1
            elif not gt or float(score) > z[member]:
                z[member] = float(score)
        return n

    @staticmethod
    def _bound(value):
        if value == '-inf': return float('-inf')
        if value == '+inf': return float('inf')
        return float(value)

    def zrangebyscore(self, key, minimum, maximum):
        lo, hi = self._bound(minimum), self._bound(maximum)
        z = self.data.get(key, {})
        return [member for member, score in sorted(z.items(), key=lambda item: (item[1], str(item[0])))
                if lo <= score <= hi]

    def zremrangebyscore(self, key, minimum, maximum):
        z = self.data.get(key, {})
        old = self.zrangebyscore(key, minimum, maximum)
        for member in old:
            del z[member]
        return len(old)

    def lpush(self, key, value):
        self.data.setdefault(key, []).insert(0, value)
        return len(self.data[key])

    def ltrim(self, key, start, stop):
        values = self.data.get(key, [])
        self.data[key] = values[start:stop + 1]
        return True
