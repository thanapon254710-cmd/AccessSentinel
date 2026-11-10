"""Redis-backed memory of ids already made durable by a processor (scope = processor name).
Ids are marked only AFTER the write succeeded, so marking can never cause loss; a crash between write and
mark can at worst leave a duplicate, which history reads and compaction remove by id."""
from typing import Dict, Iterable, List, Set


class RedisDedup:
    def __init__(self, redis, scope: str, ttl_seconds: int):
        self.r, self.scope, self.ttl = redis, scope, ttl_seconds

    def _k(self, i: str) -> str:
        return f"dedup:{self.scope}:{i}"

    def seen(self, ids: List[str]) -> Set[str]:
        if not ids:
            return set()
        pipe = self.r.pipeline(transaction=False)
        for i in ids:
            pipe.exists(self._k(i))
        return {i for i, hit in zip(ids, pipe.execute()) if hit}

    def mark(self, ids: Iterable[str]) -> None:
        pipe = self.r.pipeline(transaction=False)
        n = 0
        for i in ids:
            pipe.set(self._k(i), 1, ex=self.ttl)
            n += 1
        if n:
            pipe.execute()


class MemoryDedup:
    """Test double / single-process fallback."""
    def __init__(self):
        self.ids: Dict[str, int] = {}

    def seen(self, ids: List[str]) -> Set[str]:
        return {i for i in ids if i in self.ids}

    def mark(self, ids: Iterable[str]) -> None:
        for i in ids:
            self.ids[i] = 1
