"""Live feed for the dashboard: a non-committing Kafka consumer (reads from 'latest', never affects other groups)
that keeps ring buffers of recent records and fans them out to WebSocket subscribers, plus an offset sampler that
measures the real event rate of each topic."""
import asyncio
import json
import logging
import threading
import time
import uuid
from collections import deque
from typing import Dict, List, Optional

log = logging.getLogger("accesssentinel.api.live")

KINDS = {"access-events": "event", "risk-scores": "score", "escalations.realtime": "escalation",
         "escalations.background": "escalation", "group-scores": "group", "pdp-decisions": "decision"}


class LiveFeed:
    def __init__(self, bootstrap: str, prefix: str = "", maxlen: int = 1000):
        self.bootstrap, self.prefix = bootstrap, prefix
        self.buffers: Dict[str, deque] = {k: deque(maxlen=maxlen) for k in set(KINDS.values())}
        self.rates: deque = deque(maxlen=360)                 # (t, {topic: msgs/s}) every 5 s -> 30 min
        self._subs: List[asyncio.Queue] = []
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop = threading.Event()
        self._lock = threading.Lock()

    def _topic(self, name):
        return f"{self.prefix}.{name}" if self.prefix else name

    # ------------------------------------------------------------------ lifecycle
    def start(self, loop):
        self._loop = loop
        threading.Thread(target=self._consume, name="live-consumer", daemon=True).start()
        threading.Thread(target=self._sample, name="live-rates", daemon=True).start()

    def stop(self):
        self._stop.set()

    def _consume(self):
        from confluent_kafka import Consumer
        while not self._stop.is_set():
            try:
                c = Consumer({"bootstrap.servers": self.bootstrap, "group.id": f"dashboard-{uuid.uuid4().hex[:8]}",
                              "auto.offset.reset": "latest", "enable.auto.commit": False})
                c.subscribe([self._topic(t) for t in KINDS])
                while not self._stop.is_set():
                    for m in c.consume(500, 0.5):
                        if m.error():
                            continue
                        name = m.topic()[len(self.prefix) + 1:] if self.prefix else m.topic()
                        try:
                            self.publish(KINDS[name], json.loads(m.value()))
                        except (ValueError, KeyError):
                            continue
                c.close()
            except Exception as e:  # noqa: BLE001
                log.warning("live consumer error (%s); reconnecting", type(e).__name__)
                time.sleep(3)

    def _sample(self):
        from confluent_kafka import Consumer, TopicPartition
        prev, prev_t = None, None
        c = None
        while not self._stop.is_set():
            try:
                c = c or Consumer({"bootstrap.servers": self.bootstrap, "group.id": "dashboard-rates", "enable.auto.commit": False})
                md = c.list_topics(timeout=5)
                ends = {}
                for t in KINDS:
                    name = self._topic(t)
                    if name in md.topics:
                        ends[t] = sum(c.get_watermark_offsets(TopicPartition(name, p), timeout=5)[1] for p in md.topics[name].partitions)
                now = time.time()
                if prev is not None:
                    dt = max(now - prev_t, 1e-6)
                    with self._lock:
                        self.rates.append((now, {t: max(0, ends.get(t, 0) - prev.get(t, 0)) / dt for t in ends}))
                prev, prev_t = ends, now
            except Exception as e:  # noqa: BLE001
                log.warning("rate sampler error (%s)", type(e).__name__)
                c = None
            self._stop.wait(5.0)

    # ------------------------------------------------------------------ data access
    def publish(self, kind: str, data: dict) -> None:
        with self._lock:
            self.buffers[kind].appendleft(data)
        if self._loop is not None:
            msg = {"type": kind, "data": data}
            for q in list(self._subs):
                self._loop.call_soon_threadsafe(self._offer, q, msg)

    @staticmethod
    def _offer(q: asyncio.Queue, msg):
        if q.qsize() < 500:                                   # slow client: drop rather than grow without bound
            q.put_nowait(msg)

    def recent(self, kind: str, limit: int = 200, pred=None) -> List[dict]:
        with self._lock:
            items = list(self.buffers[kind])
        out = [x for x in items if pred is None or pred(x)]
        return out[:limit]

    def rate_history(self) -> List[dict]:
        with self._lock:
            return [{"t": t, **r} for t, r in self.rates]

    def subscribe(self) -> asyncio.Queue:
        q = asyncio.Queue()
        self._subs.append(q)
        return q

    def unsubscribe(self, q) -> None:
        if q in self._subs:
            self._subs.remove(q)
