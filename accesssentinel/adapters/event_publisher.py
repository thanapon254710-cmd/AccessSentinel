"""Non-blocking access-event publisher used by the gateway (D3 Step 2: `access-events`, key = user id).

* `publish()` never waits for Kafka: confluent-kafka buffers the record and a background thread serves
  delivery callbacks, so request latency is independent of broker latency (D3 Step 8, request-time).
* Idempotent producer, acks=all: retries do not create duplicates inside Kafka.
* If the local buffer is full or delivery finally fails (broker down longer than message.timeout.ms),
  the record is appended to a local JSONL spool instead of being dropped. Replaying the spool into Kafka
  is part of Phase 4. Spooled records keep their event_id, so consumers can deduplicate a replay.
"""
import json
import os
import threading
import time
from typing import Callable, Optional

from ..events import AccessEvent
from .kafka_broker import ev_to_bytes


class EventPublisher:
    def __init__(self, bootstrap: str, topic: str, spool_dir: str, client_id: str = "gateway",
                 producer_factory: Optional[Callable] = None, message_timeout_ms: int = 30_000):
        if producer_factory is None:
            from confluent_kafka import Producer
            producer_factory = Producer
        self.topic, self.spool_dir = topic, spool_dir
        self.producer = producer_factory({
            "bootstrap.servers": bootstrap, "client.id": client_id,
            "enable.idempotence": True, "acks": "all", "linger.ms": 5,
            "message.timeout.ms": message_timeout_ms, "queue.buffering.max.messages": 100_000,
        })
        self._lock = threading.Lock()
        self.counts = {"submitted": 0, "delivered": 0, "spooled": 0}
        self.last_error: Optional[str] = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, name="event-publisher", daemon=True)
        self._thread.start()

    # ------------------------------------------------------------------ public
    def publish(self, ev: AccessEvent) -> None:
        payload = ev_to_bytes(ev)
        self._inc("submitted")
        try:
            self.producer.produce(self.topic, key=ev.u.encode(), value=payload, on_delivery=self._on_delivery)
        except BufferError:
            self._spool(payload, "local-buffer-full")
        except Exception as e:  # noqa: BLE001 - KafkaException etc.: never fail the request
            self._spool(payload, f"produce-error:{type(e).__name__}")

    def stats(self) -> dict:
        with self._lock:
            out = dict(self.counts)
        try:
            out["in_flight"] = len(self.producer)
        except TypeError:
            out["in_flight"] = None
        out["last_error"] = self.last_error
        return out

    def close(self, timeout: float = 10.0) -> None:
        self._stop.set()
        self._thread.join(timeout=2)
        try:
            self.producer.flush(timeout)
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------ internals
    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                self.producer.poll(0.2)
            except Exception as e:  # noqa: BLE001
                self.last_error = type(e).__name__
                time.sleep(0.5)

    def _on_delivery(self, err, msg) -> None:
        if err is None:
            self._inc("delivered")
        else:
            self.last_error = str(err)
            self._spool(msg.value(), f"delivery-failed:{err}")

    def _inc(self, k: str) -> None:
        with self._lock:
            self.counts[k] += 1

    def _spool(self, payload: bytes, reason: str) -> None:
        os.makedirs(self.spool_dir, exist_ok=True)
        path = os.path.join(self.spool_dir, f"{self.topic}-{time.strftime('%Y%m%d', time.gmtime())}.jsonl")
        line = json.dumps({"reason": reason, "topic": self.topic, "event": json.loads(payload)})
        with self._lock:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
            self.counts["spooled"] += 1
