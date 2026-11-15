"""System Monitoring data for the dashboard, measured from the running stack (cached for a few seconds):
consumer lag per group, consumer-group membership (stream-service health), TCN worker latency, escalation and PDP
counters, HDFS usage, and reachability of Kafka, PostgreSQL, Redis, HDFS and the gateway."""
import json
import time
from typing import Dict

from ...streaming.lag import GROUP_TOPICS, group_lag
from ...topics import topic_name


class SystemProbe:
    def __init__(self, settings, redis, engine, lake_factory, ttl: float = 10.0):
        self.s, self.r, self.engine, self.lake_factory, self.ttl = settings, redis, engine, lake_factory, ttl
        self._cache, self._at = None, 0.0

    def snapshot(self) -> Dict:
        if self._cache is not None and time.time() - self._at < self.ttl:
            return self._cache
        out = {"t": time.time(), "lag": self._lag(), "groups": self._groups(), "tcn_workers": self._tcn(),
               "escalation": self._escalation(), "pdp": self._hash("pdp:stats"), "hdfs": self._hdfs(),
               "health": self._health()}
        self._cache, self._at = out, time.time()
        return out

    def _lag(self):
        out = {}
        for g, topics in GROUP_TOPICS.items():
            try:
                res = group_lag(self.s.kafka_bootstrap, g, [topic_name(t, self.s.kafka_topic_prefix) for t in topics], timeout=5)
                out[g] = {"total": res["total"], "by_topic": {t: sum(v.values()) for t, v in res["partitions"].items()}}
            except Exception as e:  # noqa: BLE001
                out[g] = {"error": type(e).__name__}
        return out

    def _groups(self):
        try:
            from confluent_kafka.admin import AdminClient
            a = AdminClient({"bootstrap.servers": self.s.kafka_bootstrap})
            futs = a.describe_consumer_groups(list(GROUP_TOPICS))
            out = {}
            for g, f in futs.items():
                d = f.result(timeout=5)
                out[g] = {"state": str(d.state).split(".")[-1], "members": len(d.members)}
            return out
        except Exception as e:  # noqa: BLE001
            return {"error": type(e).__name__}

    def _hash(self, key):
        return {(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
                for k, v in (self.r.hgetall(key) or {}).items()}

    def _tcn(self):
        out = []
        for k in self.r.scan_iter("tcn:worker:*"):
            h = self._hash(k)
            out.append({"worker": (k.decode() if isinstance(k, bytes) else k).split(":", 2)[-1],
                        "model_version": h.get("model_version"), "scored": int(h.get("scored", 0) or 0),
                        "rechecks": int(h.get("rechecks", 0) or 0), "last_batch_ms": float(h.get("last_batch_ms", 0) or 0),
                        "updated_at": float(h.get("updated_at", 0) or 0)})
        return sorted(out, key=lambda x: x["worker"])

    def _escalation(self):
        windows = [json.loads(x) for x in self.r.lrange("esc:windows", 0, 199)]
        now = time.time()
        recent = [w for w in windows if w.get("t", 0) >= now - 3600]
        return {"stats": self._hash("esc:stats"), "last_hour": {
            "windows": len(recent), "escalated": sum(w.get("escalated", 0) for w in recent),
            "by_path": {p: sum(w.get(p, 0) for w in recent) for p in ("high", "resource", "sweep", "sample")}},
            "recent_windows": windows[:60]}

    def _hdfs(self):
        try:
            lake = self.lake_factory()
            c = lake.client.content(lake.base) if hasattr(lake.client, "content") else None
            return {"reachable": True, "base": lake.base,
                    "bytes": c.get("length") if c else None, "files": c.get("fileCount") if c else None,
                    "directories": c.get("directoryCount") if c else None,
                    "event_partitions": len(lake.partitions())}
        except Exception as e:  # noqa: BLE001
            return {"reachable": False, "error": type(e).__name__}

    def _health(self):
        h = {}
        try:
            self.r.ping(); h["redis"] = "ok"
        except Exception:  # noqa: BLE001
            h["redis"] = "down"
        try:
            from sqlalchemy import text
            with self.engine.connect() as c:
                c.execute(text("SELECT 1"))
            h["postgres"] = "ok"
        except Exception:  # noqa: BLE001
            h["postgres"] = "down"
        try:
            from confluent_kafka.admin import AdminClient
            AdminClient({"bootstrap.servers": self.s.kafka_bootstrap}).list_topics(timeout=5)
            h["kafka"] = "ok"
        except Exception:  # noqa: BLE001
            h["kafka"] = "down"
        try:
            import httpx
            h["gateway"] = "ok" if httpx.get(f"{self.s.gateway_internal_url}/readyz", timeout=3).status_code == 200 else "degraded"
        except Exception:  # noqa: BLE001
            h["gateway"] = "down"
        return h
