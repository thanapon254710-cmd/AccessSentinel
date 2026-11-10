"""Step 9: monitoring-pattern feedback loop. Only analyst-verified outcomes enter the supervised set."""
from collections import defaultdict
from .storage import HistoricalStore


class FeedbackLoop:
    def __init__(self, store: HistoricalStore):
        self.store = store
        self.records, self.verified = [], {}      # verified: u -> bool (malicious)

    def record(self, u, t, s_ind, s_coll, decision):
        self.records.append({"u": u, "t": t, "s_ind": s_ind, "s_coll": s_coll, "decision": str(decision.value)})

    def analyst_confirm(self, u: str, malicious: bool):
        self.verified[u] = malicious

    def training_labels(self):
        """Only analyst-verified outcomes (avoids self-confirmation bias)."""
        return dict(self.verified)

    def unverified_for_drift(self):
        return [r for r in self.records if r["u"] not in self.verified]

    def drift_summary(self, bucket=3600.0):
        agg = defaultdict(list)
        for r in self.records: agg[int(r["t"] // bucket)].append(r["s_ind"])
        return {k: sum(v) / len(v) for k, v in sorted(agg.items())}

    def flush(self):
        self.store.write_feedback(self.records)

