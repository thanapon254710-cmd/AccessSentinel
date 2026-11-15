"""End-to-end AccessSentinel pipeline (D3): Gateway events -> Kafka(dual view) -> {HDFS sink, TCN, graph path}
-> risk tiers -> four-path escalation -> TGN -> PDP -> enforcement callbacks -> feedback."""
import heapq, itertools
from collections import deque
import numpy as np
from .broker import DualViewBroker
from .config import Config
from .escalation import EscalationPolicy
from .events import AccessEvent
from .feedback import FeedbackLoop
from .features import FeatureEncoder
from .graph import NeighborhoodBuilder
from .pdp import PDP, Decision, PolicyContext
from .risk import RiskMonitor, Tier
from .state_store import UserStateStore
from .storage import HistoricalStore


class AccessSentinel:
    def __init__(self, cfg: Config, tcn_scorer, tgn_scorer, store, seed=0, broker=None):
        self.cfg, self.tcn, self.tgn, self.store = cfg, tcn_scorer, tgn_scorer, store
        self.broker = broker if broker is not None else DualViewBroker(cfg.stream)
        self._now = float('-inf')                # event-time watermark (max event time consumed)
        self.g_sink = self.broker.user_topic.group("hdfs-sink")
        self.g_tcn = self.broker.user_topic.group("tcn")
        self.g_graph = self.broker.resource_topic.group("graph")
        self.state = UserStateStore(cfg.tcn.window_len, cfg.tcn.feature_dim, FeatureEncoder())
        self.risk = RiskMonitor(cfg.risk)
        self.esc = EscalationPolicy(cfg.escalation, np.random.default_rng(seed))
        self.builder = NeighborhoodBuilder(cfg.graph)
        self.pdp = PDP(cfg.pdp, cfg.risk)
        self.feedback = FeedbackLoop(store)
        self.ctx = PolicyContext()
        self.recent = deque()
        self.on_decision = []                    # callbacks(rec) -> e.g. Gateway.apply_decision
        self.block_log = []                      # (t, user, reason)
        self.first_block = {}
        self.stats = dict(events=0, tcn_calls=0, rechecks=0, windows=0, tgn_calls=0,
                          escalated={"high": 0, "res": 0, "sweep": 0, "samp": 0}, seeds_total=0)
        self._win_end = None
        self._last_sweep = None
        self.coll_log = []        # diagnostic: (t_end, user, S_coll, group members) for every TGN group score
        # event-time reordering buffer: (t, kind, seq, event); kind 0 = TCN view, 1 = resource view
        self._buf, self._seq = [], itertools.count()
        self._wm = {0: float('-inf'), 1: float('-inf')}   # max event time consumed per view

    # ---- ingest -----------------------------------------------------------
    def warmup(self, events):
        """Replay history to warm state/store without scoring (used before the evaluation period)."""
        for e in events:
            self.state.update(e); self.store.append(e)
            self.recent.append(e)
            while self.recent and self.recent[0].t < e.t - self.cfg.graph.window: self.recent.popleft()
        if events: self._last_sweep = events[-1].t; self._win_end = events[-1].t + self.cfg.escalation.analysis_window

    def publish(self, ev: AccessEvent):
        """Gateway side: put the event on the stream (user-keyed + resource-keyed views)."""
        self.stats["events"] += 1
        self.broker.publish_event(ev)

    def drain(self, timeout: float = 0.0, final: bool = False) -> int:
        """Consumer side: storage path, TCN detection path, resource-keyed graph path.
        Consumed events are buffered and released in event-time order, up to the slower view's
        watermark (everything when final=True), so window closing does not depend on how many
        records a poll happened to return."""
        n = 0
        for _, _, e in self.g_sink.poll(timeout=timeout): self.store.append(e); n += 1      # storage path
        for k, grp in ((0, self.g_tcn), (1, self.g_graph)):
            for _, _, e in grp.poll():
                heapq.heappush(self._buf, (e.t, k, next(self._seq), e))
                self._wm[k] = max(self._wm[k], e.t); n += 1
        limit = float('inf') if final else min(self._wm.values())
        while self._buf and self._buf[0][0] <= limit:
            t, k, _, e = heapq.heappop(self._buf)
            if k == 0: self._tcn_step(e)
            else: self.esc.observe(e); self.recent.append(e)
            self._now = max(self._now, t)
            if self._buf and self._buf[0][0] <= limit and self._buf[0][0] == t: continue   # same-time items first
            self._recheck(self._now); self._advance(self._now)
        return n

    def ingest(self, ev: AccessEvent):
        self.publish(ev); self.drain()

    def run(self, events):
        for e in events: self.ingest(e)
        self.drain(final=True)
        self.store.flush(); self.feedback.flush()

    def run_stream(self, events, idle_timeout: float = 5.0, poll_timeout: float = 0.2, batch: int = 200):
        """Real-broker mode: publish asynchronously, consume until the stream has been idle for `idle_timeout`."""
        import time
        for i, e in enumerate(events):
            self.publish(e)
            if (i + 1) % batch == 0: self.drain(0.0)
        if hasattr(self.broker, "flush"): self.broker.flush()
        last = time.time()
        while time.time() - last < idle_timeout:
            if self.drain(poll_timeout): last = time.time()
        self.drain(final=True)
        self.store.flush(); self.feedback.flush()

    # ---- TCN path ---------------------------------------------------------
    def _tcn_step(self, ev):
        w = self.state.update(ev)
        s = float(self.tcn.score(w[None])[0]); self.stats["tcn_calls"] += 1
        self.risk.update(ev.u, s, ev.t)
        self._refresh(ev.u, ev.t)

    def _recheck(self, now):
        for u in self.risk.due_rechecks(now):
            s = float(self.tcn.score(self.state.window_at(u, now)[None])[0])
            self.stats["rechecks"] += 1
            self.risk.update(u, s, now)
            self._refresh(u, now)

    def _refresh(self, u, t):
        rec, changed = self.pdp.decide(u, self.risk.score.get(u), t, self.ctx)
        self.feedback.record(u, t, rec.s_ind, rec.s_coll, rec.decision)
        if changed and rec.decision == Decision.BLOCK:
            self.block_log.append((t, u, rec.reason)); self.first_block.setdefault(u, (t, rec.reason))
        if changed:
            for cb in self.on_decision: cb(rec)

    # ---- escalation + TGN -------------------------------------------------
    def _advance(self, now):
        W = self.cfg.escalation.analysis_window
        if self._win_end is None:
            self._win_end = now + W; self._last_sweep = now
        while now >= self._win_end:
            self._close_window(self._win_end); self._win_end += W
            if now - self._win_end > 100 * W: self._win_end = now - ((now - self._win_end) % W) + W

    def _close_window(self, t_end):
        while self.recent and self.recent[0].t < t_end - self.cfg.graph.window: self.recent.popleft()
        if not self.recent: return
        self.stats["windows"] += 1
        sweep_due = (t_end - self._last_sweep) >= self.cfg.escalation.t_sweep
        active = {e.u for e in self.recent}
        res = self.esc.compute(t_end, self.risk, active, sweep_due, self.store)
        if sweep_due: self._last_sweep = t_end
        for k in ("high", "res", "sweep", "samp"): self.stats["escalated"][k] += len(getattr(res, k))
        if self.tgn is None: return
        rt = {u for u in res.realtime if u in active}
        if rt: self._run_tgn(rt, list(self.recent), t_end, self.cfg.graph.window)          # real-time workload
        if res.sweep:                                                                       # background workload (HDFS history)
            hist = self.store.read_range(t_end - self.cfg.graph.sweep_window, t_end)
            self._run_tgn(res.sweep, hist, t_end, self.cfg.graph.sweep_window)
        bg = {u for u in res.samp if u in active} - rt
        if bg: self._run_tgn(bg, list(self.recent), t_end, self.cfg.graph.window)

    def _run_tgn(self, seeds, events, t_end, window):
        sample = self.builder.build(seeds, events, t_end - window, t_end)
        if sample.n_edges == 0: return
        self.stats["tgn_calls"] += 1; self.stats["seeds_total"] += len(seeds)
        for u, (s, members) in self.tgn.group_scores(sample, self.tgn.score_edges(sample)).items():
            self.coll_log.append((t_end, u, float(s), tuple(members)))
            self.pdp.set_collective(u, s, t_end)
            self._refresh(u, t_end)