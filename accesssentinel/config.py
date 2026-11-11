"""All hyper-parameters. Values marked [D3] come from the Deliverable 3 text;
values marked [IMPL] are implementation choices D3 leaves open (confirm with team)."""
from dataclasses import dataclass, field
from typing import Tuple


@dataclass
class TCNConfig:
    window_len: int = 20                      # [D3] L = 20 (Step 4, following [2])
    feature_dim: int = 50                     # [D3] d = 50
    dilations: Tuple[int, ...] = (1, 2, 4, 6, 8, 10, 12, 14, 16, 18)   # [D3]
    filters: Tuple[int, ...] = (512, 256, 128, 64, 64, 32, 32, 32, 16, 12)  # [D3]
    kernel_size: int = 2                      # [IMPL] not stated in D3
    spatial_dropout: float = 0.3              # [D3]
    dense_dropout: float = 0.5                # [D3]
    dense_units: int = 32                     # [IMPL]
    batch_size: int = 512                     # [D3]
    lr: float = 1e-3                          # [D3] Adam
    epochs: int = 30                          # [IMPL]
    focal_gamma: float = 2.0                  # [IMPL] Focal Loss is [D3]; gamma/alpha are not
    focal_alpha: float = 0.5                  # [IMPL]
    folds: int = 5                            # [D3] 5-fold CV
    test_frac: float = 0.10                   # [D3] 72/18/10 = 0.9*(0.8/0.2) / 0.1


@dataclass
class RiskConfig:
    theta_low: float = 0.4                    # [D3]
    theta_high: float = 0.6                   # [D3]
    t_recheck: float = 600.0                  # [D3] wall-clock interval, initialised to 10 min (Step 5)


@dataclass
class EscalationConfig:
    analysis_window: float = 60.0             # [IMPL] window W between escalation evaluations (s)
    kappa: int = 3                            # [D3] calibrate empirically
    delta_res: float = 15 * 60.0              # [D3] 15 min
    t_sweep: float = 2 * 3600.0               # [D3] 2 h
    sweep_min_users: int = 4                  # [IMPL] sweep selection rule is unspecified in D3
    sweep_batch: int = 50                     # [IMPL] B_sweep
    gamma: float = 0.02                       # [D3] calibrate vs TGN budget
    # ---- deployed escalation service (Phase 6); all [IMPL] choices D3 leaves open
    realtime_window: float = 5.0              # [IMPL] A/B candidates are unioned for this long before emission
    realtime_cooldown: float = 300.0          # [IMPL] a user is escalated in real time at most once per cooldown
    background_cooldown: float = 3600.0       # [IMPL] ... and in the background at most once per cooldown
    max_realtime_per_window: int = 100        # [IMPL] TGN budget per realtime window; overflow is deferred
    max_background_per_window: int = 50       # [IMPL] TGN budget per analysis window; overflow is shed
    sweep_lookback: float = 24 * 3600.0       # [IMPL] history examined by each sweep for rules C1/C2
    sweep_baseline_days: int = 14             # [IMPL] history that defines "usual" behaviour ...
    sweep_baseline_gap_days: int = 7          # [IMPL] ... ending this long before the lookback, so a slow attack
                                              #        cannot turn itself into "usual" within a week (Phase 6 finding)
    sweep_rare_max: int = 1                   # [IMPL] a resource is rare for a user if used <= this in baseline
    sweep_min_rare: int = 3                   # [IMPL] C1: rare sensitive accesses needed in the lookback
    sweep_min_baseline_events: int = 20       # [IMPL] C1 ignores users without enough history (cold start)
    sweep_min_medium: int = 5                 # [IMPL] C2: Medium-tier scores without any High in the lookback


@dataclass
class GraphConfig:
    window: float = 15 * 60.0                 # [IMPL] observation window (= delta_res)
    sweep_window: float = 3600.0              # [IMPL] history window used for sweep seeds
    max_resources: int = 40                   # [IMPL] neighbourhood bound
    max_users_per_resource: int = 15          # [IMPL]
    app_coaccess_weight: float = 0.25         # [IMPL] D3 links users sharing an application
    min_shared_sensitive: int = 2             # [IMPL] defines the behaviourally coupled group g(u)
    group_top_frac: float = 0.25              # [IMPL] group score = mean of top fraction of edge scores


@dataclass
class TGNConfig:
    node_feat_dim: int = 10                   # [IMPL]
    emb_dim: int = 50                         # [D3]
    gcn_layers: int = 2                       # [D3]
    window_T: int = 10                        # [D3] attention window T = 10 time steps
    lr: float = 1e-3                          # [D3]
    epochs: int = 300                         # [D3] (demo overrides to fewer)
    weight_decay: float = 1e-6                # [D3]
    train_frac: float = 0.5                   # [D3] first 50% train, rest test
    graphs_per_step: int = 8                  # [IMPL]


@dataclass
class PDPConfig:
    theta_coll: float = 0.6                   # [IMPL] D3 gives no collective threshold (deployed: calibrated TGN value)
    coll_ttl: float = 3600.0                  # [IMPL] collective score expires after this long
    block_ttl: float = 24 * 3600.0            # [IMPL] a risk-based Block lasts until released or this expiry (A3)
    release_grace: float = 3600.0             # [IMPL] after an analyst release, ML evidence cannot re-block for this long
    max_score_age: float = 600.0              # [IMPL] older risk scores (e.g. a backlog after restart) are not enforced
    max_group_score_age: float = 900.0        # [IMPL] same for TGN group scores
    policy_poll_seconds: float = 15.0         # [IMPL] how often organisational policy C is re-read


@dataclass
class StreamConfig:
    n_partitions: int = 8                     # [IMPL]
    time_bucket: float = 15 * 60.0            # [IMPL] salt for resource-keyed composite key


@dataclass
class Config:
    tcn: TCNConfig = field(default_factory=TCNConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    escalation: EscalationConfig = field(default_factory=EscalationConfig)
    graph: GraphConfig = field(default_factory=GraphConfig)
    tgn: TGNConfig = field(default_factory=TGNConfig)
    pdp: PDPConfig = field(default_factory=PDPConfig)
    stream: StreamConfig = field(default_factory=StreamConfig)
