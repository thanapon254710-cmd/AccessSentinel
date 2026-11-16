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
class TCNv3Config:
    """Revised v3 TCN (Step 4, Eq. 4). [v3] = value from the revised paper, [IMPL] = implementation choice."""
    window_len: int = 128                                     # [v3] L
    feature_dim: int = 43                                     # [v3] d (Table III)
    kernel_size: int = 2                                      # [v3] k
    dilations: Tuple[int, ...] = (1, 2, 4, 8, 16, 32)         # [v3] six residual blocks, two convs each
    filters: Tuple[int, ...] = (256, 256, 128, 128, 64, 64)   # [v3]
    dropout: float = 0.2                                      # [IMPL]
    head_hidden: int = 32                                     # [IMPL]
    lambdas: Tuple[float, float, float, float] = (1.0, 1.0, 0.5, 1.0)   # [IMPL] l1..l4 of Eq. 6
    focal_gamma: float = 2.0                                  # [IMPL] Focal Loss on L_sup only [v3]
    focal_alpha: float = 0.25                                 # [IMPL]
    lr: float = 1e-3                                          # [IMPL] Adam
    batch_size: int = 256                                     # [IMPL]
    pretrain_epochs: int = 3                                  # [IMPL] self-supervised (l4 = 0)
    finetune_epochs: int = 8                                  # [IMPL] all four terms; early stop on val PR-AUC
    alpha_low: float = 0.05                                   # [IMPL] Eq. 8 quantile levels on nominal data:
    alpha_high: float = 0.01                                  #        theta = Q_{1-alpha}(S_ind | D_cal_nom)
    alpha_block: float = 0.001                                #        alpha_B < alpha_h < alpha_l

    def receptive_field(self) -> int:
        return 1 + 2 * (self.kernel_size - 1) * sum(self.dilations)        # Eq. 4: 1 + 2 * 63 = 127

    def to_dict(self) -> dict:
        from dataclasses import asdict
        return {k: list(v) if isinstance(v, tuple) else v for k, v in asdict(self).items()}

    @classmethod
    def from_dict(cls, d: dict) -> "TCNv3Config":
        return cls(**{k: tuple(v) if isinstance(v, list) else v for k, v in d.items() if k in cls.__dataclass_fields__})


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
    # ---- Revised v3 compute admission (Algorithm 1, Eqs. 26-30); a work unit = 1 ms of SGE inference
    cost_c0: float = 35.59                    # [MEASURED Phase 6] C_u = c0 + c1 |E(N_h(u))| + c2 |V(N_h(u))| in ms of
    cost_c1: float = 0.0889                   # SGE wall time; least squares over 20 seeded neighbourhoods (|E| 22-3520,
    cost_c2: float = 0.8212                   # |V| 12-177), R2 0.92, MAPE 33%: evaluation/results/raw/sge-cost-fit-20261115-run2.json
    rho_graph: float = 0.7                    # [v3 example] rho' target utilisation of the TGN workers
    tgn_workers: int = 2                      # deployed SGE workers (tgn-rt, tgn-bg)
    mu_graph: float = 1000.0                  # [IMPL] work units per second per worker (1 ms units -> 1000/s)
    res_deadline: float = 900.0               # [IMPL] a resource-triggered user waits at most this long for budget
    use_resource_windows: bool = True         # [v3] Path B from finalised event-time windows (resource-windows)


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
class TGNv3Config:
    """Revised v3 TGN = CMU + SGE (Step 7, Eqs. 11-19). [v3] = initial value from the paper (following [23]),
    [IMPL] = implementation choice the paper leaves open. All values are illustrative and uncalibrated (v3 Lim. 3)."""
    mem_dim: int = 100                        # [v3] node memory dimension
    msg_dim: int = 100                        # [v3] message dimension
    time_dim: int = 100                       # [v3] time-encoder dimension
    emb_dim: int = 100                        # [IMPL] SGE output (= memory dimension)
    n_neighbors: int = 10                     # [v3] neighbours per hop
    n_hops: int = 1                           # [v3]
    n_heads: int = 2                          # [IMPL] graph attention heads
    edge_feat_dim: int = 7                    # [IMPL] s, deny, operation one-hot (4), unmapped path
    lr: float = 1e-4                          # [v3] Adam
    epochs: int = 50                          # [v3] maximum; early stopping on validation [IMPL]
    weight_decay: float = 1e-6                # [v3] L2
    batch_size: int = 200                     # [IMPL] events per training batch (memory updated per batch)
    self_supervised_epochs: int = 3           # [IMPL] edge reconstruction only, then joint with collusion labels
    sup_weight: float = 1.0                   # [IMPL] weight of the supervised collusion term
    # candidate coordination graph and collective score (Eqs. 15-19), all [IMPL]
    window: float = 3600.0                    # analysis window W for the SGE neighbourhood and H_W (s)
    delta_c: float = 1800.0                   # co-access proximity Delta_c (s)
    h_min: int = 1                            # shared resources for a candidate pair. Calibrated on VALIDATION
                                              # anchors (Phase 4): h_min=2 put 18 % of collusion seeds and 27 % of
                                              # benign anchors in a group (worse than chance); h_min=1: 79 % / 57 %
    d_cap: int = 25                           # resources with window degree above this are excluded from pairs
    tau: float = 900.0                        # decay time-scale of tie weights (s)
    n_min: int = 2                            # minimum group size
    ns_min: int = 1                           # minimum sensitive resources shared by the group
    delta_min: float = 0.3                    # minimum group density
    trim_pi: float = 0.1                      # pi of the trimmed mean of edge anomaly scores
    alpha_coll: float = 0.01                  # theta_C = Q_{1-alpha}(S_coll | nominal validation)
    alpha_attr: float = 0.05                  # theta_A = Q_{1-alpha}(A_u | nominal validation)


@dataclass
class PDPConfig:
    theta_coll: float = 0.6                   # [IMPL] D3 gives no collective threshold (deployed: calibrated TGN value)
    coll_ttl: float = 3600.0                  # [IMPL] collective score expires after this long
    block_ttl: float = 24 * 3600.0            # [IMPL] Terminate (and a D3 Block) lasts until released or this expiry (A3)
    restrict_ttl: float = 24 * 3600.0         # [IMPL] v3 Restrict (collective, A_u >= theta_A) expiry
    stepup_ttl: float = 3600.0                # [IMPL] v3 StepUp (collective, A_u < theta_A) expiry
    throttle_ttl: float = 900.0               # [IMPL] v3 Throttle (theta_high <= S_ind < theta_B) expiry
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
