# Deliverable 4 — V. Experimental Evaluation

Status: **all experiments A–H run and reported.**
- Every number in the results sections comes from the generated tables in `evaluation/results/tables/`, which
  `python -m evaluation.figures` produces from `evaluation/results/raw/`. Figures are in `evaluation/results/figures/`
  (PNG 300 dpi and PDF).
- Nothing in this section is an anticipated result.
- All model results are on **synthetic** data (Section L).

**Summary of findings.**
- **Individual detection (A).** On this data the v3 TCN is *not* better than the baselines: PR-AUC 0.714 against
  0.969 for the D3 TCN. Its auxiliary self-supervised heads are what costs it (F).
- **Collusion (B).** The graph path finds groups that the individual path misses: TGN and TCN together detect
  61 of 75 test groups, against 46 for the TCN alone. It does so with almost no false alarms on benign
  collaboration.
- **Compute budget (E).** The deployed budget B_W is never binding.
- **Enforcement (G).** The PDP implements Eq. 21 exactly (200/200).
- **Failure modes (H).** No loss and no duplicates under crashes.
- **Latency (C/D).** Up to 50 requests / s on one laptop:
  - an individual attack is decided in a median of 25–45 ms, and all 45 were decided within 0.5 s;
  - the collective path adds a fixed ~5 s ([IMPL] batching window);
  - no stage saturated.
- **Robustness defects.** The live experiments exposed five defects (memory exhaustion in three services, a
  near-full Redis, and a stalled PDP loop). They were fixed and the affected runs repeated (Section L).

---

## A. Objectives

The evaluation tests the Revised v3 architecture against eight questions. Each one maps to an experiment.

| RQ | Question | Experiment |
|---|---|---|
| RQ1 | Does the v3 TCN detect individual misuse better than LSTM, Isolation Forest, a rule, a generic supervised TCN and the D3 TCN, at calibrated thresholds? | A |
| RQ2 | Does the CMU + SGE graph path find colluding groups that the individual path misses, and at what false-alarm cost on benign collaboration? How do recall and time-to-detect vary with group size? | B |
| RQ3 | What is the end-to-end latency of the individual and collective paths (Eqs. 31–32), how many requests pass before enforcement (Eq. 33), and with what probability is a latency target met (Eq. 34)? | C |
| RQ4 | How do throughput, consumer lag and resource use behave as load grows? | D |
| RQ5 | Does PriorityAdmit keep graph work within the budget B_W (Eq. 29), and how much admission coverage is lost as the budget shrinks? | E |
| RQ6 | What does each escalation path and each v3 TCN addition contribute? | F |
| RQ7 | Does the PDP implement Eq. 21 exactly? How fast do decisions reach the gateway, and do ψ ordering, TTL expiry, the sensitivity fallback and policy precedence behave as specified? | G |
| RQ8 | How do the Table V failure modes behave: loss, duplicates, recovery time? | H |

## B. Setup

**Hardware and software.**
- One laptop: Intel i5-13500HX (14 cores / 20 threads), 15.7 GB RAM, NVIDIA RTX 4050 Laptop GPU (6 GB), Windows 11.
- Docker Desktop VM: 20 vCPU, 7.6 GB.
- Python 3.11. PyTorch 2.14.1+cu130 for training (GPU); PyTorch 2.15 (CPU) inside the services.
- Every raw result file records `evaluation/env_info.collect()`, the git commit and whether the tree was dirty.

**System under test.** The full stack runs as 24 containers:
- Kafka 3.9 (KRaft), HDFS 3.4.1, PostgreSQL 16, Redis 7;
- the gateway and three demonstration applications;
- the sequencer, repartitioner and resource-window stages;
- two TCN workers, two CMU tasks, two SGE workers, the escalation service, the PDP, the HDFS sink and the dashboard.

All processes share one host clock, so per-stage latencies need no clock synchronisation. The flip side: no
cross-host skew is measured.

**Data.** All model results use **synthetic** data from `accesssentinel.datasets.synth`. Labels stand in for
analyst-verified outcomes. The CERT r4.2 insider-threat dataset was not used: its licence and size have to be
checked and its download approved first (decision D3 in `docs/V3_AUDIT.md`). ThreatFeed and DevPosture (part of
z_u(t)) are simulated.

| Dataset (preset) | Use | Users × days | Split (days) | Scenarios |
|---|---|---|---|---|
| `tcn_v3_eval(seed = 23 + s)` | A, F (TCN) | 400 × 70 | 36 / 14 / 20, attacks after the Eq. 7 gap | compromised account (unusual time and context), insider exfiltration (repeated sensitive access), BAC probe (vertical abuse), low-and-slow, horizontal abuse, unusual sequence, unusual time; benign overtime, travel, new device, month-end, flash crowd |
| `collusion_v3_eval(seed = 11 + s)` | B, E, F (paths) | 1,400 × 28 | 16 / 5 / 7 | coordinated collection, split exfiltration, cross-application, low-and-slow groups; sizes 2 / 3 / 5 / 8; hard negatives: recurring team meetings, temporary project teams, flash crowds |

The dataset fingerprints are recorded with every result. The D3 presets keep their fingerprints, so the D3 results
stay reproducible.

## C. Protocol and metrics

**Split.** Chronological train / validation / test (Eq. 7). A gap of L − 1 = 127 events per user separates the
splits, so no evaluated window overlaps the previous split. Attacks are placed after the gap. Attacker users are
disjoint across splits.

**Calibration.** Every model, baselines included, gets its thresholds from Eq. 8: quantiles of **its own** scores
on nominal validation windows (θ_low / θ_high / θ_B at α = 0.05 / 0.01 / 0.001). No model uses the illustrative
0.4 / 0.6, and no threshold is tuned on the test period. θ_C and θ_A (Eq. 19) are quantiles over nominal
validation group members.

**Seeds and statistics.**
- Model experiments: 5 seeds, each changing both the data seed and the model seed; mean ± standard deviation, with
  95 % bootstrap confidence intervals in `summary.json`.
- System experiments: ≥ 3 runs per configuration.
- End-to-end latency percentiles come from per-event traces, never from summed stage percentiles.

**Metrics.**

| Level | Metrics |
|---|---|
| Event (A) | ROC-AUC, PR-AUC; precision, recall, F1 and FPR at θ_high and θ_B |
| Instance and user-day (A) | attack instances detected at θ_high, median minutes to the first detection, share of normal user-days with ≥ 1 flag |
| Escalated seed (B) | is the escalated seed in a colluding group: ROC-AUC, PR-AUC, precision, recall and FPR at θ_C |
| Group (B) | groups detected, by size and kind, by the TGN, by TCN-only, or by either; member attribution precision / recall; flag rate on each hard-negative kind |
| Admission (E, F) | admission coverage (≥ 1 user of an instance admitted to graph analysis during its activity + 1 h) — a necessary condition for graph detection, **not** detection; admitted work per window (Eq. 26, measured coefficients) against B_W |
| Latency (C) | Lat_gw, Lat_kafka, Lat_reorder, Lat_TCN-q, Lat_TCN (trace), Lat_PDP, Lat_enf; Lat_ind decision-ready and observed; Lat_esc, TGN time; N_exposed; P(Lat_ind ≤ x) |

**Cost model.** Eq. 26 is C_u = 35.59 + 0.0889 |E| + 0.8212 |V| ms. It was fitted on 20 measured SGE runs
(R² 0.92, MAPE 33 %; `evaluation/results/raw/sge-cost-fit-20261115-run2.json`).

## D. Baselines

The baselines are implemented here, on the same input pipeline. **None is a reproduction of a cited paper**; where
one belongs to a cited method family, the table says so.

| Name | Definition |
|---|---|
| TCN (supervised only) | The v3 TCN body without the self-supervised heads and without z_u(t), trained with the focal loss only (a generic supervised TCN in the style of TCN detectors such as [1], [2]) |
| LSTM | 2 × 128 LSTM over the same Table III inputs, focal loss only, no z_u |
| Isolation Forest | 200 trees on window summaries (mean / max / last of the numeric features, z_u, resource diversity), fitted on nominal training windows, unsupervised (same method family as [4]) |
| Rule (D3) | The hand-written D3 heuristic over the newest event |
| TCN (D3) | The Deliverable 3 TCN (L = 20, d = 50), retrained on the same data |
| Collusion rules (D3) | R1: ≥ 2 shared sensitive resources; R2: concurrent co-access of a sensitive resource unfamiliar to both users |
| TCN-only (collusion) | The promoted v3 TCN at its calibrated θ_high, applied unchanged to the collusion data |
| D3 TGN / static GNN | Compared with the v3 TGN on the **D3 collusion dataset** (same 690 test anchors) in `docs/PROGRESS.md`; not re-run on the Phase 8 dataset (2,337 s and 17,565 s per run on CPU) |

## E. Results — individual detection (Exp. A)

Source: `evaluation/results/tables/exp_a_detection.md`, `exp_a_scenarios.md`, `exp_a_benign.md`; Figs.
`exp_a_fig1_prf` – `exp_a_fig5_latency`. All numbers are on synthetic data, 5 seeds, mean ± standard deviation, each
model at its own Eq. 8 thresholds.

**Overall (Table `exp_a_detection`).**

| Model | ROC-AUC | PR-AUC | P / R @ θ_high | FPR @ θ_high | instances detected |
|---|---|---|---|---|---|
| TCN v3 | 0.879 ± 0.020 | 0.714 ± 0.027 | 0.527 / 0.702 | 0.0131 | 117 / 153 |
| TCN (supervised only) | 0.979 ± 0.009 | 0.860 ± 0.044 | 0.646 / 0.828 | 0.0094 | 144 / 153 |
| LSTM | 0.964 ± 0.014 | 0.853 ± 0.045 | 0.641 / 0.832 | 0.0097 | 140 / 153 |
| TCN (D3) | 0.997 ± 0.001 | 0.969 ± 0.014 | 0.748 / 0.965 | 0.0069 | 153 / 153 |
| Isolation Forest | 0.854 ± 0.027 | 0.385 ± 0.041 | 0.343 / 0.430 | 0.0178 | 93 / 153 |
| Rule (D3) | 0.909 ± 0.015 | 0.513 ± 0.041 | 0.128 / 0.752 | 0.1065 | 112 / 153 |

**RQ1 answer: no.** On this data the v3 TCN is **not** better than the baselines. It ranks below the D3 TCN, the
supervised-only TCN and the LSTM on every threshold-free and thresholded metric. It is better than the Isolation
Forest and the rule on PR-AUC and precision. The D3 TCN is the strongest model on this generator.

**Where the v3 TCN loses (Table `exp_a_scenarios`).**
- It catches the loud scenarios: compromised account 0.990 event recall, BAC probe 0.958.
- It mostly misses the quiet ones: low-and-slow 0.197, unusual sequence 0.130, unusual time 0.160. On the same
  scenarios the supervised-only TCN reaches 0.593 / 0.562 / 0.892.
- The ROC curve (Fig. `exp_a_fig2_roc_pr`) shows the same: TPR ≈ 0.7 at near-zero FPR, then a slow rise. About 30 %
  of attack windows score like normal ones (Fig. `exp_a_fig3_confusion`: 29.8 % of attack windows below θ_high).

**Why (Table `exp_f_tcn_ablation`, Section H).** Removing the auxiliary self-supervised heads raises PR-AUC from
0.714 ± 0.027 to 0.872 ± 0.063, the level of the supervised-only TCN (0.860 ± 0.044). Removing z_u(t) changes little
(0.730 ± 0.029). In this implementation and on this data, the auxiliary heads are what costs detection. No
hyper-parameter search was run (Section L), so this is a finding about the [IMPL] defaults, not about the idea.

**False alarms (Table `exp_a_benign`).** At θ_high the v3 TCN flags:
- 0.0127 of normal events;
- 0.2140 of month-end events, the highest benign rate of any model on any benign scenario;
- nothing on flash crowds (0.0000), where the LSTM flags 0.1261 and the D3 TCN 0.1162.

Share of normal user-days with at least one flag: 0.115 ± 0.010 (D3 TCN 0.090, rule 0.857).

**Speed (Fig. `exp_a_fig5_latency`).** Median per-window inference at batch 1:
- v3 TCN: 6.90 ± 0.84 ms on CPU, 3.02 ± 0.68 ms on CUDA;
- LSTM: 2.94 / 1.44 ms;
- Isolation Forest: 12.81 ms on CPU.

The "TCN v3 − z_u" CPU figure (15.30 ± 15.29 ms) has a seed-level outlier. Training jobs shared the machine during
these timings (Section L), so the timings are indicative only.

**Time to detect.** The median minutes from an instance's first attack event to its first flag are small for every
model: v3 TCN 0.7 ± 0.2, D3 TCN 0.0, Isolation Forest 1.1 ± 2.2.

## F. Results — collusion (Exp. B)

Source: `exp_b_seed_level.md`, `exp_b_groups_by_size.md`, `exp_b_groups_by_kind.md`, `exp_b_anchor_kinds.md`; Fig.
`exp_b_fig1_by_size`. Synthetic `collusion_v3_eval`, 5 seeds; 75 test groups pooled (20 each of sizes 2, 3 and 5;
15 of size 8).

**Seed level (Table `exp_b_seed_level`).** "Is the escalated seed in a colluding group?":

| Model | ROC-AUC | PR-AUC | precision | recall | FPR |
|---|---|---|---|---|---|
| TGN v3 (θ_C) | 0.626 ± 0.049 | 0.397 ± 0.066 | 0.862 ± 0.043 | 0.135 ± 0.040 | 0.0036 ± 0.0007 |
| Rule R1 (shared sensitive resources) | 0.639 ± 0.042 | 0.242 ± 0.023 | 0.178 ± 0.027 | 0.521 ± 0.095 | 0.4316 ± 0.0037 |
| Rule R2 (unfamiliar co-access) | 0.499 ± 0.008 | 0.155 ± 0.007 | 0.121 ± 0.142 | 0.012 ± 0.015 | 0.0131 ± 0.0032 |

The TGN is precise and conservative: when it flags, it is usually right, but it flags few seeds. The shared-resource
rule finds about half the colluding seeds while flagging 43 % of the others.

**Groups (Tables `exp_b_groups_by_size`, `exp_b_groups_by_kind`).** Pooled over the 75 test groups:
- TGN v3 detects 39 / 75.
- The v3 TCN alone, on the group's own events, detects 46 / 75.
- **Either path detects 61 / 75.**
- The shared-resource rule detects 63 / 75, at the false-alarm cost below.

"TCN fires on any event of a member during the group's span" would give 59 / 75. That measure is not used, because
random non-colluding users of the same size and span reach 35–81 % by chance (column "chance rate").

**RQ2 answer.** The graph path finds groups that the individual path misses. Of the 75 groups, 15 are detected by
the TGN and not by the TCN on own events (61 − 46), and the TGN costs almost no false alarms on benign
collaboration. It does not replace the individual path: the TCN alone finds more groups. The union is what the
architecture deploys.

By size (TGN v3 / TCN own events / either):

| size | TGN v3 | TCN own | either |
|---|---|---|---|
| 2 | 9/20 | 9/20 | 15/20 |
| 3 | 13/20 | 12/20 | 17/20 |
| 5 | 10/20 | 14/20 | 17/20 |
| 8 | 7/15 | 11/15 | 12/15 |

TGN recall does not grow with group size here. Larger groups are found more often by the TCN, whose chance of
catching at least one member grows with the number of members.

By kind, the TGN is strongest on split exfiltration (17/20) and weakest on low-and-slow groups (4/20). On
low-and-slow groups, either path together reaches 12/20, the lowest of the four kinds. The median time from a
group's first access to TGN detection is 48.7 h (split exfiltration) to 148.6 h (low-and-slow). These groups unfold
over days in the generator, so hours are the relevant unit.

**False alarms on hard negatives (Table `exp_b_anchor_kinds`; flag rate, mean over seeds).**

| anchor kind | TGN v3 | rule R1 |
|---|---|---|
| recurring team meeting | 0.003 | 0.535 |
| temporary project team | 0.000 | 0.248 |
| flash crowd | 0.028 | 0.099 |
| Path-B-like (individually suspicious, not colluding) | 0.011 | 0.601 |
| Path-D-like (random sampled user) | 0.001 | 0.255 |

**Comparison with the D3 TGN.** It is measured on the **D3 collusion dataset** (same 690 test anchors;
`docs/PROGRESS.md`, v3 Phases 4–6):
- v3 TGN: seed ROC-AUC 0.681, PR-AUC 0.355.
- D3 TGN: 0.843 / 0.568.

On that data the v3 TGN is worse at seed level and better at member precision. The D3 TGN was not re-run on the
Phase 8 dataset (Section D).

## G. Results — latency and throughput (Exp. C, D)

Source: `exp_c_latency.md`, `exp_c_stages.md`, `exp_d_throughput.md`, `exp_d_resources.md`; Fig.
`exp_c_fig_stages`; raw `evaluation/results/raw/exp_c/`.

**Set-up.** Live stack, PDP in enforce mode, 3 runs of 120 s at each background load of 5, 20 and 50 requests / s.
- **Background load:** 40 fresh staff accounts, Poisson arrivals.
- **Attackers:** 5 fresh accounts per run, each starting at a random moment and sending a sensitive probe every
  0.2 s until it is refused.
- **Measurement:** every latency is taken from the per-event trace of the triggering request. Stage percentiles
  are never summed.
- **Validity:** a run is valid only if the gateway answered at least 99 % of requests; all 9 final runs did.
- **Machine:** no training was running.

**Individual path (Table `exp_c_latency`; 15 attackers per load level, all of them enforced).**

| load (req/s) | Lat_ind decision-ready p50 / p95 / p99 (ms) | Lat_ind observed p50 / p95 / p99 (ms) | Lat_PDP median (ms) | E[N_exposed] | P(Lat_ind ≤ 0.5 s) |
|---|---|---|---|---|---|
| 5 | 26 / 161 / 357 | 203 / 335 / 554 | 3 | 0.1 | 1.00 |
| 20 | 45 / 59 / 59 | 208 / 220 / 222 | 2 | 0.0 | 1.00 |
| 50 | 25 / 177 / 378 | 204 / 330 / 553 | 2 | 0.1 | 1.00 |

- **Decision-ready latency** (Eq. 32) runs from the attacker's request entering the gateway to the
  Terminate / Block decision being published. The median is 25–45 ms, and every one of the 45 attacks was decided
  within 0.5 s.
- **Observed latency** runs to the first request the gateway actually refused. Its median of about 203 ms is set
  by the attacker's 0.2 s request interval: the next request after the decision is the first one that can be
  refused.
- **Requests that passed after the trigger** (N_exposed, Eq. 33) averaged 0.0–0.1 per attack.
- v3 gives no numeric Lat_SLO, so P(Lat_ind ≤ x) is reported for 0.5, 1, 2 and 5 s; it is 1.00 at every load
  level.

**Stages (Table `exp_c_stages`, Fig. `exp_c_fig_stages`; median over runs of each run's p50 / p99).**

| load | gateway | Kafka | reorder | TCN queue | TCN |
|---|---|---|---|---|---|
| 5 | 1.9 / 11.1 | 6.7 / 23.5 | 0.0 / 0.0 | 6.4 / 61.7 | 7.7 / 47.9 |
| 20 | 2.5 / 16.9 | 7.1 / 36.7 | 0.0 / 0.0 | 9.1 / 48.9 | 8.6 / 31.8 |
| 50 | 2.0 / 18.7 | 6.6 / 85.8 | 0.0 / 0.0 | 7.8 / 83.5 | 8.0 / 43.9 |

- The median gateway-to-score time is 25–30 ms at every load. No stage grows with load in the median.
- The reorder wait is 0 in every run. That is consistent with each user's events coming in order from the single
  gateway; out-of-order delivery was not exercised here.
- Tails exist. Gateway-to-score p99 per load is 132 / 89 / 183 ms (median over runs), but one run at 50 req/s had a
  p99 of 2,828 ms (raw `rate50-run1.json`).

**Collective path.**
- Every one of the 45 attackers was escalated and group-scored by the TGN.
- Median score → escalation was 5.0–5.1 s and trigger → group score 5.1 s.
- Nearly all of that is the [IMPL] real-time union window `realtime_window` = 5 s in `config.py` (not from v3).
  Path A / B candidates are held for up to 5 s, so that candidates arriving together are admitted as one batch.
- TGN analysis itself took a median of 38 ms per escalation (p95 307 ms, max 684 ms over the 45; raw per-attacker
  `lat_tgn_total_ms`).

**Throughput and resources (Exp. D; Tables `exp_d_throughput`, `exp_d_resources`).**

| load (req/s) | gateway req/s | scores/s | gateway response p50 / p99 (ms) | status | max backlog: TCN / PDP / HDFS sink (records) |
|---|---|---|---|---|---|
| 5 | 4.2 | 4.3 | 8.1 / 38.4 | all 200 | 10 / 3 / 349 |
| 20 | 18.7 | 18.7 | 10.0 / 43.4 | all 200 | 13 / 2 / 1,367 |
| 50 | 47.7 | 47.8 | 7.7 / 43.7 | all 200 | 222 / 16 / 2,255 |

- **Every request was scored:** scores/s equals the gateway rate. The realised rate is slightly below the target
  because each client waits for its response before its next random sleep.
- **Backlogs stayed small and drained.**
  - The HDFS sink's backlog is a saw-tooth that empties at each 30 s batch flush.
  - The largest TCN backlog (222 records, one sample at 50 req/s) was back to 10 at the next sample.
- **Memory peaks over all runs** (`exp_d_resources`, docker stats about every 10 s):
  - Kafka 896 of 1,024 MiB (88 %);
  - the two CMU tasks about 280 of 384 MiB (72–74 %);
  - the gateway 211 of 384 MiB;
  - Redis 185 MiB, against its own `maxmemory` of 256 MB.
  - Kafka and the CMU tasks are the closest to their limits.
- **CPU:** Kafka averaged 58 % of one core (p95 196 %); the gateway, the CMU tasks and each TCN worker averaged
  23–26 %.

**RQ3 / RQ4 answer.**
- On one laptop up to 50 requests / s, an individual attack is decided in a median of 25–45 ms and refused at the
  attacker's next request.
- The collective path adds a fixed ~5 s from its [IMPL] batching window.
- No stage saturates at these loads.
- Higher loads, multi-host deployment and Eq. 24 / 28 at scale were not tested (Section L).

## H. Results — compute budget and ablation (Exp. E, F)

Source: `exp_e_strategies.md`, `exp_f_tcn_ablation.md`; Figs. `exp_e_fig_budget`, `exp_f_fig_paths`,
`exp_f_fig_tcn_ablation`. Replay of the collusion test period (seed 0 data) through the escalation service's
admission logic. Work is computed with Eq. 26 and the measured coefficients. The deployed budget is
B_W = 84,000 ms per 60 s window.

**Admission coverage is not detection.** An instance counts as covered when at least one of its users is admitted to
graph analysis during its activity (+ 1 h). Whether the TGN then flags it is Experiment B. This test period has
4 individual instances, so individual coverage moves in steps of 0.25.

**Strategies (Table `exp_e_strategies`).**

| strategy | group coverage | individual coverage | work / window mean (p99) ms | users admitted |
|---|---|---|---|---|
| analyse every active user (estimate) | 1 by construction | 1 by construction | 965.1 (5,107.7) | — |
| Path A only (s_ind ≥ θ_high) | 0.667 | 1.000 | 18.6 (264.2) | 2,434 |
| union of all paths, unbounded | 1.000 | 1.000 | 171.0 (7,277.5) | 21,474 |
| PriorityAdmit, B_W | 1.000 | 1.000 | 171.0 (7,277.5) | 21,474 |
| PriorityAdmit, 0.1 · B_W | 1.000 | 1.000 | 170.6 (7,311.1) | 21,392 |
| PriorityAdmit, 0.01 · B_W | 1.000 | 1.000 | 159.5 (837.9) | 19,469 |
| PriorityAdmit, 0.001 · B_W | 1.000 | 0.750 | 29.9 (83.7) | 4,943 |

**RQ5 answer.**
- At the deployed B_W the budget is never binding: the p99 window needs 7,277.5 ms of the 84,000 ms available, so
  PriorityAdmit admits exactly what the unbounded union admits.
- No window exceeded its budget at any budget level ("windows over budget" = 0 in every row).
- Coverage is kept down to 1 % of B_W. At 0.1 % of B_W, group coverage is still 1.0 and individual coverage falls
  to 0.75 (one of the 4 instances lost).
- The overflow counters show where the cut falls: background sweep work is deferred first, then sampled users are
  dropped, then real-time users are kept on PDP action alone.
- Compared with analysing every active user, the union admits users whose graph work averages 171.0 ms per window
  against an estimated 965.1 ms.

**Escalation path ablation (Exp. F, Fig. `exp_f_fig_paths`; union with one path removed, unbounded).**
- Removing Path A (high individual risk) halves individual coverage (0.500).
- Removing the resource path cuts the admitted users from 21,474 to 6,862 and the mean work to 44.9 ms per window,
  with no coverage lost on this data.
- Removing sampling or the sweep changes nothing measurable here.
- Path A alone covers 2 of 3 group instances (0.667).

So on this data the resource path is the most expensive path and the one that adds no coverage; Path A is the
cheapest and is needed for individual coverage. One seed and one test period: these are single runs (except
union and admit-1, 3 runs each), not distributions.

**TCN ablation (Exp. F, Table `exp_f_tcn_ablation`).** See Section E:
- removing the auxiliary heads: PR-AUC 0.714 → 0.872;
- removing z_u(t): 0.714 → 0.730.

## I. Results — enforcement (Exp. G)

Source: `exp_g_enforcement.md`; raw `evaluation/results/raw/exp_g/`. Live stack, PDP in enforce mode.

| check | result |
|---|---|
| Eq. 21: live decision = pure mapping (`pdp_engine.decide`), 200 random cases around the calibrated thresholds | **200 / 200** (Terminate 70/70, Throttle 42/42, Restrict 30/30, StepUp 15/15, Allow 43/43) |
| score on Kafka → decision in the gateway's cache | p50 12.6 ms, p95 103.5 ms, p99 793.4 ms, max 2,085.6 ms (n = 157) |
| stale decision (ψ ≤ ψ_cur) written into the cache | rejected 20 / 20 |
| Terminate with a 60 s TTL | Allow decision 1.0 s after `expires_at`; login allowed again 1.1 s after it |
| policy precedence: a policy Block replaces an ML Terminate, an analyst release is refused, a low score does not lift it, lifting the policy restores access | correct in 3 / 3 runs |
| PDP stopped: sensitivity fallback onset | 31.0 s after the stop (decision TTL 30 s) |
| during the fallback | s = 1 refused 73 / 73 samples; s = 0 allowed 73 / 73 |
| PDP started again: sensitive requests served again | 7.3 s after the start |

The fallback onset and recovery compare the host's clock (stop and start commands) with the probe's (the Docker
VM), so they are accurate only to the offset between the two clocks.

**RQ7 answer.** The PDP implements Eq. 21 exactly on every tested case. Decisions reach the gateway's cache in a
median of 12.6 ms. ψ ordering, TTL expiry, the sensitivity fallback and policy precedence behave as specified.
- The fallback starts about one decision-TTL after the PDP stops.
- During it, the gateway refuses only sensitive resources.
- Recovery takes the PDP's start-up time (about 7 s).

**Defect found and fixed during this experiment.**
- With the gateway unreachable, the PDP retried every pending session revocation in each loop iteration, at a
  connect timeout each. N pending users stalled the decision path for N × ~4 s.
- In the first (discarded) run this delayed decisions by seconds; it was not an Eq. 21 error.
- Revocations now stop after the first failure and back off (1 s doubling to 30 s), with a test.
- Revocation is hygiene only: the cached decision already denies requests.

**Probe design.** The fallback probe acts on a fresh hr_manager account. The live TCN acts on such an account for
reading payroll (a cold-start effect recorded in `docs/PROGRESS.md`, v3 Phase 7), which hid the fallback in the first probe. The reported probe therefore puts
its user under an analyst release grace, during which no ML evidence can act on it.

Verification runs, which are not the experiment, are in `docs/PROGRESS.md` (v3 Phase 7):
- six actions end to end;
- stale ψ rejected;
- fallback with the PDP stopped;
- one measured interaction: a refused sensitive request under Restrict is scored by the live TCN and escalates
  to Terminate.

## J. Results — failure modes (Exp. H)

Source: `exp_h_crash.md`, `exp_h_outage.md`; raw `evaluation/results/raw/exp_h/`. Live stack, PDP in enforce
mode.

**Crash recovery (Table `exp_h_crash`).** In each run, 3,000 events are sent at 100 / s. The named container gets
SIGKILL 12 s into the burst and is started again 5 s later; 3 runs per target.

| run | lost (sequencer / TCN) | duplicates out | largest output gap, sequencer / TCN (s) |
|---|---|---|---|
| baseline | 0 / 0 | 0 | 2.4 / 0.7 |
| kill sequencer (×3) | 0 / 0 | 0 | 7.4–7.8 / 7.7–8.0 |
| kill TCN worker 1 (×3) | 0 / 0 | 0 | 0.2 / 33.7–35.3 |
| kill CMU (user side) (×3) | 0 / 0 | 0 | 0.2 / 0.2 |

**Duplicates.** 200 events were each produced 3 times (600 records). The sequencer and the TCN each emitted 200
unique records and 0 duplicates.

**RQ8 answer (crash and duplicates).**
- No event was lost and none was duplicated at the sequencer or the TCN, in any run: exactly-once holds under these
  faults.
- Recovery time depends on the stage:
  - The sequencer is back in about 7–8 s.
  - The TCN output stalls for about 34–35 s after its worker is killed.
  - The difference matches the consumer configuration. The sequencer uses static group membership (10 s session),
    so its restarted instance takes its partitions back at once. The TCN workers use dynamic membership with a 45 s
    session (`streaming/consumer.py`), so their partitions wait for the dead member's session to expire. Static
    membership for the TCN workers is the obvious fix; it was not applied or measured here.
  - Killing the CMU does not affect the individual path.

**Dependency outages (Table `exp_h_outage`).** `docker pause` of Kafka or Redis for 20 s during an 80 s probe
(one gateway request every 0.2 s), 3 runs each.

| outage | requests | allowed (200) | errors | events later on Kafka |
|---|---|---|---|---|
| Kafka paused (×3) | 387–388 | all | 0 | = requests (387–388) |
| Redis paused (×3) | 292 | 289 | 3 × ReadTimeout | 289 |

- **Kafka paused.** The gateway kept serving with unchanged latency (probe p50 5.5 ms), and every request's event
  was on Kafka after the pause.
- **Redis paused.** The gateway answered **no** request while Redis was paused:
  - In every run, the three requests sent during the pause hit the probe's 5 s client timeout.
  - The fourth request completed after the unpause (about 4.9 s).
  - All 289 successful requests fall outside the pause.
  - Nothing was allowed without a decision lookup, so the gateway did not fail open. But it hung instead of
    answering 503 at once.
  - The fast-503 path is the one the unit test covers for a refused connection (`test_redis_outage_fails_closed`).
    A paused (silent) Redis shows a slower path, through the 2 s socket timeouts of the gateway's Redis client.
    Which calls add up to more than 5 s per request was not traced.

The Table V items that are design or process mitigations are not evaluated experimentally:
- adversarial label poisoning;
- feedback self-confirmation;
- sampling contamination;
- cross-tenant correlation.

For each, the evidence is the mechanism and its tests: analyst-verified labels only (Phase 9), namespaced r*, and
per-member scores.

## K. Comparison with reference papers

- **Method A (reproduced).** No reference method was reproduced. The baselines in D are implementations of method
  families, not reproductions.
- **Method B (reported).** No reference numbers are used:
  - [24] is a placeholder in v3; a candidate exists but needs team confirmation.
  - The full text of [25] (Prov-Trust) is not accessible.
  - [1] and [4] were identified (title, venue, DOI), but their full texts could not be retrieved here. They also
    evaluate other data (network traffic; CERT).
  - So every quantitative comparison cell is **Not Reported** (no verified number) or **Not Comparable**
    (different data and task).
- The qualitative capability comparison is the paper's own Table I.
- Verification log: `evaluation/references/REFERENCES.md`.

## L. Threats to validity

- **Synthetic data.** The generator encodes the scenarios the detectors are meant to find. Results show behaviour
  on these scenarios, not field performance. CERT r4.2 would be the first external check.
- **Simulated context.** ThreatFeed and DevPosture are simulated; z_u(t) carries only what the simulators encode.
- **One host.** All latency and throughput results come from one laptop. Docker Desktop's VM shares CPU with
  everything else, so absolute numbers do not transfer to a cluster. Eqs. 24 and 28 are evaluated only as far as
  one host allows.
- **Untuned hyper-parameters.** The v3 TGN uses the paper's untuned defaults; the v3 TCN uses the [IMPL] values
  in `config.py`. No search was run on the test design.
- **Admission coverage is not detection.** Experiment E measures whether a group reaches the graph path, not
  whether the graph path then flags it; Experiment B measures the latter.
- **Interactions.** PDP-caused denials enter the TCN's input like ACL denials (v3 Eq. 1). This couples Restrict
  to Terminate (Section I).
- **Shared machine for the model experiments.** Experiments A and B trained on the same GPU at the same time (A's
  seed 2 took 6,087 s, against about 2,500–3,600 s for the others). Detection metrics do not depend on this; the
  inference timings in Fig. `exp_a_fig5_latency` do, so they are indicative only. The live experiments (C, D, G, H)
  ran with no training on the machine.
- **Discarded live runs.**
  - In the first live run (2026-11-15) the gateway container stopped 4 minutes into Experiment C and stayed down
    for 55 minutes.
  - The rerun reproduced the stop 9 s into C: exit 137, `OOMKilled = true`.
  - The cause was password hashing. Each Argon2id check allocates 64 MiB (argon2-cffi defaults), and the gateway
    ran up to 40 of them in parallel inside a 384 MB container, so the burst of 45 test logins at the start of a
    run exhausted it.
  - The same mechanism is a denial-of-service path: about six concurrent unauthenticated logins could do it.
  - Fix: Argon2 operations are now bounded per process (`security.py`, `AS_PASSWORD_HASH_CONCURRENCY`, default 2).
    A live burst of 45 concurrent logins then succeeded 45/45 in 4.7 s, with the gateway at 83 MiB.
  - The first stop had exit code 0 and no OOM flag. It happened at the same moment (the start-of-run login
    burst), but its cause is not confirmed.
  - **Escalation service, also OOM.** During the rerun, the escalation service was OOM-killed in its 2-hourly
    background sweep.
    - The sweep loaded the last 24 h of events and risk scores from HDFS as full objects. After a day of
      experiment load that exceeded 384 MB.
    - With no restart policy, the collective path stayed down: its consumer backlog grew to about 8,000 records.
    - Fix: lake reads are streamed per record batch and read only the needed columns; the sweep keeps only the
      fields its rules use. A test shows the sweep candidates are unchanged.
    - A forced sweep over the same day now peaks at 103.6 MiB, against 383.8 MiB of 384 with streaming alone.
    - Experiment C was rerun in full afterwards.
  - **HDFS sink, also OOM.** In the next C rerun, the HDFS sink was OOM-killed while compacting today's
    partition, which it merged as one in-memory table.
    - Fix: a streaming merge. It then compacted the 108,144-row partition at about 233 MiB.
    - The sink is not on the measured path, but 5 runs had gone without it, so C was rerun again.
  - **Redis near its limit.** Redis held 243 of its 256 MB (`noeviction`: at the limit every write fails), mostly
    the sink's 24 h dedup markers.
    - Fix: the TTL was cut to 15 min; duplicates are also removed by id on read and compaction.
    - The final C runs started at 171 MB.
  - **Interrupted run.** A further C run was stopped after 1 of 9 runs when the host logged off overnight
    (2026-11-15). It was discarded. The reported C/D results come from a complete run on 2026-11-16 (09:20–09:53
    UTC), with no gateway or PDP restart and no container out of memory.
  - **Leftover setting.** The crashed first run had set `pdp.block_ttl` to 60 s for its TTL test and never restored
    it. It was restored to 86,400 s before the final C runs. Latency to the first Terminate does not depend on it.
  - Every C run after the first, and every G part except eq21, ran against a missing gateway.
  - eq21 was distorted by a PDP defect that this outage exposed (Section I).
  - Those results are archived, not reported.
  - The harness now checks the gateway before each part, marks a C run INVALID when more than 1 % of its requests
    fail, and logs any gateway or PDP restart during a part. C and G were rerun.
  - H ran after the gateway was back and is reported.

## M. Reproducibility

```
.venv-gpu/Scripts/python -m evaluation.exp_a --seeds 0 1 2 3 4 --models tcn-v3 tcn-sup lstm iforest rule tcn-d3 tcn-noz tcn-noaux
.venv-gpu/Scripts/python -m evaluation.exp_b --seeds 0 1 2 3 4
.venv-gpu/Scripts/python -m evaluation.exp_e
bash evaluation/run_live.sh all          # C, D, G, H on the running stack, otherwise idle machine
.venv/Scripts/python -m evaluation.figures
```
