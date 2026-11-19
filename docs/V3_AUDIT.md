# AccessSentinel Revised v3: Phase 1 Audit and Roadmap

*Date: 2026-11-15. Branch `v3/phase1-audit`, from `main` at `2560afe` (Phases 1–9 of the D3 implementation).*

This document compares the running implementation, which was built to Deliverable 3 (D3), with the revised
paper **AccessSentinel_Revised_v3_highlighted.pdf** (10 pages; the authoritative specification from now on). It
then lays out the implementation and evaluation roadmap for Deliverable 4.

Nothing below is a measurement of the v3 system. The only measured values are the hardware facts (§2) and the
earlier D3 results, which are labelled as such.

---

## 1. Inputs

| Input | Status |
|---|---|
| Revised paper | Read in full. The file provided is `AccessSentinel_Revised_v3_highlighted.pdf` (the prompt names `...(2).pdf`; same content assumed, please confirm) |
| Example figure "Label Query Latency Breakdown" | **Not attached** to the message. Figures will use a standard IEEE grouped/stacked-bar style until it is provided |
| Existing repository | Inspected: 171 tracked files, Phases 1–9 (D3), 152 tests pass |

---

## 2. Experiment environment (measured, `evaluation/results/raw/environment-phase1.json`)

| Item | Value | Consequence |
|---|---|---|
| CPU | Intel Core i5-13500HX, 14 cores / 20 threads | All system experiments run on **one laptop**; no multi-node results |
| RAM | 15.7 GB total; ~0.7 GB free while the 20-service stack runs | Memory is the binding constraint for scale experiments |
| GPU | NVIDIA RTX 4050 Laptop, 6 GB, driver 610.47 | Usable for training, but the installed PyTorch is the **CPU-only** build (decision D2) |
| Docker | 28.3.3; VM 20 CPUs, 7.6 GB RAM | Kafka, HDFS, PostgreSQL, Redis and all workers share 7.6 GB |
| Disk | 81 GB free | Enough for CERT r4.2 if chosen (decision D3) |
| Software | Python 3.11, PyTorch 2.15 (CPU), scikit-learn 1.5.2, pyarrow 26, confluent-kafka 2.16.1, matplotlib 3.9.2 | No PyTorch Geometric. The SGE will be written in plain PyTorch, as the D3 TGN was |

**Realistic scale.** The prompt's 50,000 and 100,000-user workloads cannot run through the full Docker stack on
this machine. The plan (Experiment D) is to:
- run the full stack at the largest scale that stays stable (expected 1,000–10,000 simulated users, to be measured);
- run the scoring components in-process at larger populations;
- report the scale actually tested, never an extrapolated value as a measurement.

---

## 3. Requirement-by-requirement compliance

Legend: ✅ implemented and tested · 🟡 partial · ❌ missing · ⚠️ implemented but **conflicts** with v3.

### 3.1 Event schema and streaming (v3 Step 1–3, Table II, R1, R3, R5) — *updated after Phase 2*

| v3 requirement | Current code | Status |
|---|---|---|
| Event tuple `(eid, u, te, ti, ν, a, r*, s, p, o, c, y, vpolicy, vschema)` (Eq. 1) | `schema.py` AccessEventV1: `event_id, u, t, a, r, s, p, o, c, y, schema_version, correlation_id, meta` | ✅ Phase 2: schema v2 adds `seq` (ν), `stream`, `tenant` → `rstar`, `ti` (broker LogAppendTime), `v_policy`; v1 still accepted |
| User-keyed Kafka stream | `access-events`, key = user | ✅ |
| Resource-keyed derived view | `repartitioner.py`: key = `app:res#bucket` | ✅ Phase 2: transactional repartitioner, key `r*\|b\|hash(u) mod Jr` |
| Two-stage shard-and-merge for hot resources | Distinct users counted in one Redis structure per resource | ✅ Phase 2: `resource_windows.py` Stage 1 + Stage 2, exact participant sets, one final per (r*, b) (escalation still uses Redis until Phase 5) |
| Per-key event-time watermark `Wt = max te − δlate` (Eq. 2) | Events processed in **arrival** order | ✅ Phase 2: sequencer (watermark, ν fast path, idle bound); TCN reads `access-events.ordered` |
| Four late-event policies by sensitivity | — | ✅ Phase 2 ("rescore" is approximate in the D3 TCN until Phase 3) |
| eid-based deduplication | `streaming/dedup.py` (Redis SET NX + TTL) | ✅ |
| Checkpointed keyed state consistent with committed offsets (R3) | Manual commits after state writes (at-least-once); crash tests in Phase 4 | ✅ for Kafka stages (Phase 2: transactions + changelog, crash-tested); 🟡 TCN state in Redis is at-least-once + eid dedup |
| Named stream-processing engine (Flink / Kafka Streams) | Own Python consumer framework (`streaming/consumer.py`) | ✅ D1 taken: Python stages with Kafka transactions (`streaming/transactional.py`) |
| HDFS time/app-partitioned Parquet, compaction | `adapters/hdfs_lake.py`, `hdfs_sink.py` | ✅ |
| Hourly feature aggregates + resource-participant summaries for the sweep | The sweep reads raw HDFS history | ✅ Phase 2: `history_aggregates.py` (sweep rewiring: Phase 5) |
| Version stamps on scores and decisions (R5) | Scores carry `model_version`; decisions carry `version` | ✅ Events carry `v_policy` (Phase 2); decision records carry `(v_model, v_policy, ψ)` (Phase 7, schema_version 2) |

### 3.2 Individual detection: TCN (Step 4–5, Eqs. 3–9, Table III)

| v3 requirement | Current code (D3) | Status |
|---|---|---|
| L = 128 | L = 20 | ✅ Phase 3 (`TCNv3Config.window_len`) |
| d = 43 with Table III encoding (resource 12, perm/op 6, network 6, device 4, Δt 1, time 4, y 1, s 1, history 6, versions 2) | d = 50, one-hot layout of `ml/features.py` | ✅ Phase 3 (`EncoderV3`; ThreatFeed / DevPosture simulated) |
| 6 residual blocks, **two** dilated convs each, dilations 1–32, filters [256,256,128,128,64,64], R = 127 | 10 blocks, **one** conv each, dilations (1,2,4,6,…,18), filters (512 … 12) | ✅ Phase 3 (`tcn.TCNv3`, R = 127) |
| Timer context `zu(t)` = (inactivity, session age, ThreatFeed, DevPosture) at the scoring head, not in Xu | Inactivity is folded into the event features; no session age / ThreatFeed / DevPosture | ✅ Phase 3 (z_u at the scoring head; ThreatFeed / DevPosture simulated) |
| Objective `λ1 Lnext-res + λ2 Lnext-op + λ3 Ltime-gap + λ4 Lsup`, Focal only on Lsup (Eq. 6) | Supervised Focal loss only | ✅ Phase 3 (`tcn.multitask_loss`) |
| Chronological split with gap g ≥ L, rolling-origin validation, incidents grouped per user, scalers fit on train only (Eq. 7) | Train/val by user, separate test **period**; scaler fit on train; 5-fold user-grouped CV | ✅ Phase 3 (`build_v3` gap L − 1, rolling origin, 12-day folds) |
| Thresholds θlow, θhigh, θB as quantiles of scores on held-out **nominal** data (Eq. 8) | Platt calibration + fixed 0.4 / 0.6 | ✅ Phase 3 (quantiles α 0.05 / 0.01 / 0.001 on nominal validation; carried in every score) |
| Medium tier re-scored every Trecheck with zu(t) (Eq. 9) | Recheck exists; z not used | ✅ Phase 3 (`_recheck_v3` with z at re-check time) |
| Stateless TCN replicas, keyed state in the stream tier, model pinned during replay | Workers keep per-user state in Redis; hot reload of CURRENT | 🟡 |

### 3.3 Escalation and compute admission (Step 6, Step 10, Algorithm 1, Eqs. 10, 25–30)

| v3 requirement | Current code | Status |
|---|---|---|
| Four paths (high, resource, sweep, sample) | `escalation_engine.py` A–D, all verified live (Phase 6) | ✅ |
| Set union, at most once per window | Union per user + cooldowns | ✅ |
| Cost model `Cu = c0 + c1·|E(Nh(u))| + c2·|V(Nh(u))|` (Eq. 26) | — | ✅ Phase 5–6 (Eq. 26; c0..c2 fitted from measured SGE time, R² 0.92) |
| Budget `BTGN_W = ⌊ρ′·N·µgraph·W⌋` (Eq. 29), provisioning from pre-admission demand (Eqs. 27–28) | Fixed count caps (`max_realtime_per_window = 100`, `max_background_per_window = 50`) | ✅ Phase 5 (`graph_budget`, per-window ledger); Exp. E: never over budget; 🟡 Eqs. 27–28 provisioning not evaluated (one host) |
| PriorityAdmit order res > high > sweep > samp; overflow drop / defer / queue-with-deadline / PDP-acts-alone | Realtime overflow deferred, background shed | ✅ Phase 5 (`priority_admit`, Algorithm 1) |
| Property `Σ Cu ≤ BTGN_W` | — | ✅ Phase 5 (200-case property test + live check) |
| Resource trigger exactly once per (resource, finalized window) | Cooldown-based | ✅ Phase 2 + 5 (escalation consumes final `resource-windows`) |

### 3.4 Relational detection: TGN = CMU + SGE (Step 7, Eqs. 11–19)

| v3 requirement | Current code (D3 / [14] style) | Status |
|---|---|---|
| **CMU** updates user and resource memory for **every** event (GRU, message dim 100, memory dim 100, time encoder 100) | No learned node memory. `graph_index` keeps edge statistics for every event, but the TGN embeds a neighbourhood **only when a user is escalated** | ✅ Phase 4 (`cmu-user` / `cmu-resource`, every event; crash-tested) |
| Single-writer memory per node, snapshot reads, bounded staleness | — | ✅ Phase 4 (one task per node side; counterpart = latest snapshot; staleness measured) |
| **SGE**: temporal attention over node memories + bounded neighbours (10 per hop, 1 hop), time encoder Φ | Dense 2-layer GCN + GRU + attention over T = 10 time steps per neighbourhood | ✅ Phase 6 (`tgn.SGE`, 10 neighbours, 1 hop, admitted users only) |
| Edge decoder `ϵe = Dec(hu, hr*)` (Eq. 14), edge-reconstruction objective | Edge scorer exists (supervised + self-supervised stage) | ✅ Phase 4 (`EdgeDecoder`; design marked unvalidated as in v3, D4) |
| Graph: access edges user→resource, membership edges resource→application; **no** user-user edges from shared application | `GraphParams.app_coaccess_weight = 0.25` adds user-user edges through a shared application | 🟡 v3 graph has no user-user application edges ✅; resource→application membership edges are NOT modelled (node-type slot reserved only) |
| Candidate coordination graph HW (Eq. 15: ≥ hmin shared resources within ∆c; dcap) | Group = component of users sharing ≥ 2 sensitive resources | ✅ Phase 6 (Eq. 15, Δc, d_cap; h_min = 1 calibrated on validation) |
| Popularity-discounted weights `wuv` (Eq. 16), group filter |g|, |Rsens|, density (Eq. 17) | — | ✅ Phase 6 (Eqs. 16–17) |
| Attribution `Au` (Eq. 18), per-member `Scoll = Cal(ω1·TMπ(ϵ) + ω2·Au + ω3·Nov)` (Eq. 19) | S_coll = mean of top-25 % edge scores for the whole group | ✅ Phase 6 (Eqs. 18–19, ω fitted by logistic regression); per-member scores published |
| Hyperparameters: Adam 1e-4, 50 epochs, L2 1e-6 | Adam, other values | ✅ Phase 4 (as written in v3; not tuned) |

### 3.5 PDP, enforcement, feedback (Step 8–9, Eqs. 20–21)

| v3 requirement | Current code | Status |
|---|---|---|
| Six actions Allow / StepUp / Throttle / Restrict / Terminate / Block | Allow / Block | ✅ Phase 7 |
| Ordered mapping Eq. 21 (policy deny → Block; S_ind ≥ θB → Terminate; S_coll ≥ θC ∧ Au ≥ θA → Restrict; … → StepUp; θhigh ≤ S_ind < θB → Throttle) | Block if S_ind ≥ 0.6 or S_coll ≥ θcoll or policy deny | ✅ Phase 7 (`decide`, sticky most-severe action, calibrated θB / θC / θA) |
| Policy denial cannot be overridden by ML | Policy blocks are locked against analyst release (Phase 8) | ✅ (re-tested with six actions, Phase 7) |
| Decision version ψ; gateway rejects ψ ≤ ψcur | PDP writes increasing `version`; gateway reads the latest value but does not compare | ✅ Phase 7 (global ψ, gateway rejects ψ ≤ ψcur; verified live) |
| Decision TTL + **sensitivity-based fallback** (s = 0 fail-open, s = 1 fail-closed Restrict) | Gateway fails **closed for everything** (HTTP 503) when Redis is down | ✅ Phase 7 (PDP heartbeat TTL, s=0 open / s=1 Restrict, per-app override; verified live) |
| Request-time and session-time enforcement | Cached decision + session revocation | ✅ |
| Feedback: only analyst-verified outcomes become labels | `ml/feedback_data.py`, verified live in Phase 9 | ✅ label collection (verdicts → verified retraining set) |
| Retraining from verified feedback (`ml/retrain.py`, validation gate, promotion) | Written for the D3 models | 🟡 works for the D3 TCN/TGN only: with the v3 models CURRENT, `retrain tcn` fails (`TcnScorerV3` has no D3 preprocessor `pre`; checked 2026-11-17). v3 retraining (Table III encoder, z_u, multi-task fine-tune, Eq. 8 re-calibration) is not implemented |

### 3.6 Formal properties (Table IV) and failure modes (Table V)

| Property / failure mode | Test today | Status |
|---|---|---|
| Per-user ordering after watermark | — | ✅ Phase 2 (property tests + live) |
| No duplicate graph inference per window | Union + cooldown tests | 🟡 |
| Compute bound | — | ✅ Phase 5 (property test + live ledger check) |
| Eventual enforcement within LatSLO with p ≥ 0.99 | Exp. C: 45 / 45 attacks decided within 0.5 s at 5–50 req/s | 🟡 Distribution measured; v3 gives no numeric Lat_SLO, and n = 45 cannot establish p ≥ 0.99 |
| No stale overwrite (ψ) | — | ✅ Phase 7 (unit + live); Exp. G 20 / 20 |
| State recovery consistent with committed offsets | Exp. H: SIGKILL of sequencer, TCN worker, CMU (3 × each) | ✅ 0 lost, 0 duplicates; 🟡 TCN takeover ≈ 34 s (dynamic group membership) |
| Resource-trigger exactly once per (r*, window) | — | ✅ Phase 2 (live `smoke_v3_stream windows`) |
| Duplicate delivery, consumer restart, HDFS outage, ML outage | Phase 4/8 tests; Exp. H (200 events × 3 copies → 200 out) | ✅ |
| Gateway / PDP outage fallback | Exp. G (PDP stopped 60 s) | ✅ onset 31.0 s (TTL 30 s), s = 1 refused, s = 0 allowed |
| CMU staleness | Phase 4 (measured) | 🟡 measured, not an experiment |
| Model-version mismatch, hot partitions | — | ❌ not tested |

### 3.7 Summary

| Area | Reusable as is | Must change | New |
|---|---|---|---|
| Gateway, demo apps, Kafka, HDFS sink, dedup, dashboard, feedback | ✅ | — | — |
| Event schema / streaming | most | schema fields, repartition key | watermarks, late policies, shard-merge, aggregates, exactly-once |
| TCN | training/eval harness, registry, workers | features, architecture, objective, calibration | timer context, multi-task heads |
| Escalation | four paths | caps → budget | cost model, PriorityAdmit, deadlines |
| TGN | graph index, data generation | model | CMU service, SGE, coordination graph, collective scoring |
| PDP | lifecycle, outbox, audit | decision rule, actions | 4 new actions at the gateway, ψ check, fallback |

---

## 4. Obsolete or conflicting code

Kept, labelled as the **D3 baseline**; not deleted. They become baselines in Experiments A, B and F.
- `config.TCNConfig` (L = 20, d = 50, 10 blocks) and the promoted model `tcn-20261110T194520Z-beeb9b`.
- `tgn.py` dense-GCN TGN and `tgn-20261111T104626Z-ccc61d` (an on-demand graph model, i.e. a "no CMU" baseline).
- `GraphParams.app_coaccess_weight`: user-user edges via shared applications. **Disabled in v3 mode**, since v3 forbids them.
- Fixed thresholds 0.4 / 0.6 / θcoll = 0.9 in the PDP. Replaced by calibrated quantiles; the old values are kept only for the D3 baseline.
- Count-based escalation caps. Replaced by the work-unit budget.
- The in-process reference pipeline (`pipeline.py`, `broker.py`, `storage.py`, `gateway.py`, `simulator.py`). Not part of the deployed system; left untouched.

---

## 5. Decisions needed from the team

| # | Decision | Options | Recommendation |
|---|---|---|---|
| **D1** | Stream-processing engine (v3 names Flink / Kafka Streams and assumes `exactly_once_v2`) | (a) keep the Python framework and add event-time watermarks, late-event policies, checkpointed keyed state and **Kafka transactions** (read-process-write exactly-once inside Kafka; supported by confluent-kafka) (b) introduce Apache Flink (PyFlink): a large rewrite needing several GB of extra memory on an already-full 7.6 GB Docker VM | **(a)**, stated in the paper as "Kafka transactional processing, equivalent to `exactly_once_v2` for Kafka-to-Kafka stages; Redis/HDFS side effects are idempotent, not transactional". Exactly-once is then *measured* in Experiment H, not assumed |
| **D2** | Use the RTX 4050 | Install the CUDA build of PyTorch in a separate environment (about 3 GB download) for training; keep CPU inference in the containers | **Yes for training** (L = 128 with 256 filters and multi-task heads is slow on CPU). Report inference latency on CPU (the deployed path) and optionally GPU |
| **D3** | External dataset | CERT Insider Threat r4.2 (CMU SEI / KiltHub, doi 10.1184/R1/12841247; reportedly about 20 GB, 1,000 users, 17–18 months; file list and licence to verify before download) | Use it **in addition to** synthetic data, for Experiment A only (individual detection; it has no collusion labels). Download needs your OK |
| **D4** | Design choices the paper's change log marks "Needs your team's confirmation" | ϵe decoder, definition of Au, Throttle rule, dcap, entry point of zu(t), d = 43 | Implement exactly as written in v3 and list them as unvalidated (as the paper does), unless the team changes them |
| **D5** | Reference [24] | Search found a strong candidate: A. Modell, J. Larson, M. Turcotte, A. Bertiger, "A Graph Embedding Approach to User Behavior Anomaly Detection," IEEE BigData 2021 workshop GTA³ 2.0 (dynamic bipartite user–resource graph, embeddings updated over time, SharePoint logs: one enterprise service, as Table I says) | **Team to confirm** it is the intended work; not used in comparisons until confirmed |
| **D6** | Reference [25] Prov-Trust | Exists: X. Li, L. Cao, J. Bai, H. Lv, X. Du, "Prov-Trust: Zero-trust dynamic access control technology based on anomaly detection of provenance graph," Springer journal, 2026, doi 10.1007/s44443-026-00731-5. The full text is behind a login; only the abstract was seen ("> 90 % of APT attack chains blocked in real time", unverified) | Please provide the PDF if available. Until then, Not Reproduced / qualitative only |
| **D7** | Example latency figure | Not attached | Send it; otherwise IEEE-standard styling |

---

## 6. Datasets

| Dataset | Use | Labels | Notes |
|---|---|---|---|
| **Synthetic generator v3** (extends `datasets/synth.py`) | All experiments | Exact per-event, per-group ground truth | Must add v3 fields (ν, tenant, vpolicy), timer context (session age, ThreatFeed, DevPosture), and the scenarios of Experiments A/B: horizontal/vertical abuse, unusual sequences, unusual time, repeated sensitive access, low-and-slow; benign collaboration; 2-, 5- and ≥ 8-user collusion; cross-application; low-and-slow groups; groups keeping individual scores below θhigh |
| **CERT r4.2** (decision D3) | Experiment A external validity | Insider scenarios per user/day | Not web-application access logs: needs a documented mapping (logon/file/http/device → resource, operation). Results reported separately |
| Public collusion data for access logs | — | — | **None found.** Collusion results will be on synthetic data only, stated as a limitation |

**Circularity risk.** The same team writes the generator and the detectors. Mitigations:
- freeze generator parameters before model tuning;
- hold out whole attack *families* for the test set;
- use CERT for an independent check of individual detection;
- report all of this in §J of the evaluation.

---

## 7. Baselines and reference papers

### 7.1 Method A: reproduced on our data and hardware

| Baseline | For | Source |
|---|---|---|
| D3 TCN (current promoted model) | A, F | This repo |
| LSTM sequence classifier | A | Standard; same windows and splits |
| Isolation Forest on per-user-day features | A | Al-Shehari et al. [4] (feature set to be taken from the paper) |
| Statistical / rule detector | A | Existing rule baseline (Phase 5) |
| "Reference TCN" configuration | A | Mezina et al. [1] or Zhang et al. [2] architecture on our data, if their configurations are fully specified |
| Static GCN | B | Phase 7 "T = 1" ablation, ported to the v3 graph |
| Dynamic-graph baseline without continuous memory | B, F | D3 TGN (DynAD-style [14]) |
| Escalation strategies: full graph, high-only, four-path unbounded, four-path + PriorityAdmit | E | v3 code with switches |

### 7.2 Method B: published results ("Reported by Reference Paper", with page citation)

| Ref | Verified bibliographic data | Results to extract | Status |
|---|---|---|---|
| [1] Mezina et al. 2021, IEEE Access 9 | In PDF | 97 % precision (cited in v3) | Original to be retrieved for page numbers |
| [2] Zhang et al. 2022, ICCC | In PDF | 92 % negative-sample recall | To retrieve |
| [4] Al-Shehari et al. 2023, IEEE Access 11 | In PDF | IF on CERT; dataset version and metrics | To retrieve |
| [14] Zhu et al. 2020, TrustCom | In PDF | DynAD results | To retrieve |
| [18] Skopik et al. 2023, IEEE TDSC 20(4) | In PDF | 5,706 → 75 alerts | To retrieve |
| [21] Yuan et al. 2021, DSN | In PDF | AUC 99.99 % on CERT r6 | To retrieve |
| [22] Shehnepoor et al. 2024, IEEE TNNLS 35(5) | In PDF | Group detection results | To retrieve |
| [23] Rossi et al. 2020, ICML GRL workshop | In PDF (arXiv 2006.10637) | Model only; no access-control results | Open access |
| [24] | **Unverified** (D5) | — | Blocked |
| [25] Prov-Trust | Existence verified (D6) | Abstract only | Blocked on full text |

Rule: no speedup or percentage improvement is computed between our numbers and Method B numbers. Method B
values sit in separate tables or columns with their dataset and conditions.

---

## 8. Experiment plan (Deliverable 4 §V)

| Exp. | Question | Data | Main figures (generated from raw results) | Feasibility on this machine |
|---|---|---|---|---|
| A Individual detection | Does the v3 TCN detect the individual scenarios better than LSTM / IF / rule / D3 TCN? | Synthetic (+ CERT) | Fig. 1 grouped P/R/F1; ROC, PR curves; confusion matrices; inference latency | ✅ (GPU helps training) |
| B Collusion | Does CMU+SGE find groups that TCN-only misses? Benign-collaboration false alarms? | Synthetic | Fig. 2; recall and time-to-detect vs group size; risk vs time; group confusion matrix | ✅ |
| C Latency | Per-stage latency (Eqs. 31–32), end-to-end p50/p95/p99 from traces, exposure (Eq. 33), SLO (Eq. 34) | Live stack, generated load | Fig. 3 individual stacked breakdown vs load; Fig. 4 collective breakdown | ✅ Needs tracing (trace id + per-stage timestamps); one host, so one clock |
| D Throughput / scale | Events/s, lag, queue depth, CPU/memory vs users, rate, partitions, replicas; Eq. 24/28 predicted vs observed | Live stack + in-process | Fig. 5, Fig. 8 | 🟡 Full stack limited by RAM (§2) |
| E Compute budget | Σ Cu ≤ B; workload saved vs recall lost for 4 strategies | Synthetic replay | Fig. 6, Fig. 7, admitted vs budget | ✅ |
| F Ablation | Contribution of each path, CMU/SGE split, admission control | Synthetic replay | Fig. 9 + table | ✅ |
| G Enforcement | 6 actions, Eq. 21 correctness, propagation delay, requests before enforcement, TTL expiry, fail-open/closed, ψ ordering, policy precedence | Live stack | Table + latency chart | ✅ after Phase 7 |
| H Faults | Table V failure modes: recovery time, loss, duplicates, consistency | Live stack, fault injection | Fig. 10 + table | 🟡 "Task migration" = consumer-group rebalance (no Flink) |
| Comparison | Method A / Method B tables per criterion | — | Fig. 11 | 🟡 Depends on paper access |

**Statistics.** Model experiments use 5 seeds (mean ± std, or 95 % bootstrap CI). System experiments use ≥ 3
runs per configuration. End-to-end percentiles come from per-event traces, never by summing stage percentiles.

---

## 9. Roadmap

Each phase ends with:
- tests run and passed,
- measurements collected (if any),
- remaining issues,
- an entry in `docs/PROGRESS.md`.

| Phase | Work | Acceptance evidence |
|---|---|---|
| 2 Infrastructure / events | Schema v2 (ν, ti, tenant-namespaced r*, vpolicy, vschema) with compatibility; event-time watermark + δlate; 4 late-event policies; two-stage shard-merge on `(r*, b, hash(u) mod Jr)`; Kafka transactions for Kafka-to-Kafka stages (D1); checkpointed keyed state; HDFS hourly aggregates + resource-participant summaries; per-stage trace timestamps for Exp. C | Unit tests for ordering, lateness policies, dedup, shard-merge = exact count; crash/rebalance tests: state = committed offsets |
| 3 TCN v3 | Table III features (d = 43) + zu(t); 6 × 2-conv residual blocks, R = 127, L = 128; multi-task heads, Focal on Lsup only; chronological split with gap g ≥ L, rolling origin; quantile thresholds θlow/θhigh/θB on nominal held-out data; registry v2, live worker | RF = 127 asserted; leakage tests; training report; live scoring parity |
| 4 CMU | Memory store (dm = 100), message function, time encoder, GRU updater for user and resource nodes on **every** event; single-writer per key; snapshot publication + bounded staleness metric; checkpoint/restore | Every event updates two memories; idempotent on duplicates; restore = pre-crash state |
| 5 Escalation + PriorityAdmit | Cost Cu (Eq. 26) with c0, c1, c2 **fitted from measured SGE time**; budget Eq. 29; Algorithm 1 with overflow semantics; demand/provisioning metrics (Eqs. 25, 27, 28) | Property tests: Σ Cu ≤ B; each user at most once; priority order; overflow actions |
| 6 SGE + collective scoring | Temporal attention over memories, 10 neighbours, 1 hop; decoder ϵe; HW (Eqs. 15–17) with ∆c, hmin, dcap, nmin, ns_min, δmin; Au, Nov, Scoll (Eqs. 18–19); training per [23] hyperparameters | Unit tests per equation; no user-user edges from application membership |
| 7 PDP v3 | Six actions, Eq. 21 order, θB/θC/θA calibrated; ψ triple + gateway stale rejection; TTL + sensitivity fallback; gateway StepUp (re-authentication), Throttle (rate limit), Restrict (sensitive-resource deny), Terminate (session revoke) | Tests for each action, ordering, policy precedence, ψ, fallback |
| 8 Datasets + baselines | Generator v3 scenarios; CERT loader (if D3); LSTM, IF, reference-TCN, static GCN, D3 models; frozen splits with hashes | Dataset cards; split-leakage checks |
| 9 Model evaluation | Experiments A, B (5 seeds) | Raw CSV/JSON + figures + reports |
| 10 Systems evaluation | Experiments C, D, E, F, G, H | Raw traces + figures + reports |
| 11 Reference comparison | Retrieve papers; Method A / B tables with page citations | `evaluation/references/` |
| 12 Deliverable 4 | §V Experimental Evaluation draft, figure set, limitations, untested-requirements list | `evaluation/reports/DELIVERABLE4_EVALUATION.md` |

**Status (2026-11-16): Phases 8–12 done** (`docs/PROGRESS.md`, v3 Phase 8). Deviations from the plan:
- The report is `docs/D4_EVALUATION.md`, not `evaluation/reports/`.
- Figures and tables are in `evaluation/results/{figures,tables}/`.
- There is no `run_all.py`: `evaluation/run_live.sh` runs the live parts, and D4 §M lists every command.
- CERT r4.2 was not used (decision D3 is still open), so all model results are synthetic.
- The static GCN and the D3 TGN were compared only on the D3 collusion dataset.
- Phase 11: no reference paper's numbers were verified, so every quantitative comparison cell is Not Reported or
  Not Comparable (`evaluation/references/REFERENCES.md`).

Planned layout (adapted to the repo; `experiments/results/` keeps the D3 results):

```
evaluation/
  env_info.py            # done (Phase 1)
  configs/ datasets/ baselines/ experiments/<A..H>/ results/{raw,processed}/ figures/ reports/ references/
  plotting.py            # one shared matplotlib style, 300-dpi PNG + PDF
  run_all.py
  README.md
```

---

## 10. Known risks

- **Memory**: the full v3 stack adds a CMU service and larger TCN models to a machine with ~0.7 GB RAM headroom.
  Experiments may need the dashboard and other non-essential services stopped; this will be documented per run.
- **Single machine**: no network latency between services; latency results are a lower bound for a distributed
  deployment.
- **Synthetic data**: detection results show behaviour on the scenarios we modelled, not real-world accuracy.
- **Timer-context inputs** (ThreatFeed, DevPosture) have no real source; they will be simulated with logged
  histories, as v3 Limitation 4 assumes.
- **Scope**: this is a multi-week research programme. Phases will be delivered and verified one at a time.
