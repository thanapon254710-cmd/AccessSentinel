# AccessSentinel — Phase 1: Repository Audit and Implementation Plan

Date: 2026-11-11 · Branch: `phase1/audit-and-plan` · Specification: *Deliverable 3.pdf* (D3), read in full (Sections I–III, Steps 1–10, Eqs. 1–12).

Status vocabulary used throughout this document:

| Label | Meaning |
|---|---|
| **VERIFIED** | Code exists and was executed on this machine during the audit |
| **TESTED-WITH-FAKES** | Code exists and passes unit tests against fake clients; never run against the real service |
| **STAND-IN** | In-process substitute for a D3 component (development only, not the real component) |
| **PARTIAL** | Exists but deviates from D3 or from the project requirements (see defects) |
| **MISSING** | Not implemented |

---

## 1. Summary of the current repository

A single Python package (`accesssentinel/`, ~1,600 lines) written as a **library-level reference implementation of D3 Steps 1–10**, plus a demo script and unit tests. Two commits on `main` (`Initial commit`, `First Demo`).

| Item | Finding |
|---|---|
| Language | Python 3.10+ only. No JavaScript/TypeScript, no web framework, no database code |
| ML stack | PyTorch (CPU), NumPy, pandas. No PyTorch Geometric (TGN is a dense implementation) |
| Infra clients | `confluent-kafka`, `pyarrow`, `hdfs` (WebHDFS) — used only by the two adapters |
| Deployment | `deploy/docker-compose.yml` with Kafka 3.9 (KRaft) + Hadoop 3 namenode/datanode. Marked untested by its author |
| Runtime model | Everything runs in **one Python process**: `run_demo.py` simulates events, trains both models, then replays the second half of the synthetic log through `AccessSentinel.run()` |
| Configuration | Dataclasses in `config.py`; every value tagged `[D3]` (from the paper) or `[IMPL]` (implementation choice). No environment-variable configuration |
| Data | Synthetic only (`simulator.py`): 120 users, 3 apps × 25 resources, 4 days, individually-anomalous attackers, colluding groups, benign flash crowds |
| Tests | 17 unit tests (`tests/test_core.py`, `tests/test_adapters.py`) |

### Verification performed during this audit

| Check | Result |
|---|---|
| `pytest -q tests` (Python 3.11, torch 2.15 CPU, in project `.venv`) | **17 passed** in 25 s |
| `python run_demo.py --quick` | See §1.1 |
| Docker Desktop | Installed (28.3.3) but **engine stopped** — compose file could not be tested |
| Local toolchain | Python 3.11, Node 24.18 / npm 11.16, Java 17, Git 2.46. No conda |
| Host resources | 16 GB RAM, 20 logical cores. Free RAM was ~0.3 GB at audit time → the Docker stack must be memory-budgeted (§4.6) |

### 1.1 Quick demo result on this machine (`run_demo.py --quick`, CPU, 53 s, synthetic data)

| Measure | Value |
|---|---|
| Events | 13,178 (6,664 train / 6,514 test), 120 users |
| TCN (1 fold, 6 epochs) | fold val AUC 0.999, "held-out" test AUC 0.999 — **inflated, see D1/D2** |
| TGN (15 epochs) | test edge AUC 0.984 (chronological split) |
| Pipeline on unseen 2nd half | 4/4 test attackers blocked (0.4–9.7 min after attack start); 8/8 colluders blocked (6 via collective score, 2 via individual score) |
| User-level | precision 0.857, recall 1.0, F1 0.923 (12 TP, 2 FP: `u056`, `u065`, 0 FN) |
| Escalations (1,436 windows) | high 208, resource 282, sweep 120, sampling 1,208; 1,422 TGN calls; 1.27 escalated users/window of 120 |

Interpretation: this confirms that the **code runs end to end and the four escalation paths fire**. Because of D1 (window-level split leakage) and D2 (trivially separable synthetic attackers), these figures are **not evidence that the method works** and must not be reported as research results. The full-mode log committed in `demo_output_example.log` was produced on another machine and is not reproduced here.

---

## 2. Already-implemented components

| D3 component / step | Module | Status | Notes |
|---|---|---|---|
| Step 1 event tuple `e_t=(u,t,a,r,s,p,o,c,y)` | `events.py` | VERIFIED | `eid` int helper; no UUID, schema version or correlation id yet |
| Step 2 Gateway (first-line ACL, forward every event, request- and session-time enforcement) | `gateway.py` | VERIFIED (library only) | In-process class. No HTTP proxy, authentication, real sessions, or protected apps |
| Step 2 Kafka user-keyed view + resource-keyed view with `(resource, time-bucket)` composite key | `broker.py` | STAND-IN | In-memory partitioned topics and consumer groups |
| Real Kafka adapter | `adapters/kafka_broker.py` | TESTED-WITH-FAKES | Never run against a live broker |
| Step 3 Historical store | `storage.py` | STAND-IN | Local hour-partitioned JSONL |
| Real HDFS adapter (WebHDFS + Parquet, compaction) | `adapters/hdfs_store.py` | TESTED-WITH-FAKES | Never run against a live cluster |
| Step 4 features d=50, sliding window L=20, stateless scorer | `features.py`, `state_store.py` | VERIFIED | Window state is in-process (not shared across workers) |
| Step 4 TCN (10 causal dilated layers, D3 dilations/filters, spatial 0.3 / dense 0.5 dropout, Focal Loss, Adam 1e-3, batch 512, 10% test + 5-fold CV) | `tcn.py` | VERIFIED | Matches D3 architecture. Platt calibration added `[IMPL]`. No checkpoint save/load |
| Step 5 three tiers (0.4/0.6) + medium-tier re-scoring | `risk.py` | PARTIAL | `t_recheck = 10.0` **seconds**; D3 states a 10-minute wall-clock interval |
| Step 6 four-path escalation, union, real-time vs background split | `escalation.py`, `pipeline.py` | VERIFIED | Path A, B (κ, Δ_res=15 min), C (T_sweep=2 h from history store), D (γ sampling) all present |
| Step 7 graph: user/resource/app nodes; access, membership, co-access edges; bounded neighbourhood | `graph.py` | VERIFIED | Built from an in-process deque of recent events |
| Step 7 TGN (2 GCN layers, emb 50, GRU, attention T=10, Adam 1e-3, wd 1e-6, edge score σ(W[h_i;h_j]+b), chronological 50/50 split) | `tgn.py` | PARTIAL | Architecture follows D3/[14]. Training is supervised-only (D3 requires self-supervised stage 1 + fine-tuning stage 2) |
| Step 8 PDP `D_u = h(S_ind, S_coll, C)`, group-wide block, decision cache | `pdp.py` | PARTIAL | Blocks are not sticky (see defect D3) |
| Step 9 feedback loop, analyst-verified-only labels | `feedback.py` | PARTIAL | In-memory only; analyst labels are not persisted; no retraining pipeline |
| Step 10 capacity equations (8)–(12) | `scaling.py` | VERIFIED | Pure functions |
| Synthetic data with ground truth | `simulator.py` | VERIFIED | Labels for individual anomaly and collusion per event |
| Metrics (ROC-AUC, P/R/F1) | `metrics.py` | VERIFIED | No PR-AUC, confusion matrix, FPR or latency yet |

**Conclusion:** the *algorithmic core* of D3 exists and is a sound base to keep. What exists is a **simulation of the architecture inside one process**, not a deployed event-driven system.

---

## 3. Defects and scientific risks in the existing code

These must be fixed before any result is reported. Each is tied to the phase that fixes it.

| # | Severity | Finding | Evidence | Fix in |
|---|---|---|---|---|
| D1 | **High (validity)** | **TCN train/test leakage.** `make_splits()` randomly permutes *windows*. Consecutive windows of one user share 19 of 20 events, so near-duplicate windows appear in train, validation and test. The reported CV/test AUC (≈0.999–1.0) is inflated | `tcn.py: make_splits`, `simulator.py: tcn_dataset` | Phase 5: split by time **and** hold out users/attackers |
| D2 | **High (validity)** | **Synthetic attacks are trivially separable.** Attackers always use a new `172.x` IP prefix and device ids ≥5000, never used by normal users. A single context feature separates them. The demo's 1.0 precision/recall therefore does not show the method works | `simulator.py: attack()` | Phase 5/10: add insider-misuse (home IP/device), low-and-slow, and cross-app scenarios; add rule baselines |
| D3 | **High (security)** | **Blocks are not sticky.** `PDP.decide()` recomputes from the latest score; a blocked user returns to Allow as soon as their next TCN score drops below 0.6 or the collective score passes its TTL. A blocked user's denied requests are still scored, so blocks can flap. A unit test asserts the auto-unblock | `pdp.py: decide`, `tests/test_core.py::test_pdp_individual_collective_and_policy` | Phase 8: explicit block lifecycle (expiry, analyst release, override) — ML unavailability never releases a block |
| D4 | High | **Kafka delivery semantics mis-stated.** Consumers use `enable.auto.commit=True` with default offset storing, which can commit an offset before processing finishes (at-most-once on crash, not at-least-once as the README says). No retries, no DLQ, a malformed message crashes the consumer, no dedup | `adapters/kafka_broker.py` | Phase 4: manual commit after processing, DLQ topic, idempotent consumers keyed by `event_id` |
| D5 | Medium | **`T_recheck` is 10 s instead of 10 min.** README gap #2 predates D3's final wording ("wall-clock interval, initialized to 10 minutes") | `config.py: RiskConfig.t_recheck` | Phase 2 (config) |
| D6 | Medium | **Single-process pipeline.** Storage, TCN and graph consumer groups are polled sequentially in one loop; window state and the graph's recent-event buffer live in process memory, so multiple TCN workers or a restart lose state | `pipeline.py: drain` | Phases 4–7: separate services, Redis state |
| D7 | Medium | **Compose file will not work for containerised services.** Kafka advertises only `localhost:9092`; HDFS writes require editing the Windows hosts file (admin rights); no volumes (data lost on restart); no health checks | `deploy/docker-compose.yml` | Phase 2 |
| D8 | Medium | **Collusion scoring is gated by a rule.** `S_coll` is only computed for connected components of users sharing ≥2 sensitive resources (`groups_from_graph`). Without a rule-only baseline, it is unclear whether the TGN adds detection power beyond this rule | `tgn.py: group_scores` | Phase 7: report TGN vs rule baseline; consider learned grouping |
| D9 | Medium | **TGN training is supervised only.** D3 Step 7 specifies a self-supervised stage on benign history, then fine-tuning on analyst-verified incidents | `tgn.py: train_tgn` | Phase 7 |
| D10 | Medium | **No model persistence or versioning.** Models exist only in the demo process | `run_demo.py` | Phase 5 / 9 |
| D11 | Low | Resource identity is hashed into 16 buckets (75 synthetic resources collide); time-of-day uses synthetic seconds with no timezone handling for live events | `features.py` | Phase 5 |
| D12 | Low | HDFS layout is `hour=` only (requirement: by date and application); feedback rows written as one JSON blob; `FeedbackLoop.flush()` does not clear records (re-flush duplicates) | `adapters/hdfs_store.py`, `feedback.py` | Phase 4 / 9 |
| D13 | Low | Escalation has no global TGN workload cap or priority queue (only the sweep batch is bounded) | `escalation.py` | Phase 6 |
| D14 | Low | Platt calibration emits `requires_grad` and `exp` overflow warnings | `tcn.py: fit_platt, predict` | Phase 5 |

---

## 4. Missing components

| Area | Missing |
|---|---|
| Gateway | HTTP reverse proxy (FastAPI), login, password hashing, session cookies, session store, per-request decision-cache lookup, session revocation endpoint, audit log |
| Protected applications | No demo web applications. Need ≥2 apps with a shared identity and resources of different sensitivity |
| Kafka pipeline | Live-tested producers/consumers, topic provisioning, manual offsets, retries, DLQ, dedup, repartitioner service for the resource-keyed view, score/decision/feedback topics |
| HDFS | Live-tested sink service, date/application partitioning, retrieval API for user history, storage of scores/decisions/feedback, training-set export |
| State | Redis-backed user windows, risk tiers, decision cache, sessions, Path B resource windows, temporal edge index for TGN neighbourhoods |
| Operational DB | PostgreSQL schema: users, apps, resource catalog, ACL rules, incidents, analyst feedback, decision audit, model registry, settings |
| ML lifecycle | Standalone train/evaluate CLIs, checkpoints, model registry, versioned deployment, retraining from analyst-verified HDFS data, TGN two-stage training |
| Services | TCN worker, escalation scheduler (wall-clock), TGN worker, PDP service, dashboard API (REST + WebSocket) |
| Dashboard | Entire React/TypeScript frontend (7 pages) and analyst authentication |
| Evaluation | Leak-free splits, PR-AUC, confusion matrices, FPR, latency, throughput, consumer lag, worker scaling, per-path escalation effectiveness, rule baselines, experiment runner |
| Security | API auth/authorisation, secrets via env, input validation, privacy-conscious logging, health checks, backpressure |
| Docs | Windows setup guide, progress log, experiment protocol |

---

## 5. Architecture and dependency plan

### 5.1 Guiding decision: keep the library, wrap it in services

The existing `accesssentinel` package stays the **single source of algorithmic truth** (features, TCN, escalation rules, graph construction, TGN, PDP logic). New code adds:

1. **Adapters** for real state (Redis, PostgreSQL) next to the existing Kafka/HDFS adapters.
2. **Thin service entry points** (`python -m accesssentinel.services.<name>`) that wire adapters to library logic.
3. **One Docker image** for all Python services, started with different commands — avoids duplicate builds and keeps RAM low.

The in-process `broker.py` / `storage.py` stand-ins are kept **only** for unit tests and the offline experiment runner, and are labelled development-only.

### 5.2 Runtime topology

```
Browser ──► Gateway (FastAPI :8000) ──► Protected apps (HR :8101, Finance :8102, Docs :8103)
               │  ▲ reads decision cache + sessions (Redis), ACL (Postgres, cached)
               │  │
               ▼  │
        Kafka: access-events (key = u)
          ├─► hdfs-sink ──────────────► HDFS /accesssentinel/events/date=…/app=…/*.parquet
          ├─► repartitioner ──► access-events-by-resource (key = a:r#bucket)
          │                        └─► graph-builder ─► Redis temporal edge index + Path B windows
          └─► tcn-worker ×N (group "tcn", Redis windows) ─► risk-scores (key = u)
                                                             ├─► pdp
                                                             └─► escalation (Paths A–D, wall-clock)
                                                                    └─► escalations.realtime / .background
                                                                           └─► tgn-worker ─► group-scores ─► pdp
        pdp ─► Redis decision cache + session revocation ─► pdp-decisions ─► hdfs-sink, api
        api (FastAPI :8080, REST + WebSocket) ◄─► Postgres / Redis / Kafka consumer "dashboard"
        dashboard (React + TS, :5173 dev / nginx :3000)
        analyst feedback ─► Postgres + feedback topic ─► HDFS ─► retraining CLI ─► model registry
```

### 5.3 Kafka topics

| Topic | Key | Producer | Consumer groups |
|---|---|---|---|
| `access-events` | `u` | gateway | `hdfs-sink`, `tcn`, `repartitioner`, `dashboard` |
| `access-events-by-resource` | `a:r#⌊t/bucket⌋` | repartitioner | `graph-builder` |
| `risk-scores` | `u` | tcn-worker | `pdp`, `escalation`, `hdfs-sink`, `dashboard` |
| `escalations.realtime` / `escalations.background` | `u` | escalation | `tgn` (real-time consumed first) |
| `group-scores` | group id | tgn-worker | `pdp`, `hdfs-sink`, `dashboard` |
| `pdp-decisions` | `u` | pdp | `hdfs-sink`, `dashboard` |
| `analyst-feedback` | incident id | api | `hdfs-sink` |
| `<topic>.dlq` | original | any consumer | manual inspection |

Delivery: idempotent producer; consumers commit offsets **after** processing (at-least-once) and dedup on `event_id` (Redis `SET NX` with TTL). Ordering is assumed only within a partition.

### 5.4 State placement

| State | Store | Reason |
|---|---|---|
| User window `X_u` (20×50 float32), last timestamp, first-seen resources | Redis | D3: windows live in the state store so TCN workers stay stateless |
| Risk score / tier per user, medium-tier recheck schedule | Redis (sorted set by due time) | Shared by tcn-worker, escalation, pdp |
| Path B: distinct users per sensitive resource in Δ_res | Redis sorted sets | Sliding window by timestamp |
| Temporal edge index (recent user→resource accesses) | Redis sorted sets, bounded retention | TGN neighbourhood construction across workers |
| Decision cache, block lifecycle, sessions | Redis (+ audit copy in Postgres) | Gateway reads per request without waiting for Kafka/ML |
| Users, apps, resource catalog, ACL, incidents, feedback, model registry, settings | PostgreSQL | Durable relational state |
| Raw events, scores, decisions, feedback history | HDFS (Parquet) | D3 Step 3 historical store and training source |

### 5.5 Time semantics `[IMPL]`

- **Live mode:** wall-clock time. `T_recheck` = 10 min, `Δ_res` = 15 min, `T_sweep` = 2 h, as in D3.
- **Replay/experiment mode:** a replay tool feeds historical or synthetic events through the same Kafka topics with an injected clock, so experiments are reproducible and can compress days into minutes.
The library already takes `now` as an argument, so both modes share the same logic.

### 5.6 Dependencies

| Layer | Choice | Note |
|---|---|---|
| Kafka | `apache/kafka:3.9.0` KRaft, dual listeners (`kafka:29092` internal, `localhost:9092` host) | Reuses existing image |
| HDFS | `apache/hadoop:3` namenode + datanode, named volumes | Reuses existing image; services reach HDFS inside the Docker network so no hosts-file edit is needed (host access stays optional) |
| PostgreSQL | `postgres:16-alpine` | |
| Redis | `redis:7-alpine` (AOF on) | |
| Python | FastAPI, uvicorn, pydantic-settings, SQLAlchemy 2 + psycopg 3, redis-py, confluent-kafka, pyarrow, hdfs, passlib[argon2] or bcrypt, PyJWT, torch (CPU), numpy, pandas, scikit-learn (metrics only) | |
| Graph ML | Keep the existing dense PyTorch TGN (bounded neighbourhoods make it adequate). PyTorch Geometric is **not** added unless needed | Avoids a heavy dependency on Windows |
| Frontend | React 18 + TypeScript + Vite, a chart library, a graph-visualisation library (e.g. Cytoscape.js) | Node 24 already installed |
| Monitoring | Metrics collected by services into Redis/Postgres and read by the API; no Prometheus/Grafana | Keeps the stack small |

### 5.7 Memory budget (16 GB host)

Target ≤ 7 GB for Docker: Kafka ~1 GB, HDFS ~1.5 GB, Postgres + Redis ~0.4 GB, torch services (2× tcn, 1× tgn) ~1.5 GB, other Python services ~1 GB, dashboard ~0.1 GB. JVM heaps will be capped in compose. A `core` compose profile will allow running infra + one TCN worker on low memory.

---

## 6. Proposed folder structure

Existing files keep their paths; new items are marked `+`.

```
AccessSentinel/
├─ accesssentinel/                 # existing algorithmic core (kept)
│  ├─ events.py features.py state_store.py tcn.py risk.py escalation.py
│  ├─ graph.py tgn.py pdp.py feedback.py scaling.py metrics.py simulator.py
│  ├─ broker.py storage.py         # in-process stand-ins: tests/replay only
│  ├─ config.py                    # D3/IMPL hyper-parameters (kept)
│  ├─ + settings.py                # environment-based runtime settings
│  ├─ adapters/
│  │  ├─ kafka_broker.py hdfs_store.py   # existing
│  │  ├─ + redis_state.py          # windows, tiers, decision cache, sessions, edge index
│  │  └─ + db.py / + models_db.py  # SQLAlchemy models + session
│  ├─ + services/                  # one entry point per deployable process
│  │  ├─ gateway/ apps/ repartitioner.py hdfs_sink.py tcn_worker.py
│  │  ├─ graph_builder.py escalation_service.py tgn_worker.py pdp_service.py
│  │  └─ api/                      # dashboard backend (REST + WebSocket)
│  └─ + ml/                        # train_tcn.py train_tgn.py evaluate.py registry.py datasets.py
├─ + dashboard/                    # React + TypeScript (Vite)
├─ + experiments/                  # experiment configs + runner; results written to experiments/results/
├─ deploy/
│  ├─ docker-compose.yml hadoop.env   # existing, to be fixed
│  ├─ + Dockerfile                 # single image for all Python services
│  └─ + init/                      # topic creation, Postgres schema/seed
├─ + scripts/                      # check_infra.py, seed_demo.py, replay.py
├─ tests/                          # existing unit tests (kept)
│  ├─ + integration/               # require running Docker stack (pytest -m integration)
├─ + docs/                         # PHASE1_AUDIT.md, PROGRESS.md, SETUP_WINDOWS.md, ASSUMPTIONS.md
├─ + .env.example
├─ run_demo.py README.md requirements.txt
```

---

## 7. Prioritised implementation roadmap

Each phase ends with automated tests and an entry in `docs/PROGRESS.md`. A phase is "done" only when its acceptance check has been executed.

| Phase | Deliverable | Acceptance check |
|---|---|---|
| **2 Infrastructure** | Fixed compose (Kafka dual listeners, HDFS in-network, Postgres, Redis, volumes, health checks, memory caps), topic provisioning, `.env.example`, `settings.py`, Dockerfile, `scripts/check_infra.py`, Windows setup guide | `check_infra.py` produces/consumes on Kafka, writes/reads Parquet on HDFS, round-trips Postgres and Redis — all against live containers |
| **3 Gateway + apps** | Postgres schema + seed (users, apps, resources with sensitivity, ACL), FastAPI gateway with login/argon2/session cookies, reverse proxy to 3 demo apps, event `e_t` + `event_id`/`schema_version`/`correlation_id`, Kafka publish, decision-cache read, revocation hook | Log in as different users across apps; allowed and denied requests appear on `access-events`; integration tests |
| **4 Streaming + HDFS** | Hardened consumers (manual commit, retry, DLQ, dedup), repartitioner, hdfs-sink with `date=/app=` Parquet, history query API, compaction | Kill/restart consumers without loss or duplicates in HDFS; resource topic co-locates same resource+bucket |
| **5 TCN** | Leak-free dataset builder, harder synthetic scenarios, train/eval CLI, checkpoints + registry, Redis-backed tcn-worker (N replicas), medium-tier recheck every 10 min | Reported metrics on user- and time-disjoint test set; 2 workers process disjoint users with shared state; model version visible in scores |
| **6 Escalation** | Escalation service: Paths A–D with reasons, union/dedup, priority queues, workload cap | Scripted scenarios trigger each path independently; counts recorded per path |
| **7 TGN** | Neighbourhood builder over Redis edge index + HDFS, two-stage training (self-supervised link prediction on benign history → fine-tune on verified incidents), tgn-worker, rule baseline | TGN vs rule baseline metrics on held-out collusion scenarios, including cross-application |
| **8 PDP + enforcement** | PDP service with sticky block lifecycle, expiry, analyst release/override, group-wide enforcement, session revocation, decision audit | Blocked user stays blocked when scores drop or ML is down; active session terminated without a new request |
| **9 Feedback + dashboard** | API (auth, REST, WebSocket), 7 dashboard pages, incident review, feedback → Postgres + HDFS, retraining pipeline from verified labels only | Analyst confirms an incident in the UI; it appears in HDFS and in the next retraining dataset |
| **10 Validation** | Experiment runner: TCN/TGN metrics, throughput, end-to-end latency, consumer lag, worker scaling, per-path effectiveness; results labelled synthetic vs real | Reproducible run from a single command with stored configs and seeds |

---

## 8. First concrete code changes (start of Phase 2)

1. **`deploy/docker-compose.yml`** — Kafka with `PLAINTEXT://kafka:29092` (internal) and `PLAINTEXT_HOST://localhost:9092` (host); HDFS namenode/datanode reachable as `namenode:9870` / `datanode:9864` from the Docker network; add `postgres`, `redis`, `kafka-init` (topic creation); named volumes; health checks; JVM heap caps.
2. **`.env.example`** + **`accesssentinel/settings.py`** (pydantic-settings) — all hosts, ports, credentials and secrets from the environment; no defaults for secrets.
3. **`requirements.txt` split** into `requirements/core.txt`, `services.txt`, `dev.txt` (adds FastAPI, SQLAlchemy, psycopg, redis, pydantic-settings, pytest).
4. **`deploy/Dockerfile`** — one CPU-torch Python image for all services.
5. **`scripts/check_infra.py`** + **`tests/integration/test_infra.py`** (`pytest -m integration`) — live round-trip against Kafka, HDFS, Postgres, Redis, using the existing `KafkaDualViewBroker` and `WebHdfsStore` adapters (their first live run).
6. **`config.py`** — `RiskConfig.t_recheck = 600.0` (10 min, D3 Step 5) and update README gap #2.
7. **`docs/SETUP_WINDOWS.md`** — Docker Desktop (WSL 2) memory settings, start/stop commands, ports.

No algorithmic module is restructured in Phase 2.

---

## 9. Assumptions and decisions needing team confirmation

| # | Topic | Proposed default |
|---|---|---|
| A1 | Dataset | No access-control dataset with user–resource sequences is in the repository. Primary: extended synthetic generator with explicit labels. Optional external check: map CERT Insider Threat r4.2/r6.2 (logon/file/http per user) to the event schema — it is itself synthetic, and would be labelled as such |
| A2 | Gateway behaviour when Redis (decision cache) is unreachable | First-line ACL is always enforced. Sensitive (`s=1`) resources fail closed; non-sensitive fail open with an alert. A cached Block is never released by ML unavailability |
| A3 | Block lifecycle | Individual block lasts until analyst release or a configurable expiry (default 24 h); group block applies to all members of `g(u)`; analyst overrides are audited |
| A4 | θ_coll | 0.6 initial (D3 gives none); calibrated on held-out data in Phase 7 |
| A5 | TGN self-supervised objective | Temporal link prediction with negative sampling on benign windows (sampling path supplies the benign baseline per D3) |
| A6 | Sweep selection rule (Path C) | Keep the existing `[IMPL]` rule (sensitive resources touched by ≥4 distinct users over T_sweep) and add a per-user slow-accumulation rule; both documented |
| A7 | Demo applications | HR portal, Finance portal, Document store — shared identity, mixed-sensitivity resources, role-based ACL |
| A8 | Model–service integration | Dense PyTorch TGN kept; no PyTorch Geometric |
