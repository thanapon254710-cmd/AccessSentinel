# AccessSentinel — Implementation Progress Log

Running record of completed work, test results and remaining work. Status labels are defined in `docs/PHASE1_AUDIT.md`.

## Phase status

| Phase | Status |
|---|---|
| 1 Audit and architecture | Done (2026-11-11) |
| 2 Infrastructure | Done (2026-11-11) — verified live |
| 3 Gateway + protected apps | Done (2026-11-11) — verified live |
| 4 Kafka streaming + HDFS | Done (2026-11-11) — verified live incl. crash tests |
| 5 TCN | Done (2026-11-11) — trained, evaluated (synthetic), deployed, verified live |
| 6 Four-path escalation | Done (2026-11-11) — evaluated (synthetic), deployed, each path verified live |
| 7 TGN | Done (2026-11-11) — two-stage TGN trained and evaluated (synthetic), deployed, live parity verified |
| 8 PDP + enforcement | Not started (library logic exists; see audit D3) |
| 9 Feedback + dashboard | Not started |
| 10 Integration + experiments | Not started |

## Log

### 2026-11-11 — Phase 1

- Read *Deliverable 3.pdf* in full and every file in the repository.
- Created project `.venv` (inherits global torch 2.15 CPU) and installed `pytest`, `pyarrow`, `hdfs`, `confluent-kafka`.
- `pytest -q tests` → **17 passed**.
- `run_demo.py --quick` → completed in 53 s; 12/12 synthetic malicious users blocked, 2 false-positive users (P 0.857, R 1.0). Synthetic only and inflated by audit defects D1/D2 — not a research result.
- Docker Desktop installed but engine stopped: compose stack **not tested**.
- Wrote `docs/PHASE1_AUDIT.md` (inventory, 14 defects, architecture, roadmap, first changes).

### 2026-11-11 — Phase 2 (infrastructure)

Added / changed:
- `deploy/docker-compose.yml` rewritten: Kafka 3.9 KRaft with internal (`kafka:29092`) and host (`localhost:9092`)
  listeners, auto-topic-creation off; HDFS 3.4.1 NameNode + DataNode running as the non-root `hadoop` user;
  PostgreSQL 16.4; Redis 7.4.1 (password, AOF, `noeviction`); named volumes; health checks; memory caps
  (3.4 GB total); `kafka-init` and `tools` one-shot services. Audit defect **D7 fixed**.
- `deploy/Dockerfile` (one CPU-torch image for all Python services, non-root user), `.dockerignore`.
- `.env.example` (no real secrets; `COMPOSE_FILE` so `docker compose` works from the repo root);
  local `.env` generated with random passwords (git-ignored).
- `accesssentinel/settings.py` (pydantic-settings, `AS_*` env vars, secrets required + masked),
  `accesssentinel/topics.py` (single topic registry: 8 topics + 8 DLQs), `accesssentinel/provision.py`
  (idempotent topic creation).
- `requirements/` split into core / services / dev; root `requirements.txt` kept as an alias.
- `config.py`: `T_recheck` = 600 s per D3 Step 5. Audit defect **D5 fixed**.
- `adapters/kafka_broker.py`: deployment topic names without prefix; topic creation via `provision.py`.
- `adapters/hdfs_store.py`: `read_range` lists existing hour partitions once instead of one WebHDFS call per
  hour in the range (found live: a wide range never finished). Regression test added.
- `scripts/check_infra.py`, `tests/test_settings_topics.py`, `tests/integration/test_infra.py`, `docs/SETUP_WINDOWS.md`.

Verified (all executed on this machine):
- `pytest -q tests` → **25 passed, 4 skipped** (skipped = live tests, opt-in).
- `docker compose run --rm tools` → **4/4 PASS**: Kafka (16 topics × 8 partitions; 40-event round trip through
  `KafkaDualViewBroker`, per-user partition + order and resource-view co-location verified), HDFS (Parquet
  write/read over 3 hour partitions, compaction, feedback write through `WebHdfsStore`), PostgreSQL, Redis.
  First live run of the two adapters written before Phase 1.
- Host-side `AS_INTEGRATION=1 pytest tests/integration -k "not hdfs"` → 3 passed.
- `docker compose down` + `up`: HDFS directory and Redis key survived, NameNode did not reformat, `kafka-init`
  reported all 16 topics `exists`.
- Idle memory: Kafka 400 MB, NameNode 379 MB, DataNode 308 MB, PostgreSQL 18 MB, Redis 4 MB.

Not done / known limits:
- HDFS from host Python needs a hosts-file entry (`127.0.0.1 datanode`); in-network access needs none.
- `run_demo.py --kafka/--hdfs` (whole pipeline on live services) not run yet → Phase 4.
- Kafka consumer semantics (audit D4) unchanged → Phase 4.
- No PostgreSQL schema yet → Phase 3.

### 2026-11-11 — Phase 3 (gateway + protected applications)

Added:
- **Event schema v1** (`events.py`, `schema.py`): the nine D3 fields unchanged, plus `event_id`, `schema_version`,
  `correlation_id`, `meta` (non-feature audit details). Pydantic validation; fixed vocabularies
  o ∈ {view, edit, delete, export}, p ∈ {read, write, admin, export}. Old/simulator records still load.
- **PostgreSQL schema v1** (`db/models.py`): users, applications, resources (sensitivity s ∈ {0,1}), ACL rules,
  audit log, schema version. `db/init_db.py` creates it idempotently; `db/demo_catalog.py` + `--seed-demo`
  load 3 apps, 15 resources (9 sensitive), 7 roles, 63 ACL rules, 10 users (Argon2id, per-user salt).
- **Gateway** (`services/gateway/`): Argon2id login, lockout after 5 failures, Redis server-side sessions (only
  sha256 of the token stored; idle 30 min / absolute 8 h), role-based default-deny ACL, PDP decision-cache read
  (request-time enforcement), login refused while blocked, internal session-revocation API (session-time
  enforcement hook for Phase 8), reverse proxy with an internal upstream token, security headers, correlation
  IDs, readiness and internal stats endpoints, server-rendered login and application pages.
- **Non-blocking Kafka publisher** (`adapters/event_publisher.py`): idempotent producer, acks=all, background
  delivery thread, local JSONL spool when Kafka cannot accept an event.
- **Demo protected applications** (`services/demo_app.py`, deployed 3x): deterministic fake data,
  list/get/create/update/delete/CSV export; accept only requests carrying the gateway token; no host ports;
  receive only that token from `.env` (least privilege).
- Compose: `db-init`, `app-hr`, `app-finance`, `app-docs`, `gateway` (port 8000), `gateway-spool` volume.
- `scripts/smoke_gateway.py` (live end-to-end check), `tests/test_gateway.py` (26 tests).

Design decisions (implementation choices D3 leaves open):
- Request -> (r, o, p) mapping in `services/gateway/access.py`; p is the permission the operation requires.
- An unmapped path in a known app is denied (404) and recorded as `r = unmapped:<segment>`, `s = 1`.
- Unauthenticated requests produce **no** access event (D3 monitors post-authentication behaviour); failed
  logins go to the audit log.
- Audit assumption A2 resolved as **fail closed**: sessions and decisions both live in Redis, so if Redis is
  unreachable protected requests get 503. ML/Kafka unavailability never affects authorisation: the
  first-line ACL and cached Blocks are enforced regardless.
- A cached Block is lifted only by the PDP rewriting/deleting it or by its explicit `expires_at`.
- Context c = {ip, device (12-hex hash of User-Agent), session ("new" on the session's first request, else
  "active"), session_ref (hash)}. Raw User-Agent, cookies and tokens are never stored in events or logs.
- No Alembic yet: schema created with `create_all` + a version row; migrations come when a released schema changes.

Verified (all executed on this machine):
- `pytest -q tests` -> **51 passed, 4 skipped**.
- `scripts/smoke_gateway.py` against the live stack -> **29/29 PASS**: allowed, ACL-denied, unknown-resource,
  PDP-blocked, unblocked, revoked-session and anonymous requests across all three apps; each produced exactly
  the expected `access-events` record (or none), keyed by user, one partition per user; audit rows present.
- Demo apps reject direct in-network access and forged tokens (401).
- No password, token or session cookie appears in any service log.
- Kafka stopped: the gateway kept serving (9-15 ms per request); after the 30 s delivery timeout the 5 events
  were in the spool; publishing resumed after Kafka restarted.
- `docker compose run --rm tools` still 4/4. Idle memory: gateway 76 MB, each app 39 MB.

Not done / known limits:
- Spool replay into Kafka -> Phase 4.
- The demo apps have no authorisation of their own, by design (first-line enforcement is at the gateway).
- HTTPS is not configured (`AS_COOKIE_SECURE=false` in dev).
- The in-process `accesssentinel/gateway.py` (simulator model) is unchanged and separate from the service.

### 2026-11-11 — Phase 4 (Kafka streaming + HDFS history)

Added:
- **Reliable consumer framework** (`streaming/consumer.py`): manual offset commits only after the handler
  reports its work durable; per-record dead-letter queue (`<topic>.dlq`, headers: reason, source topic/
  partition/offset, group, time) whose writes must be acknowledged before the offset is committed; transient
  failures retried with exponential backoff (nothing committed meanwhile = backpressure); checkpoint-and-commit
  on rebalance, discard-and-reread if that fails; graceful SIGTERM; heartbeat-file health checks.
  Audit defect **D4 fixed**.
- **History lake** (`adapters/hdfs_lake.py`): Parquet on HDFS, `events/date=YYYY-MM-DD/app=<a>/` and
  `<dataset>/date=YYYY-MM-DD/` for risk-scores, group-scores, pdp-decisions, analyst-feedback; date/app
  partition pruning; user history; id-based dedup on read; compaction with dedup; drop-in for the pipeline's
  `HistoricalStore` interface (sweeps in Phase 6). `LocalFsClient` = labelled development-only fallback
  (`AS_HISTORY_BACKEND=local`). Audit defect **D12 (layout) fixed**.
- **HDFS sink** service (`streaming/hdfs_sink.py`, group `hdfs-sink`): independent of the real-time path;
  buffers and writes one file per partition at 2000 rows or 30 s; Redis marks of written ids (set only after
  the write) skip redelivered records; compacts partitions above 24 files.
- **Repartitioner** service (`streaming/repartitioner.py`, group `repartitioner`): `access-events` (key u) ->
  `access-events-by-resource` (key `app:resource#floor(t/900 s)`), payload unchanged, committed only after Kafka
  acknowledged the batch. Shared `broker.resource_key()` used by the in-process and Kafka brokers.
- **Spool replay** in the gateway (every 60 s): rename-then-replay, file deleted only when nothing is in flight.
- Tools: `python -m accesssentinel.streaming.lag` (consumer lag), `python -m accesssentinel.history`
  (partitions, user history, time range, records, compact, export to Parquet for training),
  `scripts/smoke_streaming.py` (traffic / verify / poison).
- `adapters/parquet_io.py`: single-threaded Parquet reads (see "Found while verifying").
- `tests/test_streaming.py` (22 tests).

Design decisions:
- Delivery: at-least-once everywhere, made effectively-once in HDFS by (1) skip-if-already-written marks in
  Redis and (2) id dedup on read and in compaction. The resource-keyed topic may carry a redelivered record
  after a crash; its consumers (Phases 6-7) drop it by event_id.
- Only schema-v1 events are accepted on the deployed `access-events` topic; anything else is dead-lettered.
- Other topics are stored generically (record_id, ts, key, JSON payload) until their schemas exist (Phases 5-9).
- Crash-consistency test hook `AS_FAULT_INJECT=crash-before-mark|crash-after-mark` (sink only; ignored when
  `AS_ENVIRONMENT=prod`).

Verified (all executed on this machine, inside the Docker network):
- `pytest -q tests` -> **73 passed, 4 skipped**.
- 120 gateway requests: **10/10** checks - lag 0; all 120 in HDFS exactly once in date/app partitions with
  matching (u, a, r, y); all 120 in the resource-keyed topic with key `app:resource#bucket`, one partition per key.
- Poison record on `access-events`: dead-lettered by both groups with reason + source offset; lag back to 0.
- Gateway replayed the 5 events spooled during the Phase 3 Kafka-outage test; spool empty afterwards.
- Timed SIGKILLs of sink and repartitioner (400 queued events): 400/400 exactly once. Note: the log showed both
  kills landed before the sink's first flush, so this did not exercise the write/commit window; hence:
- **Crash experiment A** (`crash-after-mark`, exit 137 after write + mark, before commit; 200 events): restart
  skipped all 200 redelivered records; 0 raw duplicates; 10/10.
- **Crash experiment B** (`crash-before-mark`, worst case; 200 events): 200 raw duplicate rows as predicted,
  history read still returned each event exactly once; compaction removed exactly the duplicates; 10/10.
- Scale-out: 2 repartitioner replicas split partitions 0-3 / 4-7.
- Regression: infra check 4/4, gateway smoke 29/29. Memory: hdfs-sink 88 MB, repartitioner 28 MB.

Found while verifying:
- Processes that read Parquet in the Linux image aborted at exit ("terminate called without an active
  exception", exit 134) in 30-50 % of runs with pyarrow 21, 22 and 26; `use_threads=False` reads: 0/40.
  All reads now go through `adapters/parquet_io.py`; history CLI 0/10 aborts afterwards.
- Unit tests caught: a lost DLQ write could have let its offset be committed (failed DLQ records are now
  re-sent before retrying); offsets of a batch could have been committed on shutdown while that batch was
  failing (offsets now advance only after the handler succeeds).

Not done / known limits:
- No Kafka transactions (exactly-once in Kafka itself); duplicates are handled by ids as described.
- Small-file compaction runs inside the sink for partitions it writes to; no scheduled compaction of past days.
- History reads fetch whole Parquet files over WebHDFS and filter in Python (adequate at prototype scale).
- HDFS replication factor is 1 (single DataNode, development cluster).

### 2026-11-11 — Phase 5 (TCN individual detection)

Added:
- **Synthetic dataset v2** (`datasets/synth.py`): shaped like the deployed catalog (3 apps, 15 resources,
  7 roles, real ACL), schema-v1 events, 260 users x 28 days = 132,881 events. Normal behaviour with hard
  negatives (overtime, VPN travel, laptop replacement, month-end finance exports, flash crowds). Individual
  attacks: compromised account, insider exfiltration (own device + office network), access-control probing,
  low-and-slow. Collusion: coordinated collection, split exfiltration (for Phase 7). Time splits
  (18 / 4 / 6 days), attacker users disjoint across splits, at most 25 % attackers per role, labels kept
  outside the events. Audit defect **D2 fixed** (insiders are no longer separable by network/device alone).
- **Behavioural features** (`ml/features.py`, d = 50): catalog one-hot (no hashing, audit **D11 fixed**) plus
  context relative to the user's own history (new network/device/resource, familiarity, deny rate, short-term
  rates, sensitive/export volume, sequential object ids, hour deviation, rate ratio, cold start).
  Profile = serialisable stream state; Preprocessor (vocab + scaler) fitted on the training period only.
- **Leak-free dataset** (`ml/dataset.py`): time-ordered online replay, train-only preprocessing, index-based
  window gathering (27 MB instead of ~500 MB). Audit defect **D1 fixed**.
- **Training + evaluation** (`ml/train_tcn.py`, `ml/evaluation.py`): D3 TCN unchanged; early stopping on
  validation PR-AUC; Platt calibration on validation; 5-fold user-grouped CV; test-period evaluation against a
  hand-written rule baseline and a last-event logistic-regression baseline; per-scenario, per-instance,
  user-day and latency metrics; JSON + Markdown report. `tcn.py` calibration warnings fixed (audit **D14**).
- **Model registry** (`ml/registry.py`): immutable versions with model, preprocessor and manifest (data
  fingerprint, config, git commit, test summary); atomic `CURRENT` promotion; `TcnScorer` can only be built
  from a saved trained model. Audit defect **D10 fixed**.
- **TCN worker** service (`streaming/tcn_worker.py`, 2 replicas): Redis-held profiles (stateless workers,
  audit **D6 fixed**), batched scoring, `risk-scores` records with model_version, tiers 0.4 / 0.6, Redis risk
  state, Medium-tier recheck queue (T_recheck = 600 s, live traffic only), hot model reload, refuses to consume
  without a trained model. StreamConsumer gained a `tick` hook.
- `scripts/replay_synthetic.py` (publish / verify / recheck-demo), `tests/test_tcn_pipeline.py` (12 tests).

Results — **synthetic data only** (`experiments/results/tcn-20261110T194520Z-beeb9b.md`; seed 7, wall time 37 min on a 20-thread CPU):

| Test period (unseen attackers) | ROC-AUC | PR-AUC | P@0.6 | R@0.6 | F1@0.6 | FPR@0.6 | attacks detected | normal user-days flagged |
|---|---|---|---|---|---|---|---|---|
| TCN | 0.990 | 0.917 | 0.636 | 0.922 | 0.753 | 0.017 | 13/13 | 5.8 % |
| Logistic regression (last event) | 0.990 | 0.886 | 0.683 | 0.835 | 0.751 | 0.013 | 12/13 | 9.0 % |
| Rule (untrained) | 0.944 | 0.634 | 0.973 | 0.128 | 0.226 | 0.000 | 6/13 | 0.2 % |

- 5-fold user-grouped CV (training period): ROC-AUC 0.998 ± 0.002, PR-AUC 0.961 ± 0.036.
- Recall @0.6 by attack: compromised account 0.997, access-control probing 0.917, insider exfiltration 0.910
  (logistic regression 0.658), low-and-slow 0.662 (logistic regression 0.730: the TCN's weakest case, which
  D3's periodic sweep, Path C, is meant to cover).
- **Distribution-shift finding:** 335 of the TCN's 450 false positives @0.6 are month-end finance exports
  (40 % of those benign events flagged). The 28-day window contains one month-end and it lies in the test
  period, so neither training nor validation contained that seasonal pattern. Reported as is; motivates the
  analyst-feedback/retraining loop (Phase 9) and longer training histories.
- The behavioural features carry most of the signal (a non-sequential model is close on ROC-AUC); the sequence
  model adds recall on insider exfiltration and reduces false alarms on normal user-days (5.8 % vs 9.0 %).
- Latency (CPU): 7.4 ms per single window (20 threads, offline); 0.36 ms per event in the batched workers.

Verified live:
- Workers without a model: healthy, not consuming (1,009 events waiting). After promotion both loaded the model
  within ~1 s, split partitions 0-3 / 4-7 and drained the backlog.
- Replay of the held-out test users (13 unseen attackers + 30 normal users, 21,410 events) with one worker
  SIGKILLed mid-run (it had scored ~8,500): **19/19** - every event scored once (22,418 total = backlog +
  replay, 0 duplicates), **live scores = offline scores (max |diff| 4.8e-8)**, all 13 attacks detected with the
  same timings as offline, 10/118 normal test user-days flagged.
- Medium-tier recheck (test settings T_recheck = 60 s): last event 0.4927 Medium (= offline) -> recheck 60 s
  later 0.1621 Low -> removed from the queue.
- Regression after recreating all services: infra 4/4, gateway smoke 29/29, streaming 10/10.

Found while verifying:
- **Bug fixed in the Phase 4 sink:** risk scores carry the event_id of the event they score; the sink used that
  id and a dedup memory shared across topics, so 22,598 of 22,599 scores were skipped as "duplicates" of their
  access events. Dedup keys are now namespaced by dataset and records prefer their own id; regression test
  added; the sink's `risk-scores` offsets were rewound and all 22,599 scores are now in HDFS.
- Generator v1 of this phase made 10 of 11 finance analysts attackers (a "role = attacker" shortcut); fixed
  with a larger population and a per-role cap enforced by the generator and a test.
- Two of my own test assertions were vacuous (always true); rewritten to test real behaviour.
- The 1,008 events produced by Phases 3-4 test scripts (machine-speed bursts, many deliberate denials) were
  mostly scored High. That is consistent with the model but means a realistic live demo needs human-paced
  traffic: follow-up.

Not done / known limits:
- All metrics are synthetic; no real access-log dataset has been evaluated.
- Thresholds stay at D3's initial 0.4 / 0.6 (not re-calibrated to a target alert rate).
- Collusion scenarios exist in the data but are evaluated in Phase 7 (their volumes are small for now).
- Rechecks use the inactivity-refreshed window only (D3 Step 5); no new event context is synthesised.

### 2026-11-11 — Phase 6 (four-path escalation)

Added:
- **Escalation engine** (`escalation_engine.py`): Paths A (S_ind >= 0.6), B (> kappa distinct users on a sensitive
  resource within delta_res = 15 min, all participants), C (periodic sweep every T_sweep = 2 h over history),
  D (each active Low-risk user with probability gamma). Union per user with every path's evidence; real-time
  (A, B) unioned for 5 s and emitted to `escalations.realtime`, background (C, D) once per W = 60 s to
  `escalations.background`; per-user cooldowns (300 s / 3600 s) so a user is analysed once per window;
  workload caps - real-time overflow deferred (never dropped), background overflow shed and counted
  (audit defect **D13 fixed**). All state in Redis; cooldowns and windows on an injected clock, so the same code
  runs live (wall clock) and in the offline evaluation (event time).
- **Path C rules** (`sweep_candidates`, [IMPL]): C1 rare_sensitive (>= 3 accesses in 24 h to sensitive resources
  the user used <= 1 time in a 14-day baseline that ends 7 days before the lookback; users with >= 20 baseline
  events), C2 threshold_evasion (>= 5 Medium scores, no High), C3 uncommon_coaccess (>= 4 users on a sensitive
  resource that is rare for them, within T_sweep). Top B_sweep = 50 per sweep.
- **Escalation service** (`streaming/escalation_service.py`): consumes `risk-scores` + `access-events-by-resource`,
  sweeps HDFS history, re-stages candidates if a publish fails; `sweep-now --at <time> [--publish]` for operator
  or experiment sweeps at any reference time. HDFS sink now archives both escalation topics.
- **Offline evaluation** (`ml/escalation_eval.py`): event-time replay of the synthetic timeline with the promoted
  TCN's scores through the same engine; per-path triggers vs escalations, per-member collusion coverage, benign
  load, workload vs Eq. (11); kappa sensitivity study.
- `scripts/smoke_escalation.py` (one live check per path), `tests/test_escalation.py` (15 tests).

Results — **synthetic data only** (`experiments/results/escalation-*`; model tcn-20261110T194520Z-beeb9b):

| kappa | benign escalations / day | unique escalations per W=60 s (mean / max) | individual test attacks escalated | collusion groups reached | colluders never scored High, escalated via B/C/D |
|---|---|---|---|---|---|
| 3 | 523.5 | 0.49 / 15 | 13/13 | 4/4 | 3/3 |
| 6 | 251.2 | 0.24 / 17 | 13/13 | 4/4 | 3/3 |
| 10 | 110.0 | 0.11 / 13 | 13/13 | 4/4 | 3/3 |
| 15 | 78.0 | 0.09 / 9 | 13/13 | 4/4 | 3/3 |

- Every path contributes (kappa 3, test attacks): A 13/13, B 7/13, C 7/13, D 1/13; all 4 collusion groups were
  reached by B, C and D as well as A.
- Core D3 claim, demonstrated (small sample): 3 of the 11 colluders never received a High TCN score; all 3 were
  escalated through Paths B/C/D at every kappa.
- kappa is a pure budget knob in this population (coverage unchanged from 3 to 15 while load falls ~6x). The code
  default stays 3 (the live demo has only 10 users); the synthetic 260-user population suggests ~10.
- Eq. (11) overestimates the measured load (e.g. 0.66 vs 0.11 per W at kappa 10) because it ignores union
  overlap and cooldowns, as D3 notes for capacity planning.

Verified live (each path triggered independently):
- Path A: replayed unseen compromised account -> realtime escalation, S_ind 1.0, model version in evidence.
- Path B: bob, judy, frank, grace read finance/audit-reports through the **gateway** -> realtime escalation,
  evidence lists all 4 participants (distinct_users 4 > kappa 3).
- Path C: `sweep-now` at the end of held-out low_and_slow-test-2 over **HDFS** (322 lookback / 10,433 baseline
  events) selected syn-u089 by rule C1 and published it to `escalations.background`.
- Path D: gamma = 1 (test setting) sampled a fresh low-risk user; gamma restored to 0.02 afterwards.
- All consumer groups at lag 0; escalations archived in HDFS (realtime: high, resource; background: sweep, sample).

Found while verifying:
- **Cooldowns used Redis TTLs (wall clock)** while the offline replay runs on event time, so 28 simulated days
  passed in ~95 s and cooldowns never expired: the first evaluation report was invalid and was discarded.
  Cooldowns now store their end time on the injected clock; regression test added.
- Path C/D triggers were not recorded by the first evaluation (only A/B were): fixed with a trigger hook.
- **Low-and-slow laundering:** with the baseline ending at the lookback start, an attacker's own accesses entered
  the baseline after one day and C1 stopped selecting them (live test failed at the end of the attack). Fixed
  with a 7-day guard gap (`sweep_baseline_gap_days`); regression test added; live test then passed.
- Operator sweeps at a past reference time were suppressed by cooldowns set on the live clock: `sweep-now` now
  emits its sweep candidates directly (`drain_sweep`); regression test added.
- Cap accounting counted cooling users as "shed"/"deferred"; fixed so these count only genuine budget overflow.
- My Path D live stimulus included sensitive resources, so Path B fired first and (correctly) suppressed the
  sample; the stimulus now uses non-sensitive activity only.

Not done / known limits:
- All four collusion groups in the synthetic data fall in the training split: the generator's per-role attacker
  cap left too few users for validation/test groups (11 planned, 4 formed). To be fixed before TGN evaluation
  (Phase 7).
- Path B fires very often on popular sensitive resources at kappa 3 (synthetic: ~1.3 trigger users per minute).
- Escalations are produced but not yet consumed: the TGN worker is Phase 7.

### 2026-11-11 — Phase 7 (TGN collusion detection)

Added:
- **Collusion-study dataset preset** (`SynthConfig.collusion_study()`, opt-in flags `collusion_v2`, `benign_teams`;
  the default Phase 5 dataset stays byte-identical, fingerprint beeb9bffdb1d7160): 700 users, 347,914 events,
  23 collusion groups (train 11 / validation 4 / test 8; 6 "familiar" groups abusing resources their members
  usually use), coordinated collection with shared cross-app targets and split exfiltration with disjoint record
  ranges in synchronised bursts; 34 recurring benign team meetings (hard negatives). Groups that cannot be formed
  under the per-role cap now raise instead of being silently skipped (the Phase 6 finding).
- **Temporal graph** (`ml/tgn_graph.py`, D3 Eq. 5): user/resource/application nodes; timestamped access edges with
  label-free attributes (familiarity = distinct earlier days, sequential record ids, sensitivity, operation,
  outcome, hour), membership and co-access edges; bounded neighbourhoods (1 h window, <= 20 resources,
  <= 8 co-accessors each); T = 10 slices, 17 node features (incl. synchronised co-access and burstiness).
- **Two-stage training** (`ml/tgn_train.py`, `ml/train_tgn.py`): D3 TGN unchanged (2 GCN, emb 50, GRU, attention
  over T = 10, sigma(W[h_i;h_j]+b), Adam 1e-3, L2 1e-6). Stage 1 self-supervised on benign graphs only (real vs
  corrupted edges, 300 epochs); stage 2 fine-tuning on collusion labels (synthetic stand-ins for analyst-verified
  incidents); tau_edge and theta_coll calibrated on validation. Audit defect **D9 fixed**.
- **Learned grouping g(u)** from TGN-suspicious edges (score >= tau_edge) on shared resources; S_coll = mean of the
  top suspicious edge scores; undefined for users without a coupled group. Audit defect **D8 fixed**.
- **TGN registry** (`ml/tgn_registry.py`), **graph-index** service (`streaming/graph_index.py`), **TGN workers**
  for real-time and background escalations (`streaming/tgn_worker.py`) publishing `group-scores` and `coll:<u>`.
- `scripts/smoke_tgn.py` (live parity check), `tests/test_tgn.py` (14 tests).

Results — **synthetic data only** (`experiments/results/tgn-20261111T104626Z-ccc61d.md`; one training seed; 8 test groups, 64 positive
test windows, so uncertainty is wide):

| Test period: escalated seed in a colluding group? | ROC-AUC | PR-AUC | precision | recall | FPR | team meetings flagged | test groups detected |
|---|---|---|---|---|---|---|---|
| TGN, two-stage (D3; promoted) | 0.843 | 0.568 | 0.643 | 0.422 | 0.024 | 12 % | 8/8 |
| TGN, stage 2 only (ablation) | 0.736 | 0.495 | 0.358 | 0.531 | 0.097 | 41 % | 7/8 |
| Static GNN, T = 1 (ablation) | 0.872 | 0.643 | 0.500 | 0.625 | 0.064 | 50 % | 8/8 |
| Rule: >= 2 shared sensitive resources | 0.581 | 0.111 | 0.099 | 0.453 | 0.423 | 41 % | 8/8 |
| Rule: uncommon concurrent co-access | 0.510 | 0.097 | 0.222 | 0.031 | 0.011 | 0 % | 1/8 |

- Learned graph models clearly outperform both rules (the co-access rule flags 63 % of ordinary sensitive-access
  windows).
- Stage-1 self-supervision helps: PR-AUC 0.568 vs 0.495 and a four-times lower false-positive rate.
- **The temporal component did not improve ranking in this study**: the static T = 1 ablation has the higher
  PR-AUC (0.643). Its advantage is fewer false alarms on recurring team meetings (12 % vs 50 %). A likely reason:
  the strongest coordination features (burstiness, synchronised co-access) are computed over the whole window,
  so the static model sees them too. The D3 TGN is promoted for fidelity and its lowest false-positive rate;
  the comparison is reported, not hidden.
- Member identification: recall 1.00, precision 0.54 (groups include extra co-accessors of busy resources).
- Weakest case: coordinated collection on unfamiliar resources (22 % of windows flagged).
- Latency: 8.7 ms median per escalated user (featurise + TGN + grouping).
- Wall time 5.6 h: training ran while the live stack competed for the CPU (static ablation alone 4.9 h).

Verified live:
- TGN workers without a model: healthy, not consuming (58 + 15 escalations waiting); loaded the promoted model
  within ~1 s and drained the backlog.
- **Train/serve parity** (`smoke_tgn.py`, histories replayed through gateway topic -> repartitioner -> graph index
  -> escalation -> TGN worker): 4/4 cases identical to offline (members and S_coll, max diff 6e-8).
  Outcomes: split_exfiltration-test-1 **detected** (S_coll 0.9996, group contains all 3 colluders + 1 other);
  coordinated_collection-test-0 and split_exfiltration-test-0 missed at the tested window (S_coll 0.65 / 0.57);
  a benign HR team meeting correctly not flagged (0.805 < 0.9).
- group-scores archived in HDFS; all consumer groups at lag 0; infra 4/4, gateway smoke 29/29; 18 services healthy.

Found while verifying:
- **Out-of-order annotation bug:** the resource-keyed view (key includes a time bucket) can deliver one user's
  accesses to one resource out of time order; annotating edges on arrival produced impossible familiarity counts
  (up to 96 "earlier days" in a 28-day dataset) and broke parity. The index now stores order-independent state
  (day sets, time-keyed objects) and derives edge attributes at query time; regression test with shuffled
  arrival order added; index rebuilt from Kafka.
- My first live check mixed two replays of the same synthetic window after the rebuild; replays are now shifted
  by whole weeks (all features invariant) so runs are isolated.
- The collusion-study generator initially exceeded the per-role attacker cap (27 %) because group members were
  checked one at a time; fixed and enforced by a test; population raised to 700.
- The week-shifted replays spread records over hundreds of dates, so the HDFS sink wrote ~1,900 small files
  (transient lag 21k, drained in ~90 s). Scheduled compaction of past dates remains open.

Not done / known limits:
- Single training seed; no confidence intervals. Synthetic data only.
- No persistent cross-call node memory (D3/[14] design: GCN + GRU + attention per neighbourhood).
- The temporal advantage claimed by D3 is not demonstrated on this dataset (see above).

### Remaining

Phases 8–10 of `docs/PHASE1_AUDIT.md` §7. Open audit defects: D3 (sticky blocks, Phase 8), D12's
`FeedbackLoop.flush()` re-flush (Phase 9).
