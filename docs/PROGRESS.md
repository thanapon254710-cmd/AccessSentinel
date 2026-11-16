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
| 8 PDP + enforcement | Done (2026-11-11) — sticky lifecycle, outbox audit, enforcement verified live |
| 9 Feedback + dashboard | In progress — built and verified live; analyst confirmation in the UI pending (needs a sign-in) |
| 10 Integration + experiments | Superseded by the Revised v3 / Deliverable 4 roadmap (`docs/V3_AUDIT.md`) |
| v3 1 Audit + roadmap | Done (2026-11-15) |
| v3 2 Event pipeline (ordering, exactly-once, resource windows) | Done (2026-11-15) — verified live incl. crash test |
| v3 3 TCN v3 | Done (2026-11-15) — trained (GPU), calibrated, deployed, verified live |
| v3 4–6 CMU + SGE + PriorityAdmit | Done (2026-11-15) — trained, deployed, verified live incl. CMU crash test; TGN v3 below D3 at seed level (reported) |
| v3 7 PDP six actions, ψ, TTL fallback | Done (2026-11-15) — verified live |
| v3 8–12 Scenarios, baselines, experiments A–H, comparison, D4 | Not started |

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

### 2026-11-11 — Phase 8 (Policy Decision Point and enforcement)

Added:
- **PDP engine** (`pdp_engine.py`, D3 Eq. 7): Block when S_ind >= 0.6 (individual), S_coll >= the TGN's calibrated
  theta_coll for every member of g(u) (collective), or organisational policy C denies (policy); otherwise Allow
  subject to the gateway ACL. **Explicit Block lifecycle** (audit defect **D3 fixed**, also in the in-process
  `pdp.py`): sticky - lower or missing scores never release a Block; risk blocks end at expiry (24 h) or by
  analyst release, which opens a grace period (1 h) so the same ongoing evidence cannot re-block at once; policy
  blocks end only when the policy is lifted; evidence older than 10 / 15 min (backlogs) is not enforced; a new
  kind of evidence on an already-blocked user is recorded as an `evidence` transition; manual analyst blocks with
  optional TTL; **shadow mode** (decide and audit without enforcing) for calibration.
- **Transactional outbox**: each state change and its decision record are written in one Redis transaction; the
  service drains the outbox to Kafka `pdp-decisions` (-> HDFS), the PostgreSQL `pdp_decisions` audit table and
  session revocation (queued before the outbox is acknowledged, retried on failure).
- **PDP service** (`streaming/pdp_service.py`): consumes `risk-scores` + `group-scores`, re-reads policy C from
  PostgreSQL every 15 s (outage -> existing decisions stay in force), expires blocks, revokes sessions through the
  gateway's internal API. Request-time enforcement uses the existing gateway decision cache.
- PostgreSQL schema v2: `policy_blocks` (policy C), `pdp_decisions` (audit trail).
- `pdp_admin` CLI (status, list, release, block, policy-block, policy-unblock, audit) - the Phase 9 dashboard
  uses the same engine; `scripts/reset_demo_state.py` (test environments only); `scripts/smoke_pdp.py`;
  `tests/test_pdp.py` (14 tests).

Verified live (enforce mode, real gateway, real demo accounts):
- First start over the full history: 71,345 risk scores ignored as stale, 0 users blocked.
- **ivan** (staff): 5 requests at human pace -> Low tier, not blocked. Access-control probing -> **suspended after
  7 attack requests, 1.4 s after the attack began** (D3's exposure window: gateway -> Kafka -> TCN -> PDP ->
  decision cache); active session terminated (old cookie -> 401); new login refused (403); still blocked 20 s
  later; Block in the audit trail; analyst release restored access. 9/9.
- **henry**: manual 20 s block enforced, access returned after expiry, expiry recorded. 4/4.
- **dave**: policy block refused login; analyst release refused (policy-locked); lifting the policy restored
  access. 3/3.
- **Group-wide**: a TGN group above theta_coll (held-out split_exfiltration-test-1, 4 members) -> every member
  blocked; the 3 true colluders had already been blocked individually by the TCN (their collective evidence is
  recorded as `evidence` transitions), the 4th member - not a colluder - was blocked only collectively. 3/3.
- **ML outage**: all TCN and TGN workers stopped for 45 s -> all 8 blocked users stayed blocked.
- Audit trail: 17 decision transitions in PostgreSQL = 17 records in HDFS.
- Regression (PDP in shadow mode, demo state reset): infra 4/4, gateway smoke 29/29, streaming 10/10, escalation
  A 1/1, C 3/3; shadow mode recorded 9 would-be blocks without affecting the gateway. Enforce mode restored.
- `pytest -q tests` -> 128 passed, 4 skipped.

Found while verifying:
- Group-wide enforcement first looked incomplete: members already blocked individually kept their original
  reason and the collective evidence was silently dropped. Now recorded as an `evidence` transition (block and
  expiry unchanged), so the audit shows every reason; regression test added.
- D3's group-wide rule blocks every member of g(u); with the TGN's member precision of 0.54 (Phase 7) this
  blocks innocent co-accessors, as happened live (1 of 4). Analyst release is the remedy; a narrower scope is a
  policy choice for the team.

Operational notes:
- The Phase 3-6 smoke scripts drive demo accounts at machine speed; with enforcement on they are (correctly)
  blocked. Run them with `AS_PDP_MODE=shadow` after `scripts/reset_demo_state.py --all-demo`.
- Before a live demo, reset the demo users' monitoring state; their earlier test traffic is bot-like.

Not done / known limits:
- No step-up authentication or partial restrictions: decisions are Allow/Block only (as in D3).
- Thresholds are the initial D3 values (theta_high) and the TGN's validation calibration (theta_coll), not
  calibrated to a target alert rate.

### 2026-11-11 — Phase 9 (feedback and monitoring dashboard)

Added:
- **Incidents** (`incidents.py`, `streaming/incident_service.py`, consumer group `incidents`): opened from PDP
  Block decisions (individual; collective incl. `evidence` transitions) and TGN groups above theta_coll;
  release / expiry / policy-lifted update the per-user enforcement status. Idempotent by evidence id. One open
  case per user (a repeat Block merges into the unreviewed incident) and per colluding group (re-scoring the same
  group yields a new `group_id`, so records merge by seen `group_id` or member overlap within 24 h). Incidents
  with `syn-*` users are `source = synthetic-replay`. PostgreSQL schema v3: `incidents`, `feedback`,
  `settings_changes`.
- **Dashboard API** (`services/api/`, FastAPI, port 8080): separate login for security staff only (roles
  `security_analyst`, `security_admin`; accounts `analyst1`, `secadmin` seeded from `AS_ANALYST_PASSWORD`),
  Argon2id, Redis sessions in their own namespace, cookie HttpOnly + SameSite=Strict, CSRF header required on
  every mutation, login lockout, audit log, admin-only settings / catalog / model promotion, security headers +
  CSP. Endpoints for every page; WebSocket `/ws/live` fed by a non-committing Kafka consumer (reads from
  `latest`, never affects other groups); topic rates measured from Kafka end offsets.
- **Dashboard UI** (`dashboard/`, React 18 + TypeScript + Vite, recharts, cytoscape): Overview, Access Activity,
  Individual Risk, Collusion Detection (interactive user–resource graph from the live edge index, edges coloured
  by the TGN's edge score vs tau_edge), Security Incidents (review, verdicts, release, missed-detection reports,
  retraining-set view), System Monitoring, Settings (thresholds, escalation parameters incl. sampling rate gamma,
  sensitivity catalog, app registration, model versions). Every number comes from backend state; synthetic
  records are labelled. Built in a Node stage of `deploy/Dockerfile` and served by the `api` service.
- **Analyst feedback**: a verdict writes a `feedback` row, closes / triages the incident and is published to Kafka
  `analyst-feedback` (acknowledged before the API answers) -> HDFS. Window = incident activity window
  (-15 min / +5 min).
- **Retraining** (`ml/feedback_data.py`, `ml/retrain.py`): reads ONLY the HDFS `analyst-feedback` dataset; labels
  from analyst verdicts (confirmed_malicious -> 1, false_positive / benign_explained -> 0; needs_more_info and
  model outputs excluded). TCN / TGN fine-tune a copy of the live model on base synthetic data + feedback
  (oversampled), re-calibrate, compare with the live model on the base test set and on held-out feedback; the
  gate (base PR-AUC not more than 0.01 lower, feedback recall not lower, false-positive rate not higher) decides
  `--promote-if-better`; otherwise the candidate waits for an admin. Refuses to train when the feedback labels
  no data. Audit defect **D12 fixed** (`FeedbackLoop.flush()` no longer re-writes flushed records).
- **Runtime settings** (`runtime_settings.py`): thresholds, PDP and escalation parameters in Redis with a
  version counter; TCN workers, escalation and PDP services re-read them within ~5 s (validated, theta_low <
  theta_high, history in PostgreSQL).
- Compose services `incidents`, `api`; `scripts/smoke_dashboard.py`; `tests/test_dashboard.py` (20 tests).

Verified live:
- `smoke_dashboard.py` 30/30: alice (monitored user, correct password) refused; analyst signs in, cookie flags;
  mutation without CSRF header -> 403; all 12 page endpoints answer; health redis/postgres/kafka/gateway ok, HDFS
  reachable; feedback on a synthetic incident -> incident closed -> record in HDFS `analyst-feedback` -> in the
  retraining dataset; analyst cannot change settings (403); invalid change rejected; admin changed Path B kappa
  3 -> 4 -> 3, and the escalation service logged both values, TCN workers and PDP re-read the version.
- Attack scenario (`smoke_pdp.py individual`, enforce mode) 9/9; the incident service opened / merged ivan's
  case; its feedback window covers 46 of ivan's accesses in HDFS.
- Regression (PDP in shadow mode, demo state reset): infra 4/4, gateway 29/29, streaming 10/10 + poison 3/3,
  escalation A 1/1, C 3/3. Enforce mode restored.
- `pytest -q tests` -> 148 passed, 4 skipped; `tsc --noEmit` clean; production build 973 kB JS (291 kB gzip).

Found while verifying:
- First backfill produced 28 incidents with duplicates (4 incidents for one synthetic group; 2 open incidents
  for ivan). Fixed by the merge rules above; rebuilt from Kafka: 18 incidents.
- **Incident windows used the PDP decision time.** Replayed synthetic events keep their original timestamps
  (39 days earlier), so the first two smoke-test confirmations labelled zero accesses, and `retrain tcn` still
  produced a candidate (fine-tuned on base data only). Fixed: PDP decision records carry the evidence's event
  time (`basis.event_t`, `basis.event_window`), incidents use event time, release/expiry no longer stretch the
  window, and retraining stops when feedback labels no data. That candidate was deleted. Incidents rebuilt from
  decisions recorded before this fix still use decision time (equal to event time for live traffic).
- The tools container did not mount `experiments/results`, so retraining reports were lost with the container;
  mounted now.
- `.env.example` was never in git: the `.env.*` ignore rule matched it, so a fresh clone could not follow the
  setup guide. Un-ignored (it holds only `change-me` placeholders); `.env` stays ignored.

Added later the same day (presentation scenario "a new user attacks"):
- **User Accounts** page + API (`/api/accounts`, secadmin only for changes): create monitored accounts with a
  role from the access catalog (no self-registration; security roles refused), deactivate / activate, reset
  password; deactivate and reset end the user's gateway sessions through the internal revoke API; passwords
  never logged (audit records only "generated" / "set by admin"). 3 tests.
- `scripts/demo_new_user.py`: the full scenario on the live stack with a brand-new account;
  `PRESENTATION_DEMO.md`: the demo script for the presentation.
- Login audit fix: a failed login with an unrecognised user ID is audited as `unknown-user` and the lockout
  counter is keyed by a hash. Before, the typed ID was logged, so a password typed into the User ID field was
  stored in plain text. Found live: 3 such audit rows were redacted and the analyst password rotated. 1 test.

Verified live with brand-new accounts (cold start, enforce mode):
- Probing at 5 requests/s: normal work (8 requests at human pace) stayed Low; blocked after 8 and 11 attack
  requests (1.7 s, 2.3 s; S_ind 0.63 >= 0.6, reason individual); session ended; re-login refused; incident
  opened. 8/8.
- Enumeration of allowed records at 20 requests/s: blocked after 429 requests (25.2 s).
- Probing by hand (1 every 2.5 s): blocked after 3 attempts (7.5 s), but by the TGN (collective): earlier test
  accounts had probed the same resources within the TGN's 1-hour window, and the TCN alone was at 0.53 (Medium)
  at that point. Repeated an hour later in isolation (no other recent test activity): blocked after 4 attempts
  (10.0 s) by the TCN alone, S_ind 0.94, reason individual. 8/8.
- The first dry run reported a miss: the block arrived as session termination (401), not the 403 "suspended"
  page my script waited for. The script now accepts either and confirms with the PDP decision.

Sharing / running on other computers (2026-11-15):
- The promoted TCN and TGN versions are now tracked in Git (`models/`, 2.5 MB; other versions stay local), so a
  fresh copy runs without training.
- Kafka, HDFS, PostgreSQL and Redis host ports are bound to 127.0.0.1 (Kafka and HDFS have no authentication);
  only the gateway (8000) and the dashboard (8080) are reachable from the network.
- `start.bat` / `start.ps1` (Windows) and `start.sh`: create `.env` with random secrets on first run, start the
  stack, wait until healthy, print where the passwords are. `stop.bat`. `.gitattributes` pins their line
  endings. Clean ZIPs are made with `git archive` (never contains `.env`).
- Fresh-install test (clean ZIP unzipped elsewhere, separate Compose project, image deleted first): the first
  attempt crashed Docker Desktop. Every service inherited the `build:` section, so Compose built the same 2.3 GB
  image about 15 times in parallel and ran Docker out of memory; it never showed on the development PC because
  the image already existed. Now only `db-init` builds the image and the other services reuse it
  (`pull_policy: never`). Re-test from a cold cache: image built once, all services healthy,
  `demo_new_user.py` 8/8 (new account blocked after 8 attack requests, 1.6 s), dashboard and gateway answer.
- Reason: a ZIP of the folder could not be started on another PC. Without `.env` (and its `COMPOSE_FILE`),
  `docker compose` finds no configuration, and every `change-me` secret had to be replaced by hand.

Pending:
- Analyst confirmation of ivan's incident in the UI, followed by `retrain tcn` on it and a promote / roll-back
  check through the Settings page. Not yet done: signing in needs the analyst password, which stays with the
  user.

Not done / known limits:
- Live lists (activity, escalations, group scores, recent decisions) cover what the API has seen since it
  started (ring buffers); history is in HDFS (per-user view on Individual Risk).
- No multi-factor authentication or SSO for the dashboard; one shared seed password for the two demo security
  accounts (development only).
- Retraining is triggered manually (CLI), not scheduled.

### 2026-11-15 — Revised v3 / Deliverable 4: Phase 1 (audit and roadmap)

New specification: `AccessSentinel_Revised_v3_highlighted.pdf`. Branch `v3/phase1-audit`.

Done:
- Read the v3 paper in full; compared every requirement (Eqs. 1–34, Algorithm 1, Tables II–V) with the code:
  `docs/V3_AUDIT.md` §3. Largest gaps: TCN architecture/features/objective/calibration (L 20 -> 128,
  d 50 -> 43, multi-task), no continuous node memory (CMU) — the current TGN only runs for escalated users,
  which v3 rules out — no SGE / coordination graph / per-member Scoll, no compute budget or PriorityAdmit, PDP
  has 2 of 6 actions, no event-time watermarks or late-event policies, gateway fails closed instead of
  sensitivity-based fallback, no ψ check at the gateway.
- Environment recorded by `evaluation/env_info.py` (`evaluation/results/raw/environment-phase1.json`):
  i5-13500HX 14C/20T, 15.7 GB RAM, RTX 4050 6 GB (unused: CPU-only PyTorch), Docker VM 7.6 GB.
- References: [25] Prov-Trust found (doi 10.1007/s44443-026-00731-5, full text not accessible); [24] has a
  candidate (Modell et al., IEEE BigData 2021 workshop) that the team must confirm. `evaluation/references/`.
- Roadmap Phases 2–12 and experiment plan A–H: `docs/V3_AUDIT.md` §8–9.

Waiting on team decisions D1–D7 (`docs/V3_AUDIT.md` §5). No v3 code changed; tests unchanged (152 pass).

### 2026-11-15 — Revised v3 Phase 2 (event pipeline: schema, ordering, exactly-once, resource windows)

Decision D1 taken: no Flink. The Python stream tier gained event-time ordering, keyed-state checkpoints and
Kafka transactions instead.

Added:
- **Event schema v2** (v3 Eq. 1): `seq` (ν, per gateway/session stream), `stream`, `tenant` (r* =
  `tenant/app:resource` via `AccessEvent.rstar`), `ti` (Kafka ingestion time, broker `LogAppendTime` on
  `access-events`, filled by consumers), `v_policy` (hash of the gateway's catalog). v1 / synthetic records
  still validate (synthetic data stays v1, dataset fingerprints unchanged). The gateway stamps `ts_gw_in` and
  `ts_gw_pub`.
- **Event-time ordering** (`streaming/event_time.py`): per-user watermark W = max te − δlate (Eq. 2), release
  in (te, ν) order, idle bound (an event never waits more than δlate, so Lat_reorder ≤ δlate). The ν fast path
  releases at once while a user has a single active session. The four late-event policies are history_only,
  rescore, retro_alert and discard, chosen by sensitivity (defaults: s=0 history_only, s=1 rescore).
- **Exactly-once stages** (`streaming/transactional.py`): each iteration is one Kafka transaction holding the
  outputs, the changelog of changed keyed state (compacted topic) and the input offsets (KIP-447 fencing).
  After restart or rebalance, state is rebuilt from the changelog (R3). Static group membership is used.
  Scope: Kafka outputs + state + offsets; Redis/HDFS side effects stay at-least-once with idempotent writes.
- **Sequencer service**: access-events → `access-events.ordered` (+ `late-events` audit). The TCN workers
  now read the ordered stream (consumer group `tcn-v3`), honour late policies, and put a per-stage `trace`
  into every risk score (input for Experiment C).
- **Resource-keyed view, two-stage shard-and-merge** (`streaming/resource_windows.py`):
  - The repartitioner is now transactional; key `r*|b|hash(u) mod J_r`.
  - Stage 1 keeps exact per-shard participant sets of sensitive resources per event-time window b
    (Δres = 900 s). It finalises a window by the minimum watermark over SOURCE partitions, with idle-source
    handling.
  - Stage 2 merges shards and emits `resource-windows` exactly once per (r*, b), plus corrections for late
    shards.
- **HDFS hourly aggregates** (`history_aggregates.py`): `agg-user-hourly` and `agg-resource-participants`,
  computed by a background thread in hdfs-sink for closed hours. The sink also archives `late-events` and
  `resource-windows`.
- `streaming.consumer.fetch` (low-latency batch fetch) and idle position commits, used by all consumers.
- Topic configs enforced by provisioning (compacted changelogs, LogAppendTime).
- Compose services `sequencer` and `resource-windows`; new consumer groups in the lag monitor.
- Tests: 32 new (event-time property tests, exactly-once with injected commit failure and crash/restart on an
  in-memory transactional Kafka, shard-merge exactness and finality, aggregates, consumer). Full suite: 176
  passed, 4 skipped.

Verified live:
- `smoke_v3_stream.py order` 7/7: 30 events from two interleaved sessions + 2 late events → ordered stream
  exactly once, in event-time order; late events flagged history_only (s=0) / rescore (s=1); reorder wait
  ≤ δlate (max 2.009 s); ti filled; the TCN scored 31 events (not the history-only one) with trace.
- `smoke_v3_stream.py windows` 5/5: 5 users on a sensitive resource → exactly one final window with exactly
  those users (3 of 4 shards reported); a late event after finalisation is audited, no second final.
- **Crash test**: sequencer killed with SIGKILL 14 s into a 30 s burst of 3,000 events, then restarted →
  3,000/3,000 events on the ordered stream, 0 duplicates, in sequence order; 49 state records restored. Run
  twice (before and after static membership).
- Regression (PDP shadow, demo state reset): gateway 29/29, streaming 10/10 (now incl. "resource view exactly
  once": 0 duplicates), poison 3/3, escalation A 1/1, B 3/3, C 3/3. Enforce mode: `smoke_pdp individual` 9/9
  (suspended after 5 attack requests, 1.0 s), `demo_new_user` 8/8 ×3.
- Preview of Experiment C, measured on gateway traffic of two fresh single-session users (22 events; not the
  experiment): gateway-in → TCN score median 19.0 ms, p95 23.1 ms (gateway 1.3, gateway → sequencer 7.0,
  reorder 0.0, sequencer → TCN 7.3, TCN batch 2.8 ms). New-user attack blocked after 3 attack requests (0.6 s)
  instead of 7 (1.4 s) before this phase.

Found while verifying (all fixed, with regression tests):
- Two interleaved sessions + the ν fast path: in-lateness events of the other session were overtaken and
  treated as late (R1 violation). The fast path is now off while another session of the user was active within
  60 s (`AS_SEQUENCER_CONCURRENCY_WINDOW`). Trade-off: a session silent for > 60 s that resumes with a delayed
  event can produce a "behind_released" late event (handled by its policy, never lost).
- Window finalisation used max te per by-resource partition. Because the repartitioner merges source
  partitions that are fetched in chunks, this declared lagging sources' events late. Fixed with a min over
  source-partition watermarks and idle handling.
- Lateness in Stage 1 was judged per shard key; a late user hashing to an empty shard counted as new data.
  Now judged per window.
- Idle stages committed ~300 empty transactions a minute; now a transaction is opened only when there is
  something to write.
- After a hard kill the sequencer processed nothing for ~38 s (dead member's 45 s session). Static membership
  + 10 s session: ~1 s from process start to resume.
- Consumers of transactional topics showed one message of "lag" per partition (commit markers). Idle
  consumers now commit their position.
- At light load each stage waited its full poll timeout (librdkafka batch consume): median 2.9 s gateway →
  score in the first trace, mostly the 1 s TCN poll and 2 s idle reorders of multi-session test users. Fixed
  by `fetch` and the concurrency window (19 ms median above).
- The new stages wrote dead-letter headers in a different format; aligned.

Not done / limits:
- "rescore" corrects the window only approximately in the D3 TCN (no re-ordering of already-observed events);
  exact correction comes with the v3 TCN state (Phase 3).
- Resource triggers are emitted when a window is FINAL (v3 Table IV), i.e. up to Δres + grace after the first
  access. The existing real-time Path B (Redis counting) still runs; Phase 5 rewires escalation to
  `resource-windows` and Experiment C will measure the delay.
- TCN keyed state stays in Redis (at-least-once, eid de-duplication), not in a changelog.
- The trace uses wall clock on one host: no cross-host clock skew is measured.

### 2026-11-15 — Revised v3 Phase 3 (TCN v3: architecture, features, objective, calibration)

Implemented in the existing modules (D3 code kept alongside as the baseline; the registry serves either by
manifest `arch`):
- **Architecture** (`tcn.py` `TCNv3`, `config.TCNv3Config`): L = 128, d = 43 (Table III), 6 residual blocks of two
  causal convolutions, dilations 1–32, filters [256, 256, 128, 128, 64, 64], receptive field 127, dropout 0.2.
- **Features** (`ml/features.py` `EncoderV3`, `UserStateV3`): the 43 Table III features. The timer context
  z_u(t) enters at the scoring head. ThreatFeed and DevPosture are **simulated**
  (`SimulatedThreatFeed`, `SimulatedDevPosture`, MDM registry recorded by the generator, no extra RNG draw).
- **Objective** (`tcn.multitask_loss`): next-resource, next-operation and time-gap self-supervised heads, plus a
  focal loss on the supervised head (λ = 1, 1, 0.5, 1; γ 2, α 0.25). Self-supervised pre-training for 3 epochs,
  then joint fine-tuning for 8.
- **Split / calibration** (`ml/dataset.build_v3`, `ml/train_tcn.calibrate_v3`): chronological split with a gap of
  L − 1 events per user (Eq. 7), rolling-origin validation, θ_low / θ_high / θ_B = quantiles of S_ind on nominal
  validation windows (α = 0.05 / 0.01 / 0.001, Eq. 8). 0.4 / 0.6 are no longer used for v3 models.
  - Calibrated thresholds travel with every v3 score; the PDP and escalation use them unless an operator set
    `risk.theta_high` explicitly (`pdp_engine.theta_high_for`).
- **Serving**: `tcn_worker` v3 path (state `tcn3:state:<u>`, re-check with the timer context at re-check time).
- **Data**: `SynthConfig.tcn_v3_study()` (400 users, 70 days, split 36/14/20 days, attack margin, fingerprint
  0c7a4e032ad5bd45). The default fingerprint beeb9bffdb1d7160 is unchanged.

Result (synthetic only), `experiments/results/tcn3-20261115T091825Z-0c7a4e.md`, trained on the RTX 4050
(`.venv-gpu`, 1529 s):

| | value |
|---|---|
| test windows / attack events | 89,843 / 1,180 |
| ROC-AUC / PR-AUC | 0.979 / 0.890 |
| θ_low / θ_high / θ_B | 0.0284 / 0.0482 / 0.0849 |
| at θ_high: precision / recall / FPR | 0.480 / 0.890 / 1.28 % |
| at θ_B: precision / recall / FPR | 0.859 / 0.855 / 0.19 % |
| attack instances detected at θ_high | 18 / 19 (median 0.7 min) |
| normal user-days flagged at θ_high | 6.9 % |
| rolling-origin PR-AUC (3 origins) | 0.729 / 0.794 / 0.956 (0.826 ± 0.095) |
| inference, one window, batch 1 | CPU 9.7 ms, CUDA 5.7 ms (median) |

Weak scenarios (reported as measured): split_exfiltration 0/19 events, coordinated_collection 0.125,
low_and_slow 0.215 — these are what the graph path is for. The D3 TCN report
(`tcn-20261110T194520Z-beeb9b`, PR-AUC 0.917) used a **different dataset** (beeb9b) and is not a like-for-like
comparison; Phase 8 runs the baselines on the same data.

Verified live: `demo_new_user` 8/8 ×2 (blocked after 2 attack requests, 0.4 s; normal work S_ind 0.004),
`smoke_pdp individual` 9/9 (ivan stopped after 12 requests, 2.4 s).

Found while verifying (fixed, with regression tests):
- The first v3 model scored every live window 0.0. The version features (schema_v, policy_h) were constant in
  synthetic training (std floor 1e-6), so live values scaled to ~500,000.
  - EncoderV3 now neutralises features that are constant in training, leaves binary/cyclic features unscaled
    and clips to ±8.
  - The defective model was withdrawn (live rolled back to D3 first); its report is kept as
    `tcn3-20261115T083751Z-0c7a4e-DEFECTIVE.md`.
- Rolling-origin folds of 4 days were shorter than the gap and gave undefined PR-AUC. Now 12-day folds, and folds
  without positives report None.
- `smoke_pdp` counted a 401 (session revoked first) as "not blocked".

### 2026-11-15 — Revised v3 Phases 4–6 (CMU on every event, SGE + coordination graph, PriorityAdmit)

Implemented:
- **TGN v3** (`tgn.py`, `config.TGNv3Config`; Rossi et al. defaults as written in v3): TimeEncode 100, message
  MLP 100, GRU memory 100, 10 neighbours, 1 hop, Adam 1e-4, L2 1e-6, edge decoder ε_e.
  - Coordination graph (Eqs. 15–17), A_u (Eq. 18) and S_coll = σ(ω·[TM_π(ε), A_u, Nov] + b) (Eq. 19), fitted by
    logistic regression on train.
  - θ_C and θ_A are quantiles of nominal validation members (α 0.01 / 0.05).
  - Training: `ml/tgn_train.train_tgn_v3`; `ml/train_tgn --arch v3`; registry `tgn-v3`.
- **CMU services** `cmu-user` / `cmu-resource` (`streaming/tgn_worker.CmuHandler`): every event updates its user
  memory (ordered stream) and its resource memory (new `access-events-by-rstar` view, emitted by the
  repartitioner in the same transaction). This also covers users who are never escalated.
  - Counterpart memory is the latest published snapshot (bounded staleness, measured).
  - Idempotency: per-node event-id ring plus Kafka (partition, offset).
- **SGE** (`analyse_v3`): runs only for admitted users, on live memories and window edges; publishes per-member
  S_coll_u and A_u (`group-scores`, `model_kind tgn-v3`).
- **Algorithm 1 PriorityAdmit** (`escalation_engine.priority_admit`, `graph_budget`):
  - Priority res > high > sweep > sample; Eq. 26 cost; budget B_W = ⌊ρ′·N·μ·W⌋ (Eq. 29) with a per-window ledger.
  - Overflow: sample dropped, sweep deferred, resource queued with a deadline, high left to the PDP alone.
  - Path B now comes from finalised `resource-windows`.

TGN v3 result (synthetic, same dataset and the same 690 test anchors as the D3 TGN),
`experiments/results/tgn3-20261115T094228Z-ccc61d.md`:

| model | seed ROC-AUC | seed PR-AUC | precision / recall / FPR at threshold | groups | member P / R |
|---|---|---|---|---|---|
| TGN v3 (CMU + SGE) | 0.681 | 0.355 | 0.65 / 0.23 / 1.3 % (θ_C 0.861) | 7/8 | 0.65 / 0.84 |
| D3 TGN (two-stage) | 0.843 | 0.568 | 0.64 / 0.42 / 2.4 % (θ 0.9) | 8/8 | 0.54 / 1.00 |

**The v3 TGN is worse than the D3 TGN at seed level on this data.** It has higher member precision, flags 1 of 34
benign team meetings, and its validation collusion-edge PR-AUC was 0.11. The v3 hyper-parameters were not tuned
for access-control data (the paper says so too). This is reported as measured, not explained away.
- Eq. 19 weights: TM 6.23, A_u 5.19, Nov −0.47, bias −3.09; θ_A 0.364.
- h_min was calibrated on validation (h_min = 2 was worse than chance there: 18 % of collusion vs 27 % of benign
  members kept); now 1.

Cost model (Eq. 26), measured. `smoke_tgn.py cost-fit` seeds 20 neighbourhoods (|E| 22–3,520, |V| 12–177)
through the real pipeline and times `analyse_v3` inside the tgn-worker container (5 runs, median):
- C_u = 35.59 + 0.0889 |E| + 0.8212 |V| ms; R² 0.92, MAPE 33 % (noise at small sizes).
- Now the `EscalationConfig` defaults (were illustrative 5 / 1 / 0.5).
- Raw: `evaluation/results/raw/sge-cost-fit-20261115-run2.json`.
- The first run is kept as `…run1-superseded.json`: its admission-time estimate counted one edge per neighbour
  user and under-estimated |E| up to 7×. `neighbourhood_size_fn` now counts the neighbours' window edges
  (ZCOUNT), and a test checks it against what the SGE gathers.

Verified live:
- `smoke_tgn.py cmu` 5/5: all 12 events of a never-escalated user applied to its memory (100-d, non-zero);
  resource memory updated; both tasks counted; staleness measured.
- `smoke_tgn.py admit` 4/4:
  - a fresh attacker was blocked by the TCN path;
  - the escalation was admitted with cost 64.5 (old coefficients);
  - the SGE ran on live memories: 93 edges, 20 nodes, 668 ms, max memory staleness 3,518 s, S_coll 0.647 < θ_C;
  - every window's used work ≤ B_W 84,000.
- **CMU crash test**: cmu-user killed with SIGKILL during a 2,000-event burst, then restarted. All 2,000 records
  are on the ordered stream (offsets 961–2961), and the CMU's last applied position is exactly [2, 2961]; the
  offset check skips redelivered records (no double update).
- `smoke_tgn.py collusion` / `team` (v3 path):
  - Users and resources are renamed per run, so earlier CMU state cannot leak in. The offline reference uses the
    same CmuHandler + analyse_v3 code on an in-process store.
  - Both 5/5: live = offline (same members; S_coll None in both).
  - Reported outcomes: coordinated_collection-test-0 → no coordination group around the seed (1 of 3 colluders);
    the team meeting is not flagged.
  - Offline screen of all 8 test groups at the smoke anchor: only split_exfiltration-test-1 is flagged
    (S_coll 0.922, 3/3 colluders + 1 other). Consistent with the 0.23 seed recall.

Found while verifying (fixed):
- `cmu:stats` counters went backwards after a restart (per-process totals written with HSET); now HINCRBY deltas.
- The CMU event-id ring (64) could not cover a 500-event redelivery; per-node Kafka offsets were added.
- CMU round assignment was O(n²).
- `anchor_replay_v3` skipped anchors after the last batch.
- D3 `member_scores` floats broke the v3 writer.
- The replay check's lag query failed on NOT_COORDINATOR after a restart; it now retries.

Not done / limits:
- Resource→application membership edges (v3 Step 7) are not modelled; only the node-type slot exists.
- Provisioning from pre-admission demand (Eqs. 27–28) is measured in Experiment D, not yet.
- TGN v3 hyper-parameters are the paper's untuned defaults; no search was run (it would be tuning on the test
  design).

### 2026-11-15 — Revised v3 Phase 7 (PDP: six actions, ψ, TTL fallback)

Implemented:
- **Action set and Eq. 21** (`pdp_engine.py`):
  - Allow / Throttle / StepUp / Restrict / Terminate / Block, ranked in the top-down order of Eq. 21:
    Block ← policy deny; Terminate ← S_ind ≥ θ_B; Restrict ← S_coll_u ≥ θ_C ∧ A_u ≥ θ_A;
    StepUp ← S_coll_u ≥ θ_C ∧ A_u < θ_A; Throttle ← θ_high ≤ S_ind < θ_B.
  - `decide()` is the pure mapping, for the tests and Experiment G.
  - Per-member S_coll_u and A_u come from the v3 group score. θ_B and θ_C / θ_A are the calibrated values
    carried in the records.
  - D3 records: S_ind ≥ θ_high → Terminate, group ≥ θ_coll → Restrict for every member.
- **Lifecycle** stays sticky: a more severe action replaces a milder one, and a milder one never downgrades.
  - Different evidence that does not warrant more is recorded as "evidence" (audit).
  - Per-action TTL [IMPL]: Terminate / Restrict 24 h, StepUp 1 h, Throttle 15 min.
  - Analyst release and grace as before; a policy Block only ends when the policy is lifted.
- **Version triple** (v_model, v_policy, ψ) on every decision.
  - ψ is a global, strictly increasing counter (`pdp:psi`), seeded from the clock in ms, so a flushed store
    cannot re-issue an old ψ.
  - v_policy = hash of the policy-C snapshot.
- **Gateway** (`services/gateway/decisions.DecisionView`, `app.py`):
  - ψ ordering: a decision is applied only if ψ > ψ_cur(u); older ones are rejected and counted in `/internal/stats`.
  - Terminate / Block → 403, session ended, logins refused.
  - Restrict → 403 on s = 1 only.
  - StepUp → 401 `step_up` and the session ends; a session created after the decision passes.
  - Throttle → 429 above `AS_THROTTLE_PER_MINUTE` (20, [IMPL]).
  - **Decision TTL**: the PDP writes a heartbeat (`pdp:alive`) each tick. When it is older than
    `AS_PDP_DECISION_TTL` (30 s, [IMPL]), s = 0 fails open and s = 1 fails closed (Restrict), with a
    per-application override `AS_PDP_FALLBACK_APPS` (open / closed). An applied Terminate / Block stays in force.
    `/readyz` reports the PDP state. Redis down still fails closed (sessions live in Redis).
- PDP service: sessions are revoked for Terminate / Block only (Restrict / StepUp / Throttle keep them).
- Incidents and the dashboard show the action. Schema v4: `pdp_decisions.decision` VARCHAR(8) → (16), because
  "Terminate" did not fit and the audit insert would have blocked the outbox. `init_db` now has idempotent
  in-place upgrades; applied live.
- Tests: Eq. 21 table (10 cases incl. boundaries), Throttle → Terminate escalation without downgrade, operator
  θ_high > θ_B, per-member Restrict / StepUp, ψ monotonicity across a flushed counter, gateway Restrict /
  StepUp / Throttle / Terminate / stale ψ / TTL fallback with overrides. Full suite: 214 passed, 4 skipped.

Verified live (`smoke_pdp.py actions`, fresh `syn-act-*` hr_manager accounts, scores injected on Kafka):
- 12/12 on three consecutive runs. The first run after a redeploy had one failed check that was not captured.
- Group score → Restrict for the core member (A_u 0.80 ≥ θ_A) and StepUp for the peripheral one (A_u 0.10).
  - s = 0 allowed, s = 1 refused.
  - StepUp: 401, then signing in again passes.
- A stale lower-ψ Terminate written into the cache is not applied.
- Throttle (S_ind 0.07) → escalation to Terminate (0.40); logins refused; audit Throttle then Terminate.
- Rate limit: 20 × 200 then 5 × 429.
- The smoke reported score → decision cache ≈ 0.51 s in every run. **That was the measuring tool, not the PDP.**
  The smoke created a new idempotent Kafka producer per injected score, so connection set-up and InitProducerId
  were inside the timed interval. With one warmed producer (`evaluation/live.py`) the same path measured p50
  12 ms / max 35 ms (20 cases, while Experiment A was training; not a reported result). Experiment G reports it.

`smoke_pdp.py fallback` with the PDP container stopped 35 s: 4/4. The gateway reports the PDP unavailable,
login works, payroll (s = 1) 403, directory (s = 0) 200. After restart `/readyz` shows the PDP ok.

Regression after Phase 7 (demo state reset):
- **Enforce mode:** `demo_new_user` 8/8; `smoke_pdp individual` 9/9, expiry 4/4, policy 3/3;
  `smoke_v3_stream` order 7/7, windows 5/5; `smoke_tgn` cmu 5/5, admit 4/4.
  - `smoke_pdp group --index 5` 3/3: the live v3 TGN flagged split_exfiltration-test-1, and each member got its
    Eq. 21 action.
- **PDP shadow mode**, because these scripts deliberately send suspicious traffic: `smoke_gateway` 29/29,
  `smoke_streaming` verify 10/10, poison 3/3, `smoke_escalation` a 1/1, b 3/3, c 3/3.
  - In enforce mode, the v3 TCN (θ_B 0.085, where D3 used 0.6) terminated alice, bob and carol during
    `smoke_gateway`'s ACL-deny / unknown-resource probes, so those checks could not log in. That is the detector
    working, not a regression.
- `smoke_escalation b` was updated for v3: Path B arrives from the FINAL resource window, keyed by r*
  (`default/finance:audit-reports`). The check now waits up to 20 min (Δres = 15 min + grace); the delay itself is measured in Experiment C.
- Fixed: the lag monitor still measured the escalation group against `access-events-by-resource`, which it no
  longer consumes since Phase 5 (a stale, ever-growing "lag" of 134 k). It now measures `resource-windows`.

Findings (measured, not changed — team decision):
- **A refused sensitive request under Restrict escalates to Terminate through the live TCN.**
  - In every run, the core member's single refused payroll request scored S_ind 0.33–0.50 (≥ θ_B) a moment later.
  - PDP-caused denials enter the event stream as y = 0 like ACL denials (v3 Eq. 1).
  - So, in practice, Restrict becomes Terminate as soon as the user touches a sensitive resource. Excluding
    PDP-caused denials (`meta.reason` = pdp-*) from the TCN's y would change that; v3 does not say.
- A fresh hr_manager account reading payroll right after creation scored 0.051 (> θ_high 0.048) → Throttle.
  This is cold-start behaviour of the calibrated thresholds.
- An injected S_ind 0.40 also triggered a Path A escalation; the v3 TGN found a group around the account and its
  StepUp evidence was recorded under the active Terminate (no downgrade), as designed.

Not done / limits:
- StepUp is a password re-authentication: the prototype has no second factor.
- The Throttle rate, the per-action TTLs and the decision TTL are [IMPL] values; v3 gives none.
- ψ_cur lives in each gateway process's memory, so a restarted gateway starts from the stored decision.

### 2026-11-15/16 — Revised v3 Phase 8 (Deliverable 4 experiments A–H)

All results are in `docs/D4_EVALUATION.md`, written from the generated tables in `evaluation/results/tables/`.
- **Offline (synthetic):** A and B (5 seeds each), E/F.
- **Live:** C/D, G and H on the running stack, with no training on the machine.

Main measured findings (details and numbers in D4):
- **A:** on this data the v3 TCN ranks below the D3 TCN, the LSTM and the supervised-only TCN. The auxiliary
  self-supervised heads are what costs it (ablation: PR-AUC 0.714 → 0.872 without them).
- **B:** the TGN and TCN together detect 61 of 75 test groups. The TGN has almost no false alarms on benign
  collaboration.
- **E/F:** the deployed budget B_W is never binding.
- **G:** Eq. 21 is exact (200/200).
- **H:** no loss and no duplicates under crashes. The TCN takes about 34 s to recover (dynamic group
  membership).

Defects found by the experiments and fixed (each with a test):
- **Gateway OOM-killed by parallel password hashing.**
  - Argon2id with argon2-cffi's defaults allocates 64 MiB per check, and the services ran up to 40 checks in
    parallel. 45 test logins exhausted the gateway's 384 MB. About six concurrent unauthenticated logins could do
    the same, so it was also a denial-of-service path.
  - Fix: `security.py` bounds Argon2 operations per process (`AS_PASSWORD_HASH_CONCURRENCY`, default 2).
  - Live check: 45 concurrent logins succeeded 45/45 in 4.7 s, with the gateway at 83 MiB.
- **PDP decision path stalled by session-revocation retries while the gateway was down.**
  - Fix: `pdp_service.retry_revocations` stops after the first failure and backs off (1 s doubling to 30 s).
- **Escalation service OOM-killed by its background sweep.**
  - The sweep loaded 24 h of events and scores from HDFS as full objects.
  - Fix: `hdfs_lake` streams record batches and reads only the needed columns (`iter_event_rows`,
    `read_records(fields=…)`), and `lake_loader` keeps only what the Path C rules read. The sweep candidates are
    unchanged (test).
  - A forced sweep over the same day peaks at 103.6 MiB instead of exhausting 384 MB.
- **HDFS sink OOM-killed by compaction.**
  - `HistoryLake.compact` concatenated a whole partition into Python dicts. The sink re-merges today's partition
    whenever it passes 24 part files, so after a day of load it died.
  - Fix: a streaming merge (record batches, a unified schema, dedup by id; test with mixed schemas and duplicates).
  - Live check: the restarted sink compacted the 108,144-row risk-score partition at about 233 MiB and drained a
    backlog of more than 44 k records.
- **Redis close to its limit.**
  - Redis used 243 of its `maxmemory` 256 MB under `noeviction`, where reaching the limit stops every Redis write
    (gateway, PDP, pipeline).
  - 508 k keys were the HDFS sink's dedup markers, kept for 24 h. They are only a first line of dedup: reads and
    compaction remove duplicates by id.
  - Fix: `dedup_ttl_seconds` reduced from 86,400 to 900 s (about 90 k keys at 50 req/s). The old markers were
    deleted.
- **Dashboard API OOM-killed by the per-user risk page.**
  - The page parsed every risk score of the last 24 h, for all users, to keep one user's.
  - Fix: `read_records(keys=…)` filters on the stored Kafka key (the user) before parsing (test).
  - `smoke_dashboard` 30/30.
- **Dashboard collusion graph broken under the v3 TGN.**
  - The endpoint called the D3 scorer's `gp`, `edge_scores` and `analyse`, which the v3 scorer does not have.
  - Fix: for a v3 model it shows the live neighbourhood (without user-user application edges) and the user's latest
    v3 collective result (S_coll, A_u, θ_C, θ_A).
  - Live: `demo_new_user` 8/8 (a new account is terminated after 2 attack requests, 0.4 s; incident opened).
- **Test harness (evaluation/ only):**
  - `run_live.sh` pre-flight and restart checks;
  - INVALID marking of failed C runs;
  - a retry for the test tool's Kafka watermark read;
  - escalation latency paired with the escalation's own triggering score;
  - the fallback probe's user put under an analyst grace.

Discarded and archived (not reported):
- the first live run of C and G (gateway down);
- the C runs before the escalation fix;
- one confounded fallback probe.

The crashed first run left `pdp.block_ttl` at 60 s; it was restored to 86,400 s.

Not changed (decisions for the team):
- TCN workers use dynamic group membership (about 34 s takeover after a crash); static membership would remove it.
- A paused (silent) Redis makes gateway requests hang until the client times out. Nothing is allowed meanwhile,
  but the gateway does not answer 503 at once either.
- No restart policy in `deploy/docker-compose.yml`: a crashed service stays down.
- Per-user TCN state (`tcn3:state:<u>`) is about 22 KB per user and has no TTL, by design: it is the user's
  history window. About 10,000 users would need more than Redis's 256 MB, so Redis has to be sized to the number of
  users.
- Compaction still re-reads today's whole partition each time it passes 24 files. Memory is now bounded, but the
  I/O grows during the day; compacting closed days only would avoid it.

### Remaining

Phase 9: analyst confirmation in the UI (see Pending above). Phase 10 of `docs/PHASE1_AUDIT.md` §7.
