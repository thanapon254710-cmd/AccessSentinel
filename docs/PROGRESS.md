# AccessSentinel — Implementation Progress Log

Running record of completed work, test results and remaining work. Status labels are defined in `docs/PHASE1_AUDIT.md`.

## Phase status

| Phase | Status |
|---|---|
| 1 Audit and architecture | Done (2026-11-11) |
| 2 Infrastructure | Done (2026-11-11) — verified live |
| 3 Gateway + protected apps | Not started |
| 4 Kafka streaming + HDFS | Not started |
| 5 TCN | Not started (library model exists; see audit D1, D2, D10) |
| 6 Four-path escalation | Not started (library logic exists) |
| 7 TGN | Not started (library model exists; see audit D8, D9) |
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

### Remaining

Phases 3–10 of `docs/PHASE1_AUDIT.md` §7. Open audit defects: D1–D4, D6, D8–D14.
