# AccessSentinel — Windows setup

Tested on Windows 11, Docker Desktop 28.3 (WSL 2 backend), Python 3.11, on 2026-11-11.

## 1. Prerequisites

| Tool | Version used | Needed for |
|---|---|---|
| Docker Desktop (WSL 2 backend) | 28.3.3 | Kafka, HDFS, PostgreSQL, Redis, app image |
| Python | 3.11 | Unit tests, training, host-side scripts |
| Git | 2.46 | |
| Node.js / npm | 24 / 11 | Dashboard (Phase 9) |

**Docker memory.** The Phase 2 stack is capped at about 3.5 GB (Kafka 1 GB, NameNode 768 MB, DataNode 768 MB,
PostgreSQL 512 MB, Redis 384 MB). Later phases add the model services; plan for 6–8 GB. With the WSL 2 backend,
Docker gets 50 % of RAM by default (8 GB on a 16 GB machine), which is enough. To change it, create
`%UserProfile%\.wslconfig` with:

```ini
[wsl2]
memory=8GB
```

and run `wsl --shutdown`, then restart Docker Desktop.

## 2. One-time configuration

Run all commands from the repository root (the folder containing `README.md`).

```powershell
copy .env.example .env
```

Open `.env` and replace every `change-me` value (`AS_POSTGRES_PASSWORD`, `AS_REDIS_PASSWORD`, `AS_UPSTREAM_TOKEN`,
`AS_INTERNAL_TOKEN`, `AS_DEMO_USER_PASSWORD`) with random strings, for example:

```powershell
python -c "import secrets; print(secrets.token_urlsafe(24))"
```

`.env` is git-ignored. Never commit it. `COMPOSE_FILE` in `.env` lets plain `docker compose` commands find
`deploy/docker-compose.yml`, so no `-f` flag is needed.

## 3. Start the stack

```powershell
docker compose up -d --build
```

This builds one image (`accesssentinel/app:dev`, CPU PyTorch) and starts:

| Service | Purpose | Host port |
|---|---|---|
| `kafka` | Apache Kafka 3.9 (KRaft, single node) | `localhost:9092` |
| `kafka-init` | Creates every topic in `accesssentinel/topics.py`, then exits | — |
| `namenode` | HDFS NameNode, WebHDFS and web UI | http://localhost:9870 |
| `datanode` | HDFS DataNode (WebHDFS data transfer) | `localhost:9864` |
| `postgres` | PostgreSQL 16 (operational state) | `localhost:5432` |
| `redis` | Redis 7 (shared state, decision cache; AOF on, no eviction) | `localhost:6379` |
| `db-init` | Creates the PostgreSQL schema and the demo catalog/users, then exits | — |
| `app-hr`, `app-finance`, `app-docs` | Protected demo applications (internal network only) | — |
| `gateway` | Proxy / Access-Control Gateway: login, first-line ACL, PDP decision cache, Kafka events | http://localhost:8000 |
| `repartitioner` | Builds the resource-keyed view `access-events-by-resource` | — |
| `hdfs-sink` | Writes access events, scores, decisions and feedback to HDFS (Parquet) | — |
| `tcn-worker` (×2) | TCN individual risk scores -> `risk-scores` (needs a trained model) | — |
| `escalation` | Four-path escalation (A high, B resource, C sweep, D sample) -> `escalations.*` | — |
| `graph-index` | Temporal edge index in Redis for TGN neighbourhoods | — |
| `tgn-worker-rt`, `tgn-worker-bg` | TGN collusion analysis of escalated users -> `group-scores` (needs a trained TGN) | — |

Containers talk to each other by service name (`kafka:29092`, `namenode:9870`, `postgres`, `redis`).
Data lives in named Docker volumes and survives restarts.

Check status:

```powershell
docker compose ps
```

## 4. Verify the infrastructure

Recommended: run the check inside the Docker network:

```powershell
docker compose run --rm tools
```

Expected result: four `PASS` lines (kafka, hdfs, postgres, redis) and `4/4 checks passed`. Each check does a
real round trip through the same adapters the pipeline uses, then deletes its temporary data.

## 5. Use the protected applications

Open http://localhost:8000 and sign in. Every demo user has one identity across the three applications:

| User ID | Role | Notable access |
|---|---|---|
| `alice` | hr_manager | HR employees, payroll (incl. export), reviews, recruiting |
| `bob`, `judy` | finance_analyst | Finance invoices, budget, payments, vendor banking (read) |
| `carol`, `dave`, `ivan` | staff | HR directory, handbook, policies only |
| `erin` | legal | Contracts, IP filings |
| `frank` | executive | Board minutes, budget, audit reports |
| `grace` | auditor | Audit reports, payments, payroll (read) |
| `henry` | it_admin | Policies and handbook (write) |

The password for all demo users is the value of `AS_DEMO_USER_PASSWORD` in your `.env`.
The start page lists every resource, whether it is sensitive, and your permissions on it. Try links you are
not allowed to open as well: denied requests are recorded too. URL forms: `/apps/<app>/<resource>`,
`/apps/<app>/<resource>/<id>`, `/apps/<app>/<resource>/export`.

End-to-end check (logs in as three users, verifies every resulting Kafka event and the audit log):

```powershell
.venv\Scripts\python scripts\smoke_gateway.py
```

Watch events arrive in Kafka:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic access-events --property print.key=true
```

### Historical data in HDFS

Run these inside Docker (they reach HDFS by service name):

```powershell
docker compose run --rm tools python -m accesssentinel.history partitions
docker compose run --rm tools python -m accesssentinel.history user alice --hours 24
docker compose run --rm tools python -m accesssentinel.streaming.lag
```

Browse files in the HDFS UI: http://localhost:9870 -> Utilities -> Browse the file system -> `/accesssentinel/events`.

Streaming end-to-end check (traffic through the gateway, then HDFS + resource-view verification):

```powershell
docker compose run --rm -T tools python scripts/smoke_streaming.py traffic --n 120 > run.json
Get-Content run.json | docker compose run --rm -T tools python scripts/smoke_streaming.py verify
```

### Train the TCN (individual risk model)

The workers only score with a trained, promoted model. Train on the host (about 40 minutes on a 20-thread CPU;
add `--cv-folds 0` to skip cross-validation, about 15 minutes):

```powershell
.venv\Scripts\python -m accesssentinel.ml.train_tcn --model-dir models --promote
```

This writes `models\tcn\<version>\` (git-ignored; mounted read-only into the workers, which pick up a newly
promoted version within 30 s) and an evaluation report in `experiments\results\<version>.md`. All training
data is synthetic (`accesssentinel/datasets/synth.py`).

Replay held-out synthetic users through the live pipeline and check the workers (parity + detection):

```powershell
docker compose run --rm -T tools python scripts/replay_synthetic.py publish --normal 30 --reset > replay.json
Get-Content replay.json | docker compose run --rm -T tools python scripts/replay_synthetic.py verify
```

### Escalation (which users go to the graph analysis)

Each path can be triggered on its own (Path D needs the sampling rate raised for the test, then restored):

```powershell
docker compose run --rm -T tools python scripts/smoke_escalation.py a
docker compose run --rm -T tools python scripts/smoke_escalation.py b
docker compose run --rm -T tools python scripts/smoke_escalation.py c
$env:AS_ESC_GAMMA="1"; docker compose up -d escalation
docker compose run --rm -T tools python scripts/smoke_escalation.py d
Remove-Item Env:AS_ESC_GAMMA; docker compose up -d escalation
```

Run a sweep (Path C) at any reference time over the HDFS history:

```powershell
docker compose run --rm tools python -m accesssentinel.streaming.escalation_service sweep-now --at 2026-10-03T18:00 --publish
```

Offline evaluation of the policy, including a kappa sensitivity study (about 5 minutes; needs a trained TCN):

```powershell
.venv\Scripts\python -m accesssentinel.ml.escalation_eval --model-dir models --kappas 3,6,10,15
```

### Train the TGN (collusion model)

```powershell
.venv\Scripts\python -m accesssentinel.ml.train_tgn --model-dir models --promote
```

Two-stage training plus two ablations on the synthetic collusion-study dataset; about 1-2 hours on an idle
20-thread CPU (much longer while the Docker stack is busy). Report in `experiments\results\tgn-<version>.md`.
The TGN workers pick up a promoted model within 30 s.

Live check (replays a held-out group or a benign team meeting through the pipeline and compares with offline):

```powershell
docker compose run --rm -T tools python scripts/smoke_tgn.py collusion --index 5
docker compose run --rm -T tools python scripts/smoke_tgn.py team
```

## 6. Host-side Python environment (tests, training)

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python -m pytest -q tests
```

PyTorch: `requirements/core.txt` lists `torch`; on Windows, `pip install torch` installs a CPU build.

Run the live checks from the host (Kafka, PostgreSQL, Redis):

```powershell
.venv\Scripts\python scripts\check_infra.py --only kafka,postgres,redis
```

**HDFS from the host is optional.** WebHDFS answers a write by redirecting the client to the DataNode by
its hostname, `datanode`, which Windows cannot resolve. Inside Docker this works without changes. To use HDFS
from host Python, add this line to `C:\Windows\System32\drivers\etc\hosts` (needs an administrator editor):

```
127.0.0.1 datanode
```

## 7. Stop, restart, reset

```powershell
docker compose stop            # stop, keep containers and data
docker compose up -d           # start again
docker compose down            # remove containers, keep data volumes
docker compose down -v         # remove containers AND all data (Kafka, HDFS, PostgreSQL, Redis)
```

## 8. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `error during connect ... dockerDesktopLinuxEngine` | Docker Desktop is not running. Start it and wait until it says *Engine running* |
| `Set AS_POSTGRES_PASSWORD in .env` | `.env` is missing or a password is empty (§2) |
| `bind: address already in use` | Another program uses 9092 / 9870 / 9864 / 5432 / 6379. Stop it, or change the left side of the `ports:` mapping in `deploy/docker-compose.yml` and the matching `AS_*_PORT` in `.env` |
| `topics not provisioned` in the Kafka check | `kafka-init` did not run or failed: `docker compose logs kafka-init`, then `docker compose up kafka-init` |
| HDFS check fails mentioning `datanode` from the host | Expected without the hosts-file entry (§5). Run the check in Docker instead |
| Containers restart or are killed | Not enough Docker memory (§1) |
| Demo login fails with *Invalid user ID or password* | The password is `AS_DEMO_USER_PASSWORD` as it was when `db-init` first ran. After changing it: `docker compose run --rm db-init python -m accesssentinel.db.init_db --seed-demo --reset-demo-passwords` |
| `Too many failed attempts` | 5 failed logins lock that user ID for 15 minutes |
| `hdfs-sink` or `repartitioner` unhealthy | `docker compose logs hdfs-sink`; the health check fails when the consume loop has not run for 60 s |
| Dead-lettered records | Inspect `<topic>.dlq` with the console consumer and `--property print.headers=true` |
| Slow file operations | The repository is inside OneDrive. Consider pausing OneDrive sync for `.venv\` |
