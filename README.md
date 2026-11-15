# AccessSentinel — Deliverable 3 reference implementation

> New here? Start with the plain-language overview: **[PROJECT_SUMMARY.md](PROJECT_SUMMARY.md)**.

Implements the D3 architecture (Steps 1–10) as a runnable, tested Python package.
The deployed Docker stack uses real Kafka, HDFS, PostgreSQL and Redis; the in-process reference pipeline
(`pipeline.py`, `run_demo.py`) uses stand-ins that keep the same contracts (see "Not real yet").

| D3 step / component | Module |
|---|---|
| Step 1 event tuple (Eq. 1), Gateway | `events.py`, `schema.py`, `gateway.py` (simulator model), `services/gateway/` (deployed) |
| Step 2 Kafka, user-keyed + resource-keyed views, composite key | `broker.py` (in-process), `streaming/` (deployed: consumer framework, repartitioner) |
| Step 3 historical store (HDFS) | `storage.py` (in-process), `adapters/hdfs_lake.py` + `streaming/hdfs_sink.py` (deployed), `history.py` (CLI) |
| Step 4 TCN, sliding window X_u (Eqs. 2–3), d=50, L=20 | `tcn.py` (model); in-process: `features.py`, `state_store.py`; deployed: `ml/` (features, dataset, training, registry), `streaming/tcn_worker.py` |
| Step 5 tiers, T_recheck | `risk.py` (in-process), `streaming/tcn_worker.py` (deployed) |
| Step 6 four-path escalation (Eq. 4) | `escalation.py` (in-process); deployed: `escalation_engine.py`, `streaming/escalation_service.py`; evaluation: `ml/escalation_eval.py` |
| Step 7 graph (users/resources/apps; access, membership, co-access) + TGN | `tgn.py` (model); in-process: `graph.py`; deployed: `ml/tgn_graph.py`, `ml/tgn_train.py`, `ml/train_tgn.py`, `ml/tgn_registry.py`, `streaming/graph_index.py`, `streaming/tgn_worker.py` |
| Step 8 PDP Allow/Block, request- and session-time enforcement | `pdp.py`, `gateway.py` (in-process); deployed: `pdp_engine.py`, `streaming/pdp_service.py`, `pdp_admin.py`, gateway decision cache |
| Step 9 feedback loop (analyst-verified only), dashboard | `feedback.py` (in-process); deployed: `incidents.py` + `streaming/incident_service.py`, `services/api/` (dashboard API), `dashboard/` (React + TypeScript UI), `ml/feedback_data.py`, `ml/retrain.py`, `runtime_settings.py` |
| Step 10 capacity equations (8)–(12) | `scaling.py` |
| Orchestration | `pipeline.py` |
| Synthetic data with ground truth | `simulator.py` (v1, in-process demo), `datasets/synth.py` (v2, training/evaluation) |

All hyper-parameters are in `config.py`; each is tagged `[D3]` (taken from the paper) or `[IMPL]` (a choice D3 leaves open).

## Project status and infrastructure
- Implementation status, defects and roadmap: `docs/PHASE1_AUDIT.md`; running log: `docs/PROGRESS.md`.
- Docker stack (Kafka, HDFS, PostgreSQL, Redis) and Windows setup: `docs/SETUP_WINDOWS.md`. Short version:

      copy .env.example .env        # then set the two passwords
      docker compose up -d --build
      docker compose run --rm tools # live check: Kafka, HDFS, PostgreSQL, Redis
      # gateway + 3 protected demo apps: http://localhost:8000 (users/password: docs/SETUP_WINDOWS.md §5)
      # security operations dashboard:   http://localhost:8080 (analyst1 / secadmin, AS_ANALYST_PASSWORD)

## Run on another computer

Everything runs in Docker, so another computer needs only **Docker Desktop** (Windows, macOS or Linux; about
8 GB of memory for Docker, about 10 GB of free disk). The trained, promoted models are included (`models/`,
about 2.5 MB), so no training is needed.

**Easiest (Windows):** unzip the folder, start Docker Desktop, double-click **`start.bat`**. On first run it
creates `.env` with random passwords, builds and starts everything (10-20 minutes the first time), waits until
all services are healthy and opens the dashboard. `stop.bat` stops it. macOS/Linux: `sh start.sh`.

**Sharing as a ZIP:** make the ZIP from Git, not by zipping the folder, so your own `.env` (your passwords),
`.venv`, `node_modules` and local data are never included:

```powershell
git archive --format=zip -o AccessSentinel.zip HEAD
```

**Manually, from GitHub:**

```powershell
git clone https://github.com/thanapon254710-cmd/AccessSentinel.git
cd AccessSentinel
copy .env.example .env          # macOS/Linux: cp .env.example .env
# edit .env: replace every change-me with your own random value
docker compose up -d --build    # first time 10-20 min (downloads images, builds the app and dashboard)
docker compose ps               # wait until every service is healthy
```

Then open http://localhost:8080 (dashboard: `analyst1` / `secadmin`, password `AS_ANALYST_PASSWORD`) and
http://localhost:8000 (protected apps: demo users such as `alice`, password `AS_DEMO_USER_PASSWORD`). The new
installation starts empty (no history, incidents or extra accounts); the demo in `PRESENTATION_DEMO.md` works
the same way. Check it with `docker compose run --rm -T tools python scripts/smoke_dashboard.py`.

**Using one running installation from other computers on the same network:** open `http://<that PC's IPv4
address>:8080` and `:8000` (find the address with `ipconfig`; allow Docker through the firewall on private
networks only). Only the gateway (8000) and the dashboard (8080) are reachable from the network. Kafka, HDFS,
PostgreSQL and Redis listen on `127.0.0.1` only, because Kafka and HDFS have no authentication. Do not expose
8000/8080 to the internet: this is a research prototype served over plain HTTP.

## Run
    pip install -r requirements.txt   # python>=3.10; torch CPU build is enough
    pytest -q tests
    python run_demo.py --quick     # a few minutes on CPU
    python run_demo.py             # fuller training

## Not real yet
- Kafka/HDFS are in-process stand-ins (`broker.py`, `storage.py`). Swap in `confluent-kafka` / Flink or Kafka Streams / pyarrow-HDFS behind the same interfaces.
- Data is synthetic. Numbers from `run_demo.py` say the code works, not that the method works on real logs.
- TGN is the D3/[14] dense GCN+GRU+attention variant, not Rossi et al.'s memory-based TGN (that is the Revised version's CMU/SGE split).

## D3 gaps filled by implementation choices (confirm with the team)
1. TCN kernel size (2), dense width, focal-loss gamma/alpha, epochs.
2. T_recheck = 600 s (D3 Step 5: wall-clock interval initialised to 10 min). A forced re-score of an unchanged window would return the same score, so `window_at()` refreshes the newest inter-arrival feature to "time since last event".
3. Sweep selection rule (sensitive resources touched by >= 4 distinct users over T_sweep) and sampling (exactly max(1, round(gamma*N)) users).
4. Group g(u) = connected component of users sharing >= 2 sensitive resources; S_coll = mean of the top 25% edge scores over the group; users with no coupled group get no S_coll.
5. theta_coll (PDP) and S_coll TTL are not in D3.
6. TCN scores are Platt-calibrated on the validation fold so the fixed 0.4/0.6 thresholds are meaningful.
7. TGN is trained supervised (BCE, edge labels = analyst-confirmed collusion) since D3 does not specify the objective.

## Real Kafka + real HDFS (all free / open source)
Adapters: `accesssentinel/adapters/kafka_broker.py` (confluent-kafka) and `adapters/hdfs_store.py` (WebHDFS + Parquet).
They plug into the same pipeline (`AccessSentinel(..., store=..., broker=...)`).

1. Start the services (see `docs/SETUP_WINDOWS.md`; `.env` must exist):

       docker compose up -d --build

2. HDFS from your host: WebHDFS redirects writes to the datanode by hostname, so add this line to your hosts file
   (`/etc/hosts`, or `C:\Windows\System32\drivers\etc\hosts`): `127.0.0.1 datanode`. Check the UI at http://localhost:9870.

3. Install and run:

       pip install -r requirements.txt
       python run_demo.py --kafka localhost:9092 --hdfs http://localhost:9870

   `--kafka` and `--hdfs` are independent; omit either to keep the in-process stand-in for that part.

**Status:** both adapters were run against the live Docker stack by `scripts/check_infra.py` (Kafka: per-user
partitioning/order and resource-view co-location; HDFS: Parquet write/read, compaction, feedback write). The
deployed system (Phases 4-9) uses its own consumer framework with manual offset commits (audit D4 fixed) and keeps
TCN window state in Redis, so restarts do not lose it; see `docs/PROGRESS.md`. In `run_demo.py --kafka/--hdfs`
the TCN window state is still in-process.
