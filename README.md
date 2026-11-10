# AccessSentinel — Deliverable 3 reference implementation

Implements the D3 architecture (Steps 1–10) as a runnable, tested Python package.
Kafka and HDFS are replaced by in-process stand-ins that keep the same contracts (see "Not real yet").

| D3 step / component | Module |
|---|---|
| Step 1 event tuple (Eq. 1), Gateway | `events.py`, `gateway.py` |
| Step 2 Kafka, user-keyed + resource-keyed views, composite key | `broker.py` |
| Step 3 historical store (HDFS) | `storage.py` |
| Step 4 TCN, sliding window X_u (Eqs. 2–3), d=50, L=20 | `features.py`, `state_store.py`, `tcn.py` |
| Step 5 tiers, T_recheck | `risk.py` |
| Step 6 four-path escalation (Eq. 4) | `escalation.py` |
| Step 7 graph (users/resources/apps; access, membership, co-access) + TGN | `graph.py`, `tgn.py` |
| Step 8 PDP Allow/Block, request- and session-time enforcement | `pdp.py`, `gateway.py` |
| Step 9 feedback loop (analyst-verified only) | `feedback.py` |
| Step 10 capacity equations (8)–(12) | `scaling.py` |
| Orchestration | `pipeline.py` |
| Synthetic data with ground truth | `simulator.py` |

All hyper-parameters are in `config.py`; each is tagged `[D3]` (taken from the paper) or `[IMPL]` (a choice D3 leaves open).

## Project status and infrastructure
- Implementation status, defects and roadmap: `docs/PHASE1_AUDIT.md`; running log: `docs/PROGRESS.md`.
- Docker stack (Kafka, HDFS, PostgreSQL, Redis) and Windows setup: `docs/SETUP_WINDOWS.md`. Short version:

      copy .env.example .env        # then set the two passwords
      docker compose up -d --build
      docker compose run --rm tools # live check: Kafka, HDFS, PostgreSQL, Redis

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

**Sta**Status (2026-11-11):** both adapters have now been run against the live Docker stack by `scripts/check_infra.py`
(Kafka: per-user partitioning/order and resource-view co-location; HDFS: Parquet write/read, compaction, feedback write).
The full `run_demo.py --kafka/--hdfs` pipeline has not yet been run live; that is part of Phase 4.

: user-keyed partitioning relies on Kafka's default key partitioner; offsets auto-commit, which can commit before
processing finishes (a crash may skip events; fixed in Phase 4, audit D4). The keyed TCN window state is still in-process; with several consumer processes, each owns the
partitions (users) Kafka assigns it, but state is lost on restart until you move it to Flink/Kafka Streams/Redis.
Without a state backend, the first events after a restart are scored on short windows.
