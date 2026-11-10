"""Train TCN + TGN on the first half of a synthetic log, then run the full pipeline on the second half.
Usage: python run_demo.py [--quick]"""
import argparse, shutil, tempfile, time
import numpy as np
from accesssentinel.config import Config
from accesssentinel.graph import NeighborhoodBuilder
from accesssentinel.metrics import prf
from accesssentinel.pipeline import AccessSentinel
from accesssentinel.simulator import SimConfig, simulate, tcn_dataset, tgn_training_samples
from accesssentinel.storage import HistoricalStore
from accesssentinel.tcn import TCNScorer, cross_validate_tcn
from accesssentinel.tgn import TGNScorer, train_tgn

ap = argparse.ArgumentParser(); ap.add_argument("--quick", action="store_true")
ap.add_argument("--kafka", default=None, help="bootstrap servers, e.g. localhost:9092 (real Kafka)")
ap.add_argument("--hdfs", default=None, help="WebHDFS URL, e.g. http://localhost:9870 (real HDFS)")
ap.add_argument("--prefix", default=None, help="topic/group prefix (default: unique per run)")
ap.add_argument("--tcn-epochs", type=int, default=None); ap.add_argument("--tgn-epochs", type=int, default=None)
args = ap.parse_args()
cfg = Config()
tcn_ep = args.tcn_epochs or (6 if args.quick else 15)
tgn_ep = args.tgn_epochs or (15 if args.quick else 60)
folds = 1 if args.quick else 2

t0 = time.time()
sim = simulate(SimConfig())
train_ev = [e for e in sim.events if e.t < sim.split_t]; test_ev = [e for e in sim.events if e.t >= sim.split_t]
print(f"events={len(sim.events)} train={len(train_ev)} test={len(test_ev)} users={len(sim.users)}")
print(f"train attackers={len(sim.train_attackers)} test attackers={len(sim.test_attackers)} groups train={sim.train_groups} test={sim.test_groups}")

print("\n== Train TCN (focal loss, Adam 1e-3, K-fold) ==")
X, y = tcn_dataset(train_ev, sim.truth, cfg.tcn)
print(f"windows={X.shape} positives={int(y.sum())}")
model, cv, test_auc = cross_validate_tcn(X, y, cfg.tcn, max_folds=folds, epochs=tcn_ep)
print("CV:", cv, f"held-out test AUC={test_auc:.3f}")

print("\n== Train TGN (2xGCN + GRU + attention, first 50% train / rest test) ==")
builder = NeighborhoodBuilder(cfg.graph)
samples = tgn_training_samples(train_ev, sim.truth, builder, sim.split_t, window=cfg.graph.window)
tgn_model, info = train_tgn(samples, cfg.tgn, cfg.graph, epochs=tgn_ep)
print(info)

print("\n== Run pipeline on the unseen second half ==")
import uuid
tmp = tempfile.mkdtemp()
if args.hdfs:
    from accesssentinel.adapters.hdfs_store import WebHdfsStore, make_client
    store = WebHdfsStore(make_client(args.hdfs), base=f"/accesssentinel/{uuid.uuid4().hex[:6]}")
else:
    store = HistoricalStore(tmp)
broker = None
if args.kafka:
    from accesssentinel.adapters.kafka_broker import KafkaDualViewBroker
    broker = KafkaDualViewBroker(args.kafka, cfg.stream, prefix=args.prefix or f"as{uuid.uuid4().hex[:6]}")
sys_ = AccessSentinel(cfg, TCNScorer(model), TGNScorer(tgn_model, cfg.tgn, cfg.graph), store, broker=broker)
sys_.warmup(train_ev)
print(f"storage={'HDFS' if args.hdfs else 'local'} stream={'Kafka' if args.kafka else 'in-memory'}")
sys_.run_stream(test_ev) if args.kafka else sys_.run(test_ev)

truth_mal = set(sim.test_attackers) | {m for g in sim.test_groups for m in sim.groups[g]}
blocked = {u for u in sys_.first_block if sys_.first_block[u][0] >= sim.split_t}
print("\n-- Individually anomalous (test attackers) --")
for u in sim.test_attackers:
    ep = [(a, b) for a, b in sim.attackers[u] if a >= sim.split_t]
    fb = sys_.first_block.get(u)
    if fb:
        evs = [e for e in test_ev if e.u == u and sim.truth[e.eid][0] == 1]
        after = sum(1 for e in evs if e.t > fb[0])
        print(f" {u}: blocked after {(fb[0]-ep[0][0])/60:.1f} min ({fb[1]}); attack events after block: {after}/{len(evs)}")
    else: print(f" {u}: NOT blocked")
print("-- Colluding groups (members individually below theta_high unless noted) --")
for g in sim.test_groups:
    for m in sim.groups[g]:
        fb = sys_.first_block.get(m)
        print(f" {g}/{m}: " + (f"blocked ({fb[1]})" if fb else "NOT blocked"))
pred = [int(u in blocked) for u in sim.users]; true = [int(u in truth_mal) for u in sim.users]
print("\nuser-level (test period):", prf(true, pred))
fps = sorted(blocked - truth_mal); print("false-positive users:", fps)
s = sys_.stats; W = max(s["windows"], 1)
print(f"\nstats: {s}")
print(f"avg escalated per window (all paths): {sum(s['escalated'].values())/W:.2f} of {len(sim.users)} users; TGN calls={s['tgn_calls']}")
print(f"done in {time.time()-t0:.0f}s"); shutil.rmtree(tmp, ignore_errors=True)
