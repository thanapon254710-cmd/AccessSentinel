"""Measured, same-dataset collective, escalation and load experiments.

Fig. 5: native AccessSentinel TGN-v3 collusion at test *anchors* (and
reference score proxies projected onto those same anchors).
Fig. 6: wall-clock, locally paced replay through actual TCNv3, TGNv3 and
Escalator components. No Kafka/network claims.
Fig. 7: six defined policies using one trained TCN/TGN pair and independent
replays of the real four-path Escalator for each path removal.

Only evaluation outputs of trained models are cached: no synthetic metric
numbers, no random pseudo-scores and no paper-data substitutions.
"""
from __future__ import annotations

import bisect
import json
import math
import platform
import queue
import random
import threading
import time
from collections import Counter, defaultdict
from dataclasses import asdict, replace
from pathlib import Path

import numpy as np
import torch
from sklearn.metrics import average_precision_score, precision_recall_fscore_support, roc_auc_score

from accesssentinel.config import EscalationConfig, RiskConfig, TCNv3Config, TGNv3Config
from accesssentinel.escalation_engine import Escalator, baseline_range
from accesssentinel.ml.tgn_train import StreamV3, MemoryStateV3
from accesssentinel.ml.train_tgn import evaluate_v3
from accesssentinel.ml.train_tcn import scores_v3
from accesssentinel.tcn import TCNv3
from accesssentinel.tgn import TGNv3
from research_comparison.memory_redis import MemoryRedis

ABLATIONS = (
    'TCN only', 'TCN + TGN',
    'TCN + TGN without resource-triggered escalation',
    'TCN + TGN without periodic sweep',
    'TCN + TGN without random sampling',
    'Full AccessSentinel',
)


def _json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str, allow_nan=False), encoding='utf-8')


def _percentile(x, q):
    return float(np.percentile(x, q)) if x else None


def load_or_train_tgn(ds, seed: int, device: str, epochs: int, cache: Path, reuse=False):
    """Real TGN-v3 training + group-level anchor replay; cache keyed by dataset."""
    meta_file = cache / f'tgn_native_seed{seed}.json'
    weight_file = cache / f'tgn_native_seed{seed}.pt'
    fp = ds.fingerprint()
    if reuse:
        if not (meta_file.exists() and weight_file.exists()):
            raise FileNotFoundError('TGN-v3 weights/group anchors absent; run once without --reuse-cache')
        meta = json.loads(meta_file.read_text(encoding='utf-8'))
        if meta['dataset_fingerprint'] != fp or meta['dataset_events'] != len(ds.events):
            raise RuntimeError('TGN checkpoint dataset does not match this generated dataset')
        if int(meta['epochs_requested']) != epochs:
            raise RuntimeError('TGN epochs differ from cached training configuration')
        cfg = TGNv3Config(**meta['config'])
        model = TGNv3(cfg).to(device)
        model.load_state_dict(torch.load(weight_file, map_location=device, weights_only=True))
        return model.eval(), cfg, meta
    cfg = TGNv3Config()
    cfg.epochs = int(epochs)
    cfg.self_supervised_epochs = min(2, max(0, cfg.epochs - 1))
    started = time.perf_counter()
    res = evaluate_v3(ds, seed=seed, device=device, cfg=cfg, log=lambda *a: print(*a, flush=True))
    model = res['model'].eval()
    meta = {
        'status': 'complete', 'seed': seed, 'dataset_fingerprint': fp,
        'dataset_events': len(ds.events), 'epochs_requested': epochs,
        'config': asdict(cfg), 'training_seconds': time.perf_counter() - started,
        'best_val_edge_pr_auc': res['best_ap'], 'history': res['history'],
        'thresholds': res['th'], 'calibration': res['cal'],
        'test_anchors': res['res']['anchors'],
        'native_seed_metrics': res['res']['seed_level'],
        'native_group_coverage': res['res']['groups_detected'],
        'native_per_group': res['res']['per_group'],
        'evaluation_unit': 'Test anchors, same dataset. TGNv3 native member/group score, not raw event anomaly.'
    }
    cache.mkdir(parents=True, exist_ok=True)
    torch.save({k: v.detach().cpu() for k, v in model.state_dict().items()}, weight_file)
    _json(meta_file, meta)
    return model, cfg, meta


def save_tcn_checkpoint(result, idx, enc, seed, out: Path, ds):
    model = result.get('_trained_model')
    if model is None:
        raise RuntimeError('TCN model was not returned by run_accesssentinel_tcn')
    cfg = model.cfg
    w = out / f'tcn_native_seed{seed}.pt'
    meta = out / f'tcn_native_seed{seed}.json'
    out.mkdir(parents=True, exist_ok=True)
    torch.save({k: v.detach().cpu() for k, v in model.state_dict().items()}, w)
    _json(meta, {'seed':seed, 'dataset_fingerprint':ds.fingerprint(),
                 'dataset_events':len(ds.events), 'config':cfg.to_dict(),
                 'n_res':enc.n_res, 'n_op':enc.n_op,'n_net':enc.n_net})


def load_tcn_checkpoint(ds, seed, device, out: Path):
    meta = json.loads((out / f'tcn_native_seed{seed}.json').read_text(encoding='utf-8'))
    if meta['dataset_fingerprint'] != ds.fingerprint() or meta['dataset_events'] != len(ds.events):
        raise RuntimeError('TCN checkpoint dataset mismatch')
    cfg = TCNv3Config.from_dict(meta['config'])
    model = TCNv3(cfg, meta['n_res'],meta['n_op'],meta['n_net']).to(device)
    model.load_state_dict(torch.load(out / f'tcn_native_seed{seed}.pt', weights_only=True, map_location=device))
    return model.eval()


def tcn_all_scores(ds, idx, model, device, seed: int, out: Path, reuse: bool):
    p = out / f'tcn_all_scores_seed{seed}.npz'
    if p.exists():
        with np.load(p, allow_pickle=False) as f:
            if str(f['fingerprint']) == ds.fingerprint() and len(f['scores']) == len(idx):
                return np.asarray(f['scores'], dtype=np.float32)
        if reuse:
            raise RuntimeError('Stored TCN full-event predictions are for an incompatible dataset')
    if reuse:
        raise FileNotFoundError('No cached TCN full-event predictions for --reuse-cache')
    if model is None:
        raise RuntimeError('TCN model required to score full chronological stream')
    result = scores_v3(model, idx, np.arange(len(idx)), device, batch=1024)
    out.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(p, fingerprint=np.array(ds.fingerprint()), scores=result.astype(np.float32))
    return result


def _traces(ds, idx, tcn_scores: np.ndarray, seed: int, config: str, theta_low: float, theta_high: float):
    """Replay actual Escalator with independent state for each path ablation."""
    assert config in ABLATIONS[2:], config
    use_resource = 'without resource-triggered' not in config
    use_sweep = 'without periodic sweep' not in config
    use_sampling = 'without random sampling' not in config
    cfg = EscalationConfig()
    if not use_sampling:
        cfg = replace(cfg, gamma=0.0)
    rc = RiskConfig(theta_low=theta_low, theta_high=theta_high)
    esctime = [0.0]
    triggers = []
    esc = Escalator(MemoryRedis(), cfg, rc, rng=random.Random(seed + 1991),
                    clock=lambda: esctime[0], on_trigger=lambda t,u,p: triggers.append((t,u,p)))
    events = sorted(ds.events, key=lambda e: (e.t,e.event_id))
    # WindowIndexV3 is in precisely the same order as the generator chronological stream.
    assert all(e.event_id == idx.event_ids[i] for i,e in enumerate(events))
    scores = np.asarray(tcn_scores, dtype=float)
    times = np.array([e.t for e in events])
    recs = []
    for e, v in zip(events, scores):
        tier = 'high' if v >= theta_high else ('medium' if v >= theta_low else 'low')
        recs.append({'u':e.u,'s_ind':float(v),'t':e.t,'tier':tier,'score_id':e.event_id,'trigger':'event'})
    escalations = []
    sweep_invocations = 0

    def load(now):
        start = now - cfg.sweep_lookback
        a,b = bisect.bisect_right(times,start),bisect.bisect_right(times,now)
        t0,t1 = baseline_range(start,cfg)
        c,d = bisect.bisect_right(times,t0),bisect.bisect_right(times,t1)
        return {'lookback_events':events[a:b],'baseline_events':events[c:d],'lookback_scores':recs[a:b]}

    started = time.perf_counter()
    for e, rec in zip(events, recs):
        esctime[0] = e.t
        if use_resource:
            esc.on_event(e)
        esc.on_score(rec)
        escalations.extend(esc.flush_realtime())
        if esc.background_due():
            if use_sweep and esc.sweep_due():
                esc.run_sweep(load)
                sweep_invocations += 1
            escalations.extend(esc.flush_background())
    esctime[0] = events[-1].t + cfg.realtime_window + 1
    escalations.extend(esc.flush_realtime(force=True))
    # Allow final sweep candidates a background opportunity without crossing test period.
    by_user = defaultdict(list)
    for e in escalations:
        by_user[e['u']].append(float(e['t']))
    return {u: sorted(ts) for u,ts in by_user.items()}, {
        'configuration':config,'triggers_by_path':dict(Counter(p for _,_,p in triggers)),
        'escalations':len(escalations),'users_escalated':len(by_user),
        'sweeps':sweep_invocations,'wall_seconds':time.perf_counter()-started,
        'enabled_paths':{'high':True,'resource':use_resource,'sweep':use_sweep,'sample':use_sampling},
        'state_backend':'research_comparison.memory_redis.MemoryRedis',
        'budget_admission':'accesssentinel.escalation_engine.Escalator.priority_admit',
    }


def _latest_user_scores(ds, result):
    """Time-causal lookup of event anomaly at a user and an anchor (test only)."""
    id_to = {e.event_id:e for e in ds.events}
    per = defaultdict(list)
    for eid, val in zip(result.get('_test_event_ids',[]), result.get('_test_scores',[])):
        e = id_to.get(str(eid))
        if e and val is not None and np.isfinite(val):
            per[e.u].append((e.t, e.event_id, float(val)))
    obj = {}
    for u, items in per.items():
        items.sort()
        obj[u] = ([i[0] for i in items], [i[2] for i in items])
    return obj


def _at_anchor(series, user, t):
    s = series.get(user)
    if s is None: return None
    i = bisect.bisect_right(s[0], t) - 1
    return s[1][i] if i >= 0 else None


def _binary_metrics(labels, scores, threshold):
    y = np.asarray(labels, dtype=int)
    s = np.asarray([np.nan if x is None else x for x in scores], dtype=float)
    ok = np.isfinite(s)
    if not ok.any():
        return {'precision':None,'recall':None,'f1':None,'roc_auc':None,'pr_auc':None,
                'n_scored':0,'n_total':len(y),'positive_anchors':int(y.sum()),'coverage':0.0}
    pred = s[ok] >= threshold
    precision,recall,f1,_ = precision_recall_fscore_support(y[ok], pred, average='binary',zero_division=0)
    return {'precision':float(precision),'recall':float(recall),'f1':float(f1),
            'roc_auc':float(roc_auc_score(y[ok],s[ok])) if np.unique(y[ok]).size==2 else None,
            'pr_auc':float(average_precision_score(y[ok],s[ok])) if np.unique(y[ok]).size==2 else None,
            'n_scored':int(ok.sum()),'n_total':len(y),'positive_anchors':int(y.sum()),
            'coverage':float(ok.mean()),'tp':int(np.sum(pred & (y[ok]==1))),
            'fp':int(np.sum(pred & (y[ok]==0))), 'fn':int(np.sum(~pred & (y[ok]==1)))}


def evaluate_anchors(ds, tgn_meta, records, seed, tcn_scores_all, idx, output: Path):
    """Fig.5 native TGN score and six *non-group* method proxies at common test anchors."""
    anchors = tgn_meta['test_anchors']
    y = [int(a['seed_label']) for a in anchors]
    thr = tgn_meta['thresholds']['theta_C']
    rows = [{'method':'AccessSentinel TCN + TGN (native group)', 'seed':seed,
             'threshold':thr, 'evaluation_level':'native TGN group/member anchor',
             **_binary_metrics(y, [a['s_coll'] for a in anchors], thr)}]
    for result in records:
        # All reference adaptations provide event anomaly, not group S_coll.
        if result['method'].startswith('AccessSentinel '): continue
        scores = _latest_user_scores(ds, result)
        val_ids = result.get('_validation_event_ids',[])
        valid_scores = np.asarray(result.get('_validation_scores',[]),float)
        # Reference anomaly scores are calibrated against nominal *individual* events.
        nominal = [v for eid,v in zip(val_ids,valid_scores)
                   if eid in ds.labels and ds.labels[eid].individual==0 and np.isfinite(v)]
        threshold = float(np.quantile(nominal,.95)) if nominal else math.inf
        rows.append({'method':result['method'], 'seed':seed,'threshold':threshold,
                     'evaluation_level':'latest same-user event anomaly proxy (NOT group output)',
                     **_binary_metrics(y, [_at_anchor(scores, a['seed'],a['t']) for a in anchors],threshold)})
    _json(output, {'status':'complete', 'seed':seed, 'dataset_events':len(ds.events),
                   'dataset_fingerprint':ds.fingerprint(), 'anchor_sampling':'AccessSentinel TGN evaluation_v3 test anchors',
                   'n_test_anchors':len(anchors), 'comparability_warning':
                   'Only AccessSentinel outputs a native collective score; reference adapters are anchor-projected individual event anomaly proxies, not faithful group detectors.',
                   'measurements':rows})
    return rows


def evaluate_ablation(ds, idx, tcn_scores_all, tgn_meta, seed, output: Path):
    anchors = tgn_meta['test_anchors']
    y = [int(a['seed_label']) for a in anchors]
    threshold = tgn_meta['thresholds']['theta_C']
    val = idx.mask('val')
    nominal = tcn_scores_all[val][idx.y[val] == 0]
    if not len(nominal):
        raise RuntimeError('No nominal validation TCN predictions to calibrate escalation')
    theta_low = float(np.quantile(nominal,.95))
    theta_high = float(np.quantile(nominal,.99))
    times_by_user = defaultdict(list)
    for i in idx.mask('test'):
        times_by_user[idx.users[i]].append((float(idx.t[i]),float(tcn_scores_all[i])))
    series = {u:([t for t,v in sorted(rows)], [v for t,v in sorted(rows)])
              for u,rows in times_by_user.items()}
    ind = [_at_anchor(series, a['seed'], a['t']) for a in anchors]
    tgn = [float(a['s_coll']) if a.get('s_coll') is not None else 0.0 for a in anchors]
    measurements = []
    trace_reports = {}
    for name in ABLATIONS:
        if name == 'TCN only':
            predictions = [float(v is not None and v >= theta_high) for v in ind]
            meta = {'execution':'TCN only; group scorer disabled'}
        elif name == 'TCN + TGN':
            predictions = [float((v is not None and v >= theta_high) or s >= threshold)
                           for v,s in zip(ind,tgn)]
            meta = {'execution':'Unrestricted TGN at all sampled anchors (upper-bound oracle compute; no escalation gating)'}
        else:
            by_user,meta = _traces(ds,idx,tcn_scores_all,seed,name,theta_low,theta_high)
            predictions=[]
            for a,v,s in zip(anchors,ind,tgn):
                candidates=by_user.get(a['seed'],[])
                j=bisect.bisect_right(candidates,a['t'])-1
                # Current collective evidence is valid only inside the graph window (no future escalation).
                admitted = j >= 0 and a['t']-candidates[j] <= TGNv3Config().window
                predictions.append(float((v is not None and v >= theta_high) or (admitted and s>=threshold)))
        row={'configuration':name,'seed':seed,'threshold_tcn_high':theta_high,
             'threshold_tgn_collective':threshold,'n_test_anchors':len(anchors),
             **_binary_metrics(y,predictions,0.5)}
        measurements.append(row)
        trace_reports[name]=meta
    _json(output, {'status':'complete','seed':seed,'dataset_events':len(ds.events),
                   'dataset_fingerprint':ds.fingerprint(),'evaluation_unit':'collusion-labelled test anchors',
                   'notes':'TCN+TGN is an ungated full-compute reference. Other TGN configs require an actual Escalator admission in the previous 3600 event-time seconds; TCN remains online for every variant. Independent Escalator state per ablation.',
                   'measurements':measurements,'escalation_trace_metadata':trace_reports})
    return measurements


def _sync(device):
    if str(device).startswith('cuda') and torch.cuda.is_available():
        torch.cuda.synchronize(device)


def local_inference_latency(model, idx, ids, device, repeats=80):
    """Measured event->local TCN decision; excludes network, broker, PDP, UI and enforcement."""
    if model is None or not len(ids):
        raise ValueError('Local latency requires a fitted TCN model and samples')
    lat=[]
    ids=np.asarray(ids,dtype=np.int64)
    with torch.no_grad():
        for i in range(repeats+5):
            j=ids[i % len(ids)]
            _sync(device)
            start=time.perf_counter_ns()
            score=scores_v3(model,idx,np.array([j]),device,batch=1)[0]
            decision=bool(score >= 0.5)
            _sync(device)
            duration=(time.perf_counter_ns()-start)/1e6
            if i>=5: lat.append(duration)
            _ = decision
    return {'boundary':'window gather -> model forward -> sigmoid -> local threshold decision',
            'excluded':'gateway, Kafka, network, queuing, PDP, enforcement, Web UI',
            'metric_type':'local_event_to_decision_ms','samples':repeats,
            'p50_latency_ms':_percentile(lat,50),'p95_latency_ms':_percentile(lat,95),
            'p99_latency_ms':_percentile(lat,99),'device':str(device),
            'torch_threads':torch.get_num_threads()}


def run_load_test(ds, idx, tcn_model, tgn_model, cfg_tgn, seed, device,
                  duration: float, rates: list[int], out: Path, quick=False, history_scores=None):
    """Offer actual wall-paced event requests using one producer and one consumer.

    Every request executes TCN-v3 single-window prediction, TGN-v3 CMU/SGE
    edge scoring, resource path, and due sweep/background sampling. This is
    an offline in-process component stress test, NOT deployed system throughput.
    """
    if duration <= 0: raise ValueError('duration must be >0')
    test = idx.mask('test')
    if not len(test): raise ValueError('No test events for measured load')
    st = StreamV3(ds)
    historical_times = np.asarray(st.t, dtype=float)
    # A sweep needs actual past access and score history, as in escalation_eval.
    if history_scores is None or len(history_scores) != len(st.events):
        raise ValueError('Measured sweep requires chronologically complete TCN history scores')
    historical_scores = np.asarray(history_scores, dtype=float)
    # It has exactly the same sorted chronology as the window index.
    assert len(st.events)==len(idx.event_ids)
    assert all(st.events[i].event_id == idx.event_ids[i] for i in (0,len(st.events)//2,len(st.events)-1))
    records=[]
    for rate in rates:
        requested = max(2, int(round(rate*duration)))
        if requested > len(test):
            raise ValueError('Duration/rate requests more distinct test events than available: no repeated fake requests')
        # Contiguous chronological events; no re-use across a single rate run.
        selected = np.asarray(test[:requested],dtype=np.int64)
        q:queue.Queue=queue.Queue()
        barrier=threading.Event()
        store=MemoryRedis(); event_clock=[float(idx.t[selected[0]])]
        esc=Escalator(store, EscalationConfig(),RiskConfig(),
                      clock=lambda:event_clock[0],rng=random.Random(seed+2026))
        ms=MemoryStateV3(st,cfg_tgn,device)
        tcn_model.eval();tgn_model.eval()
        component_lat=defaultdict(list);calls=Counter()
        worker_errors=[]
        total_lat=[]; offered_stamps=[]; completed_stamps=[]
        start_wall=[0.0]; n_processed=[0]
        def producer():
            barrier.wait()
            for k, event_i in enumerate(selected):
                deadline=start_wall[0] + k/rate
                sleep=deadline-time.perf_counter()
                if sleep>0: time.sleep(sleep)
                arrived=time.perf_counter()
                offered_stamps.append(arrived)
                q.put((int(event_i),arrived))
            q.put(None)
        def consume():
            with torch.no_grad():
                while True:
                    item=q.get()
                    if item is None: break
                    i,arrived=item
                    e=st.events[i];event_clock[0]=float(e.t)
                    t0=time.perf_counter_ns()
                    value=scores_v3(tcn_model,idx,np.array([i],dtype=np.int64),device,batch=1)[0]
                    _sync(device)
                    component_lat['TCN'].append((time.perf_counter_ns()-t0)/1e6);calls['TCN']+=1
                    t0=time.perf_counter_ns()
                    M2=ms.apply_pending(tgn_model,grad=False)
                    nodes=np.array([st.u[i],st.r[i]],dtype=np.int64)
                    t=np.full(2,st.t[i])
                    h=ms.embed(tgn_model,M2,nodes,t)
                    _ = torch.sigmoid(tgn_model.dec(h[:1],h[1:2],torch.from_numpy(st.f[i:i+1]).to(device)))
                    ms.commit(M2,np.array([i],dtype=np.int64))
                    _sync(device)
                    component_lat['TGN'].append((time.perf_counter_ns()-t0)/1e6);calls['TGN']+=1
                    t0=time.perf_counter_ns()
                    esc.on_event(e)
                    component_lat['Resource-triggered escalation'].append((time.perf_counter_ns()-t0)/1e6)
                    calls['Resource-triggered escalation']+=1
                    # Let the same Escalator process score/threshold paths.
                    esc.on_score({'u':e.u,'t':e.t,'s_ind':float(value),
                                  'tier':'high' if value>=0.99 else ('medium' if value>=0.95 else 'low'),
                                  'score_id':e.event_id,'trigger':'event'})
                    esc.flush_realtime()
                    if esc.background_due():
                        if esc.sweep_due():
                            # Full causal synthetic history is accessible in-process, like HDFS.
                            t0=time.perf_counter_ns()
                            def history(now):
                                from accesssentinel.escalation_engine import baseline_range
                                look0=now-esc.cfg.sweep_lookback
                                a,b=np.searchsorted(historical_times,[look0,now],side='right')
                                t_a,t_b=baseline_range(look0,esc.cfg)
                                c,d=np.searchsorted(historical_times,[t_a,t_b],side='right')
                                history_recs=[{'u':st.events[k].u,'t':float(st.t[k]),'s_ind':float(historical_scores[k]),
                                               'tier':('high' if historical_scores[k]>=.99 else
                                                       ('medium' if historical_scores[k]>=.95 else 'low'))}
                                              for k in range(a,b)]
                                return {'lookback_events':st.events[a:b],
                                        'baseline_events':st.events[c:d],'lookback_scores':history_recs}
                            esc.run_sweep(history)
                            component_lat['Periodic sweep'].append((time.perf_counter_ns()-t0)/1e6)
                            calls['Periodic sweep']+=1
                        t0=time.perf_counter_ns()
                        esc.flush_background()
                        component_lat['Random sampling'].append((time.perf_counter_ns()-t0)/1e6)
                        calls['Random sampling']+=1
                    end=time.perf_counter()
                    completed_stamps.append(end)
                    total_lat.append((end-arrived)*1e3)
                    n_processed[0]+=1
        def checked_consume():
            try:
                consume()
            except Exception as exc:
                worker_errors.append(exc)
        thread=threading.Thread(target=producer,daemon=True)
        consumer=threading.Thread(target=checked_consume,daemon=True)
        thread.start();consumer.start()
        start_wall[0]=time.perf_counter();barrier.set()
        thread.join();consumer.join()
        if worker_errors:
            raise RuntimeError(f'Fig6 consumer failed at offered rate {rate}/s') from worker_errors[0]
        if n_processed[0] != requested:
            raise RuntimeError(f'Fig6 processed {n_processed[0]} of {requested} events at {rate}/s')
        elapsed=time.perf_counter()-start_wall[0]
        end_offered=offered_stamps[-1]-offered_stamps[0] if len(offered_stamps)>1 else 0.0
        for comp in ('TCN','TGN','Resource-triggered escalation','Periodic sweep','Random sampling'):
            lat=component_lat[comp]
            if not lat: continue
            records.append({'event_rate_per_second':rate,'component':comp,
                            'throughput_events_per_second':calls[comp]/elapsed,
                            'p50_latency_ms':_percentile(lat,50),'p95_latency_ms':_percentile(lat,95),
                            'p99_latency_ms':_percentile(lat,99),'n_operations':calls[comp],
                            'operation_unit':'work items (not access events)' if comp in ('Periodic sweep','Random sampling') else 'access events',
                            'measured_elapsed_seconds':elapsed,'offered_count':len(offered_stamps),
                            'attained_end_to_end_events_per_second':n_processed[0]/elapsed,
                            'event_response_p95_ms':_percentile(total_lat,95),
                            'emission_span_seconds':end_offered})
        print(f'  -> Fig6 rate {rate}/s: {n_processed[0]}/{len(selected)} processed in {elapsed:.3f}s, '
              f'attained {n_processed[0]/elapsed:.2f}/s',flush=True)
    output={'status':'complete','seed':seed,'dataset_events':len(ds.events),'dataset_fingerprint':ds.fingerprint(),
            'quick_smoke_only':quick,'device':device,'python':platform.python_version(),
            'torch':torch.__version__,'torch_threads':torch.get_num_threads(),
            'load_duration_seconds':duration,'rates_offered_per_second':rates,
            'test_method':'wall-clock independently paced producer -> in-process worker queue. All component callbacks actually invoked, with local model weights trained on this dataset.',
            'limitations':['In-process CPU/GPU component load test; excludes Kafka, HDFS, Redis server, service network, PDP and enforcement.',
                           'Each rate uses the first distinct chronological test windows; TGN memory starts cold in each rate; event times are accelerated to the offered wall-clock rate.',
                           'Periodic sweep uses causal earlier generated synthetic events and cached TCN scores as historical lake input; data access is in-process, not a measured HDFS call.',
                           'TGN edge inference is deliberately invoked for every event to measure maximum component demand; production escalation gates most graph runs.',
                           'Background scheduling work items are not access events; throughput plots must not compare their counts directly to event throughput.',
                           'Load test accuracy is not measured here. Accuracy experiments use full chronological event-time replay.'],
            'measurements':records}
    _json(out,output)
    return records
