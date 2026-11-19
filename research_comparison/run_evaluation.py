"""Reproducible synthetic AccessSentinel evaluation, Figures 2--9.

Full paper run (real measurement, expensive):
  python -m research_comparison.run_evaluation --figures 2,3,4,5,6,7,8,9 --seeds 0,1,2,3,4 --device cuda

Engineering run (strictly NOT reportable):
  python -m research_comparison.run_evaluation --quick --seeds 0 --tgn-epochs 2 --load-duration 0.6

Replot measured data without retraining:
  python -m research_comparison.run_evaluation --reuse-cache --seeds 0 --figures 2,3,4,5,6,9 --tgn-epochs 2 --quick

Figures 7/8/9 are never fabricated, and a plot is not written if mandatory
measurements are missing. Full and quick outputs use completely separate roots.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import random
import statistics
import time
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import torch

from accesssentinel.ml.train_tgn import collusion_preflight
from accesssentinel.ml.tgn_graph import GraphParams
from accesssentinel.ml.tgn_train import Corpus
from research_comparison.figures_6_7 import plot_fig7, plot_fig8, plot_fig9
from research_comparison.measured_experiments import (
    evaluate_ablation, evaluate_anchors, load_or_train_tgn,
    load_tcn_checkpoint, local_inference_latency, run_load_test,
    save_tcn_checkpoint, tcn_all_scores,
)
from research_comparison.selected_adapters import (
    _window_encoder, _window_index, make_dataset,
    run_accesssentinel_tcn, run_all_selected,
)

ROOT=Path(__file__).resolve().parent
METHOD_ORDER = [
    'AccessSentinel TCN v3', 'Ref. 14 DynAD', 'Ref. 17 PG-AID',
    'Ref. 21 ACOBE', 'Ref. 22 FGDT', 'Ref. 24 User-resource embedding',
    'Ref. 25 DEEPRO',
]
DISPLAY_NAMES = {
    'AccessSentinel TCN v3': 'AccessSentinel',
    'Ref. 14 DynAD': 'DynAD [14]', 'Ref. 17 PG-AID': 'PG-AID [17]',
    'Ref. 21 ACOBE': 'ACOBE [21]', 'Ref. 22 FGDT': 'FGDT [22]',
    'Ref. 24 User-resource embedding': 'User-resource embedding [24]',
    'Ref. 25 DEEPRO': 'DEEPRO [25]',
    'AccessSentinel TCN + TGN (native group)': 'AccessSentinel',
}
# Display-only change: accept historic cache names as well as the cited labels.
METHOD_ALIASES = {label: method for method, label in DISPLAY_NAMES.items()}
METHOD_ALIASES.update({
    'DynAD':'Ref. 14 DynAD', 'PG-AID':'Ref. 17 PG-AID',
    'ACOBE':'Ref. 21 ACOBE', 'FGDT':'Ref. 22 FGDT',
    'User-resource embedding':'Ref. 24 User-resource embedding',
    'DEEPRO':'Ref. 25 DEEPRO',
    'AccessSentinel TCN+TGN':'AccessSentinel TCN + TGN (native group)',
})
def canonical_method(method):
    return METHOD_ALIASES.get(method, method)
def display_method(method):
    return DISPLAY_NAMES.get(canonical_method(method), method)

ATTACKS = {'compromised_account', 'insider_exfiltration', 'bac_probe', 'low_and_slow',
           'horizontal_abuse', 'unusual_sequence', 'unusual_time'}


def write_csv(path, rows, columns):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w',newline='',encoding='utf-8-sig') as fp:
        w=csv.DictWriter(fp,fieldnames=columns,extrasaction='ignore')
        w.writeheader();w.writerows(rows)


def _save_result(out, seed, ds, result):
    result['method']=canonical_method(result['method'])
    result['seed'] = seed  # retain seed identity in-memory as well as in JSON
    name=result['method'].lower().replace(' ','_').replace('.','').replace('-','-')
    meta={k:v for k,v in result.items() if not k.startswith('_')}
    meta.update({'seed':seed,'dataset_events':len(ds.events),'dataset_fingerprint':ds.fingerprint()})
    p=out/'raw'/f'{name}_seed{seed}.json';p.parent.mkdir(parents=True,exist_ok=True)
    p.write_text(json.dumps(meta,indent=2,default=str),encoding='utf-8')
    cache=out/'cache'/f'{name}_seed{seed}.npz';cache.parent.mkdir(parents=True,exist_ok=True)
    np.savez_compressed(cache,validation_event_ids=np.asarray(result.get('_validation_event_ids',[]),str),
                        validation_scores=np.asarray(result.get('_validation_scores',[]),float),
                        test_event_ids=np.asarray(result.get('_test_event_ids',[]),str),
                        test_scores=np.asarray(result.get('_test_scores',[]),float))


def _cached_results(out, seed, ds):
    out_rows=[]
    for p in sorted((out/'raw').glob(f'*_seed{seed}.json')):
        if p.stem.startswith(('fig5_','fig6_','fig7_')):continue
        meta=json.loads(p.read_text(encoding='utf-8'))
        if not isinstance(meta,dict) or 'method' not in meta:continue
        if meta.get('dataset_fingerprint')!=ds.fingerprint():
            raise RuntimeError(f'{p.name} does not match the current dataset fingerprint')
        c=out/'cache'/f'{p.stem}.npz'
        if not c.exists():raise FileNotFoundError(f'Missing predictions: {c}')
        with np.load(c,allow_pickle=False) as z:
            for kind in ('validation','test'):
                meta[f'_{kind}_event_ids']=z[f'{kind}_event_ids'].astype(str).tolist()
                meta[f'_{kind}_scores']=z[f'{kind}_scores'].astype(float).tolist()
        meta['method']=canonical_method(meta['method'])
        out_rows.append(meta)
    if not out_rows:raise FileNotFoundError(f'No cached results for seed={seed}. First run without --reuse-cache.')
    return out_rows


def _summary(records):
    rows=[]
    for method in METHOD_ORDER:
        sub=[r for r in records if canonical_method(r['method'])==method]
        for metric in ('precision','recall','f1','roc_auc','pr_auc'):
            vals=[float(r['metrics'][metric]) for r in sub if r['metrics'].get(metric) is not None]
            rows.append({'method':method,'metric':metric,'n_seeds':len(vals),
                         'mean':statistics.mean(vals) if vals else None,
                         'sd':statistics.stdev(vals) if len(vals)>1 else (0.0 if vals else None)})
    return rows


def _grouped_plot(rows,methods,metrics,out,title,quick=False):
    present=[m for m in methods if all(any(canonical_method(r['method'])==m and r['metric']==metric and r.get('mean') is not None for r in rows) for metric in metrics)]
    if not present:return False
    fig,ax=plt.subplots(figsize=(14,6.2))
    x=np.arange(len(present)); width=.8/len(metrics)
    for k,metric in enumerate(metrics):
        rr=[next(r for r in rows if canonical_method(r['method'])==m and r['metric']==metric) for m in present]
        ax.bar(x+(k-(len(metrics)-1)/2)*width,[r['mean'] for r in rr],width,
               yerr=[r['sd'] or 0 for r in rr],capsize=3,label=metric.replace('_','-').upper())
    ax.set_xticks(x,[display_method(m) for m in present],rotation=23,ha='right');ax.set_ylim(0,1)
    ax.set_ylabel('Score (0–1)');ax.set_title(title+ (' — SMOKE ONLY' if quick else ''))
    ax.grid(axis='y',alpha=.22);ax.legend(ncol=len(metrics),frameon=False)
    fig.tight_layout();out.parent.mkdir(parents=True,exist_ok=True);fig.savefig(out,dpi=240);plt.close(fig)
    return True


def _nominal_threshold(ds, result):
    scores=np.asarray(result.get('_validation_scores',[]),dtype=float)
    ids=result.get('_validation_event_ids',[])
    if len(scores)!=len(ids):raise RuntimeError(f'Prediction cache alignment mismatch in {result["method"]}')
    nominal=[v for eid,v in zip(ids,scores) if eid in ds.labels and ds.labels[eid].individual==0 and np.isfinite(v)]
    return float(np.quantile(nominal,.95)) if nominal else math.inf


def _ttd(ds, result, seed):
    thr=_nominal_threshold(ds,result)
    scored={str(eid):float(s) for eid,s in zip(result.get('_test_event_ids',[]),result.get('_test_scores',[]))
            if s is not None and np.isfinite(s)}
    grouped=defaultdict(list)
    for e in ds.events:
        lab=ds.labels[e.event_id]
        if lab.instance and lab.scenario in ATTACKS and ds.split_of(e.t)=='test':
            grouped[lab.instance].append(e)
    out=[]
    for inst,events in sorted(grouped.items()):
        events.sort(key=lambda e:(e.t,e.event_id))
        first=next((e.t for e in events if scored.get(e.event_id,float('-inf'))>=thr),None)
        out.append({'seed':seed,'method':canonical_method(result['method']),'attack_instance':inst,
                    'scenario':ds.labels[events[0].event_id].scenario,'n_attack_events':len(events),
                    'attack_start_epoch_s':events[0].t,'first_detection_epoch_s':first,
                    'time_to_detect_seconds':float(first-events[0].t) if first is not None else None,
                    'detected':int(first is not None),'threshold':thr})
    return out


def _detection_summary(ttd):
    """Reuse identical per-attack measurements for Figures 4 and 5."""
    methods=[m for m in METHOD_ORDER if any(canonical_method(r['method'])==m for r in ttd)]
    coverage=[]; delay=[]; counts=[]
    for method in methods:
        sub=[r for r in ttd if canonical_method(r['method'])==method]
        observed=[float(r['time_to_detect_seconds']) for r in sub
                  if r['detected'] and r['time_to_detect_seconds'] is not None
                  and np.isfinite(float(r['time_to_detect_seconds']))]
        coverage.append(len(observed)/len(sub) if sub else np.nan)
        delay.append(float(np.median(observed))/60.0 if observed else np.nan)
        counts.append(f'{len(observed)}/{len(sub)}')
    return methods, coverage, delay, counts


def _fig4_coverage_plot(ttd, out, quick=False):
    """Figure 4: attack-instance detection coverage, including missed attacks."""
    methods, coverage, _, counts=_detection_summary(ttd)
    if not methods:
        out.unlink(missing_ok=True)
        return False
    fig, ax=plt.subplots(figsize=(13.8,6.0))
    x=np.arange(len(methods))
    ax.bar(x,coverage)
    ax.set_xticks(x,[display_method(m) for m in methods],rotation=25,ha='right')
    ax.set_ylim(0,1.13)
    ax.set_ylabel('Attack-instance detection coverage (0–1)')
    ax.set_title('Figure 4 — Attack Detection Coverage'
                 +(' — SMOKE ONLY' if quick else ''))
    for i,(v,n) in enumerate(zip(coverage,counts)):
        if np.isfinite(v):ax.text(i,min(v+0.025,1.09),n,ha='center',fontsize=9)
    ax.grid(axis='y',alpha=.25)
    fig.tight_layout();out.parent.mkdir(parents=True,exist_ok=True)
    fig.savefig(out,dpi=240);plt.close(fig)
    return True


def _fig5_delay_plot(ttd, out, quick=False):
    """Figure 5: conditional delay; zero is measured, N/A means no detections."""
    methods, _, delay, counts=_detection_summary(ttd)
    if not methods:
        out.unlink(missing_ok=True)
        return False
    fig, ax=plt.subplots(figsize=(13.8,6.0))
    x=np.arange(len(methods))
    ax.bar(x,delay)
    ax.set_xticks(x,[display_method(m) for m in methods],rotation=25,ha='right')
    ax.set_ylabel('Median time-to-detect (minutes; detected attacks only)')
    ax.set_title('Figure 5 — Attack Detection Delay'
                 +(' — SMOKE ONLY' if quick else ''))
    for i,v in enumerate(delay):
        if not np.isfinite(v):
            ax.annotate('N/A', (i,0),xytext=(0,7),textcoords='offset points',
                        ha='center',va='bottom',fontsize=9)
        elif v == 0:
            ax.plot(i,0,marker='o',markersize=4,color='black',clip_on=False,zorder=5)
            ax.annotate('0 s', (i,0),xytext=(0,7),textcoords='offset points',ha='center',fontsize=9)
        else:
            ax.annotate(f'{v:.1f} min',(i,v),xytext=(0,7),
                        textcoords='offset points',ha='center',fontsize=9)
    ax.margins(y=.17)
    ax.grid(axis='y',alpha=.25)
    fig.text(.5,.01,'N/A = no attack instances detected; detection delay is conditional on detection.',
             ha='center',fontsize=9)
    fig.tight_layout(rect=(0,.035,1,1))
    out.parent.mkdir(parents=True,exist_ok=True)
    fig.savefig(out,dpi=240);plt.close(fig)
    return True


def _fig6_collusion_plot(rows, out, quick=False, expected_seeds=None):
    """Figure 6: native group scoring and explicitly labelled event-score proxies.

    A missing method/seed or undefined two-class metrics fails closed, not as a
    pretty but misleading chart. Proxies' scored-subset coverage is annotated.
    """
    methods = ['AccessSentinel TCN + TGN (native group)'] + METHOD_ORDER[1:]
    expected = set(expected_seeds if expected_seeds is not None
                   else (r['seed'] for r in rows))
    coverage, metric_means, metric_sds = {}, {}, {}
    for method in methods:
        sub = [r for r in rows if canonical_method(r['method']) == method]
        if {r['seed'] for r in sub} != expected or len(sub) != len(expected):
            out.unlink(missing_ok=True)
            return False
        if any(r.get('status') not in ('VALID_FULL', 'VALID_SCORED_SUBSET_ONLY')
               or any(r.get(k) is None for k in ('precision','recall','f1','coverage'))
               for r in sub):
            out.unlink(missing_ok=True)
            return False
        coverage[method] = float(np.mean([r['coverage'] for r in sub]))
        metric_means[method] = {}
        metric_sds[method] = {}
        for metric in ('precision','recall','f1'):
            vals = [float(r[metric]) for r in sub]
            metric_means[method][metric] = float(np.mean(vals))
            metric_sds[method][metric] = float(np.std(vals,ddof=1)) if len(vals)>1 else 0.0
    fig,ax=plt.subplots(figsize=(14.5,6.5))
    x=np.arange(len(methods));w=.24
    for j,metric in enumerate(('precision','recall','f1')):
        ax.bar(x+(j-1)*w,[metric_means[m][metric] for m in methods],w,
               yerr=[metric_sds[m][metric] for m in methods],capsize=3,
               label=metric.upper())
    labels=[f'{display_method(m)}\n(score coverage {coverage[m]:.0%})' for m in methods]
    ax.set_xticks(x,labels,rotation=22,ha='right')
    ax.set_ylabel('Test-anchor metrics (conditional on scored anchors)')
    ax.set_ylim(0,1.04)
    ax.grid(axis='y',alpha=.22)
    ax.legend(ncol=3,frameon=False)
    ax.set_title(f'Figure 6 — Collusion Detection Performance ({len(expected)} seeds)'
                 +(' — SMOKE ONLY' if quick else ''),fontsize=12)
    fig.text(.5,.01,'Native AccessSentinel collective score vs six same-user event anomaly proxies; NOT faithful group-detector comparisons.',
             ha='center',fontsize=9)
    fig.tight_layout(rect=(0,.035,1,1))
    out.parent.mkdir(parents=True,exist_ok=True)
    fig.savefig(out,dpi=240)
    plt.close(fig)
    return True


def _rewrite_json(p,**updates):
    obj=json.loads(p.read_text(encoding='utf-8'));obj.update(updates)
    p.write_text(json.dumps(obj,indent=2,allow_nan=False,default=str),encoding='utf-8')


def main(argv=None):
    ap=argparse.ArgumentParser(description='Measured AccessSentinel Figures 2–9, same synthetic dataset per seed')
    ap.add_argument('--figures',default='2,3,4,5,6,7,8,9')
    ap.add_argument('--seeds',default='0,1,2,3,4')
    ap.add_argument('--device',default='cuda' if torch.cuda.is_available() else 'cpu')
    ap.add_argument('--quick',action='store_true',help='Smaller data; NEVER paper results')
    ap.add_argument('--preflight',action='store_true',help='Generate and audit every seed, including group anchors; no GPU training')
    ap.add_argument('--reuse-cache',action='store_true',help='Reuse trained models/score JSON; Figs7/8 still remeasure real load unless --skip-load')
    ap.add_argument('--skip-load',action='store_true',help='Only replot existing throughput/latency JSON; do not run new offered-rate test')
    ap.add_argument('--skip-accesssentinel-training',action='store_true',help='Reference-only checks of Fig2-5 (not complete)')
    ap.add_argument('--train-accesssentinel',action='store_true',help='Compatibility flag; TCN trains by default')
    ap.add_argument('--tgn-epochs',type=int,default=6,help='Real TGN-v3 train budget per seed; same budget reused for ablations')
    ap.add_argument('--load-duration',type=float,default=2.0,help='Seconds of wall-clock offered events at each of 5,20,50,100 req/s')
    ap.add_argument('--output-dir',type=Path,default=None)
    args=ap.parse_args(argv)
    try:figures=sorted({int(s) for s in args.figures.split(',') if s.strip()})
    except ValueError:ap.error('--figures requires comma-separated numbers')
    if not figures or not set(figures)<=set(range(2,10)):ap.error('Figures must be in 2,3,4,5,6,7,8,9')
    try:seeds=[int(x.strip()) for x in args.seeds.split(',') if x.strip()]
    except ValueError:ap.error('--seeds requires comma-separated integers')
    if not seeds or len(seeds)!=len(set(seeds)):ap.error('Provide at least one unique integer seed')
    if args.tgn_epochs < 1 or args.load_duration<=0:ap.error('--tgn-epochs and --load-duration must be positive')
    if args.device.startswith('cuda') and not torch.cuda.is_available():ap.error('CUDA requested but torch.cuda.is_available() is false')
    if args.skip_accesssentinel_training and set(figures)&{6,7,8,9}:ap.error('Figures 6–9 require a trained AccessSentinel TCN')
    out=args.output_dir or (ROOT/'results'/('evaluation_v5_smoke' if args.quick else 'evaluation_v5'))
    if args.preflight:
        failures=[]
        audits=[]
        for seed in seeds:
            print(f'\n[preflight] seed {seed}: generating common synthetic dataset ...',flush=True)
            try:
                ds=make_dataset(seed,args.quick)
                status=collusion_preflight(ds)
                corpus=Corpus(ds,GraphParams())
                classes={}
                for split,k in (("train",1),("val",2),("test",3)):
                    items=corpus.evaluation_set(split,random.Random(seed+k),anchors_per_burst=2,
                        n_random=150 if split!='test' else 300,
                        n_resource=150 if split!='test' else 300)
                    p=sum(int(it.seed_label) for it in items)
                    classes[split]={'positive':p,'negative':len(items)-p}
                if any(min(x.values())<=0 for x in classes.values()):
                    raise RuntimeError(f'Non-identifiable anchor class split: {classes}')
                n=len(ds.events)
                if not args.quick and not 450000<=n<=560000:
                    raise RuntimeError(f'Unexpected full dataset size: {n}')
                audit={'seed':seed,'events':n,'fingerprint':ds.fingerprint(),
                       'collusion':status,'anchor_classes':classes}
                audits.append(audit)
                print(f'[preflight] OK seed={seed}: events={n:,}; {status}; {classes}',flush=True)
            except Exception as exc:
                failures.append({'seed':seed,'error':str(exc)})
                print(f'[preflight] INVALID seed {seed}: {exc}',flush=True)
        out.mkdir(parents=True,exist_ok=True)
        manifest={'status':'valid' if not failures else 'invalid',
                  'mode':'quick_smoke' if args.quick else 'full_synthetic',
                  'seeds_requested':seeds,'datasets':audits,'failures':failures,
                  'note':'No model training or detection measurement took place in preflight.'}
        (out/'preflight_report.json').write_text(json.dumps(manifest,indent=2,allow_nan=False),encoding='utf-8')
        return 2 if failures else 0
    for folder in ('raw','cache','figures','tables'):(out/folder).mkdir(parents=True,exist_ok=True)
    t_start=time.perf_counter()
    records=[];ttd=[];coll_rows=[];failures=[];data_meta=[];latency={}
    print(f'Evaluation results: {out}\nMode: {"QUICK / ENGINEERING ONLY" if args.quick else "FULL DATASET"}',flush=True)
    for seed in seeds:
        print(f'\n=== SEED {seed} ===',flush=True)
        ds=make_dataset(seed,args.quick)
        n=len(ds.events);fp=ds.fingerprint()
        if not args.quick and not 450000<=n<=560000:  # seed-to-seed variation of tcn_v3_eval (seeds 0-4: 477k-506k); guards only against a quick/wrong-config dataset
            raise RuntimeError(f'Full dataset must have roughly 450k-560k events (tcn_v3_eval), got {n:,} for seed {seed}')
        data_meta.append({'seed':seed,'events':n,'fingerprint':fp,'synthetic':True})
        print(f'Dataset: {n:,} events; fingerprint {fp}',flush=True)
        idx=_window_index(ds)
        requires_25=bool(set(figures)&{2,3,4,5,6})
        needs_tcn=not args.skip_accesssentinel_training
        model_results=[]
        tcn=None
        if args.reuse_cache:
            if requires_25:model_results=_cached_results(out,seed,ds)
            if needs_tcn:tcn=load_tcn_checkpoint(ds,seed,args.device,out/'cache')
        elif requires_25:
            ds_generated,model_results,errors=run_all_selected(seed,args.quick,args.device,needs_tcn)
            if ds_generated.fingerprint()!=fp:raise RuntimeError('Shared generator produced different datasets within same seed')
            failures += [{'seed':seed,**x} for x in errors]
            for result in model_results:
                result['method']=canonical_method(result['method'])
                _save_result(out,seed,ds,result)
                if result['method']=='AccessSentinel TCN v3':
                    tcn=result['_trained_model']
                    save_tcn_checkpoint(result,idx,_window_encoder(ds),seed,out/'cache',ds)
        elif needs_tcn:
            result=run_accesssentinel_tcn(ds,seed=seed,quick=args.quick,device=args.device)
            tcn=result['_trained_model']
            save_tcn_checkpoint(result,idx,_window_encoder(ds),seed,out/'cache',ds)
        for result in model_results:result['method']=canonical_method(result['method'])
        records+=model_results
        for result in model_results:
            if set(figures)&{4,5}:ttd.extend(_ttd(ds,result,seed))
        if set(figures)&{4,5} and tcn is not None:
            obj=local_inference_latency(tcn,idx,idx.mask('test')[:256],args.device,
                                        repeats=10 if args.quick else 100)
            latency.setdefault('AccessSentinel TCN v3',[]).append(obj)
            (out/'raw'/f'fig4_latency_seed{seed}.json').write_text(json.dumps({'seed':seed,'dataset_fingerprint':fp,**obj},indent=2),encoding='utf-8')
        if set(figures)&{6,7,8,9}:
            if tcn is None:raise RuntimeError(f'No AccessSentinel TCN trained for seed {seed}')
            tgn,cfg_tgn,tgn_meta=load_or_train_tgn(ds,seed,args.device,args.tgn_epochs,out/'cache',reuse=args.reuse_cache)
            all_scores=tcn_all_scores(ds,idx,tcn,args.device,seed,out/'cache',reuse=args.reuse_cache) if set(figures)&{7,8,9} else None
            if 6 in figures:
                coll_rows+=evaluate_anchors(ds,tgn_meta,model_results,seed,all_scores,idx,
                                            out/'raw'/f'fig5_collusion_seed{seed}.json')
                _rewrite_json(out/'raw'/f'fig5_collusion_seed{seed}.json',quick_smoke_only=args.quick)
            if set(figures)&{7,8} and not args.skip_load:
                run_load_test(ds,idx,tcn,tgn,cfg_tgn,seed,args.device,args.load_duration,
                              [5,20,50,100],out/'raw'/f'fig6_scalability_seed{seed}.json',args.quick,history_scores=all_scores)
            if 9 in figures:
                evaluate_ablation(ds,idx,all_scores,tgn_meta,seed,out/'raw'/f'fig7_ablation_seed{seed}.json')
                _rewrite_json(out/'raw'/f'fig7_ablation_seed{seed}.json',quick_smoke_only=args.quick)
    figure_status={}
    if 2 in figures or 3 in figures:
        summary=_summary(records)
        write_csv(out/'tables'/'fig2_fig3_mean_std.csv',summary,['method','metric','n_seeds','mean','sd'])
        for nfig,metrics,filename in ((2,['precision','recall','f1'],'fig2_precision_recall_f1.png'),
                                       (3,['roc_auc','pr_auc'],'fig3_roc_auc_pr_auc.png')):
            if nfig in figures:
                ok=_grouped_plot(summary,METHOD_ORDER,metrics,out/'figures'/filename,
                                 f'Figure {nfig} — '+ ('Precision, Recall, F1' if nfig==2 else 'ROC-AUC, PR-AUC'),args.quick)
                figure_status[f'fig{nfig}']={'generated':ok,'present':[display_method(m) for m in METHOD_ORDER if any(canonical_method(r['method'])==m for r in records)],
                                               'complete_comparison':all(any(canonical_method(r['method'])==m for r in records) for m in METHOD_ORDER)}
    if set(figures)&{4,5}:
        write_csv(out/'tables'/'fig4_fig5_time_to_detect_by_attack.csv',ttd,
                  ['method','seed','attack_instance','scenario','n_attack_events','attack_start_epoch_s',
                   'first_detection_epoch_s','time_to_detect_seconds','detected','threshold'])
        lat_aggregate={}
        for name,rows in latency.items():
            lat_aggregate[name]={k:float(np.mean([r[k] for r in rows])) for k in ('p50_latency_ms','p95_latency_ms','p99_latency_ms')}
        write_csv(out/'tables'/'fig4_fig5_local_decision_latency.csv',
                  [{'method':m,'boundary':'local window->model->score->threshold',
                    'p50_latency_ms':lat_aggregate.get(m,{}).get('p50_latency_ms'),
                    'p95_latency_ms':lat_aggregate.get(m,{}).get('p95_latency_ms'),
                    'status':'MEASURED_LOCAL_ONLY' if m in lat_aggregate else 'NO_ONLINE_ADAPTER'} for m in METHOD_ORDER],
                  ['method','boundary','p50_latency_ms','p95_latency_ms','status'])
        if 4 in figures:
            figure_status['fig4']={'generated':_fig4_coverage_plot(ttd,out/'figures'/'fig4_detection_coverage.png',args.quick),
                                   'measure':'attack-instance coverage, missed attacks included'}
        if 5 in figures:
            figure_status['fig5']={'generated':_fig5_delay_plot(ttd,out/'figures'/'fig5_detection_delay.png',args.quick),
                                   'measure':'conditional median delay, detected attacks only; N/A is not zero'}
    if 6 in figures:
        write_csv(out/'tables'/'fig6_collusion_anchors.csv',coll_rows,
                  ['method','seed','threshold','evaluation_level','status','n_scored','n_total','positive_anchors',
                   'positive_scored','unscored_positives','coverage','tp','fp','fn','precision','recall','f1','roc_auc','pr_auc'])
        figure_status['fig6']={'generated':_fig6_collusion_plot(coll_rows,out/'figures'/'fig6_collusion_detection.png',args.quick,seeds),
                               'qualification':'Native TGN member/group score vs adapted individual event-score proxies at common test anchors; proxy metrics conditional on scored anchors',
                               'native_group_comparable_to_event_proxies':False}
    if 7 in figures:
        ok,message=plot_fig7(out/'raw',out/'figures'/'fig7_throughput.png',seeds=seeds)
        figure_status['fig7']={'generated':ok,'details':message}
    if 8 in figures:
        ok,message=plot_fig8(out/'raw',out/'figures'/'fig8_component_latency.png',seeds=seeds)
        figure_status['fig8']={'generated':ok,'details':message}
    if 9 in figures:
        ok,message=plot_fig9(out/'raw',out/'figures'/'fig9_ablation.png',seeds=seeds)
        figure_status['fig9']={'generated':ok,'details':message}
    # Remove superseded chart filenames only in this output root, never the raw data.
    old_names={
        'fig4_detection_speed.png':{4,5},
        'fig5_collusion_anchors.png':{6},
        'fig6_scalability.png':{7,8},
        'fig7_ablation.png':{9},
    }
    for old_name,affected in old_names.items():
        if set(figures)&affected:
            (out/'figures'/old_name).unlink(missing_ok=True)
    # A method present once is NOT a complete five-seed comparison.
    method_seed_pairs={(canonical_method(r['method']),r.get('seed')) for r in records}
    method_seed_complete=all((m,seed) in method_seed_pairs for m in METHOD_ORDER for seed in seeds)
    if 2 in figures or 3 in figures:
        for k in ('fig2','fig3'):
            if k in figure_status:
                figure_status[k]['all_requested_seeds_present']=method_seed_complete
                if not method_seed_complete:
                    figure_status[k]['generated']=False
                    filename=('fig2_precision_recall_f1.png' if k=='fig2' else 'fig3_roc_auc_pr_auc.png')
                    (out/'figures'/filename).unlink(missing_ok=True)
    candidate_complete = bool(not args.quick and len(seeds)==5 and set(seeds)=={0,1,2,3,4}
                              and not failures and set(figures)==set(range(2,10))
                              and method_seed_complete
                              and all(v.get('generated') and v.get('complete_comparison',True)
                                      for v in figure_status.values()))
    report={'mode':'smoke_nonreportable' if args.quick else 'full_synthetic',
            'candidate_protocol_complete':candidate_complete,
            'paper_ready':False,  # Requires an independent methods audit, not merely successful code execution.

            'figures_requested':figures,'seeds':seeds,'device':args.device,'tgn_epochs':args.tgn_epochs,
            'load_duration_seconds':args.load_duration,'elapsed_seconds':time.perf_counter()-t_start,
            'datasets':data_meta,'methods_completed':sorted({r['method'] for r in records}),
            'failures':failures,'figures':figure_status,
            'limitations':['All figures use synthetic AccessSentinel tcn_v3_eval data, never real production data.',
                           'References are common-data adaptations; several are event anomaly proxies rather than native group detectors.',
                           'Figures 4 and 5 use attack-instance detections; the separate local TCN callback timing table excludes gateway, Kafka, queue, PDP and enforcement.',
                           'Figures 7 and 8 use in-process offered-rate replay with a cold-start TGN; neither validates deployed distributed throughput.',
                           'Figure 9 measures collusion anchor outcomes with actual Escalator admission; ungated TCN + TGN is an upper-bound compute baseline.',
                           'Five seeds and full dataset are required before candidate paper evidence; accuracy and methodological comparability must still be reviewed.']}
    (out/'evaluation_report.json').write_text(json.dumps(report,indent=2,allow_nan=False,default=str),encoding='utf-8')
    print('\n=== RESULTS ===\n'+json.dumps({'mode':report['mode'],'figures':figure_status,'failures':failures,'candidate_protocol_complete':report['candidate_protocol_complete'],'paper_ready':report['paper_ready']},indent=2),flush=True)
    print(f'Output root: {out}',flush=True)
    return 0 if not failures and all(v['generated'] for v in figure_status.values()) else 2


if __name__=='__main__':
    raise SystemExit(main())
