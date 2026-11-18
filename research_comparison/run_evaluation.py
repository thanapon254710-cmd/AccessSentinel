"""Reproducible synthetic AccessSentinel evaluation, Figures 2--7.

Full paper run (real measurement, expensive):
  python -m research_comparison.run_evaluation --figures 2,3,4,5,6,7 --seeds 0,1,2,3,4 --device cuda

Engineering run (strictly NOT reportable):
  python -m research_comparison.run_evaluation --quick --seeds 0 --tgn-epochs 2 --load-duration 0.6

Replot measured data without retraining:
  python -m research_comparison.run_evaluation --reuse-cache --seeds 0 --figures 2,3,4,5,7 --tgn-epochs 2 --quick

Figures 6/7 are never fabricated, and a plot is not written if mandatory
measurements are missing. Full and quick outputs use completely separate roots.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import time
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import torch

from research_comparison.figures_6_7 import plot_fig6, plot_fig7
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
    'Ref. 14 DynAD': 'DynAD', 'Ref. 17 PG-AID': 'PG-AID',
    'Ref. 21 ACOBE': 'ACOBE', 'Ref. 22 FGDT': 'FGDT',
    'Ref. 24 User-resource embedding': 'User-resource embedding',
    'Ref. 25 DEEPRO': 'DEEPRO',
}
METHOD_ALIASES = {label: method for method, label in DISPLAY_NAMES.items()}
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


def _fig4_plot(ttd, latency, out, quick=False):
    """Report coverage and conditional delay separately; missing attacks are not zero-latency."""
    methods=[m for m in METHOD_ORDER if any(canonical_method(r['method'])==m for r in ttd)]
    if not methods:return False
    fig,axes=plt.subplots(1,2,figsize=(15,6.2))
    coverage=[];delay=[];counts=[]
    for method in methods:
        sub=[r for r in ttd if canonical_method(r['method'])==method]
        observed=[float(r['time_to_detect_seconds']) for r in sub
                  if r['detected'] and r['time_to_detect_seconds'] is not None]
        coverage.append(len(observed)/len(sub) if sub else np.nan)
        delay.append(float(np.median(observed))/60.0 if observed else np.nan)
        counts.append(f'{len(observed)}/{len(sub)}')
    x=np.arange(len(methods)); labels=[display_method(m) for m in methods]
    axes[0].bar(x,coverage)
    axes[0].set_xticks(x,labels,rotation=28,ha='right')
    axes[0].set_ylim(0,1.12)
    axes[0].set_ylabel('Attack-instance detection coverage (0–1)')
    axes[0].set_title('Detection coverage (missed attacks included)')
    for i,(v,n) in enumerate(zip(coverage,counts)):
        if np.isfinite(v):axes[0].text(i,min(v+0.035,1.09),n,ha='center',fontsize=8)
    axes[1].bar(x,delay)
    axes[1].set_xticks(x,labels,rotation=28,ha='right')
    axes[1].set_ylabel('Median time-to-detect (minutes)')
    axes[1].set_title('Detected attacks only; N/A = none detected')
    for i,v in enumerate(delay):
        if not np.isfinite(v):axes[1].text(i,0,'N/A',ha='center',va='bottom',fontsize=9)
    for ax in axes:ax.grid(axis='y',alpha=.25)
    fig.suptitle('Figure 4 — Attack detection coverage and conditional delay'
                 +(' — SMOKE ONLY' if quick else ''),fontsize=12)
    fig.tight_layout();out.parent.mkdir(parents=True,exist_ok=True)
    fig.savefig(out,dpi=240);plt.close(fig)
    return True


def _fig5_plot(rows,out,quick=False):
    methods=['AccessSentinel TCN v3',
             'Ref. 14 DynAD','Ref. 17 PG-AID','Ref. 21 ACOBE','Ref. 22 FGDT','Ref. 25 DEEPRO']
    summary=[]
    for method in methods:
        sub=[r for r in rows if canonical_method(r['method'])==method]
        for metric in ('precision','recall','f1'):
            vals=[r[metric] for r in sub if r.get(metric) is not None]
            summary.append({'method':method,'metric':metric,'mean':float(np.mean(vals)) if vals else None,
                            'sd':float(np.std(vals,ddof=1)) if len(vals)>1 else 0.0})
    ok=_grouped_plot(summary,methods,['precision','recall','f1'],out,
                     'Figure 5 — Collusion test anchors (AS native group; refs event-score proxies)',quick)
    return ok


def _rewrite_json(p,**updates):
    obj=json.loads(p.read_text(encoding='utf-8'));obj.update(updates)
    p.write_text(json.dumps(obj,indent=2,allow_nan=False,default=str),encoding='utf-8')


def main(argv=None):
    ap=argparse.ArgumentParser(description='Measured AccessSentinel Figures 2–7, same synthetic dataset per seed')
    ap.add_argument('--figures',default='2,3,4,5,6,7')
    ap.add_argument('--seeds',default='0,1,2,3,4')
    ap.add_argument('--device',default='cuda' if torch.cuda.is_available() else 'cpu')
    ap.add_argument('--quick',action='store_true',help='Smaller data; NEVER paper results')
    ap.add_argument('--reuse-cache',action='store_true',help='Reuse trained models/score JSON; Fig6 still remeasures real load unless --skip-load')
    ap.add_argument('--skip-load',action='store_true',help='Only replot existing Fig6 load JSON; do not run new offered-rate test')
    ap.add_argument('--skip-accesssentinel-training',action='store_true',help='Reference-only checks of Fig2-4 (not complete)')
    ap.add_argument('--train-accesssentinel',action='store_true',help='Compatibility flag; TCN trains by default')
    ap.add_argument('--tgn-epochs',type=int,default=6,help='Real TGN-v3 train budget per seed; same budget reused for ablations')
    ap.add_argument('--load-duration',type=float,default=2.0,help='Seconds of wall-clock offered events at each of 5,20,50,100 req/s')
    ap.add_argument('--output-dir',type=Path,default=None)
    args=ap.parse_args(argv)
    try:figures=sorted({int(s) for s in args.figures.split(',') if s.strip()})
    except ValueError:ap.error('--figures requires comma-separated numbers')
    if not figures or not set(figures)<=set(range(2,8)):ap.error('Figures must be in 2,3,4,5,6,7')
    try:seeds=[int(x.strip()) for x in args.seeds.split(',') if x.strip()]
    except ValueError:ap.error('--seeds requires comma-separated integers')
    if not seeds or len(seeds)!=len(set(seeds)):ap.error('Provide at least one unique integer seed')
    if args.tgn_epochs < 1 or args.load_duration<=0:ap.error('--tgn-epochs and --load-duration must be positive')
    if args.device.startswith('cuda') and not torch.cuda.is_available():ap.error('CUDA requested but torch.cuda.is_available() is false')
    if args.skip_accesssentinel_training and set(figures)&{5,6,7}:ap.error('Figures 5–7 require a trained AccessSentinel TCN')
    out=args.output_dir or (ROOT/'results'/('evaluation_v4_smoke' if args.quick else 'evaluation_v4'))
    for folder in ('raw','cache','figures','tables'):(out/folder).mkdir(parents=True,exist_ok=True)
    t_start=time.perf_counter()
    records=[];ttd=[];coll_rows=[];failures=[];data_meta=[];latency={}
    print(f'Evaluation results: {out}\nMode: {"QUICK / ENGINEERING ONLY" if args.quick else "FULL DATASET"}',flush=True)
    for seed in seeds:
        print(f'\n=== SEED {seed} ===',flush=True)
        ds=make_dataset(seed,args.quick)
        n=len(ds.events);fp=ds.fingerprint()
        if not args.quick and not 500000<=n<=510000:
            raise RuntimeError(f'Full dataset must have approximately 506k events, got {n:,} for seed {seed}')
        data_meta.append({'seed':seed,'events':n,'fingerprint':fp,'synthetic':True})
        print(f'Dataset: {n:,} events; fingerprint {fp}',flush=True)
        idx=_window_index(ds)
        requires_25=bool(set(figures)&{2,3,4,5})
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
            if 4 in figures:ttd.extend(_ttd(ds,result,seed))
        if 4 in figures and tcn is not None:
            obj=local_inference_latency(tcn,idx,idx.mask('test')[:256],args.device,
                                        repeats=10 if args.quick else 100)
            latency.setdefault('AccessSentinel TCN v3',[]).append(obj)
            (out/'raw'/f'fig4_latency_seed{seed}.json').write_text(json.dumps({'seed':seed,'dataset_fingerprint':fp,**obj},indent=2),encoding='utf-8')
        if set(figures)&{5,6,7}:
            if tcn is None:raise RuntimeError(f'No AccessSentinel TCN trained for seed {seed}')
            tgn,cfg_tgn,tgn_meta=load_or_train_tgn(ds,seed,args.device,args.tgn_epochs,out/'cache',reuse=args.reuse_cache)
            all_scores=tcn_all_scores(ds,idx,tcn,args.device,seed,out/'cache',reuse=args.reuse_cache) if 6 in figures or 7 in figures else None
            if 5 in figures:
                coll_rows+=evaluate_anchors(ds,tgn_meta,model_results,seed,all_scores,idx,
                                            out/'raw'/f'fig5_collusion_seed{seed}.json')
                _rewrite_json(out/'raw'/f'fig5_collusion_seed{seed}.json',quick_smoke_only=args.quick)
            if 6 in figures and not args.skip_load:
                run_load_test(ds,idx,tcn,tgn,cfg_tgn,seed,args.device,args.load_duration,
                              [5,20,50,100],out/'raw'/f'fig6_scalability_seed{seed}.json',args.quick,history_scores=all_scores)
            if 7 in figures:
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
    if 4 in figures:
        write_csv(out/'tables'/'fig4_time_to_detect_by_attack.csv',ttd,
                  ['method','seed','attack_instance','scenario','n_attack_events','attack_start_epoch_s',
                   'first_detection_epoch_s','time_to_detect_seconds','detected','threshold'])
        lat_aggregate={}
        for name,rows in latency.items():
            lat_aggregate[name]={k:float(np.mean([r[k] for r in rows])) for k in ('p50_latency_ms','p95_latency_ms','p99_latency_ms')}
        write_csv(out/'tables'/'fig4_local_decision_latency.csv',
                  [{'method':m,'boundary':'local window->model->score->threshold',
                    'p50_latency_ms':lat_aggregate.get(m,{}).get('p50_latency_ms'),
                    'p95_latency_ms':lat_aggregate.get(m,{}).get('p95_latency_ms'),
                    'status':'MEASURED_LOCAL_ONLY' if m in lat_aggregate else 'NO_ONLINE_ADAPTER'} for m in METHOD_ORDER],
                  ['method','boundary','p50_latency_ms','p95_latency_ms','status'])
        ok=_fig4_plot(ttd,lat_aggregate,out/'figures'/'fig4_detection_speed.png',args.quick)
        figure_status['fig4']={'generated':ok,'latency_boundary':'local AccessSentinel TCN score and threshold; not system end-to-end'}
    if 5 in figures:
        write_csv(out/'tables'/'fig5_collusion_anchors.csv',coll_rows,
                  ['method','seed','threshold','evaluation_level','n_scored','n_total','positive_anchors',
                   'coverage','tp','fp','fn','precision','recall','f1','roc_auc','pr_auc'])
        figure_status['fig5']={'generated':_fig5_plot(coll_rows,out/'figures'/'fig5_collusion_anchors.png',args.quick),
                               'qualification':'Native TGN member/group score vs adapted individual event-score proxies at common test anchors'}
    if 6 in figures:
        ok,message=plot_fig6(out/'raw',out/'figures'/'fig6_scalability.png',seeds=seeds)
        figure_status['fig6']={'generated':ok,'details':message}
    if 7 in figures:
        ok,message=plot_fig7(out/'raw',out/'figures'/'fig7_ablation.png',seeds=seeds)
        figure_status['fig7']={'generated':ok,'details':message}
    candidate_complete = bool(not args.quick and len(seeds)==5 and set(seeds)=={0,1,2,3,4}
                              and not failures and set(figures)=={2,3,4,5,6,7}
                              and set(METHOD_ORDER)<=set(canonical_method(r['method']) for r in records)
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
                           'Figure 4 local TCN callback time excludes gateway, Kafka, queue, PDP and enforcement.',
                           'Figure 6 is in-process offered-rate replay with a cold-start TGN; it does not validate deployed distributed throughput.',
                           'Figure 7 measures collusion anchor outcomes with actual Escalator admission; ungated TCN + TGN is an upper-bound compute baseline.',
                           'Five seeds and full dataset are required before candidate paper evidence; accuracy and methodological comparability must still be reviewed.']}
    (out/'evaluation_report.json').write_text(json.dumps(report,indent=2,allow_nan=False,default=str),encoding='utf-8')
    print('\n=== RESULTS ===\n'+json.dumps({'mode':report['mode'],'figures':figure_status,'failures':failures,'candidate_protocol_complete':report['candidate_protocol_complete'],'paper_ready':report['paper_ready']},indent=2),flush=True)
    print(f'Output root: {out}',flush=True)
    return 0 if not failures and all(v['generated'] for v in figure_status.values()) else 2


if __name__=='__main__':
    raise SystemExit(main())
