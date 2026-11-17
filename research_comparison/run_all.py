"""Single-command common-data comparison for AccessSentinel and selected references.

Run from repository root: python -m research_comparison.run_all
Use --quick for a small engineering smoke run only; do not report quick-run values.
"""
from __future__ import annotations
import argparse, csv, json, statistics, sys, time
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from research_comparison.selected_adapters import run_all_selected

HERE=Path(__file__).resolve().parent
OUT=HERE/'results'

def _write_csv(path, rows, fields):
    with path.open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore'); w.writeheader(); w.writerows(rows)

def _aggregate(records):
    grouped={}
    for r in records: grouped.setdefault(r['method'],[]).append(r)
    summary=[]
    for method,items in grouped.items():
        for metric in ('roc_auc','pr_auc','precision','recall','f1'):
            vals=[float(x['metrics'][metric]) for x in items if x.get('metrics',{}).get(metric) is not None]
            summary.append({'method':method,'metric':metric,'n':len(vals),
                'mean':statistics.mean(vals) if vals else None,
                'sd':statistics.stdev(vals) if len(vals)>1 else (0.0 if vals else None)})
        times=[float(x['fit_and_score_seconds']) for x in items if x.get('fit_and_score_seconds') is not None]
        summary.append({'method':method,'metric':'fit_and_score_seconds','n':len(times),
            'mean':statistics.mean(times) if times else None,
            'sd':statistics.stdev(times) if len(times)>1 else (0.0 if times else None)})
    return summary

def _plot(summary, metric, filename, title, ylabel, bounds=None):
    rows=[r for r in summary if r['metric']==metric and r['mean'] is not None]
    if not rows: return
    fig,ax=plt.subplots(figsize=(max(9,len(rows)*1.45),5.5))
    x=list(range(len(rows))); y=[r['mean'] for r in rows]; err=[r['sd'] or 0 for r in rows]
    ax.bar(x,y,yerr=err,capsize=4)
    ax.set_xticks(x,[r['method'] for r in rows],rotation=25,ha='right')
    if bounds: ax.set_ylim(*bounds)
    ax.set_title(title); ax.set_ylabel(ylabel); ax.grid(axis='y',alpha=.2)
    fig.tight_layout(); fig.savefig(OUT/'figures'/filename,dpi=250); plt.close(fig)

def main():
    ap=argparse.ArgumentParser(description='Compare AccessSentinel with six selected paper-method adaptations')
    ap.add_argument('--seeds',default='0,1,2,3,4',help='Comma-separated seeds')
    ap.add_argument('--quick',action='store_true',help='Small smoke run; results are not research-reportable')
    ap.add_argument('--device',default='cuda' if __import__('torch').cuda.is_available() else 'cpu',help='Device for AccessSentinel TCN-v3 training')
    ap.add_argument('--skip-accesssentinel-training',action='store_true',help='Skip AccessSentinel TCN-v3 training')
    ap.add_argument('--train-accesssentinel',action='store_true',help='Also train TCN-v3 during --quick (normally skipped because CPU smoke training is expensive)')
    args=ap.parse_args(); seeds=[int(x.strip()) for x in args.seeds.split(',') if x.strip()]
    if not seeds: ap.error('At least one seed is required')
    for sub in ('raw','tables','figures'): (OUT/sub).mkdir(parents=True,exist_ok=True)
    records=[]; failures=[]; started=time.perf_counter()
    for seed in seeds:
        print(f'\n=== AccessSentinel common-data comparison | seed {seed} ===',flush=True)
        ds,results,errors=run_all_selected(seed=seed,quick=args.quick,device=args.device,train_accesssentinel=(not args.skip_accesssentinel_training and (not args.quick or args.train_accesssentinel)))
        for r in results:
            row={**r,'seed':seed,'dataset_fingerprint':ds.fingerprint(),
                 'dataset_events':len(ds.events),'quick_smoke_only':bool(args.quick)}
            records.append(row)
            path=OUT/'raw'/f"{r['method'].lower().replace(' ','_').replace('.','').replace('[','').replace(']','')}_seed{seed}.json"
            path.write_text(json.dumps(row,indent=2,default=str),encoding='utf-8')
            print(f"  {r['method']}: ROC-AUC={r['metrics']['roc_auc']:.4f}, PR-AUC={r['metrics']['pr_auc']:.4f}",flush=True)
        for e in errors: failures.append({'seed':seed,**e})
    flat=[]
    for r in records:
        m=r['metrics']; flat.append({'seed':r['seed'],'method':r['method'],**m,
            'fit_and_score_seconds':r.get('fit_and_score_seconds'),
            'dataset_fingerprint':r['dataset_fingerprint'],'dataset_events':r['dataset_events']})
    _write_csv(OUT/'tables'/'all_seed_metrics.csv',flat,
        ['seed','method','roc_auc','pr_auc','precision','recall','f1','n_scored','n_unscored','validation_threshold','fit_and_score_seconds','dataset_fingerprint','dataset_events'])
    summary=_aggregate(records)
    _write_csv(OUT/'tables'/'mean_std_summary.csv',summary,['method','metric','n','mean','sd'])
    _plot(summary,'roc_auc','roc_auc_mean_std.png','AccessSentinel vs selected references — ROC-AUC','ROC-AUC',(0,1))
    _plot(summary,'pr_auc','pr_auc_mean_std.png','AccessSentinel vs selected references — PR-AUC','PR-AUC',(0,1))
    _plot(summary,'fit_and_score_seconds','runtime_mean_std.png','Reference adaptation runtime','Seconds')
    report={'seeds_requested':seeds,'quick_smoke_only':bool(args.quick),
        'accesssentinel_tcn_training_skipped':bool(args.quick and not args.train_accesssentinel) or bool(args.skip_accesssentinel_training),
        'methods_requested':['AccessSentinel TCN v3','Reference [24]','Reference [14] DynAD','Reference [17] PG-AID','Reference [21] ACOBE','Reference [22] FGDT','Reference [25] DEEPRO'],
        'methods_with_results':sorted({r['method'] for r in records}),
        'failures':failures,'runs_completed':len(records),'elapsed_seconds':time.perf_counter()-started,
        'protocol':'same generated AccessSentinel tcn_v3_eval dataset per seed; chronological train/validation/test windows; threshold calibrated on nominal validation scores; no test-label threshold tuning',
        'timing_boundary':'Per-method fit_and_score_seconds excludes shared dataset generation and shared build_v3 window-index construction; includes method-specific graph/features/model fit and scoring.',
        'limitations':['Synthetic AccessSentinel data only; no public real-world post-authentication access-control benchmark is supplied.','References [17] and [25] use interaction-provenance approximations, not OS-level audit provenance.','Reference [21] uses role as a cohort proxy; Reference [22] adapts review-group detection to co-access groups.','Reference [24] uses its documented fixed embedding dimension and cosine-distance adaptation.','AccessSentinel TCN-v3 is trained on the common dataset for full runs; quick smoke runs skip it unless --train-accesssentinel is supplied.','Quick runs are engineering smoke tests and must not be reported as final research results.']}
    (OUT/'run_report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print('\n=== COMPARISON SUMMARY ==='); print(json.dumps(report,indent=2)); print(f'\nOutputs: {OUT}')
    if failures: print('\nSome methods did not complete; inspect run_report.json.')
    if not records: sys.exit(1)
if __name__=='__main__': main()
