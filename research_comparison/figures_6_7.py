"""Figures 6 and 7 from fully measured, reproducible JSON records only.

No placeholder PNG, empty CSV, interpolated datum or partial-configuration
figure is ever produced. A failed completeness check removes stale PNG.
"""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

COMPONENT_ORDER = ['TCN','TGN','Resource-triggered escalation','Periodic sweep','Random sampling']
ABLATION_ORDER = ['TCN only','TCN + TGN',
                  'TCN + TGN without resource-triggered escalation',
                  'TCN + TGN without periodic sweep',
                  'TCN + TGN without random sampling','Full AccessSentinel']
RATES = (5, 20, 50, 100)


def _load(raw, name, seeds=None):
    if seeds is None:
        paths = sorted(raw.glob(f'{name}_seed*.json'))
    else:
        paths = [raw / f'{name}_seed{seed}.json' for seed in seeds]
        if any(not p.exists() for p in paths):
            missing = [p.name for p in paths if not p.exists()]
            return [], f'Missing measured seed files: {missing}'
    files = [json.loads(p.read_text(encoding='utf-8')) for p in paths]
    if seeds is not None and any(f.get('seed') != int(seed) for f, seed in zip(files, seeds)):
        return [], 'Requested seed identity mismatch'
    return files, None


def _fail(out, message):
    # An invalid run must never leave a stale graph that looks reportable.
    out.unlink(missing_ok=True)
    return False, message


def _complete(files, mandatory, method, units):
    if not files: return 'No measured seed JSON files'
    seeds=set()
    for f in files:
        if f.get('status')!='complete': return f"Seed {f.get('seed')}: not complete"
        if f.get('seed') in seeds: return f"Duplicate seed {f.get('seed')}"
        seeds.add(f.get('seed'))
        if not isinstance(f.get('dataset_fingerprint'),str) or not f.get('dataset_events'):
            return 'Missing original dataset identity'
        rows=f.get('measurements',[])
        keyset={tuple(row.get(k) for k in mandatory) for row in rows}
        if len(rows)!=len(keyset):return f'Duplicate {method} measurements for seed {f.get("seed")}'
        if keyset!=units:
            missing=units-keyset
            return f'Incomplete {method} measurement matrix in seed {f.get("seed")}: missing {list(missing)[:5]}'
    return None


def plot_fig6(raw_dir:Path, out_path:Path, seeds=None):
    files,error=_load(raw_dir, 'fig6_scalability', seeds)
    if error:return _fail(out_path,error)
    expected={(rate, comp) for rate in RATES for comp in COMPONENT_ORDER}
    error=_complete(files,['event_rate_per_second','component'],'Fig6',expected)
    if error:return _fail(out_path,error)
    for record in files:
        for row in record['measurements']:
            if any(not np.isfinite(float(row[k])) for k in ('throughput_events_per_second','p95_latency_ms','attained_end_to_end_events_per_second')):
                return _fail(out_path,'Nonfinite Fig. 6 measurement')
            if row['n_operations']<=0:return _fail(out_path,'A Fig6 component did no work')
    fig, axes=plt.subplots(1,2,figsize=(14,6.2))
    x=np.array(RATES,dtype=float)
    achieved=[]; achieved_sd=[]
    for rate in RATES:
        vals=[r['attained_end_to_end_events_per_second'] for f in files for r in f['measurements']
              if r['component']=='TCN' and r['event_rate_per_second']==rate]
        achieved.append(float(np.mean(vals)))
        achieved_sd.append(float(np.std(vals,ddof=1)) if len(vals)>1 else 0.0)
    axes[0].plot(x,x,'--',label='Ideal attained = offered')
    axes[0].errorbar(x,achieved,yerr=achieved_sd,marker='o',capsize=4,label='Measured complete in-process pipeline')
    axes[0].set_xlabel('Offered rate (requests/s)');axes[0].set_ylabel('Attained throughput (requests/s)')
    axes[0].set_xticks(RATES);axes[0].set_title('Actual processed access requests');axes[0].legend(fontsize=8)
    for comp in COMPONENT_ORDER:
        means=[];errs=[]
        for rate in RATES:
            vals=[float(r['p95_latency_ms']) for f in files for r in f['measurements']
                  if r['component']==comp and r['event_rate_per_second']==rate]
            means.append(float(np.mean(vals)))
            errs.append(float(np.std(vals,ddof=1)) if len(vals)>1 else 0.0)
        axes[1].errorbar(x,means,yerr=errs,marker='o',capsize=2,label=comp)
    axes[1].set_xticks(RATES);axes[1].set_xlabel('Offered rate (requests/s)')
    axes[1].set_ylabel('P95 local component execution time (ms)')
    axes[1].set_title('Measured component service latency')
    axes[1].legend(fontsize=7,frameon=False)
    for ax in axes:ax.grid(alpha=.25)
    tag='ENGINEERING SMOKE ONLY — NOT PAPER RESULTS' if any(f.get('quick_smoke_only') for f in files) else 'In-process component replay; not deployed Kafka/Redis/HDFS throughput'
    fig.suptitle(f'Figure 6 — Scalability ({len(files)} seed(s))\n{tag}',fontsize=11)
    fig.tight_layout();out_path.parent.mkdir(parents=True,exist_ok=True)
    fig.savefig(out_path,dpi=230,bbox_inches='tight');plt.close(fig)
    return True,f'Measured 5 components × 4 rates × {len(files)} seed(s), fully populated'


def plot_fig7(raw_dir:Path, out_path:Path, seeds=None):
    files,error=_load(raw_dir,'fig7_ablation',seeds)
    if error:return _fail(out_path,error)
    units={(name,) for name in ABLATION_ORDER}
    error=_complete(files,['configuration'],'Fig7',units)
    if error:return _fail(out_path,error)
    for f in files:
        if any(int(row.get('n_test_anchors', 0)) <= 0 for row in f['measurements']):
            return _fail(out_path,'Fig7 has no test anchors')
        for row in f['measurements']:
            if any(row.get(k) is None or not 0<=float(row[k])<=1 for k in ('precision','recall','f1')):
                return _fail(out_path,'Missing/invalid Fig7 measured metric')
    fig,ax=plt.subplots(figsize=(14.5,6.5))
    x=np.arange(len(ABLATION_ORDER));width=.25
    for j,metric in enumerate(('precision','recall','f1')):
        avg,sd=[],[]
        for name in ABLATION_ORDER:
            vals=[float(r[metric]) for f in files for r in f['measurements'] if r['configuration']==name]
            avg.append(float(np.mean(vals)))
            sd.append(float(np.std(vals,ddof=1)) if len(vals)>1 else 0.0)
        ax.bar(x+(j-1)*width,avg,width,yerr=sd,capsize=3,label=metric.upper())
    ax.set_xticks(x,ABLATION_ORDER,rotation=20,ha='right');ax.set_ylim(0,1)
    ax.set_ylabel('Collusion-anchor classification (0–1)')
    ax.legend(ncol=3,frameon=False)
    ax.grid(axis='y',alpha=.25)
    tag='ENGINEERING SMOKE ONLY — NOT PAPER RESULTS' if any(f.get('quick_smoke_only') for f in files) else 'Shared test anchors; independent four-path Escalator replay per configuration'
    ax.set_title(f'Figure 7 — Six AccessSentinel ablations ({len(files)} seed(s))\n{tag}',fontsize=11)
    fig.tight_layout();out_path.parent.mkdir(parents=True,exist_ok=True)
    fig.savefig(out_path,dpi=230,bbox_inches='tight');plt.close(fig)
    return True,f'Measured all 6 ablations across {len(files)} seed(s)'
