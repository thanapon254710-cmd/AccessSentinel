"""Small deterministic engineering/unit fixtures; NOT research measurements."""
from dataclasses import replace
import json
from pathlib import Path

import numpy as np
import pytest
import torch

from accesssentinel.events import AccessEvent
from accesssentinel.datasets.synth import SynthDataset, SynthConfig, Label
from accesssentinel.ml.dataset import build_v3
from accesssentinel.ml.features import SimulatedDevPosture, SimulatedThreatFeed
from accesssentinel.config import TCNv3Config, TGNv3Config
from accesssentinel.tcn import TCNv3
from accesssentinel.tgn import TGNv3
from research_comparison.memory_redis import MemoryRedis
from research_comparison.measured_experiments import _traces, evaluate_ablation, local_inference_latency, run_load_test
from research_comparison.figures_6_7 import plot_fig7, plot_fig8, plot_fig9


def _tiny():
    cfg=replace(SynthConfig.tcn_v3_eval(seed=3),n_users=1,days=2,attacks={},collusion_groups={})
    events=[AccessEvent(u='u1',t=float(1_700_000_000+i),a='portal',r='r1' if i%2 else 'r2',
                        s=int(i%3==0),p='read',o='view',
                        c={'ip':'10.1.2.3','device':'d1'},event_id=f'event-{i:04d}') for i in range(800)]
    labels={e.event_id:Label() for e in events}
    splits={'train':(1_700_000_000.,1_700_000_300.),
            'val':(1_700_000_300.,1_700_000_550.),
            'test':(1_700_000_550.,1_700_000_850.)}
    ds=SynthDataset(events,labels,{'u1':{}},splits,{},cfg)
    idx,encoder,_=build_v3(ds.events,ds.labels,ds.split_of,SimulatedThreatFeed(),SimulatedDevPosture.from_dataset(ds))
    return ds,idx,encoder


def test_memory_redis_pipeline_and_zrange():
    r=MemoryRedis()
    p=r.pipeline(transaction=False)
    p.zadd('r',{'b':3,'a':1}).zremrangebyscore('r','-inf',2).zrangebyscore('r','-inf','+inf')
    assert p.execute()==[2,1,['b']]
    r.hset('h','u','hello')
    assert r.hget('h','u')=='hello'
    assert r.hincrby('h','count',3)==3
    assert r.set('lk',1,nx=True)
    assert not r.set('lk',2,nx=True)


def test_true_escalator_replay_accepts_all_ablations():
    ds,idx,_=_tiny()
    scores=np.linspace(.1,.9,len(idx),dtype=float)
    for config in (
        'TCN + TGN without resource-triggered escalation',
        'TCN + TGN without periodic sweep',
        'TCN + TGN without random sampling',
        'Full AccessSentinel',
    ):
        timestamps,report=_traces(ds,idx,scores,3,config,0.7,0.8)
        assert isinstance(timestamps,dict)
        assert report['configuration']==config
        assert report['enabled_paths']['high'] is True
        assert report['wall_seconds']>=0


def test_offered_rate_runs_real_tcn_and_tgn_callbacks(tmp_path):
    torch.set_num_threads(1)
    ds,idx,enc=_tiny()
    m=TCNv3(TCNv3Config(),enc.n_res,enc.n_op,enc.n_net).eval()
    g=TGNv3(TGNv3Config()).eval()
    data=run_load_test(ds,idx,m,g,TGNv3Config(),seed=3,device='cpu',duration=.12,
                       rates=[5,20,50,100],out=tmp_path/'fig6_scalability_seed3.json',quick=True,
                       history_scores=np.zeros(len(idx),dtype=float))
    assert set(row['event_rate_per_second'] for row in data)=={5,20,50,100}
    assert set(row['component'] for row in data)=={'TCN','TGN','Resource-triggered escalation','Periodic sweep','Random sampling'}
    assert all(row['n_operations']>0 for row in data)
    assert len(data)==20
    payload=json.loads((tmp_path/'fig6_scalability_seed3.json').read_text())
    assert payload['status']=='complete' and payload['dataset_fingerprint']==ds.fingerprint()
    assert plot_fig7(tmp_path,tmp_path/'fig7.png')[0]
    assert plot_fig8(tmp_path,tmp_path/'fig8.png')[0]


def test_incomplete_measurements_refuse_png(tmp_path):
    out=tmp_path/'fig9.png'
    out.write_bytes(b'stale')
    p=tmp_path/'fig7_ablation_seed0.json'
    p.write_text(json.dumps({'status':'complete','seed':0,'dataset_events':506000,
                             'dataset_fingerprint':'unit-test','measurements':[{'configuration':'TCN only','f1':1.}]}))
    ok,msg=plot_fig9(tmp_path,out)
    assert not ok and not out.exists() and 'Incomplete' in msg


def test_all_six_policy_ablations_generate_only_from_real_replay(tmp_path):
    ds, idx, _ = _tiny()
    scores = np.linspace(.1, .9, len(idx), dtype=float)
    anchors = [{'t':float(ds.events[i].t), 'seed':'u1', 'seed_label':int(i%2==0),
                's_coll':float(.25 if i%3 else .8)} for i in (570,585,610,630,660,690,720,750)]
    tgn_meta = {'test_anchors':anchors,'thresholds':{'theta_C':.6}}
    path = tmp_path/'fig7_ablation_seed3.json'
    rows=evaluate_ablation(ds,idx,scores,tgn_meta,3,path)
    assert len(rows)==6
    assert len({r['configuration'] for r in rows})==6
    obj=json.loads(path.read_text())
    assert obj['status']=='complete'
    assert obj['escalation_trace_metadata']['Full AccessSentinel']['enabled_paths'] == {
        'high':True,'resource':True,'sweep':True,'sample':True}
    ok, msg = plot_fig9(tmp_path,tmp_path/'fig9.png',seeds=[3])
    assert ok, msg
    assert (tmp_path/'fig9.png').stat().st_size > 10_000
    wrong, _ = plot_fig9(tmp_path,tmp_path/'wrong_seed.png',seeds=[0])
    assert not wrong and not (tmp_path/'wrong_seed.png').exists()


def test_local_decision_latency_uses_real_forward(tmp_path):
    torch.set_num_threads(1)
    ds, idx, enc = _tiny()
    model=TCNv3(TCNv3Config(),enc.n_res,enc.n_op,enc.n_net).eval()
    measured=local_inference_latency(model,idx,idx.mask('test')[:3],device='cpu',repeats=3)
    assert measured['samples']==3
    assert measured['p50_latency_ms'] > 0
    assert 'gateway' in measured['excluded']
