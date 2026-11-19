"""Guardrails for validity and plot coverage. Fixtures are NOT paper measurements."""
import json
from pathlib import Path

import numpy as np
import pytest

from accesssentinel.ml.train_tgn import _anchor_labels, collusion_preflight
from research_comparison.measured_experiments import _binary_metrics, _validate_native_anchors
from research_comparison.figures_6_7 import ABLATION_ORDER, plot_fig9
from research_comparison.run_evaluation import (
    _fig4_coverage_plot, _fig5_delay_plot, _fig6_collusion_plot,
    canonical_method, display_method,
)


def test_undefined_single_class_is_not_reported_as_zero_f1():
    result = _binary_metrics([0,0,0], [.1,.3,.2], .25)
    assert result['status'] == 'UNDEFINED_SINGLE_CLASS'
    assert result['f1'] is None and result['pr_auc'] is None
    assert result['positive_anchors'] == 0


def test_unscored_positives_are_explicit_and_metrics_conditional():
    result = _binary_metrics([1,0,1], [.9,.2,None], .5)
    assert result['status'] == 'VALID_SCORED_SUBSET_ONLY'
    assert result['n_scored'] == 2
    assert result['positive_anchors'] == 2
    assert result['positive_scored'] == 1
    assert result['unscored_positives'] == 1
    assert result['coverage'] == pytest.approx(2/3)


def test_native_tgn_requires_both_classes_and_valid_scores():
    with pytest.raises(RuntimeError,match='positive and 2 negative'):
        _validate_native_anchors([{'seed_label':0,'s_coll':.2}]*2,'Fig5')
    with pytest.raises(RuntimeError,match='nonfinite'):
        _validate_native_anchors([{'seed_label':1,'s_coll':float('nan')},
                                  {'seed_label':0,'s_coll':.2}],'Fig5')


def test_member_ground_truth_is_intersected_with_model_visible_members():
    rec={'features':{'a':[.1,.2,.3], 'b':[.3,.2,.1]},
         'edges':[{'u':'b','coll':1}]}  # replay edge-label is not the anchor's ground truth
    assert _anchor_labels(rec, ['a', 'missing']) == {'a'}


def test_fig6_requires_native_method_and_per_seed_completeness(tmp_path):
    names = ['AccessSentinel TCN + TGN (native group)',
             'Ref. 14 DynAD','Ref. 17 PG-AID','Ref. 21 ACOBE',
             'Ref. 22 FGDT','Ref. 24 User-resource embedding','Ref. 25 DEEPRO']
    rows=[]
    for m in names:
        for seed in (0,1):
            rows.append({'method':m,'seed':seed,'status':'VALID_FULL',
                         'precision':.1,'recall':.2,'f1':.14,'coverage':1.})
    path=tmp_path/'fig6.png'
    assert _fig6_collusion_plot(rows,path,expected_seeds=[0,1])
    assert path.stat().st_size>10000
    assert not _fig6_collusion_plot(rows[:-1],path,expected_seeds=[0,1])
    assert not path.exists()


def test_fig9_rejects_zero_positive_anchors(tmp_path):
    path=tmp_path/'fig9.png'
    path.write_bytes(b'stale')
    records=[{'configuration':m,'n_test_anchors':12,'positive_anchors':0,
              'status':'UNDEFINED_SINGLE_CLASS','precision':None,
              'recall':None,'f1':None} for m in ABLATION_ORDER]
    (tmp_path/'fig7_ablation_seed0.json').write_text(json.dumps({
        'status':'complete','seed':0,'dataset_fingerprint':'fixture','dataset_events':100,
        'positive_test_anchors':0,'negative_test_anchors':12,
        'measurements':records}),encoding='utf8')
    ok,message=plot_fig9(tmp_path,path,seeds=[0])
    assert not ok and 'absent positive' in message
    assert not path.exists()


def test_reference_labels_include_citations_without_changing_ids():
    names = {
        'DynAD': 'DynAD [14]', 'PG-AID': 'PG-AID [17]',
        'ACOBE': 'ACOBE [21]', 'FGDT': 'FGDT [22]',
        'User-resource embedding': 'User-resource embedding [24]',
        'DEEPRO': 'DEEPRO [25]',
    }
    for old, new in names.items():
        canonical = canonical_method(old)
        assert display_method(canonical) == new
        assert canonical_method(new) == canonical
    assert display_method('AccessSentinel TCN + TGN (native group)') == 'AccessSentinel'


def test_fig4_fig5_separate_from_same_attack_records(tmp_path):
    rows = [
        {'method':'AccessSentinel TCN v3','detected':1,'time_to_detect_seconds':0.},
        {'method':'AccessSentinel TCN v3','detected':1,'time_to_detect_seconds':60.},
        {'method':'Ref. 14 DynAD','detected':0,'time_to_detect_seconds':None},
        {'method':'Ref. 14 DynAD','detected':0,'time_to_detect_seconds':None},
    ]
    cov = tmp_path/'fig4_detection_coverage.png'
    delay = tmp_path/'fig5_detection_delay.png'
    assert _fig4_coverage_plot(rows,cov)
    assert _fig5_delay_plot(rows,delay)
    assert cov.stat().st_size > 10000 and delay.stat().st_size > 10000
