"""Common AccessSentinel adapters for the six selected related-paper cores.

All adapters use the same synthetic tcn_v3_eval event stream and chronological
train/validation/test boundaries. Labels are used only for fitting where the
method is supervised and for validation/test evaluation, never as input features.
The provenance and group methods are explicitly schema adaptations, not exact
reproductions of their original datasets.
"""
from __future__ import annotations
import time, warnings
from collections import defaultdict
import numpy as np
import torch
from sklearn.metrics import roc_auc_score, average_precision_score
from accesssentinel.datasets.synth import SynthConfig, generate
from accesssentinel.ml.dataset import build_v3
from accesssentinel.ml.features import SimulatedDevPosture, SimulatedThreatFeed
from research_comparison.methods.ref14_dynad import DynADCore, snapshots_from_events, train_dynad
from research_comparison.methods.ref17_pg_aid import (TemporalSnapshotBuilder, extract_process_resource_paths,
    fit_tfidf_path_selector, select_tfidf_paths, PathDoc2Vec, SnapshotTransformer,
    train_snapshot_transformer, cosine_deviation)
from research_comparison.methods.ref21_acobe import ACOBECore, role_cohorts, ASPECTS, _features
from research_comparison.methods.ref22_fgdt import build_coreview_groups, FGDTCore, fgdt_training_loss
from research_comparison.methods.ref25_deepro import build_access_provenance_graph, DEEPROCore, train_deepro
from research_comparison.methods.common import user_id, event_time

DEVICE = 'cpu'
_INDEX_CACHE = {}

def make_dataset(seed: int, quick: bool=False):
    cfg = SynthConfig.tcn_v3_eval(seed=23+seed)
    if quick:
        from dataclasses import replace
        cfg = replace(cfg, n_users=120, days=30, split_days=(16,6,8),
                      attack_margin_days=(0,3,3),
                      attacks={k:(1,1,1) for k in cfg.attacks},
                      collusion_groups={k:(1,1,1) for k in cfg.collusion_groups})
    return generate(cfg)

def _split(ds, name):
    return sorted([e for e in ds.events if ds.split_of(e.t)==name], key=lambda e:(e.t,e.event_id))

def _labels(ds, events):
    return np.asarray([int(ds.labels[e.event_id].individual) for e in events], dtype=np.int64)

def _window_index(ds):
    key=id(ds)
    if key not in _INDEX_CACHE:
        threat,posture=SimulatedThreatFeed(),SimulatedDevPosture.from_dataset(ds)
        idx,enc,_=build_v3(ds.events,ds.labels,ds.split_of,threat,posture)
        _INDEX_CACHE.clear(); _INDEX_CACHE[key]=(idx,enc)
    return _INDEX_CACHE[key][0]

def _window_encoder(ds):
    _window_index(ds)
    return _INDEX_CACHE[id(ds)][1]

def _window_targets(ds, idx, split):
    ids=idx.mask(split)
    return ids, np.asarray(idx.y[ids],dtype=np.int64), [str(idx.event_ids[i]) for i in ids]

def _select_scores(score_by_event, event_ids):
    return np.asarray([score_by_event.get(str(eid),np.nan) for eid in event_ids],dtype=float)

def _event_metrics(y, score):
    y=np.asarray(y,dtype=int); score=np.asarray(score,dtype=float)
    ok=np.isfinite(score); y=y[ok]; score=score[ok]
    if len(y)==0 or len(np.unique(y))<2: raise ValueError('test scores require both classes')
    return {'roc_auc':float(roc_auc_score(y,score)), 'pr_auc':float(average_precision_score(y,score)),
            'n_scored':int(len(y)), 'n_unscored':int((~ok).sum())}

def _calibrate(yv, sv):
    nominal=np.asarray(sv)[(np.asarray(yv)==0)&np.isfinite(sv)]
    if not len(nominal): return float('inf')
    return float(np.quantile(nominal,.95))

def _result(name, yv, sv, yt, st, start, note, extra=None):
    m=_event_metrics(yt,st); threshold=_calibrate(yv,sv)
    pred=np.asarray(st)>=threshold; yy=np.asarray(yt,dtype=int); valid=np.isfinite(st); pred=pred[valid]; yy=yy[valid]
    tp=int(((pred==1)&(yy==1)).sum()); fp=int(((pred==1)&(yy==0)).sum()); fn=int(((pred==0)&(yy==1)).sum())
    precision=tp/max(1,tp+fp); recall=tp/max(1,tp+fn)
    m.update({'precision':precision,'recall':recall,'f1':2*precision*recall/max(1e-12,precision+recall),
              'validation_threshold':threshold})
    return {'method':name,'metrics':m,'fit_and_score_seconds':time.perf_counter()-start,
            'protocol_note':note,'extra':extra or {}}

def run_dynad(ds, epochs=4):
    start=time.perf_counter(); tr,va,te=(_split(ds,x) for x in ('train','val','test')); idx=_window_index(ds)
    _,yv,vids=_window_targets(ds,idx,'val'); _,yt,tids=_window_targets(ds,idx,'test')
    train_labels={e.event_id:ds.labels[e.event_id].individual for e in tr}
    train_snaps=snapshots_from_events(tr,window_seconds=86400,feature_dim=32,labels=train_labels)
    if not train_snaps or not any(s.edge_y is not None and len(torch.unique(s.edge_y))>1 for s in train_snaps):
        raise ValueError('DynAD train snapshots do not contain both edge classes')
    torch.manual_seed(100+int(ds.config.seed)); model=DynADCore(32,hidden_dim=16,layers=1,pool_k=8)
    train_dynad(model,train_snaps,epochs=epochs,learning_rate=1e-3)
    def score(events):
        snaps=snapshots_from_events(events,window_seconds=86400,feature_dim=32)
        out=[]
        model.eval()
        with torch.no_grad():
            for snap in snaps:
                z=model.forward_snapshot(snap)['edge_scores'].cpu().numpy()
                out.extend(z[::2].tolist())
        return np.asarray(out[:len(events)],float)
    # State is advanced through validation before test, as a temporal detector should be.
    sv=score(va); st=score(te)
    sv=_select_scores(dict(zip([e.event_id for e in va],sv)),vids); st=_select_scores(dict(zip([e.event_id for e in te],st)),tids)
    return _result('Ref. 14 DynAD',yv,sv,yt,st,start,
        'Evolving GCN/GRU temporal graph core; event edges are user-resource interactions. Train labels supervise training snapshots only.')

def run_pgaid(ds, epochs=4):
    """PG-AID adaptation with train-only TF-IDF/Doc2Vec fitting and causal snapshot alignment."""
    start=time.perf_counter()
    tr,va,te=(_split(ds,x) for x in ('train','val','test'))
    idx=_window_index(ds)
    _,yv,vids=_window_targets(ds,idx,'val'); _,yt,tids=_window_targets(ds,idx,'test')
    if not tr or not va or not te:
        raise ValueError('PG-AID requires non-empty train, validation, and test splits')

    # Build one chronological snapshot stream. This avoids assuming that independently
    # built train/all streams have identical snapshot boundaries after cache forgetting.
    builder=TemporalSnapshotBuilder(snapshot_nodes=100,forgetting_rate=.2)
    all_events=tr+va+te
    all_snaps=builder.build(all_events)
    for snap in all_snaps:
        snap.paths=extract_process_resource_paths(snap,max_paths=20)

    train_end=max(event_time(e) for e in tr)
    train_snaps=[snap for snap in all_snaps if snap.end_time <= train_end]
    if len(train_snaps)<3:
        raise ValueError(f'PG-AID needs at least 3 complete train snapshots; found {len(train_snaps)}')
    if not any(snap.paths for snap in train_snaps):
        raise ValueError('PG-AID training snapshots contain no process-resource paths')

    # Fit inverse-document-frequency statistics on train snapshots only, then reuse
    # exactly those statistics for validation and test snapshots.
    tfidf_stats=fit_tfidf_path_selector(train_snaps)
    select_tfidf_paths(all_snaps,rng_seed=ds.config.seed,idf_stats=tfidf_stats)
    if not any(snap.selected_paths for snap in train_snaps):
        raise ValueError('PG-AID training snapshots have no selected meta-paths')

    encoder=PathDoc2Vec(vector_size=16,epochs=10,seed=ds.config.seed)
    train_emb=encoder.fit_transform(train_snaps)
    train_snapshot_ids={id(snap) for snap in train_snaps}
    train_embedding_by_snapshot={id(snap):snap.embedding.copy() for snap in train_snaps}

    # Infer each later snapshot independently with a deterministic per-snapshot seed.
    # No validation/test snapshot is used to fit the Doc2Vec vocabulary or IDF weights.
    inferred_count=0
    all_embeddings=[]
    for snap in all_snaps:
        if id(snap) in train_snapshot_ids:
            all_embeddings.append(train_embedding_by_snapshot[id(snap)])
            continue
        docs=[]
        for path_index,path in enumerate(snap.selected_paths):
            if not path:
                continue
            if hasattr(encoder.model,'random'):
                encoder.model.random.seed(int(ds.config.seed + 1009*snap.snapshot_id + path_index))
            docs.append(encoder.model.infer_vector(path,epochs=10))
        embedding=(np.mean(np.asarray(docs),axis=0).astype(np.float32)
                   if docs else np.zeros(16,np.float32))
        snap.embedding=embedding
        all_embeddings.append(embedding)
        inferred_count += 1
    all_emb=np.stack(all_embeddings).astype(np.float32)

    # Fit next-snapshot prediction using training snapshots only.
    sequence_length=2
    torch.manual_seed(1700+int(ds.config.seed))
    model=SnapshotTransformer(embedding_dim=16,model_dim=16,heads=4,layers=1,dropout=0.0)
    train_snapshot_transformer(model,train_emb,sequence_length=sequence_length,
                               epochs=epochs,learning_rate=1e-3)

    # Score later snapshots online. Each target snapshot is compared with a prediction
    # from its immediately preceding snapshots; no future snapshot enters its context.
    scores=np.full(len(all_emb),np.nan,dtype=np.float64)
    model.eval()
    with torch.no_grad():
        x=torch.as_tensor(all_emb,dtype=torch.float32)
        for i in range(sequence_length,len(x)):
            pred=model(x[i-sequence_length:i].unsqueeze(0))
            scores[i]=float(cosine_deviation(pred,x[i].unsqueeze(0)).item())

    def event_scores(events):
        out=[]
        for event in events:
            t=event_time(event)
            candidates=[i for i,snap in enumerate(all_snaps)
                        if snap.start_time <= t <= snap.end_time]
            out.append(scores[candidates[-1]] if candidates else np.nan)
        return np.asarray(out,dtype=float)

    sv=event_scores(va); st=event_scores(te)
    sv=_select_scores(dict(zip([e.event_id for e in va],sv)),vids)
    st=_select_scores(dict(zip([e.event_id for e in te],st)),tids)
    finite=scores[np.isfinite(scores)]
    diagnostics={
        'n_all_snapshots':int(len(all_snaps)),
        'n_train_snapshots':int(len(train_snaps)),
        'n_scored_snapshots':int(len(finite)),
        'n_inferred_snapshots':int(inferred_count),
        'snapshot_score_std':float(np.std(finite)) if len(finite) else 0.0,
        'snapshot_score_min':float(np.min(finite)) if len(finite) else None,
        'snapshot_score_max':float(np.max(finite)) if len(finite) else None,
        'tfidf_fit_snapshots':int(tfidf_stats['n_docs']),
        'doc2vec_train_only':True,
        'deterministic_inference':True,
    }
    return _result('Ref. 17 PG-AID',yv,sv,yt,st,start,
        'Cache snapshots, train-only TF-IDF and Doc2Vec, and next-snapshot Transformer. Interaction-provenance paths approximate OS provenance.',
        diagnostics)


def _acobe_training_vocab(train_events):
    rows=[_features(e) for e in train_events]
    return {aspect:{field:sorted({row[field] for row in rows}) for field in fields}
            for aspect,fields in ASPECTS.items()}


def _acobe_group_prototypes(train_events, cohorts, vocab):
    """Build fixed cohort baselines from training events only."""
    by_group=defaultdict(list)
    for event in train_events:
        by_group[str(cohorts.get(user_id(event),user_id(event)))].append(event)
    prototypes={}
    for group,events in by_group.items():
        rows=[_features(e) for e in events]
        prototypes[group]={}
        for aspect,fields in ASPECTS.items():
            prototypes[group][aspect]={}
            for field in fields:
                categories=vocab[aspect][field]
                counts=np.zeros(len(categories),dtype=np.float32)
                lookup={value:i for i,value in enumerate(categories)}
                for row in rows:
                    j=lookup.get(row[field])
                    if j is not None:
                        counts[j]+=1.0
                counts/=max(1,len(rows))
                prototypes[group][aspect][field]=counts
    return prototypes


def _acobe_split_features(events, cohorts, vocab, prototypes):
    """Create fixed-width user vectors; validation/test use only train-fitted vocab/cohorts."""
    by_user=defaultdict(list)
    for event in events:
        by_user[user_id(event)].append(event)
    users=sorted(by_user)
    matrices={aspect:[] for aspect in ASPECTS}
    for user in users:
        rows=[_features(e) for e in by_user[user]]
        group=str(cohorts.get(user,user))
        for aspect,fields in ASPECTS.items():
            parts=[]
            for field in fields:
                categories=vocab[aspect][field]
                lookup={value:i for i,value in enumerate(categories)}
                individual=np.zeros(len(categories),dtype=np.float32)
                for row in rows:
                    j=lookup.get(row[field])
                    if j is not None:
                        individual[j]+=1.0
                individual/=max(1,len(rows))
                group_vector=prototypes.get(group,{}).get(aspect,{}).get(
                    field,np.zeros(len(categories),dtype=np.float32))
                parts.extend([individual,group_vector])
            matrices[aspect].append(np.concatenate(parts).astype(np.float32))
    return {'users':users,'matrices':{a:np.stack(v) for a,v in matrices.items()},
            'vocabulary':vocab,'group_by_user':cohorts}


def run_acobe(ds, epochs=3):
    """ACOBE with fixed train-fitted vocabulary/cohort baselines and split-safe scoring."""
    start=time.perf_counter()
    tr,va,te=(_split(ds,x) for x in ('train','val','test'))
    idx=_window_index(ds)
    _,yv,vids=_window_targets(ds,idx,'val'); _,yt,tids=_window_targets(ds,idx,'test')
    if not tr or not va or not te:
        raise ValueError('ACOBE requires non-empty train, validation, and test splits')

    cohorts=role_cohorts(tr)
    vocab=_acobe_training_vocab(tr)
    prototypes=_acobe_group_prototypes(tr,cohorts,vocab)
    train_features=_acobe_split_features(tr,cohorts,vocab,prototypes)
    val_features=_acobe_split_features(va,cohorts,vocab,prototypes)
    test_features=_acobe_split_features(te,cohorts,vocab,prototypes)

    train_users=set(train_features['users'])
    y_by_user={u:max(int(ds.labels[e.event_id].individual)
                     for e in tr if user_id(e)==u) for u in train_users}
    nominal_mask=np.asarray([y_by_user.get(u,0)==0 for u in train_features['users']],dtype=bool)
    if int(nominal_mask.sum())<2:
        raise ValueError('ACOBE needs at least two nominal training users')

    core=ACOBECore(history_days=30,delta=3,votes=2)
    train_masks={aspect:nominal_mask for aspect in train_features['matrices']}
    torch.manual_seed(2100+int(ds.config.seed))
    core.fit(train_features,nominal_train_masks=train_masks,epochs=epochs,
             batch_size=64,learning_rate=1.0)
    val_score=core.score(val_features)
    test_score=core.score(test_features)

    # ACOBE's investigation priority is rank 1 for the highest anomaly score.
    # Invert the rank so the common benchmark convention remains higher = anomalous.
    val_user_score={u:float(1.0/(1.0+val_score['priority'][i]))
                    for i,u in enumerate(val_features['users'])}
    test_user_score={u:float(1.0/(1.0+test_score['priority'][i]))
                     for i,u in enumerate(test_features['users'])}
    sv=_select_scores({e.event_id:val_user_score.get(user_id(e),np.nan) for e in va},vids)
    st=_select_scores({e.event_id:test_user_score.get(user_id(e),np.nan) for e in te},tids)
    diagnostics={
        'n_train_users':int(len(train_features['users'])),
        'n_validation_users':int(len(val_features['users'])),
        'n_test_users':int(len(test_features['users'])),
        'n_nominal_train_users':int(nominal_mask.sum()),
        'n_anomalous_train_users':int((~nominal_mask).sum()),
        'priority_std':float(np.std(test_score['priority'])),
        'priority_min':int(np.min(test_score['priority'])),
        'priority_max':int(np.max(test_score['priority'])),
        'aspect_score_std':{name:float(np.std(values)) for name,values in test_score['scores'].items()},
        'vocabulary_fit_split':'train',
        'cohort_baseline_fit_split':'train',
        'future_split_features_used':False,
    }
    return _result('Ref. 21 ACOBE',yv,sv,yt,st,start,
        'Aspect autoencoders and rank-based investigation priority; train-fitted role cohorts and categorical vocabulary. User-level scores are broadcast to events.',
        diagnostics)

def _groups_for(events, feature_dim=24, max_members=8):
    groups=build_coreview_groups(events,window_seconds=7*86400,min_shared_resources=1,feature_dim=feature_dim)
    out=[]
    for g in groups:
        # Keep every user represented when a connected component is larger than the model limit.
        # Consecutive deterministic chunks are candidate subgroups of the co-access component.
        for lo in range(0,len(g.members),max_members):
            ids=np.arange(lo,min(lo+max_members,len(g.members)))
            if len(ids)<2: continue
            out.append(type(g)([g.members[i] for i in ids],[w[np.ix_(ids,ids)] for w in g.windows],g.features[ids]))
    return out

def _scale_fgdt_groups(train_groups, *other_group_sets):
    """Fit a log1p/standardization transform on train groups and reuse it elsewhere."""
    train_rows=np.concatenate([np.log1p(np.maximum(g.features,0.0)) for g in train_groups],axis=0)
    mean=train_rows.mean(axis=0)
    std=train_rows.std(axis=0)
    std=np.where(std<1e-4,1.0,std)
    def transform(groups):
        out=[]
        for g in groups:
            x=np.log1p(np.maximum(g.features,0.0))
            x=np.clip((x-mean)/std,-5.0,5.0).astype(np.float32)
            out.append(type(g)(list(g.members),list(g.windows),x))
        return out
    return transform(train_groups),[transform(groups) for groups in other_group_sets],{
        'feature_mean_abs':float(np.mean(np.abs(mean))),
        'feature_scale_median':float(np.median(std)),
        'feature_value_abs_max_after_clip':5.0,
    }


def run_fgdt(ds, epochs=4):
    start=time.perf_counter()
    tr,va,te=(_split(ds,x) for x in ('train','val','test'))
    idx=_window_index(ds)
    _,yv,vids=_window_targets(ds,idx,'val'); _,yt,tids=_window_targets(ds,idx,'test')
    train_groups_raw=_groups_for(tr); val_groups_raw=_groups_for(va); test_groups_raw=_groups_for(te)
    if not train_groups_raw or not val_groups_raw or not test_groups_raw:
        raise ValueError('FGDT needs co-access candidate groups in train, validation, and test splits')

    train_groups,(val_groups,test_groups),feature_diagnostics=_scale_fgdt_groups(
        train_groups_raw,val_groups_raw,test_groups_raw)

    # Precompute train-only labels and balanced class weights.
    train_batches=[]
    member_labels_all=[]
    group_labels_all=[]
    for g in train_groups:
        y_users={u:max([int(ds.labels[e.event_id].individual) for e in tr if user_id(e)==u] or [0])
                 for u in g.members}
        ml=torch.tensor([y_users[u] for u in g.members],dtype=torch.long)
        gl=int(any(y_users.values()))
        train_batches.append((g,ml,gl))
        member_labels_all.extend(ml.tolist())
        group_labels_all.append(gl)
    member_counts=np.bincount(np.asarray(member_labels_all,dtype=np.int64),minlength=2)
    member_weights=np.ones(2,dtype=np.float32)
    present=member_counts>0
    member_weights[present]=len(member_labels_all)/(2.0*member_counts[present])
    group_counts=np.bincount(np.asarray(group_labels_all,dtype=np.int64),minlength=2)
    group_pos_weight=float(group_counts[0]/max(1,group_counts[1]))

    torch.manual_seed(300+int(ds.config.seed))
    model=FGDTCore(feature_dim=24,max_group_size=8,hidden_dim=16,temporal_hidden=16)
    opt=torch.optim.AdamW(model.parameters(),lr=3e-4,weight_decay=1e-4)
    member_losses=[]; group_losses=[]; total_losses=[]
    for _ in range(int(epochs)):
        for g,ml,gl in train_batches:
            model.train(); opt.zero_grad(set_to_none=True)
            x=torch.as_tensor(g.features,dtype=torch.float32)
            mats=[torch.as_tensor(w,dtype=torch.float32) for w in g.windows[-4:]]
            if not mats:
                mats=[torch.zeros((len(g.members),len(g.members)),dtype=torch.float32)]
            out=model.forward_group(x,mats)
            loss,components=fgdt_training_loss(
                out,ml,gl,member_class_weights=member_weights,
                group_pos_weight=group_pos_weight,return_components=True)
            if not torch.isfinite(loss):
                raise ValueError('FGDT produced a non-finite training loss')
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(),max_norm=2.0)
            opt.step()
            total_losses.append(float(loss.detach()))
            if 'member' in components: member_losses.append(components['member'])
            if 'group' in components: group_losses.append(components['group'])

    def scores(groups):
        user_scores={}; member_values=[]; group_values=[]
        model.eval()
        with torch.no_grad():
            for g in groups:
                x=torch.as_tensor(g.features,dtype=torch.float32)
                mats=[torch.as_tensor(w,dtype=torch.float32) for w in g.windows[-4:]]
                if not mats:
                    mats=[torch.zeros((len(g.members),len(g.members)),dtype=torch.float32)]
                out=model.forward_group(x,mats)
                probs=out['member_probabilities'][:,1].cpu().numpy()
                member_values.extend(probs.tolist())
                group_values.append(float(out['group_score'].cpu()))
                for i,user in enumerate(g.members):
                    user_scores[user]=max(user_scores.get(user,0.0),float(probs[i]))
        return user_scores,np.asarray(member_values),np.asarray(group_values)

    vs,val_member_scores,val_group_scores=scores(val_groups)
    ts,test_member_scores,test_group_scores=scores(test_groups)
    sv=_select_scores({e.event_id:vs.get(user_id(e),np.nan) for e in va},vids)
    st=_select_scores({e.event_id:ts.get(user_id(e),np.nan) for e in te},tids)
    diagnostics={
        **feature_diagnostics,
        'train_member_class_0':int(member_counts[0]),
        'train_member_class_1':int(member_counts[1]),
        'train_group_class_0':int(group_counts[0]),
        'train_group_class_1':int(group_counts[1]),
        'member_class_weights':member_weights.tolist(),
        'group_positive_weight':group_pos_weight,
        'mean_training_loss':float(np.mean(total_losses)) if total_losses else None,
        'mean_member_loss':float(np.mean(member_losses)) if member_losses else None,
        'mean_group_loss':float(np.mean(group_losses)) if group_losses else None,
        'validation_member_score_std':float(np.std(val_member_scores)) if len(val_member_scores) else 0.0,
        'test_member_score_std':float(np.std(test_member_scores)) if len(test_member_scores) else 0.0,
        'test_member_score_min':float(np.min(test_member_scores)) if len(test_member_scores) else None,
        'test_member_score_max':float(np.max(test_member_scores)) if len(test_member_scores) else None,
        'test_group_score_std':float(np.std(test_group_scores)) if len(test_group_scores) else 0.0,
        'n_group_predictions':int(len(test_group_scores)),
        'input_features_log1p_standardized':True,
        'member_classifier_uses_logits':True,
    }
    return _result('Ref. 22 FGDT',yv,sv,yt,st,start,
        'Co-access groups with train-fitted feature scaling, weighted member cross-entropy and group BCE; event behavior vectors replace review representations.',
        diagnostics)

def run_deepro(ds, epochs=3):
    start=time.perf_counter(); tr,va,te=(_split(ds,x) for x in ('train','val','test')); idx=_window_index(ds)
    _,yv,vids=_window_targets(ds,idx,'val'); _,yt,tids=_window_targets(ds,idx,'test')
    # Build graph from training events only; score later split actors using train graph embeddings plus a separate
    # full-period interaction graph only if all actors are already represented. Avoid using test labels.
    graph=build_access_provenance_graph(tr,feature_dim=16)
    train_users=sorted({user_id(e) for e in tr})
    labels={u:max([ds.labels[e.event_id].individual for e in tr if user_id(e)==u] or [0]) for u in train_users}
    torch.manual_seed(500+int(ds.config.seed)); model=DEEPROCore(input_dim=16,hidden_dim=16,heads=4)
    train_deepro(model,graph,labels,epochs=epochs,learning_rate=.003)
    # Train graph process scores are the only supported out-of-sample evidence in this adaptation.
    model.eval()
    with torch.no_grad(): out=model.forward_graph(graph)
    process_nodes=[graph.node_ids[i].split(':',1)[1] for i in graph.process_indices]
    user_scores={u:float(out['process_probabilities'][i,1]) for i,u in enumerate(process_nodes)}
    sv=_select_scores({e.event_id:user_scores.get(user_id(e),np.nan) for e in va},vids); st=_select_scores({e.event_id:user_scores.get(user_id(e),np.nan) for e in te},tids)
    return _result('Ref. 25 DEEPRO',yv,sv,yt,st,start,
        'MAGNN process/file/network meta-path core trained on train-period interaction provenance; user/resource nodes proxy process/entity nodes. Unseen test users remain unscored.')

def run_accesssentinel_tcn(ds, seed=0, quick=False, device='cpu'):
    """Train AccessSentinel's TCN-v3 on the exact common dataset and window split."""
    from dataclasses import replace
    from accesssentinel.config import TCNv3Config
    from accesssentinel.ml.train_tcn import train_variant, scores_v3
    start=time.perf_counter(); idx=_window_index(ds); enc=_window_encoder(ds)
    tr,va,te=idx.mask('train'),idx.mask('val'),idx.mask('test')
    cfg = TCNv3Config()
    if quick:
        cfg = replace(
            cfg,
            pretrain_epochs=1,
            finetune_epochs=1,
            batch_size=512,
        )
    else:
        # Reduced training budget for full-dataset comparison.
        cfg = replace(
            cfg,
            pretrain_epochs=1,
            finetune_epochs=3,
        )

    model,history,best=train_variant(idx,enc,cfg,'tcn-v3',tr,va,device,seed,log=lambda *a:None)
    yv=np.asarray(idx.y[va],dtype=int); yt=np.asarray(idx.y[te],dtype=int)
    sv=scores_v3(model,idx,va,device); st=scores_v3(model,idx,te,device)
    return _result('AccessSentinel TCN v3',yv,sv,yt,st,start,
        'AccessSentinel Revised-v3 TCN trained on the same generated dataset and chronological window split as all reference adaptations.',
        {'best_validation_pr_auc':float(best),'epochs_pretrain':cfg.pretrain_epochs,'epochs_finetune':cfg.finetune_epochs,
         'training_epochs_completed':len(history) if hasattr(history,'__len__') else None,'device':device})

def run_all_selected(seed=0,quick=False,device='cpu',train_accesssentinel=True):
    np.random.seed(1000+seed); torch.manual_seed(1000+seed); torch.set_num_threads(1)
    ds=make_dataset(seed,quick)
    # Build the shared window index once, outside per-method timing, so all methods use identical samples.
    _window_index(ds)
    results=[]; errors=[]
    if train_accesssentinel:
        try:
            print('  -> run_accesssentinel_tcn',flush=True)
            results.append(run_accesssentinel_tcn(ds,seed,quick,device))
        except Exception as exc:
            errors.append({'method':'AccessSentinel TCN v3','error':f'{type(exc).__name__}: {exc}'})
            warnings.warn(f'AccessSentinel TCN v3 failed: {exc}')
    methods=[run_ref24,run_dynad,run_pgaid,run_acobe,run_fgdt,run_deepro]
    for fn in methods:
        try:
            print(f'  -> {fn.__name__}',flush=True)
            if fn is run_ref24: results.append(fn(ds,seed=seed))
            else: results.append(fn(ds,epochs=1 if quick else 4))
        except Exception as exc:
            errors.append({'method':fn.__name__,'error':f'{type(exc).__name__}: {exc}'})
            warnings.warn(f'{fn.__name__} failed: {exc}')
    return ds,results,errors

def run_ref24(ds, seed=0, dim=8):
    """Reference [24] user-resource spectral embedding on the exact shared dataset."""
    from research_comparison.methods.ref24_user_resource_embedding import UserResourceSpectralNN, Ref24Config
    start=time.perf_counter(); tr,va,te=(_split(ds,x) for x in ('train','val','test')); idx=_window_index(ds)
    _,yv,vids=_window_targets(ds,idx,'val'); _,yt,tids=_window_targets(ds,idx,'test')
    model=UserResourceSpectralNN(Ref24Config(embedding_dim=dim,distance='cosine'))
    model.fit(tr)
    for e in tr:
        model.prior_users_by_resource.setdefault(f'{e.tenant}/{e.a}:{e.r}',set()).add(str(e.u))
    sv=[]; st=[]
    val_map={}; test_map={}
    for e in va: val_map[e.event_id]=model.score_and_update(e)
    for e in te: test_map[e.event_id]=model.score_and_update(e)
    return _result('Ref. 24 User-resource embedding',yv,_select_scores(val_map,vids),
                   yt,_select_scores(test_map,tids),start,
                   'Regularized bipartite spectral embedding and nearest prior user on resource; fitted on training-period interactions only.',
                   {'embedding_dim':dim,'distance':'cosine'})

