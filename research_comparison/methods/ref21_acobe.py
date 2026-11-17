"""Reference [21] ACOBE core: weighted long-term behavioral deviations, group context,
aspect-specific autoencoders and rank-based investigation critic.

AccessSentinel mapping: event fields are grouped into four access-behavior aspects and the
available role metadata is used as a cohort proxy for the paper's organizational department.
This is an explicit common-data adaptation; it is not the CERT r6.1/r6.2 experiment.
"""
from __future__ import annotations
from collections import defaultdict
from datetime import datetime, timezone
from typing import Sequence
import numpy as np
import torch
from torch import nn
import torch.nn.functional as F
from .common import event_time, user_id

ASPECTS = {
    "resource_access": ("app","resource"),
    "operation": ("operation","permission"),
    "environment": ("device","ip_prefix","session"),
    "policy": ("sensitivity","decision"),
}

def _day_and_frame(ts: float):
    dt = datetime.fromtimestamp(float(ts),tz=timezone.utc)
    return dt.date().isoformat(), 0 if 6 <= dt.hour < 18 else 1

def _features(e):
    c = getattr(e,"c",{}) or {}
    return {
        "app":str(getattr(e,"a","")),
        "resource":str(getattr(e,"r","")),
        "operation":str(getattr(e,"o","")),
        "permission":str(getattr(e,"p","")),
        "device":str(c.get("device","unknown")),
        "ip_prefix":str(c.get("ip","unknown")).rsplit(".",1)[0],
        "session":str(c.get("session","unknown")),
        "sensitivity":str(int(getattr(e,"s",0))),
        "decision":"allow" if int(getattr(e,"y",1)) else "deny",
    }

def role_cohorts(events: Sequence) -> dict[str,str]:
    """Return each user's modal role from non-label event metadata (cohort proxy for department)."""
    from collections import Counter
    roles=defaultdict(Counter)
    for e in events:
        role=str((getattr(e,"meta",{}) or {}).get("role","unknown"))
        roles[user_id(e)][role]+=1
    return {u:c.most_common(1)[0][0] for u,c in roles.items()}

class CompoundDeviationBuilder:
    def __init__(self, history_days: int = 30, delta: float = 3.0, eps: float = 1e-6):
        self.history_days, self.delta, self.eps = int(history_days),float(delta),float(eps)

    def build(self, events: Sequence, group_by_user: dict[str,str] | None = None,
              vocabulary_events: Sequence | None = None):
        if not events: raise ValueError("ACOBE requires at least one event")
        ordered = sorted(events,key=lambda e:(event_time(e),str(getattr(e,"event_id",""))))
        users = sorted({user_id(e) for e in ordered})
        days = sorted({_day_and_frame(event_time(e))[0] for e in ordered})
        day_i = {d:i for i,d in enumerate(days)}
        # Fixed, deterministic vocabularies are learned from event fields, never labels.
        vocab_source = list(vocabulary_events) if vocabulary_events is not None else ordered
        vocab = {a:{k:sorted({v[k] for e in vocab_source for v in [_features(e)]})
                 for k in keys} for a,keys in ASPECTS.items()}
        idx = {a:{k:{v:i for i,v in enumerate(vocab[a][k])} for k in keys}
               for a,keys in ASPECTS.items()}
        raw = {u:{a:np.zeros((sum(len(vocab[a][k]) for k in ASPECTS[a]),len(days),2),np.float32)
                  for a in ASPECTS} for u in users}
        offsets={}
        for a,keys in ASPECTS.items():
            pos=0
            for k in keys:
                offsets[(a,k)]=pos; pos+=len(vocab[a][k])
        for e in ordered:
            u=user_id(e); d,frame=_day_and_frame(event_time(e)); di=day_i[d]; vals=_features(e)
            for a,keys in ASPECTS.items():
                for k in keys:
                    category_index = idx[a][k].get(vals[k])
                    if category_index is None:
                        continue  # unseen validation/test category: keep the train-fitted feature space fixed
                    j=offsets[(a,k)] + category_index
                    raw[u][a][j,di,frame]+=1.0
        groups = group_by_user or {}
        # If no cohort supplied, each user is its own group; do not invent a shared organization.
        members=defaultdict(list)
        for u in users: members[str(groups.get(u,u))].append(u)
        group_raw={}
        for g,us in members.items():
            group_raw[g]={a:np.mean([raw[u][a] for u in us],axis=0) for a in ASPECTS}
        matrices={a:[] for a in ASPECTS}
        for u in users:
            g=str(groups.get(u,u))
            for a in ASPECTS:
                user_x=raw[u][a]; group_x=group_raw[g][a]
                user_dev=self._deviations(user_x); group_dev=self._deviations(group_x)
                # Compound matrix stacks individual and group behavior before flattening.
                compound=np.concatenate([user_dev,group_dev],axis=0)
                matrices[a].append(compound.reshape(-1).astype(np.float32))
        return {"users":users,"days":days,"matrices":{a:np.stack(v) for a,v in matrices.items()},
                "vocabulary":vocab,"group_by_user":groups or {}}

    def _deviations(self,x):
        out=np.zeros_like(x,dtype=np.float32)
        for d in range(x.shape[1]):
            lo=max(0,d-self.history_days); hist=x[:,lo:d,:]
            if hist.shape[1]==0: continue
            mean=hist.mean(axis=(1,2))
            std=hist.std(axis=(1,2))
            z=(x[:,d,:]-mean[:,None])/np.maximum(std[:,None],self.eps)
            z=np.clip(z,-self.delta,self.delta)
            weights=1.0/np.log2(np.maximum(std,2.0))
            out[:,d,:]=((z+self.delta)/(2*self.delta))*weights[:,None]
        return out

class AspectAutoencoder(nn.Module):
    """Paper's fully-connected 512-256-128-64 encoder and mirrored decoder."""
    def __init__(self,input_dim: int):
        super().__init__()
        self.encoder=nn.Sequential(nn.Linear(input_dim,512),nn.BatchNorm1d(512),nn.ReLU(),
            nn.Linear(512,256),nn.BatchNorm1d(256),nn.ReLU(),
            nn.Linear(256,128),nn.BatchNorm1d(128),nn.ReLU(),
            nn.Linear(128,64),nn.BatchNorm1d(64),nn.ReLU())
        self.decoder=nn.Sequential(nn.Linear(64,128),nn.BatchNorm1d(128),nn.ReLU(),
            nn.Linear(128,256),nn.BatchNorm1d(256),nn.ReLU(),
            nn.Linear(256,512),nn.BatchNorm1d(512),nn.ReLU(),
            nn.Linear(512,input_dim),nn.Sigmoid())

    def forward(self,x): return self.decoder(self.encoder(x))

    def reconstruction_error(self,x):
        was_training=self.training; self.eval()
        with torch.no_grad(): score=((self(x)-x)**2).mean(dim=1)
        self.train(was_training)
        return score

def train_aspect_autoencoder(model: AspectAutoencoder, nominal_train: np.ndarray,
                             epochs: int = 100, batch_size: int = 64, lr: float = 1.0,
                             device: str = "cpu"):
    """Train on nominal compound matrices with Adadelta and MSE, as in the paper."""
    x=torch.as_tensor(nominal_train,dtype=torch.float32,device=device)
    if x.ndim!=2 or len(x)<2: raise ValueError("need at least two nominal training matrices")
    model.to(device); opt=torch.optim.Adadelta(model.parameters(),lr=lr)
    model.train()
    for _ in range(int(epochs)):
        perm=torch.randperm(len(x),device=device)
        for ids in perm.split(batch_size):
            batch=x[ids]
            if len(batch)<2: continue
            opt.zero_grad(set_to_none=True)
            loss=F.mse_loss(model(batch),batch); loss.backward(); opt.step()
    return model

def investigation_priority(scores_by_aspect: dict[str,np.ndarray], votes: int = 2):
    """Return users ordered by the N-th highest (smallest) rank across aspects."""
    if not scores_by_aspect: raise ValueError("at least one aspect score vector is required")
    names=list(scores_by_aspect)
    arr=np.stack([np.asarray(scores_by_aspect[n],dtype=float) for n in names],axis=1)
    if not np.isfinite(arr).all(): raise ValueError("aspect scores must be finite")
    ranks=np.zeros_like(arr,dtype=np.int64)
    for j in range(arr.shape[1]):
        order=np.argsort(-arr[:,j],kind="stable")
        ranks[order,j]=np.arange(1,len(order)+1)
    n=max(1,min(int(votes),arr.shape[1]))
    priority=np.sort(ranks,axis=1)[:,n-1]
    return np.argsort(priority,kind="stable"),priority,ranks

class ACOBECore:
    def __init__(self,history_days=30,delta=3.0,votes=2):
        self.builder=CompoundDeviationBuilder(history_days,delta)
        self.votes=votes
        self.models={}
    def build_features(self,events,group_by_user=None,vocabulary_events=None):
        return self.builder.build(events,group_by_user,vocabulary_events)
    def initialize_models(self,features):
        self.models={a:AspectAutoencoder(x.shape[1]) for a,x in features["matrices"].items()}
        return self.models
    def fit(self,features,nominal_train_masks=None,epochs=100,batch_size=64,learning_rate=1.0,device="cpu"):
        self.initialize_models(features)
        nominal_train_masks=nominal_train_masks or {}
        for aspect,x in features["matrices"].items():
            mask=np.asarray(nominal_train_masks.get(aspect,np.ones(len(x),dtype=bool)),dtype=bool)
            if mask.shape!=(len(x),): raise ValueError(f"invalid nominal mask for {aspect}")
            train_aspect_autoencoder(self.models[aspect],x[mask],epochs,batch_size,learning_rate,device)
        return self
    def score(self,features,device="cpu"):
        scores={}
        for a,x in features["matrices"].items():
            if a not in self.models: raise RuntimeError(f"model for aspect {a} is not initialized")
            xt=torch.as_tensor(x,dtype=torch.float32,device=device)
            scores[a]=self.models[a].to(device).reconstruction_error(xt).cpu().numpy()
        order,priority,ranks=investigation_priority(scores,self.votes)
        return {"users":features["users"],"scores":scores,"order":order,
                "priority":priority,"ranks":ranks}
