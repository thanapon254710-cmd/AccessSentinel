"""Reference [22] FGDT core: time-window coreview graph, HIN-RNN, temporal RNN,
two-layer GCN refinement, k-means outlier filtering and group classification.

AccessSentinel adaptation: resources replace reviewed items, time-window co-access replaces
coreviewing, and event-derived behavior vectors replace review-text SoWE plus rating ratio.
The paper's Yelp/Amazon review task is not directly reproducible from AccessSentinel logs.
"""
from __future__ import annotations
from dataclasses import dataclass
from collections import defaultdict
from typing import Sequence
import numpy as np
import torch
from torch import nn
import torch.nn.functional as F
from sklearn.cluster import KMeans
from .common import event_time,user_id,resource_id,event_token,stable_hash

@dataclass
class CandidateGroup:
    members: list[str]
    windows: list[np.ndarray]  # one symmetric collaboration matrix per time window
    features: np.ndarray       # [members, feature_dim]
    label: int | None = None

def build_coreview_groups(events: Sequence, window_seconds: float = 28*86400.0,
                          min_shared_resources: int = 1, feature_dim: int = 100):
    """Candidate groups from users sharing resources in non-overlapping time windows."""
    if not events: return []
    ordered=sorted(events,key=lambda e:(event_time(e),str(getattr(e,"event_id",""))))
    start=event_time(ordered[0]); bins=defaultdict(list)
    for e in ordered: bins[int((event_time(e)-start)//window_seconds)].append(e)
    users=sorted({user_id(e) for e in ordered})
    # Fixed feature vectors aggregate behavior; no attack labels are included.
    f={u:np.zeros(feature_dim,np.float32) for u in users}
    for e in ordered:
        for tok in event_token(e): f[user_id(e)][stable_hash(tok,feature_dim)]+=1
    windows=[]
    for _,rows in sorted(bins.items()):
        by_resource=defaultdict(set)
        for e in rows: by_resource[resource_id(e)].add(user_id(e))
        a=np.zeros((len(users),len(users)),np.float32); ui={u:i for i,u in enumerate(users)}
        shared=defaultdict(int)
        for us in by_resource.values():
            us=sorted(us)
            for i,u in enumerate(us):
                for v in us[i+1:]: shared[(u,v)]+=1
        for (u,v),n in shared.items():
            if n>=min_shared_resources: a[ui[u],ui[v]]=a[ui[v],ui[u]]=1
        windows.append(a)
    # Candidate connected components over the union of temporal coreview links.
    union=np.maximum.reduce(windows) if windows else np.zeros((len(users),len(users)))
    seen=set(); groups=[]
    for i,u in enumerate(users):
        if i in seen: continue
        stack=[i]; comp=[]; seen.add(i)
        while stack:
            j=stack.pop(); comp.append(j)
            for k in np.flatnonzero(union[j]):
                k=int(k)
                if k not in seen: seen.add(k); stack.append(k)
        if len(comp)<2: continue
        member_ids=[users[j] for j in comp]
        groups.append(CandidateGroup(member_ids,[w[np.ix_(comp,comp)] for w in windows],
                                     np.stack([f[x] for x in member_ids])))
    return groups

class HINRNNSpatial(nn.Module):
    """Autoregressive HIN-RNN-style collaboration-matrix refinement.

    The group RNN updates from the current member representation and prior group state.
    A second recurrent cell emits each member-to-previous-member link sequentially, so the
    current collaboration vector is conditioned on its already generated prefix.
    """
    def __init__(self,feature_dim:int,hidden_dim:int=128):
        super().__init__()
        self.group_cell=nn.GRUCell(feature_dim+hidden_dim,hidden_dim)
        self.link_cell=nn.GRUCell(hidden_dim+1,hidden_dim)
        self.link_score=nn.Linear(hidden_dim,1)
        self.node_proj=nn.Linear(feature_dim,hidden_dim)
        self.pair_score=nn.Bilinear(hidden_dim,hidden_dim,1)
    def forward(self,features):
        n=features.shape[0]
        h=features.new_zeros((1,self.group_cell.hidden_size))
        states=[]; rows=[]
        for i in range(n):
            if states:
                prior_context=torch.stack(states).mean(0,keepdim=True)
            else:
                prior_context=features.new_zeros((1,self.group_cell.hidden_size))
            h=self.group_cell(torch.cat([features[i:i+1],prior_context],dim=-1),h)
            current=self.node_proj(features[i:i+1])
            link_state=h
            previous_probability=features.new_zeros((1,1))
            row=[]
            for j in range(i):
                link_state=self.link_cell(torch.cat([current,previous_probability],dim=-1),link_state)
                pair=self.pair_score(current,states[j].unsqueeze(0))
                logit=self.link_score(link_state)+pair
                probability=torch.sigmoid(logit).reshape(())
                row.append(probability)
                # Autoregressive prefix: the next link conditions on the previous prediction.
                previous_probability=probability.reshape(1,1)
            states.append(current.squeeze(0))
            rows.append(row)
        adjacency=features.new_zeros((n,n))
        for i,row in enumerate(rows):
            for j,p in enumerate(row):
                adjacency[i,j]=p; adjacency[j,i]=p
        return adjacency

class TemporalCollaborationRNN(nn.Module):
    """One-to-many temporal RNN over fixed-size collaboration matrices."""
    def __init__(self,max_group_size:int,hidden_dim:int=128):
        super().__init__()
        self.n=max_group_size
        self.cell=nn.GRUCell(max_group_size*max_group_size,hidden_dim)
        self.out=nn.Linear(hidden_dim,max_group_size*max_group_size)
    def forward(self,matrices):
        if matrices.ndim!=3: raise ValueError("matrices must be [time,n,n]")
        h=matrices.new_zeros((1,self.cell.hidden_size))
        for m in matrices:
            padded=F.pad(m,(0,self.n-m.shape[1],0,self.n-m.shape[0]))
            h=self.cell(padded.reshape(1,-1),h)
        pred=torch.sigmoid(self.out(h)).view(self.n,self.n)
        pred=(pred+pred.T)/2
        return pred

class FGDTGCN(nn.Module):
    def __init__(self,input_dim:int,hidden_dim:int=128,output_dim:int=2):
        super().__init__()
        self.w0=nn.Linear(input_dim,hidden_dim,bias=False)
        self.w1=nn.Linear(hidden_dim,output_dim,bias=False)
    def forward(self,x,adj):
        """Return unnormalized class logits; apply softmax only for inference."""
        n=adj.shape[0]; a=adj+torch.eye(n,device=adj.device,dtype=adj.dtype)
        d=a.sum(1).clamp_min(1).rsqrt(); a=d[:,None]*a*d[None,:]
        h=F.relu(a@self.w0(x))
        return a@self.w1(h)

def remove_outlier_reviewers(embeddings: np.ndarray, threshold: float=0.5, random_state:int=0):
    """Paper's two-cluster TSS/WSS/BSS test; retain the dominant cluster when outliers exist."""
    z=np.asarray(embeddings,dtype=np.float64)
    if z.ndim!=2 or len(z)<3: return np.arange(len(z)),{"outliers_removed":0,"bss_ratio":0.0}
    km=KMeans(n_clusters=2,n_init=10,random_state=random_state).fit(z)
    labels=km.labels_; counts=np.bincount(labels,minlength=2)
    center=z.mean(0)
    tss=float(((z-center)**2).sum())
    wss=sum(float(((z[labels==k]-km.cluster_centers_[k])**2).sum()) for k in range(2))
    ratio=(tss-wss)/max(tss,1e-12)
    if ratio < threshold: keep=np.arange(len(z))
    else: keep=np.where(labels==int(np.argmax(counts)))[0]
    return keep,{"outliers_removed":int(len(z)-len(keep)),"bss_ratio":ratio}

class FGDTCore(nn.Module):
    def __init__(self,feature_dim:int,max_group_size:int=32,hidden_dim:int=128,
                 temporal_hidden:int=128):
        super().__init__()
        self.max_group_size=max_group_size
        self.spatial=HINRNNSpatial(feature_dim,hidden_dim)
        self.temporal=TemporalCollaborationRNN(max_group_size,temporal_hidden)
        self.gcn=FGDTGCN(feature_dim,hidden_dim,2)
        self.group_head=nn.Sequential(nn.Linear(feature_dim,hidden_dim),nn.ReLU(),
                                      nn.Linear(hidden_dim,1))
    def forward_group(self,features,collaboration_windows):
        if features.shape[0]>self.max_group_size:
            raise ValueError("group exceeds configured max_group_size")
        spatial=self.spatial(features)
        temporal=self.temporal(torch.stack(collaboration_windows))
        refined_adj=torch.maximum(spatial,temporal[:features.shape[0],:features.shape[0]])
        member_logits=self.gcn(features,refined_adj)
        member_probs=torch.softmax(member_logits,dim=-1)
        # Group representation is computed after the paper's outlier-removal step.
        keep,cluster_info=remove_outlier_reviewers(member_probs.detach().cpu().numpy())
        keep_t=torch.as_tensor(keep,device=features.device,dtype=torch.long)
        group_repr=features[keep_t].mean(0) if len(keep) else features.mean(0)
        group_logit=self.group_head(group_repr).squeeze(-1)
        return {"member_logits":member_logits,"member_probabilities":member_probs,
                "refined_adjacency":refined_adj,"kept_member_indices":keep,
                "cluster_info":cluster_info,"group_logit":group_logit,
                "group_score":torch.sigmoid(group_logit)}


def fgdt_training_loss(outputs, member_labels=None, group_label=None,
                       member_weight: float = 1.0, group_weight: float = 1.0,
                       member_class_weights=None, group_pos_weight=None,
                       return_components: bool = False):
    """Stable weighted member CE plus group BCE-with-logits objective."""
    losses = {}
    if member_labels is not None:
        logits = outputs.get("member_logits")
        if logits is None:
            logits = torch.log(outputs["member_probabilities"].clamp_min(1e-8))
        y = torch.as_tensor(member_labels, device=logits.device, dtype=torch.long)
        weights = None if member_class_weights is None else torch.as_tensor(
            member_class_weights, device=logits.device, dtype=logits.dtype)
        losses["member"] = F.cross_entropy(logits, y, weight=weights)
    if group_label is not None:
        target = torch.as_tensor(float(group_label), device=outputs["group_logit"].device)
        pos_weight = None if group_pos_weight is None else torch.as_tensor(
            float(group_pos_weight), device=target.device, dtype=outputs["group_logit"].dtype)
        losses["group"] = F.binary_cross_entropy_with_logits(
            outputs["group_logit"], target, pos_weight=pos_weight)
    if not losses:
        raise ValueError("provide member_labels and/or group_label")
    total = sum((member_weight * losses["member"] if name == "member" else
                 group_weight * losses["group"]) for name in losses)
    if return_components:
        return total, {name: float(value.detach().cpu()) for name, value in losses.items()}
    return total
