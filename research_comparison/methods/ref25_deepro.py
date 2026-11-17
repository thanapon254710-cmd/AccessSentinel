"""Reference [25] DEEPRO core: heterogeneous provenance meta-path aggregation (MAGNN),
attack-relevant process classification and causality-based campaign recovery.

AccessSentinel adaptation: users are process-like actors and application resources are
file/network-like entities. The original system audit process/file/socket causal graph and
APT campaign labels are absent, so this module builds an interaction-provenance proxy only.
"""
from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass
from typing import Sequence
import numpy as np
import torch
from torch import nn
import torch.nn.functional as F
from .common import event_time,user_id,resource_id,event_token,stable_hash

@dataclass
class HeterogeneousProvenanceGraph:
    node_ids: list[str]
    node_types: list[str]
    features: np.ndarray
    edges: list[tuple[int,int,str,float]]
    meta_paths: dict[str,dict[int,list[tuple[int,...]]]]
    process_indices: list[int]

def build_access_provenance_graph(events: Sequence, feature_dim:int=256, max_instances_per_node:int=32) -> HeterogeneousProvenanceGraph:
    """Map access events to process/resource entities and typed temporal causal edges."""
    ordered=sorted(events,key=lambda e:(event_time(e),str(getattr(e,"event_id",""))))
    node_ids=[]; node_types=[]; feats=[]; idx={}
    def add(key,typ):
        if key not in idx:
            idx[key]=len(node_ids); node_ids.append(key); node_types.append(typ)
            feats.append(np.zeros(feature_dim,np.float32))
        return idx[key]
    edges=[]
    for e in ordered:
        u=add("process:"+user_id(e),"process")
        rk=resource_id(e).lower()
        if any(x in rk for x in ("http","network","socket","api","url")): typ="network"
        else: typ="file"
        r=add(typ+":"+resource_id(e),typ)
        for token in event_token(e):
            j=stable_hash(token,feature_dim); feats[u][j]+=1; feats[r][j]+=1
        op=str(getattr(e,"o","access"))
        edges.append((u,r,op,event_time(e)))
    # Build process→entity→process meta-path instances over shared entities.
    entity_users=defaultdict(list)
    for u,r,op,t in edges: entity_users[r].append(u)
    paths={"process_process":defaultdict(list),"process_file_process":defaultdict(list),
           "process_network_process":defaultdict(list)}
    for entity,us in entity_users.items():
        us=sorted(set(us)); typ=node_types[entity]
        key="process_file_process" if typ=="file" else "process_network_process"
        for i,u in enumerate(us):
            for v in us:
                if u==v: continue
                # Bound per-node meta-path fan-out to keep the adapted graph tractable.
                # Sampling is deterministic and preserves stable node ordering.
                if len(paths[key][u]) < max_instances_per_node:
                    paths[key][u].append((u,entity,v))
                if len(paths["process_process"][u]) < max_instances_per_node:
                    paths["process_process"][u].append((u,v))
    return HeterogeneousProvenanceGraph(node_ids,node_types,np.asarray(feats,np.float32),
        edges,{k:dict(v) for k,v in paths.items()},
        [i for i,t in enumerate(node_types) if t=="process"])

class MetaPathAttention(nn.Module):
    """Multi-head attention over metapath instances for each target process."""
    def __init__(self,input_dim:int,hidden_dim:int,heads:int=4):
        super().__init__()
        if hidden_dim % heads: raise ValueError("hidden_dim must be divisible by heads")
        self.heads=heads; self.head_dim=hidden_dim//heads
        self.proj=nn.Linear(input_dim,hidden_dim)
        self.attn=nn.Parameter(torch.empty(heads,2*self.head_dim))
        nn.init.xavier_uniform_(self.attn)
        self.fallback=nn.Linear(input_dim,hidden_dim)
    def forward(self,x,instances:dict[int,list[tuple[int,...]]]):
        h=self.proj(x); n=h.shape[0]
        out=h.new_zeros((n,self.heads,self.head_dim))
        for target in range(n):
            paths=instances.get(target,[])
            if not paths:
                out[target]=h[target].view(self.heads,self.head_dim)
                continue
            reps=[]
            for path in paths:
                ids=torch.as_tensor(path,device=x.device,dtype=torch.long)
                reps.append(h[ids].mean(0).view(self.heads,self.head_dim))
            p=torch.stack(reps,dim=0)  # [instances,heads,head_dim]
            q=h[target].view(self.heads,self.head_dim).unsqueeze(0).expand(p.shape[0],-1,-1)
            pair=torch.cat([q,p],dim=-1)
            logits=F.leaky_relu((pair*self.attn.unsqueeze(0)).sum(-1),0.2)
            alpha=torch.softmax(logits,dim=0)
            out[target]=(alpha.unsqueeze(-1)*p).sum(0)
        return out.reshape(n,-1)

class MAGNNProcessDetector(nn.Module):
    def __init__(self,input_dim:int,hidden_dim:int=64,heads:int=4,metapaths=None):
        super().__init__()
        self.metapath_names=tuple(metapaths or ("process_process","process_file_process","process_network_process"))
        self.aggregators=nn.ModuleDict({name:MetaPathAttention(input_dim,hidden_dim,heads)
                                        for name in self.metapath_names})
        self.semantic=nn.Sequential(nn.Linear(hidden_dim*len(self.metapath_names),hidden_dim),
                                    nn.ELU(),nn.Dropout(0.2))
        self.classifier=nn.Linear(hidden_dim,2)
    def forward(self,x,meta_paths,process_indices):
        reps=[]
        for name in self.metapath_names:
            reps.append(self.aggregators[name](x,meta_paths.get(name,{})))
        h=self.semantic(torch.cat(reps,dim=-1))
        ids=torch.as_tensor(process_indices,device=x.device,dtype=torch.long)
        return {"node_embeddings":h,"process_logits":self.classifier(h[ids]),
                "process_probabilities":torch.softmax(self.classifier(h[ids]),dim=-1)}

class DEEPROCore(nn.Module):
    def __init__(self,input_dim:int=256,hidden_dim:int=64,heads:int=4):
        super().__init__()
        self.detector=MAGNNProcessDetector(input_dim,hidden_dim,heads)
    def forward_graph(self,graph:HeterogeneousProvenanceGraph,device="cpu"):
        x=torch.as_tensor(graph.features,dtype=torch.float32,device=device)
        return self.detector(x,graph.meta_paths,graph.process_indices)
    @staticmethod
    def recover_campaign(graph:HeterogeneousProvenanceGraph,process_scores:dict[str,float],
                         threshold:float=0.5):
        """Recover the connected interaction-provenance subgraph around flagged processes."""
        seeds={f"process:{u}" for u,s in process_scores.items() if float(s)>=threshold}
        if not seeds: return {"nodes":[],"edges":[],"seed_processes":[]}
        id_to_idx={n:i for i,n in enumerate(graph.node_ids)}
        selected=set(seeds); selected_edges=[]
        # Include direct resources of flagged processes and co-accessing process nodes.
        for src,dst,rel,t in graph.edges:
            sn,dn=graph.node_ids[src],graph.node_ids[dst]
            if sn in seeds:
                selected.add(dn); selected_edges.append((sn,dn,rel,t))
            elif dn in seeds:
                selected.add(sn); selected_edges.append((sn,dn,rel,t))
        # Retain only edges connecting selected nodes; no unsupported attack narrative is inferred.
        return {"nodes":sorted(selected),"edges":selected_edges,
                "seed_processes":sorted(seeds)}


def train_deepro(model: DEEPROCore, graph: HeterogeneousProvenanceGraph,
                 process_labels: dict[str,int], epochs: int = 100,
                 learning_rate: float = 0.005, weight_decay: float = 0.001,
                 device: str = "cpu"):
    """Train MAGNN process detector with supervised cross entropy on labelled process nodes."""
    process_ids=[graph.node_ids[i].split(":",1)[1] for i in graph.process_indices]
    if any(u not in process_labels for u in process_ids):
        raise ValueError("process_labels must cover every process node used for training")
    y=torch.as_tensor([int(process_labels[u]) for u in process_ids],dtype=torch.long,device=device)
    x=torch.as_tensor(graph.features,dtype=torch.float32,device=device)
    model.to(device); opt=torch.optim.Adam(model.parameters(),lr=learning_rate,weight_decay=weight_decay)
    history=[]
    for _ in range(int(epochs)):
        model.train(); opt.zero_grad(set_to_none=True)
        out=model.detector(x,graph.meta_paths,graph.process_indices)
        loss=F.cross_entropy(out["process_logits"],y)
        loss.backward(); torch.nn.utils.clip_grad_norm_(model.parameters(),5.0)
        opt.step(); history.append(float(loss.detach().cpu()))
    return history
