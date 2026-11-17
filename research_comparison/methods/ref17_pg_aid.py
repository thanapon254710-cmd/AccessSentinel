"""Reference [17] PG-AID core: temporal provenance snapshots, TF-IDF meta-path sampling,
Doc2Vec path embeddings and Transformer next-snapshot prediction.

AccessSentinel adaptation: a user is treated as a process-like actor and an accessed resource
as a file/network-like object. The source event schema does not expose OS process IDs,
system-call provenance, or full causal dependencies; this is therefore a documented
interaction-provenance approximation, not a faithful reconstruction of DARPA audit graphs.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from collections import Counter, defaultdict
from typing import Sequence
import math
import numpy as np
import torch
from torch import nn
import torch.nn.functional as F
from .common import event_time, user_id, resource_id, operation

@dataclass
class ProvenanceSnapshot:
    snapshot_id: int
    nodes: set[str]
    edges: list[tuple[str, str, str, float]]
    start_time: float
    end_time: float
    paths: list[list[str]] = field(default_factory=list)
    selected_paths: list[list[str]] = field(default_factory=list)
    embedding: np.ndarray | None = None

class TemporalSnapshotBuilder:
    """Cache-graph snapshot builder with bounded node count and oldest-node forgetting."""
    def __init__(self, snapshot_nodes: int = 300, forgetting_rate: float = 0.2):
        if snapshot_nodes < 2 or not 0 < forgetting_rate < 1:
            raise ValueError("snapshot_nodes >= 2 and forgetting_rate in (0,1) required")
        self.snapshot_nodes, self.forgetting_rate = snapshot_nodes, forgetting_rate

    def build(self, events: Sequence) -> list[ProvenanceSnapshot]:
        ordered = sorted(events, key=lambda e: (event_time(e), str(getattr(e, "event_id", ""))))
        cache_nodes: dict[str, float] = {}
        cache_edges: list[tuple[str,str,str,float]] = []
        out, start, sid = [], (event_time(ordered[0]) if ordered else 0.0), 0
        def add_node(n, t): cache_nodes.setdefault(n, t)
        for e in ordered:
            t = event_time(e); u = "process:" + user_id(e); r = "resource:" + resource_id(e)
            add_node(u,t); add_node(r,t)
            op = operation(e)
            cache_edges.append((u, r, op, t))
            cache_edges.append((r, u, op + "_rev", t))
            if len(cache_nodes) >= self.snapshot_nodes:
                nodes = set(cache_nodes)
                edges = [x for x in cache_edges if x[0] in nodes and x[1] in nodes]
                out.append(ProvenanceSnapshot(sid,nodes,edges,start,t))
                sid += 1
                forget = max(1, int(round(self.snapshot_nodes*self.forgetting_rate)))
                oldest = sorted(cache_nodes, key=cache_nodes.get)[:forget]
                removed = set(oldest)
                for n in oldest: cache_nodes.pop(n,None)
                cache_edges = [x for x in cache_edges if x[0] not in removed and x[1] not in removed]
                start = t
        if cache_nodes and cache_edges:
            out.append(ProvenanceSnapshot(sid,set(cache_nodes),list(cache_edges),start,
                                          event_time(ordered[-1])))
        return out

def extract_process_resource_paths(snapshot: ProvenanceSnapshot, max_paths: int = 50000) -> list[list[str]]:
    """Extract process→resource→process meta-paths; actions retain direction and semantics."""
    outgoing = defaultdict(list)
    for s,d,rel,t in snapshot.edges:
        outgoing[s].append((d,rel,t))
    paths = []
    for process in sorted(n for n in snapshot.nodes if n.startswith("process:")):
        for resource, rel1, t1 in outgoing.get(process, []):
            if not resource.startswith("resource:"): continue
            for target, rel2, t2 in outgoing.get(resource, []):
                if target.startswith("process:") and target != process:
                    paths.append([process, "action:"+rel1, resource, "action:"+rel2, target])
                    if len(paths) >= max_paths: return paths
    return paths

def fit_tfidf_path_selector(snapshots: Sequence[ProvenanceSnapshot]):
    """Fit path-node document frequencies on training snapshots only."""
    df = Counter()
    for snap in snapshots:
        df.update(set(node for path in snap.paths for node in path))
    return {"df": dict(df), "n_docs": max(1, len(snapshots))}


def select_tfidf_paths(snapshots: Sequence[ProvenanceSnapshot], rng_seed: int = 0,
                       idf_stats: dict | None = None):
    """Select each process' highest-TF-IDF path and one random path.

    Pass ``idf_stats`` fitted with :func:`fit_tfidf_path_selector` to ensure
    validation/test snapshots do not influence inverse-document frequencies.
    When omitted, statistics are fitted on the supplied snapshots for backwards
    compatibility with standalone use.
    """
    rng = np.random.default_rng(rng_seed)
    stats = idf_stats if idf_stats is not None else fit_tfidf_path_selector(snapshots)
    df = Counter(stats["df"])
    n_docs = max(1, int(stats["n_docs"]))
    for snap in snapshots:
        counts = Counter(node for path in snap.paths for node in path)
        denom = max(1, sum(counts.values()))
        def score(path):
            vals = [(counts[node] / denom) * math.log((n_docs + 1) / (df[node] + 1))
                    for node in path]
            return float(np.mean(vals)) if vals else 0.0
        by_process = defaultdict(list)
        for path in snap.paths:
            by_process[path[0]].append(path)
        selected = []
        for process in sorted(by_process):
            paths = by_process[process]
            selected.append(max(paths, key=score))
            if len(paths) > 1:
                selected.append(paths[int(rng.integers(0, len(paths)))])
        snap.selected_paths = selected
    return stats

class PathDoc2Vec:
    """Doc2Vec path encoder (Gensim); each path is treated as a tokenized document."""
    def __init__(self, vector_size: int = 128, window: int = 5, min_count: int = 1,
                 epochs: int = 30, seed: int = 7):
        self.vector_size, self.window, self.min_count = vector_size, window, min_count
        self.epochs, self.seed, self.model = epochs, seed, None

    def fit_transform(self, snapshots: Sequence[ProvenanceSnapshot]):
        from gensim.models.doc2vec import Doc2Vec, TaggedDocument
        docs = [TaggedDocument(words=path, tags=[f"s{si}p{pi}"])
                for si,s in enumerate(snapshots) for pi,path in enumerate(s.selected_paths) if path]
        if not docs: raise ValueError("PG-AID needs at least one sampled meta-path")
        self.model = Doc2Vec(documents=docs, vector_size=self.vector_size, window=self.window,
                             min_count=self.min_count, workers=1, epochs=self.epochs, seed=self.seed)
        per = [[] for _ in snapshots]
        for si,s in enumerate(snapshots):
            for pi,path in enumerate(s.selected_paths):
                if path: per[si].append(self.model.dv[f"s{si}p{pi}"])
            if per[si]:
                s.embedding = np.mean(np.asarray(per[si]),axis=0).astype(np.float32)
            else:
                s.embedding = np.zeros(self.vector_size,np.float32)
        return np.stack([s.embedding for s in snapshots])

class SnapshotTransformer(nn.Module):
    """Autoregressive Transformer encoder predicting the next snapshot embedding."""
    def __init__(self, embedding_dim: int = 128, model_dim: int = 128,
                 heads: int = 4, layers: int = 2, dropout: float = 0.1):
        super().__init__()
        if model_dim % heads: raise ValueError("model_dim must be divisible by heads")
        self.input = nn.Linear(embedding_dim, model_dim)
        self.position = nn.Parameter(torch.zeros(1, 512, model_dim))
        nn.init.normal_(self.position, mean=0.0, std=0.02)
        enc = nn.TransformerEncoderLayer(model_dim, heads, 4*model_dim, dropout,
                                         batch_first=True, norm_first=False)
        self.encoder = nn.TransformerEncoder(enc, num_layers=layers)
        self.output = nn.Linear(model_dim, embedding_dim)

    def forward(self, sequence: torch.Tensor):
        if sequence.ndim != 3: raise ValueError("sequence must be [batch,time,embedding_dim]")
        t = sequence.shape[1]
        if t > self.position.shape[1]:
            raise ValueError("sequence exceeds configured positional-embedding capacity (512)")
        mask = torch.triu(torch.ones(t,t,device=sequence.device,dtype=torch.bool),diagonal=1)
        h = self.encoder(self.input(sequence) + self.position[:, :t], mask=mask)
        return self.output(h[:,-1])

def cosine_deviation(predicted: torch.Tensor, actual: torch.Tensor) -> torch.Tensor:
    """1 - cosine similarity; higher values indicate greater snapshot novelty."""
    return 1.0 - F.cosine_similarity(predicted, actual, dim=-1, eps=1e-8)

class PGAIDCore:
    def __init__(self, snapshot_nodes: int = 300, forgetting_rate: float = 0.2,
                 embedding_dim: int = 128, seed: int = 7):
        self.builder = TemporalSnapshotBuilder(snapshot_nodes,forgetting_rate)
        self.encoder = PathDoc2Vec(vector_size=embedding_dim,seed=seed)
        self.predictor = SnapshotTransformer(embedding_dim)
        self.embedding_dim = embedding_dim

    def fit_predictor(self, embeddings, sequence_length=4, epochs=100, learning_rate=1e-3, device="cpu"):
        return train_snapshot_transformer(self.predictor, embeddings, sequence_length, epochs, learning_rate, device)

    def score(self, embeddings, sequence_length=4, device="cpu"):
        return score_snapshot_sequence(self.predictor, embeddings, sequence_length, device)

    def encode_events(self, events: Sequence):
        snaps = self.builder.build(events)
        for s in snaps: s.paths = extract_process_resource_paths(s)
        select_tfidf_paths(snaps)
        embeddings = self.encoder.fit_transform(snaps)
        return snaps, embeddings


def train_snapshot_transformer(model: SnapshotTransformer, embeddings: np.ndarray,
                               sequence_length: int = 4, epochs: int = 100,
                               learning_rate: float = 1e-3, device: str = "cpu"):
    """Fit next-snapshot prediction using cosine-distance reconstruction objective."""
    x=torch.as_tensor(embeddings,dtype=torch.float32,device=device)
    if x.ndim!=2 or len(x)<=sequence_length:
        raise ValueError("need more snapshot embeddings than sequence_length")
    sequences=torch.stack([x[i:i+sequence_length] for i in range(len(x)-sequence_length)])
    targets=torch.stack([x[i+sequence_length] for i in range(len(x)-sequence_length)])
    model.to(device); opt=torch.optim.Adam(model.parameters(),lr=learning_rate); history=[]
    for _ in range(int(epochs)):
        model.train(); opt.zero_grad(set_to_none=True)
        pred=model(sequences)
        loss=cosine_deviation(pred,targets).mean()
        loss.backward(); torch.nn.utils.clip_grad_norm_(model.parameters(),1.0)
        opt.step(); history.append(float(loss.detach().cpu()))
    return history

def score_snapshot_sequence(model: SnapshotTransformer, embeddings: np.ndarray,
                            sequence_length: int = 4, device: str = "cpu"):
    """Return one-step-ahead cosine deviations; first sequence_length snapshots are unscored."""
    x=torch.as_tensor(embeddings,dtype=torch.float32,device=device)
    if x.ndim!=2: raise ValueError("embeddings must be [snapshots,dimension]")
    scores=np.full(len(x),np.nan,dtype=np.float64)
    model.to(device); model.eval()
    with torch.no_grad():
        for i in range(sequence_length,len(x)):
            pred=model(x[i-sequence_length:i].unsqueeze(0))
            scores[i]=float(cosine_deviation(pred,x[i].unsqueeze(0)).item())
    return scores
