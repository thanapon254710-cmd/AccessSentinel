"""Reference [14] DynAD core: evolving GCN, GRU-adapted weights, local attention and edge scoring.

Paper source: supplied PDF, Sections III-A--D, Eqs. (1)--(15).
AccessSentinel mapping: chronological snapshots of the user-resource interaction graph;
event edges are labelled with operation metadata. This is a graph-task adaptation, not the
paper's original UCI/arXiv/Digg protocol.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Sequence
import torch
from torch import nn
import torch.nn.functional as F

@dataclass
class GraphSnapshot:
    x: torch.Tensor                 # [N, input_dim]
    edge_index: torch.Tensor        # [2, E], directed event edges
    edge_y: torch.Tensor | None = None
    timestamp: float = 0.0

    def validate(self):
        if self.x.ndim != 2 or self.edge_index.ndim != 2 or self.edge_index.shape[0] != 2:
            raise ValueError("snapshot requires x[N,F] and edge_index[2,E]")
        if self.edge_index.numel() and (self.edge_index.min() < 0 or self.edge_index.max() >= self.x.shape[0]):
            raise ValueError("edge_index contains an invalid node index")

def normalized_adjacency(n: int, edge_index: torch.Tensor, device=None) -> torch.Tensor:
    device = device or edge_index.device
    a = torch.eye(n, device=device)
    if edge_index.numel():
        a[edge_index[0], edge_index[1]] = 1.0
        a[edge_index[1], edge_index[0]] = 1.0
    deg = a.sum(dim=1).clamp_min(1.0)
    inv = deg.rsqrt()
    return inv[:, None] * a * inv[None, :]

class AdaptiveGCNLayer(nn.Module):
    """A GCN whose layer weight is recurrently adapted from the current snapshot."""
    def __init__(self, in_dim: int, out_dim: int, pool_k: int):
        super().__init__()
        self.in_dim, self.out_dim, self.pool_k = in_dim, out_dim, pool_k
        state_dim = in_dim * out_dim
        self.pool_score = nn.Linear(in_dim, 1)
        self.query = nn.Linear(pool_k * in_dim, state_dim)
        self.attn_w = nn.Parameter(torch.eye(state_dim) / max(1, state_dim) ** 0.5)
        self.weight_gru = nn.GRUCell(pool_k * in_dim, state_dim)
        self.register_buffer("initial_state", torch.zeros(state_dim))
        self.bias = nn.Parameter(torch.zeros(out_dim))

    def _pool(self, h: torch.Tensor) -> torch.Tensor:
        if h.shape[0] == 0:
            return h.new_zeros(self.pool_k * self.in_dim)
        k = min(self.pool_k, h.shape[0])
        idx = torch.topk(self.pool_score(h).squeeze(-1), k=k).indices
        selected = h[idx]
        if k < self.pool_k:
            selected = F.pad(selected, (0, 0, 0, self.pool_k-k))
        return selected.reshape(-1)

    def forward(self, x, adj, history: list[torch.Tensor]):
        pooled = self._pool(x)
        query = self.query(pooled)
        if history:
            states = torch.stack(history[-10:], dim=0)
            q = self.attn_w @ query
            weights = torch.softmax(states @ q / (q.norm() + 1e-8), dim=0)
            context = (weights[:, None] * states).sum(dim=0)
        else:
            context = self.initial_state
        prior = history[-1] if history else self.initial_state
        state = self.weight_gru(pooled.unsqueeze(0), prior.unsqueeze(0)).squeeze(0)
        # Context attention refines the recurrently adapted matrix.
        state = 0.5 * state + 0.5 * context if history else state
        weight = state.view(self.in_dim, self.out_dim)
        out = adj @ (x @ weight) + self.bias
        return F.relu(out), state

class DynADCore(nn.Module):
    """Reference [14] temporal graph detector.

    Call forward_snapshot sequentially in timestamp order. The model retains at most
    ``attention_window`` layer states, while the caller controls chronological snapshots.
    """
    def __init__(self, input_dim: int, hidden_dim: int = 50, layers: int = 2,
                 attention_window: int = 10, pool_k: int = 32):
        super().__init__()
        if layers < 1 or hidden_dim < 1:
            raise ValueError("layers and hidden_dim must be positive")
        dims = [input_dim] + [hidden_dim] * layers
        self.layers = nn.ModuleList([
            AdaptiveGCNLayer(dims[i], dims[i+1], pool_k) for i in range(layers)
        ])
        self.edge_head = nn.Sequential(nn.Linear(2 * hidden_dim, hidden_dim),
                                       nn.ReLU(), nn.Linear(hidden_dim, 1))
        self.attention_window = int(attention_window)
        self.state_history: list[list[torch.Tensor]] = [[] for _ in range(layers)]

    def reset_state(self):
        self.state_history = [[] for _ in self.layers]

    def forward_snapshot(self, snapshot: GraphSnapshot):
        snapshot.validate()
        x = snapshot.x
        adj = normalized_adjacency(x.shape[0], snapshot.edge_index, x.device)
        for i, layer in enumerate(self.layers):
            x, state = layer(x, adj, self.state_history[i])
            hist = self.state_history[i]
            hist.append(state)
            if len(hist) > self.attention_window:
                del hist[:-self.attention_window]
        if snapshot.edge_index.numel() == 0:
            logits = x.new_zeros((0,))
        else:
            src, dst = snapshot.edge_index
            logits = self.edge_head(torch.cat([x[src], x[dst]], dim=-1)).squeeze(-1)
        return {"node_embeddings": x, "edge_logits": logits,
                "edge_scores": torch.sigmoid(logits)}

def snapshots_from_events(events: Sequence, window_seconds: float = 86400.0,
                          feature_dim: int = 128, max_nodes: int = 20000,
                          labels: dict | None = None) -> list[GraphSnapshot]:
    """Create disjoint event-time snapshots with per-snapshot local node indexing."""
    from .common import event_time, user_id, resource_id, event_token, stable_hash
    ordered = sorted(events, key=lambda e: (event_time(e), str(getattr(e, "event_id", ""))))
    if not ordered:
        return []
    start = event_time(ordered[0]); groups = []
    for e in ordered:
        bucket = int((event_time(e) - start) // window_seconds)
        while len(groups) <= bucket: groups.append([])
        groups[bucket].append(e)
    result = []
    for bucket, group in enumerate(groups):
        node_index, feats, edges, edge_labels = {}, [], [], []
        def node(key):
            if key not in node_index:
                if len(node_index) >= max_nodes: return None
                node_index[key] = len(node_index); feats.append([0.0] * feature_dim)
            return node_index[key]
        for e in group:
            u, r = node("u:"+user_id(e)), node("r:"+resource_id(e))
            if u is None or r is None: continue
            for tok in event_token(e):
                j = stable_hash(tok, feature_dim)
                feats[u][j] += 1.0; feats[r][j] += 1.0
            edges.extend([(u, r), (r, u)])
            lab = labels.get(str(getattr(e,"event_id",""))) if labels is not None else None
            if hasattr(lab,"individual"): lab = lab.individual
            if lab is not None: edge_labels.extend([int(lab),int(lab)])
        x = torch.tensor(feats, dtype=torch.float32)
        ei = torch.tensor(edges, dtype=torch.long).T.contiguous() if edges else torch.zeros((2,0),dtype=torch.long)
        ey = torch.tensor(edge_labels,dtype=torch.float32) if len(edge_labels)==len(edges) else None
        result.append(GraphSnapshot(x, ei, ey, start + bucket * window_seconds))
    return result


def train_dynad(model: DynADCore, snapshots: Sequence[GraphSnapshot], epochs: int = 300,
                learning_rate: float = 1e-3, negative_weight: float = 1.0,
                device: str = "cpu"):
    """Train sequentially with the paper's weighted edge cross-entropy and negative sampling."""
    if not snapshots or not any(s.edge_y is not None for s in snapshots):
        raise ValueError("DynAD training requires labelled edge snapshots")
    model.to(device); opt=torch.optim.Adam(model.parameters(),lr=learning_rate)
    history=[]
    for _ in range(int(epochs)):
        model.reset_state(); opt.zero_grad(set_to_none=True); losses=[]
        for snap in snapshots:
            x=snap.x.to(device); ei=snap.edge_index.to(device)
            y=snap.edge_y
            if y is None: continue
            y=y.to(device).float().reshape(-1)
            out=model.forward_snapshot(GraphSnapshot(x,ei,y,snap.timestamp))
            logits=out["edge_logits"]
            if len(y)!=len(logits): raise ValueError("edge_y length must equal edge count")
            pos=torch.where(y>0.5)[0]; neg=torch.where(y<=0.5)[0]
            if len(pos) and len(neg):
                # Negative sampling keeps the majority class from dominating the update.
                nneg=min(len(neg),max(len(pos),1)*3)
                neg=neg[torch.randperm(len(neg),device=device)[:nneg]]
                chosen=torch.cat([pos,neg])
                logits=logits[chosen]; y=y[chosen]
            loss=(y*F.softplus(-logits)+negative_weight*(1-y)*F.softplus(logits)).mean()
            losses.append(loss)
        if not losses: raise ValueError("no labelled edges were available")
        total=torch.stack(losses).mean(); total.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(),5.0)
        opt.step(); history.append(float(total.detach().cpu()))
    return history
