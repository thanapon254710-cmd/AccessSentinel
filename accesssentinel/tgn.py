"""Step 7: Temporal Graph Network per D3 (following [14]): two GCN layers, 50-d embeddings, GRU temporal
encoder, attention over T=10 time steps, edge anomaly score sigma(W_s [h_i; h_j] + b_s) (Eq. 6 pipeline).
Dense implementation (no torch_geometric): neighbourhoods are bounded so N is small."""
import copy
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from .config import TGNConfig, GraphConfig
from .graph import GraphSample, groups_from_graph
from .metrics import roc_auc


def to_tensors(s: GraphSample, cfg: TGNConfig, app_w: float, device="cpu"):
    T, N, Fd = cfg.window_T, s.n_nodes, cfg.node_feat_dim
    X = np.zeros((T, N, Fd), dtype=np.float32); A = np.zeros((T, N, N), dtype=np.float32)
    U, R = s.n_users, len(s.resources)
    node_type = np.zeros(N, dtype=int); node_type[U:U + R] = 1; node_type[U + R:] = 2
    for n in range(N): X[:, n, node_type[n]] = 1.0
    span = max(s.t1 - s.t0, 1e-6)
    slc = np.clip(((s.e_t - s.t0) / span * T).astype(int), 0, T - 1) if s.n_edges else np.zeros(0, int)
    eu, er = s.e_user, s.n_users + s.e_res
    ea = U + R + s.e_app
    # static edges (membership, co-access)
    static = np.zeros((N, N), dtype=np.float32)
    for r, a in zip(s.mem_res, s.mem_app):
        static[U + r, U + R + a] = static[U + R + a, U + r] = 1.0
    for (i, j), c in s.co_res.items():
        static[i, j] = static[j, i] = max(static[i, j], min(c, 3) / 3.0)
    for (i, j), c in s.co_app.items():
        if static[i, j] == 0: static[i, j] = static[j, i] = app_w
    tot_deg = np.zeros(N); tot_sens = np.zeros(N); nbrs = [set() for _ in range(N)]
    for k in range(s.n_edges):
        for n in (eu[k], er[k], ea[k]):
            tot_deg[n] += 1; tot_sens[n] += s.e_s[k]
        nbrs[eu[k]].add(er[k]); nbrs[er[k]].add(eu[k]); nbrs[ea[k]].add(eu[k])
    for t in range(T):
        deg = np.zeros(N); ms = np.zeros(N); den = np.zeros(N)
        A[t] = static
        for k in np.where(slc == t)[0]:
            A[t, eu[k], er[k]] = A[t, er[k], eu[k]] = 1.0
            for n in (eu[k], er[k], ea[k]):
                deg[n] += 1; ms[n] += s.e_s[k]; den[n] += 1 - s.e_y[k]
        nz = np.maximum(deg, 1)
        X[t, :, 3] = np.log1p(deg); X[t, :, 4] = ms / nz; X[t, :, 5] = den / nz
        X[t, :, 6] = np.log1p(tot_deg); X[t, :, 7] = tot_sens / np.maximum(tot_deg, 1)
        X[t, :, 8] = t / max(T - 1, 1); X[t, :, 9] = np.log1p([len(x) for x in nbrs])
        Ah = A[t] + np.eye(N, dtype=np.float32); d = Ah.sum(1)
        A[t] = Ah / np.sqrt(np.outer(d, d))
    return (torch.from_numpy(X).to(device), torch.from_numpy(A).to(device),
            torch.from_numpy(eu).long().to(device), torch.from_numpy(er).long().to(device))


class DenseGCN(nn.Module):
    def __init__(self, i, o):
        super().__init__(); self.lin = nn.Linear(i, o)

    def forward(self, A, X):                       # A: (T,N,N), X: (T,N,i)
        return F.relu(torch.matmul(A, self.lin(X)))


class TGN(nn.Module):
    def __init__(self, cfg: TGNConfig):
        super().__init__()
        E = cfg.emb_dim
        dims = [cfg.node_feat_dim] + [E] * cfg.gcn_layers
        self.gcn = nn.ModuleList([DenseGCN(dims[i], dims[i + 1]) for i in range(cfg.gcn_layers)])
        self.gru = nn.GRU(E, E, batch_first=True)
        self.dec = nn.Linear(2 * E, 1)             # sigma(W_s [h_i; h_j] + b_s)
        self.E = E

    def embed(self, X, A):
        h = X
        for g in self.gcn: h = g(A, h)             # (T,N,E)
        out, _ = self.gru(h.permute(1, 0, 2))      # (N,T,E)  temporal encoder
        q = out[:, -1:, :]
        att = torch.softmax((q @ out.transpose(1, 2)) / self.E ** 0.5, dim=-1)   # attention over T steps
        return (att @ out).squeeze(1) + out[:, -1, :]

    def edge_logits(self, X, A, eu, er):
        h = self.embed(X, A)
        return self.dec(torch.cat([h[eu], h[er]], -1)).squeeze(-1)


def train_tgn(samples, cfg: TGNConfig, gcfg: GraphConfig, epochs=None, device="cpu", seed=0, verbose=True):
    """Chronological split: first `train_frac` of graph samples train, rest test."""
    torch.manual_seed(seed)
    samples = sorted(samples, key=lambda s: s.t0)
    cut = int(len(samples) * cfg.train_frac)
    tr, te = samples[:cut], samples[cut:]
    prep = lambda ss: [(to_tensors(s, cfg, gcfg.app_coaccess_weight, device),
                        torch.from_numpy(s.e_label.astype(np.float32)).to(device)) for s in ss if s.n_edges]
    trp, tep = prep(tr), prep(te)
    pos = sum(float(l.sum()) for _, l in trp); neg = sum(float((1 - l).sum()) for _, l in trp)
    pw = torch.tensor(min(neg / max(pos, 1.0), 20.0), device=device)
    model = TGN(cfg).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=cfg.lr, weight_decay=cfg.weight_decay)
    rng = np.random.default_rng(seed)
    for ep in range(epochs or cfg.epochs):
        model.train()
        order = rng.permutation(len(trp)); tot = 0.0
        for i in range(0, len(order), cfg.graphs_per_step):
            loss = 0.0
            for j in order[i:i + cfg.graphs_per_step]:
                (X, A, eu, er), lab = trp[j]
                loss = loss + F.binary_cross_entropy_with_logits(model.edge_logits(X, A, eu, er), lab, pos_weight=pw)
            loss = loss / len(order[i:i + cfg.graphs_per_step])
            opt.zero_grad(); loss.backward(); opt.step(); tot += loss.item()
        if verbose and (ep % 10 == 0 or ep == (epochs or cfg.epochs) - 1):
            print(f"  [TGN] epoch {ep:3d} loss={tot:.4f}")
    model.eval(); ys, ps = [], []
    with torch.no_grad():
        for (X, A, eu, er), lab in tep:
            ps.append(torch.sigmoid(model.edge_logits(X, A, eu, er)).cpu().numpy()); ys.append(lab.cpu().numpy())
    test_auc = roc_auc(np.concatenate(ys), np.concatenate(ps)) if ys else float("nan")
    return model, {"train_graphs": len(trp), "test_graphs": len(tep), "test_edge_auc": test_auc,
                   "train_pos_edges": pos}


class TGNScorer:
    def __init__(self, model, cfg: TGNConfig, gcfg: GraphConfig, device="cpu"):
        self.model, self.cfg, self.gcfg, self.device = model.eval(), cfg, gcfg, device

    @torch.no_grad()
    def score_edges(self, s: GraphSample) -> np.ndarray:
        X, A, eu, er = to_tensors(s, self.cfg, self.gcfg.app_coaccess_weight, self.device)
        return torch.sigmoid(self.model.edge_logits(X, A, eu, er)).cpu().numpy()

    def group_scores(self, s: GraphSample, edge_scores: np.ndarray):
        """S_coll_g(u): every member of the coupled group g(u) receives the same score = mean of the top
        `group_top_frac` edge anomaly scores over the group's access edges. Returns {user: (score, members)}."""
        out = {}
        for comp in groups_from_graph(s, self.gcfg.min_shared_sensitive):
            members = [s.users[i] for i in comp]
            if len(members) < 2 or not set(members) & set(s.seeds): continue   # S_coll only for a coupled group
            mask = np.isin(s.e_user, comp)
            if not mask.any(): continue
            sc = np.sort(edge_scores[mask])[::-1]
            k = max(1, int(np.ceil(self.gcfg.group_top_frac * len(sc))))
            val = float(sc[:k].mean())
            for m in members: out[m] = (val, members)
        return out
