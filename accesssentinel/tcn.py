"""Step 4: Temporal Convolutional Network, S_ind = f_TCN(X_u) (Eq. 3). Architecture per D3:
10 causal dilated layers, spatial dropout 0.3, dense dropout 0.5, Focal Loss, Adam 1e-3, 5-fold CV."""
import copy
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import Dict, Tuple

from .config import TCNConfig, TCNv3Config
from .metrics import roc_auc


class CausalConv1d(nn.Module):
    def __init__(self, cin, cout, k, dilation):
        super().__init__()
        self.pad = (k - 1) * dilation
        self.conv = nn.Conv1d(cin, cout, k, dilation=dilation)

    def forward(self, x):
        return self.conv(F.pad(x, (self.pad, 0)))


class TCNBlock(nn.Module):
    def __init__(self, cin, cout, k, dilation, p):
        super().__init__()
        self.conv = CausalConv1d(cin, cout, k, dilation)
        self.drop = nn.Dropout1d(p)               # spatial dropout
        self.res = nn.Conv1d(cin, cout, 1) if cin != cout else nn.Identity()

    def forward(self, x):
        return F.relu(self.drop(F.relu(self.conv(x))) + self.res(x))


class TCN(nn.Module):
    def __init__(self, cfg: TCNConfig):
        super().__init__()
        assert len(cfg.dilations) == len(cfg.filters)
        blocks, cin = [], cfg.feature_dim
        for d, f in zip(cfg.dilations, cfg.filters):
            blocks.append(TCNBlock(cin, f, cfg.kernel_size, d, cfg.spatial_dropout)); cin = f
        self.blocks = nn.Sequential(*blocks)
        self.fc1 = nn.Linear(cin, cfg.dense_units)
        self.drop = nn.Dropout(cfg.dense_dropout)
        self.fc2 = nn.Linear(cfg.dense_units, 1)

    def forward(self, x):                          # x: (B, L, d) -> logits (B,)
        h = self.blocks(x.transpose(1, 2))[:, :, -1]
        return self.fc2(self.drop(F.relu(self.fc1(h)))).squeeze(-1)

    @staticmethod
    def receptive_field(cfg: TCNConfig) -> int:
        return 1 + (cfg.kernel_size - 1) * sum(cfg.dilations)


def focal_loss(logits, targets, gamma=2.0, alpha=0.5):
    ce = F.binary_cross_entropy_with_logits(logits, targets, reduction="none")
    p = torch.sigmoid(logits)
    pt = p * targets + (1 - p) * (1 - targets)
    a = alpha * targets + (1 - alpha) * (1 - targets)
    return (a * (1 - pt) ** gamma * ce).mean()


def make_splits(n, cfg: TCNConfig, seed=0):
    """10% test; remaining 90% split into K folds -> each fold is 72% train / 18% val."""
    rng = np.random.default_rng(seed)
    idx = rng.permutation(n)
    n_test = int(n * cfg.test_frac)
    test, rest = idx[:n_test], idx[n_test:]
    folds = np.array_split(rest, cfg.folds)
    return test, [(np.concatenate([f for j, f in enumerate(folds) if j != i]), folds[i]) for i in range(cfg.folds)]


@torch.no_grad()
def predict_logits(model, X, batch=2048, device="cpu"):
    model.eval(); out = []
    for i in range(0, len(X), batch):
        out.append(model(torch.from_numpy(X[i:i + batch]).to(device)).cpu().numpy())
    return np.concatenate(out) if out else np.zeros(0)


def predict(model, X, batch=2048, device="cpu"):
    """Probability-like score; applies Platt calibration if fitted so theta_low/high (0.4/0.6) are meaningful."""
    z = predict_logits(model, X, batch, device)
    a, b = getattr(model, "platt", (1.0, 0.0))
    return platt_sigmoid(z, a, b)


def platt_sigmoid(z, a: float, b: float) -> np.ndarray:
    """Numerically stable sigmoid(a*z + b) (no overflow warnings for large |z|; audit D14)."""
    x = np.clip(a * np.asarray(z, dtype=np.float64) + b, -60.0, 60.0)
    return 1.0 / (1.0 + np.exp(-x))


def fit_platt(logits, y):
    """[IMPL] Focal loss yields poorly calibrated scores; D3's fixed thresholds assume probability-like scores."""
    z = torch.tensor(logits, dtype=torch.float32); t = torch.tensor(y, dtype=torch.float32)
    a = torch.ones(1, requires_grad=True); b = torch.zeros(1, requires_grad=True)
    opt = torch.optim.LBFGS([a, b], max_iter=100)
    def closure():
        opt.zero_grad(); l = F.binary_cross_entropy_with_logits(a * z + b, t); l.backward(); return l
    opt.step(closure)
    return float(a.detach()), float(b.detach())


def train_tcn(X, y, cfg: TCNConfig, train_idx, val_idx, epochs=None, device="cpu", seed=0, verbose=True):
    torch.manual_seed(seed)
    model = TCN(cfg).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=cfg.lr)
    Xt, yt = torch.from_numpy(X), torch.from_numpy(y.astype(np.float32))
    rng = np.random.default_rng(seed)
    best, best_loss = None, float("inf")
    for ep in range(epochs or cfg.epochs):
        model.train()
        perm = rng.permutation(train_idx)
        for i in range(0, len(perm), cfg.batch_size):
            b = perm[i:i + cfg.batch_size]
            loss = focal_loss(model(Xt[b].to(device)), yt[b].to(device), cfg.focal_gamma, cfg.focal_alpha)
            opt.zero_grad(); loss.backward(); opt.step()
        model.eval()
        with torch.no_grad():
            lv = model(Xt[val_idx].to(device))
            vloss = focal_loss(lv, yt[val_idx].to(device), cfg.focal_gamma, cfg.focal_alpha).item()
        if vloss < best_loss:
            best_loss, best = vloss, copy.deepcopy(model.state_dict())
        if verbose and (ep % 5 == 0 or ep == (epochs or cfg.epochs) - 1):
            auc = roc_auc(y[val_idx], torch.sigmoid(lv).cpu().numpy())
            print(f"  [TCN] epoch {ep:3d} val_loss={vloss:.4f} val_auc={auc:.3f}")
    model.load_state_dict(best)
    model.platt = fit_platt(predict_logits(model, X[val_idx], device=device), y[val_idx])
    return model


def cross_validate_tcn(X, y, cfg: TCNConfig, max_folds=None, epochs=None, device="cpu", verbose=True):
    test, folds = make_splits(len(X), cfg)
    results, best_model, best_auc = [], None, -1
    for k, (tr, va) in enumerate(folds[: max_folds or cfg.folds]):
        if verbose: print(f" fold {k}: train={len(tr)} val={len(va)} test={len(test)}")
        m = train_tcn(X, y, cfg, tr, va, epochs, device, seed=k, verbose=verbose)
        auc = roc_auc(y[va], predict(m, X[va], device=device))
        results.append({"fold": k, "val_auc": auc})
        if auc > best_auc: best_auc, best_model = auc, m
    return best_model, results, roc_auc(y[test], predict(best_model, X[test], device=device))


class TCNScorer:
    def __init__(self, model, device="cpu"):
        self.model, self.device = model.eval(), device

    def score(self, windows: np.ndarray) -> np.ndarray:
        return predict(self.model, windows.astype(np.float32), device=self.device)


# ============================================================================================================
# Revised v3 TCN (Phase 3). The D3 TCN above is kept as the baseline.
# ============================================================================================================
"""Revised v3 TCN (Step 4, Eqs. 3-6).

  input   X_u in R^{L x 43}: embeddings (resource 12, permission/operation 6, network 6) + 19 numeric features
  body    6 residual blocks, each with TWO causal dilated convolutions (kernel k = 2), dilations
          (1, 2, 4, 8, 16, 32), filters (256, 256, 128, 128, 64, 64); receptive field
          R = 1 + 2 (k - 1) sum(D) = 1 + 2 * 63 = 127 <= L = 128                                     (Eq. 4)
  heads   on the last-step representation h_T:
            next resource   (classification over the resource vocabulary)       L_next-res
            next operation  (classification over (p, o))                          L_next-op
            time gap        (regression of log1p seconds to the next event)        L_time-gap
            supervised      S_ind = sigmoid(f([h_T ; z_u(t)]))                     L_sup (Focal Loss)
          z_u(t) (timer context) joins only the supervised scoring head (Table III).
  loss    L = l1 L_next-res + l2 L_next-op + l3 L_time-gap + l4 L_sup                                (Eq. 6)
"""
from .ml.features import D_V3, EMB_NET, EMB_OP, EMB_RES, N_Z  # noqa: E402


class ResidualBlock(nn.Module):
    """Two dilated causal convolutions + residual connection (Bai et al. style TCN block)."""

    def __init__(self, cin, cout, k, dilation, dropout):
        super().__init__()
        self.c1, self.c2 = CausalConv1d(cin, cout, k, dilation), CausalConv1d(cout, cout, k, dilation)
        self.drop = nn.Dropout(dropout)
        self.skip = nn.Conv1d(cin, cout, 1) if cin != cout else nn.Identity()

    def forward(self, x):
        y = self.drop(F.relu(self.c1(x)))
        y = self.drop(F.relu(self.c2(y)))
        return F.relu(y + self.skip(x))


class TCNv3(nn.Module):
    def __init__(self, cfg: "TCNv3Config", n_res: int, n_op: int, n_net: int):
        super().__init__()
        assert len(cfg.dilations) == len(cfg.filters) == 6
        assert cfg.receptive_field() <= cfg.window_len, "receptive field must not exceed L"
        self.cfg = cfg
        self.emb_res = nn.Embedding(n_res, EMB_RES, padding_idx=0)
        self.emb_op = nn.Embedding(n_op, EMB_OP, padding_idx=0)
        self.emb_net = nn.Embedding(n_net, EMB_NET, padding_idx=0)
        blocks, cin = [], D_V3
        for d, f in zip(cfg.dilations, cfg.filters):
            blocks.append(ResidualBlock(cin, f, cfg.kernel_size, d, cfg.dropout))
            cin = f
        self.body = nn.Sequential(*blocks)
        h = cfg.filters[-1]
        self.next_res = nn.Linear(h, n_res)
        self.next_op = nn.Linear(h, n_op)
        self.time_gap = nn.Linear(h, 1)
        self.score = nn.Sequential(nn.Linear(h + N_Z, cfg.head_hidden), nn.ReLU(), nn.Dropout(cfg.dropout),
                                   nn.Linear(cfg.head_hidden, 1))

    def embed(self, res, op, net, num) -> torch.Tensor:
        x = torch.cat([self.emb_res(res), self.emb_op(op), self.emb_net(net), num], dim=-1)   # (B, L, 43)
        return x

    def forward(self, res, op, net, num, z) -> Dict[str, torch.Tensor]:
        x = self.embed(res, op, net, num).transpose(1, 2)                                      # (B, 43, L)
        h = self.body(x)[:, :, -1]                                                              # last step
        return {"logit": self.score(torch.cat([h, z], dim=-1)).squeeze(-1),
                "next_res": self.next_res(h), "next_op": self.next_op(h), "time_gap": self.time_gap(h).squeeze(-1)}


def multitask_loss(out, batch, cfg: TCNv3Config, supervised: bool) -> Tuple[torch.Tensor, Dict[str, float]]:
    """Eq. 6. Self-supervised terms use only windows that HAVE a next event in the same split (mask)."""
    l1, l2, l3, l4 = cfg.lambdas
    m = batch["next_mask"]
    parts = {}
    if m.any():
        parts["next_res"] = F.cross_entropy(out["next_res"][m], batch["next_res"][m])
        parts["next_op"] = F.cross_entropy(out["next_op"][m], batch["next_op"][m])
        parts["time_gap"] = F.smooth_l1_loss(out["time_gap"][m], batch["next_gap"][m])
    else:
        zero = out["logit"].sum() * 0.0
        parts["next_res"] = parts["next_op"] = parts["time_gap"] = zero
    total = l1 * parts["next_res"] + l2 * parts["next_op"] + l3 * parts["time_gap"]
    if supervised:
        parts["sup"] = focal_loss(out["logit"], batch["y"], gamma=cfg.focal_gamma, alpha=cfg.focal_alpha)
        total = total + l4 * parts["sup"]
    return total, {k: float(v.detach()) for k, v in parts.items()}


# ============================================================================================================
# Revised v3 Phase 8: baselines and ablations for Experiment A / F (same input layer and heads as TCNv3)
# ============================================================================================================
class LSTMBaseline(nn.Module):
    """Recurrent baseline: the Table III input embeddings of TCNv3, a 2-layer LSTM instead of the dilated causal
    convolutions, the same four heads (the baseline protocol trains only the supervised one)."""

    def __init__(self, cfg: "TCNv3Config", n_res: int, n_op: int, n_net: int, hidden: int = 128, layers: int = 2):
        super().__init__()
        self.cfg = cfg
        self.emb_res = nn.Embedding(n_res, EMB_RES, padding_idx=0)
        self.emb_op = nn.Embedding(n_op, EMB_OP, padding_idx=0)
        self.emb_net = nn.Embedding(n_net, EMB_NET, padding_idx=0)
        self.rnn = nn.LSTM(D_V3, hidden, num_layers=layers, batch_first=True, dropout=cfg.dropout)
        self.next_res = nn.Linear(hidden, n_res)
        self.next_op = nn.Linear(hidden, n_op)
        self.time_gap = nn.Linear(hidden, 1)
        self.score = nn.Sequential(nn.Linear(hidden + N_Z, cfg.head_hidden), nn.ReLU(), nn.Dropout(cfg.dropout),
                                   nn.Linear(cfg.head_hidden, 1))

    embed = TCNv3.embed

    def forward(self, res, op, net, num, z) -> Dict[str, torch.Tensor]:
        h = self.rnn(self.embed(res, op, net, num))[0][:, -1]                                 # last step
        return {"logit": self.score(torch.cat([h, z], dim=-1)).squeeze(-1),
                "next_res": self.next_res(h), "next_op": self.next_op(h), "time_gap": self.time_gap(h).squeeze(-1)}


class ZeroZ(nn.Module):
    """Runs a v3 model WITHOUT the timer context z_u(t) (zeros at the scoring head): baselines and the ablation."""

    def __init__(self, inner: nn.Module):
        super().__init__()
        self.inner, self.cfg = inner, inner.cfg

    def forward(self, res, op, net, num, z):
        return self.inner(res, op, net, num, torch.zeros_like(z))
