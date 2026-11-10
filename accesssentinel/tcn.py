"""Step 4: Temporal Convolutional Network, S_ind = f_TCN(X_u) (Eq. 3). Architecture per D3:
10 causal dilated layers, spatial dropout 0.3, dense dropout 0.5, Focal Loss, Adam 1e-3, 5-fold CV."""
import copy
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from .config import TCNConfig
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
    return 1.0 / (1.0 + np.exp(-(a * z + b)))


def fit_platt(logits, y):
    """[IMPL] Focal loss yields poorly calibrated scores; D3's fixed thresholds assume probability-like scores."""
    z = torch.tensor(logits, dtype=torch.float32); t = torch.tensor(y, dtype=torch.float32)
    a = torch.ones(1, requires_grad=True); b = torch.zeros(1, requires_grad=True)
    opt = torch.optim.LBFGS([a, b], max_iter=100)
    def closure():
        opt.zero_grad(); l = F.binary_cross_entropy_with_logits(a * z + b, t); l.backward(); return l
    opt.step(closure)
    return float(a), float(b)


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
