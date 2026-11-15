"""TGN model registry and scorer: <model_dir>/tgn/<version>/{model.pt, manifest.json}, <model_dir>/tgn/CURRENT.
Same contract as the TCN registry: immutable versions, atomic promotion, no scorer without a trained model."""
import json
import os
import time
from dataclasses import asdict
from typing import Dict, Optional

import numpy as np
import torch

from ..config import TGNConfig
from ..tgn import TGN
from .tgn_graph import GraphParams, GraphSample, edge_endpoints, featurize
from .tgn_train import groups_for


def tgn_dir(model_dir: str) -> str:
    return os.path.join(model_dir, "tgn")


def save(model_dir: str, version: str, model: TGN, cfg: TGNConfig, gp: GraphParams, tau_edge: float,
         theta_coll: float, manifest: dict) -> str:
    d = os.path.join(tgn_dir(model_dir), version)
    if os.path.exists(d):
        raise FileExistsError(f"model version {version} already exists (versions are immutable)")
    os.makedirs(d)
    torch.save({"state_dict": model.state_dict(), "tgn_config": asdict(cfg), "graph_params": asdict(gp),
                "tau_edge": tau_edge, "theta_coll": theta_coll}, os.path.join(d, "model.pt"))
    with open(os.path.join(d, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump({"version": version, "created_at": time.time(), "tau_edge": tau_edge, "theta_coll": theta_coll,
                   **manifest}, f, indent=2, default=str)
    return d


def promote(model_dir: str, version: str) -> None:
    if not os.path.isdir(os.path.join(tgn_dir(model_dir), version)):
        raise FileNotFoundError(version)
    tmp = os.path.join(tgn_dir(model_dir), f".CURRENT.{os.getpid()}")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(version)
    os.replace(tmp, os.path.join(tgn_dir(model_dir), "CURRENT"))


def current_version(model_dir: str) -> Optional[str]:
    try:
        with open(os.path.join(tgn_dir(model_dir), "CURRENT"), encoding="utf-8") as f:
            return f.read().strip() or None
    except FileNotFoundError:
        return None


class TgnScorer:
    def __init__(self, model: TGN, gp: GraphParams, tau_edge: float, theta_coll: float, version: str):
        self.model, self.gp, self.tau_edge, self.theta_coll, self.version = model.eval(), gp, tau_edge, theta_coll, version

    @classmethod
    def load(cls, model_dir: str, version: Optional[str] = None) -> "TgnScorer":
        version = version or current_version(model_dir)
        if version is None:
            raise FileNotFoundError(f"no trained TGN in {tgn_dir(model_dir)} (run accesssentinel.ml.train_tgn)")
        d = os.path.join(tgn_dir(model_dir), version)
        mpath = os.path.join(d, "manifest.json")
        manifest = json.load(open(mpath, encoding="utf-8")) if os.path.exists(mpath) else {}
        if manifest.get("arch") == ARCH_V3:                      # Revised v3 TGN (CMU + SGE)
            return TgnScorerV3.load_dir(d, version, manifest)
        ck = torch.load(os.path.join(tgn_dir(model_dir), version, "model.pt"), map_location="cpu", weights_only=True)
        cfg = TGNConfig(**ck["tgn_config"])
        model = TGN(cfg)
        model.load_state_dict(ck["state_dict"])
        return cls(model, GraphParams(**ck["graph_params"]), ck["tau_edge"], ck["theta_coll"], version)

    @torch.no_grad()
    def edge_scores(self, s: GraphSample) -> np.ndarray:
        if not s.edges:
            return np.zeros(0)
        X, A = featurize(s, self.gp)
        eu, er = edge_endpoints(s)
        h = self.model.embed(torch.from_numpy(X), torch.from_numpy(A))
        return torch.sigmoid(self.model.dec(torch.cat([h[torch.from_numpy(eu)], h[torch.from_numpy(er)]], -1)).squeeze(-1)).numpy()

    def analyse(self, s: GraphSample) -> Dict[str, dict]:
        """Per seed: g(u), S_coll (None if no coupled group) and evidence."""
        return groups_for(s, self.edge_scores(s), self.tau_edge)


# ============================================================================================================
# Revised v3 TGN (CMU + SGE). manifest["arch"] = "tgn-v3"; TgnScorer.load() returns TgnScorerV3 for it.
# ============================================================================================================
ARCH_V3 = "tgn-v3"


def save_v3(model_dir: str, version: str, model, cfg, cal: dict, thresholds: dict, manifest: dict) -> str:
    d = os.path.join(tgn_dir(model_dir), version)
    if os.path.exists(d):
        raise FileExistsError(f"model version {version} already exists (versions are immutable)")
    os.makedirs(d)
    torch.save({"arch": ARCH_V3, "state_dict": {k: v.cpu() for k, v in model.state_dict().items()},
                "tgn_config": asdict(cfg)}, os.path.join(d, "model.pt"))
    with open(os.path.join(d, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump({"version": version, "created_at": time.time(), "arch": ARCH_V3, "calibration": cal,
                   "thresholds": thresholds, **manifest}, f, indent=2, default=str)
    return d


class TgnScorerV3:
    """Trained v3 TGN: CMU step (every event), SGE + decoder (admitted users), calibrated collective score."""
    arch = ARCH_V3

    def __init__(self, model, cfg, cal: dict, thresholds: dict, version: str):
        self.model, self.cfg, self.cal, self.th, self.version = model.eval(), cfg, cal, thresholds, version
        self.theta_coll, self.theta_attr = thresholds.get("theta_C"), thresholds.get("theta_A")

    @classmethod
    def load_dir(cls, d: str, version: str, manifest: dict) -> "TgnScorerV3":
        from ..config import TGNv3Config
        from ..tgn import TGNv3
        ck = torch.load(os.path.join(d, "model.pt"), map_location="cpu", weights_only=True)
        cfg = TGNv3Config(**ck["tgn_config"])
        model = TGNv3(cfg)
        model.load_state_dict(ck["state_dict"])
        return cls(model, cfg, manifest["calibration"], manifest["thresholds"], version)

    @torch.no_grad()
    def cmu_step(self, m_self: np.ndarray, m_other: np.ndarray, dt: np.ndarray, f: np.ndarray) -> np.ndarray:
        t = lambda a: torch.from_numpy(np.ascontiguousarray(a, dtype=np.float32))
        return self.model.cmu(t(m_self), t(m_other), t(dt), t(f)).numpy()

    @torch.no_grad()
    def embed(self, m_v, vtype, nbr_m, nbr_type, nbr_f, nbr_dt, nbr_mask) -> np.ndarray:
        t = lambda a: torch.from_numpy(np.ascontiguousarray(a, dtype=np.float32))
        return self.model.sge(t(m_v), t(vtype), t(nbr_m), t(nbr_type), t(nbr_f), t(nbr_dt),
                              torch.from_numpy(np.asarray(nbr_mask, dtype=bool))).numpy()

    @torch.no_grad()
    def edge_eps(self, h_u: np.ndarray, h_r: np.ndarray, f: np.ndarray) -> np.ndarray:
        t = lambda a: torch.from_numpy(np.ascontiguousarray(a, dtype=np.float32))
        return torch.sigmoid(self.model.dec(t(h_u), t(h_r), t(f))).numpy()

    def s_coll(self, feats) -> float:
        """Eq. 19: Cal(w1 TM + w2 A + w3 Nov) (logistic calibration fitted with the weights)."""
        z = float(np.dot(self.cal["coef"], feats) + self.cal["intercept"])
        return 1.0 / (1.0 + np.exp(-z))
