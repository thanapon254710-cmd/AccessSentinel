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
