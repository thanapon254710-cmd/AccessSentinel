"""TCN model registry and scorer (audit D10: models are persisted, versioned and reloadable).

Layout:  <model_dir>/tcn/<version>/{model.pt, preprocessor.json, manifest.json}
         <model_dir>/tcn/CURRENT      text file with the version the workers serve
A version is immutable. `promote()` switches CURRENT atomically (write temp file, rename).
Every score produced by `TcnScorer` carries `version`, so each risk score is traceable to a model,
its training data fingerprint and its evaluation report.
"""
import json
import os
import time
from dataclasses import asdict
from typing import List, Optional, Sequence

import numpy as np
import torch

from ..config import TCNConfig
from ..tcn import TCN, platt_sigmoid
from .features import Preprocessor


def tcn_dir(model_dir: str) -> str:
    return os.path.join(model_dir, "tcn")


def save(model_dir: str, version: str, model: TCN, platt, pre: Preprocessor, cfg: TCNConfig, manifest: dict) -> str:
    d = os.path.join(tcn_dir(model_dir), version)
    if os.path.exists(d):
        raise FileExistsError(f"model version {version} already exists (versions are immutable)")
    os.makedirs(d)
    torch.save({"state_dict": model.state_dict(), "platt": list(platt), "tcn_config": asdict(cfg)},
               os.path.join(d, "model.pt"))
    with open(os.path.join(d, "preprocessor.json"), "w", encoding="utf-8") as f:
        json.dump(pre.to_dict(), f)
    with open(os.path.join(d, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump({"version": version, "created_at": time.time(), **manifest}, f, indent=2, default=str)
    return d


def promote(model_dir: str, version: str) -> None:
    if not os.path.isdir(os.path.join(tcn_dir(model_dir), version)):
        raise FileNotFoundError(version)
    tmp = os.path.join(tcn_dir(model_dir), f".CURRENT.{os.getpid()}")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(version)
    os.replace(tmp, os.path.join(tcn_dir(model_dir), "CURRENT"))


def current_version(model_dir: str) -> Optional[str]:
    try:
        with open(os.path.join(tcn_dir(model_dir), "CURRENT"), encoding="utf-8") as f:
            v = f.read().strip()
        return v or None
    except FileNotFoundError:
        return None


def list_versions(model_dir: str) -> List[str]:
    d = tcn_dir(model_dir)
    return sorted(n for n in os.listdir(d) if os.path.isdir(os.path.join(d, n))) if os.path.isdir(d) else []


class TcnScorer:
    """A TRAINED TCN + its preprocessor + Platt calibration. There is deliberately no way to build one
    without a saved, trained model: the system never emits scores from an untrained network."""

    def __init__(self, model: TCN, platt, pre: Preprocessor, version: str, manifest: dict):
        self.model, self.platt, self.pre, self.version, self.manifest = model.eval(), tuple(platt), pre, version, manifest

    @classmethod
    def load(cls, model_dir: str, version: Optional[str] = None) -> "TcnScorer":
        version = version or current_version(model_dir)
        if version is None:
            raise FileNotFoundError(f"no trained TCN in {tcn_dir(model_dir)} (run accesssentinel.ml.train_tcn)")
        d = os.path.join(tcn_dir(model_dir), version)
        ck = torch.load(os.path.join(d, "model.pt"), map_location="cpu", weights_only=True)
        model = TCN(TCNConfig(**{k: tuple(v) if isinstance(v, list) else v for k, v in ck["tcn_config"].items()}))
        model.load_state_dict(ck["state_dict"])
        with open(os.path.join(d, "preprocessor.json"), encoding="utf-8") as f:
            pre = Preprocessor.from_dict(json.load(f))
        with open(os.path.join(d, "manifest.json"), encoding="utf-8") as f:
            manifest = json.load(f)
        return cls(model, ck["platt"], pre, version, manifest)

    @torch.no_grad()
    def logits(self, X: np.ndarray, batch: int = 1024) -> np.ndarray:
        out = [self.model(torch.from_numpy(np.ascontiguousarray(X[i:i + batch], dtype=np.float32))).numpy()
               for i in range(0, len(X), batch)]
        return np.concatenate(out) if out else np.zeros(0)

    def score_X(self, X: np.ndarray) -> np.ndarray:
        return platt_sigmoid(self.logits(X), *self.platt)

    def score_windows(self, windows: Sequence[Sequence[dict]]) -> np.ndarray:
        """windows: per sample, the user's raw records (newest last). Returns S_ind in [0, 1]."""
        if not windows:
            return np.zeros(0)
        return self.score_X(np.stack([self.pre.encode(w) for w in windows]))
