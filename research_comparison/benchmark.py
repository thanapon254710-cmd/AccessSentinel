"""Common, reproducible benchmark utilities for faithful paper adapters."""
from __future__ import annotations
import importlib.util
import json
import os
import time
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import (average_precision_score, precision_recall_fscore_support,
                             roc_auc_score)

ROOT = Path(__file__).resolve().parents[1]
HERE = Path(__file__).resolve().parent
ADAPTERS = HERE / "adapters"
RESULTS = HERE / "results"


def load_adapters(task: str):
    found = []
    for path in sorted(ADAPTERS.glob("*.py")):
        if path.name.startswith("_") or path.name == "__init__.py":
            continue
        spec = importlib.util.spec_from_file_location(f"paper_adapter_{path.stem}", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if task in getattr(module, "TASKS", []):
            if not callable(getattr(module, "fit_predict", None)):
                raise TypeError(f"{path}: missing fit_predict(dataset, task, seed)")
            found.append((path.stem, module))
    return found


def score_predictions(y_true, y_score):
    y = np.asarray(y_true, dtype=int).reshape(-1)
    s = np.asarray(y_score, dtype=float).reshape(-1)
    if len(y) != len(s) or not len(y):
        raise ValueError("y_true and y_score must be non-empty arrays of equal length")
    if not np.isfinite(s).all():
        raise ValueError("y_score contains NaN or infinite values")
    if len(np.unique(y)) < 2:
        return {"roc_auc": None, "pr_auc": None, "precision": None, "recall": None, "f1": None,
                "metric_note": "Test split contains only one class; class-separation metrics are undefined."}
    # Threshold is calibrated on validation by the adapter; adapter returns test labels and scores.
    # Report threshold-free metrics here. Thresholded metrics are only accepted when the adapter supplies
    # a validation-calibrated threshold and test predictions (never test-tuned).
    return {"roc_auc": float(roc_auc_score(y, s)),
            "pr_auc": float(average_precision_score(y, s))}


def run(task: str, seeds: list[int], dataset_factory):
    adapters = load_adapters(task)
    if not adapters:
        raise RuntimeError(
            "No faithful paper adapters are registered for this task. See reproduction_manifest.csv. "
            "The harness deliberately refuses to substitute AccessSentinel's internal baselines or invent paper results."
        )
    RESULTS.joinpath("raw").mkdir(parents=True, exist_ok=True)
    records = []
    for seed in seeds:
        dataset = dataset_factory(seed)
        for adapter_name, adapter in adapters:
            started = time.perf_counter()
            result = adapter.fit_predict(dataset, task=task, seed=seed)
            wall_s = time.perf_counter() - started
            if not isinstance(result, dict) or not {"y_true", "y_score"}.issubset(result):
                raise ValueError(f"{adapter_name}: adapter must return y_true and y_score")
            metrics = score_predictions(result["y_true"], result["y_score"])
            latency = np.asarray(result.get("latency_ms", []), dtype=float)
            row = {"method": adapter_name, "seed": seed, "task": task, **metrics,
                   "wall_seconds": wall_s,
                   "latency_median_ms": float(np.median(latency)) if latency.size else None,
                   "latency_p95_ms": float(np.percentile(latency, 95)) if latency.size else None,
                   "metadata": result.get("metadata", {})}
            records.append(row)
            out = RESULTS / "raw" / f"{task}_{adapter_name}_seed{seed}.json"
            out.write_text(json.dumps(row, indent=2, default=str), encoding="utf-8")
    frame = pd.DataFrame(records)
    tables = RESULTS / "tables"; figures = RESULTS / "figures"
    tables.mkdir(parents=True, exist_ok=True); figures.mkdir(parents=True, exist_ok=True)
    frame.to_csv(tables / f"{task}_per_seed.csv", index=False)
    numeric = [c for c in ["roc_auc", "pr_auc", "latency_median_ms", "latency_p95_ms"] if c in frame]
    summary = frame.groupby("method")[numeric].agg(["mean", "std"]).reset_index()
    summary.to_csv(tables / f"{task}_summary.csv", index=False)
    for metric in ["roc_auc", "pr_auc", "latency_median_ms", "latency_p95_ms"]:
        valid = frame.dropna(subset=[metric])
        if valid.empty:
            continue
        stats = valid.groupby("method")[metric].agg(["mean", "std"])
        ax = stats["mean"].plot(kind="bar", yerr=stats["std"].fillna(0), capsize=4)
        ax.set_title(f"{task.title()} comparison: {metric.replace('_', ' ')}")
        ax.set_ylabel(metric.replace('_', ' ')); ax.set_xlabel("Faithfully reproduced method")
        plt.tight_layout(); plt.savefig(figures / f"{task}_{metric}.png", dpi=180); plt.close()
    return frame
