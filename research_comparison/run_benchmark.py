"""Run faithful related-paper reproductions on shared AccessSentinel synthetic data."""
from __future__ import annotations
import argparse
from dataclasses import replace
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from accesssentinel.datasets.synth import SynthConfig, generate
from research_comparison.benchmark import run


def dataset_factory(task: str, seed: int):
    if task == "individual":
        cfg = SynthConfig.tcn_v3_eval(seed=23 + seed)
    else:
        cfg = SynthConfig.collusion_v3_eval(seed=11 + seed)
    return generate(cfg)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--task", choices=["individual", "collusion"], required=True)
    p.add_argument("--seeds", nargs="+", type=int, default=[0, 1, 2, 3, 4])
    args = p.parse_args()
    try:
        frame = run(args.task, args.seeds, lambda seed: dataset_factory(args.task, seed))
    except Exception as exc:
        p.error(str(exc))
    print(frame.to_string(index=False))

if __name__ == "__main__":
    main()
