# AccessSentinel

**Continuous monitoring of broken access control in multi-web applications** — research prototype, local demo stack, and reproducible synthetic-data evaluation.

> **Start here.** This is the canonical project README. The active comparison implementation is **evaluation revision v5**, not the archived v2–v4 suites. The full five-seed run has **not** been established as completed or paper-ready by this archive. A successful smoke test is not publishable evidence.

## What the system does

AccessSentinel monitors authenticated access requests across protected web applications. A gateway records access events; Kafka distributes user-keyed and resource-keyed views; HDFS stores historical records. A **Temporal Convolutional Network (TCN)** scores individual access sequences. A **Temporal Graph Network (TGN)** maintains temporal graph memory and selectively analyzes escalated users for coordinated behavior. A **four-path escalation policy** (high-risk, sensitive-resource co-access, periodic sweep, and sampling) bounds graph inference work. A **Policy Decision Point (PDP)** applies risk-adaptive decisions, with analyst-confirmed feedback supporting later model refinement.

This repository includes both an **in-process demonstration** and a **Docker-based multi-service prototype**. The synthetic evaluation does **not** establish production security efficacy or end-to-end deployment performance.

## Repository map

| Path | Purpose |
|---|---|
| `accesssentinel/` | Gateway, event schema, streaming, TCN/TGN, escalation, PDP, storage, API, and model code |
| `dashboard/` | Analyst-facing web dashboard |
| `deploy/`, `start.*`, `stop.bat` | Service deployment and startup helpers |
| `models/` | Bundled model artifacts for the local demo |
| `research_comparison/` | Current **v5** common-data comparison, reference adapters, measured evaluation, and tests |
| `evaluation/` | Separate Experiments A–H, their figures, and recorded raw results |
| `experiments/` | Standalone research experiment scripts and retained reports |
| `requirements/` | Dependency sets; use `evaluation.txt` for research comparison |
| `tests/`, `docs/` | Tests, architecture notes, audit history, and Windows setup |
| `PROJECT_SUMMARY.md` | Plain-language project overview |
| `PRESENTATION_DEMO.md` | Demo walkthrough |

## Quick start — local application demo

Prerequisites: Python 3.10+ for the in-process demo; Docker Desktop for the service stack. From the **AccessSentinel project root**:

```bash
python -m pip install -r requirements.txt
python -m pytest -q tests
python run_demo.py --quick
```

For the deployed local stack, review [`docs/SETUP_WINDOWS.md`](docs/SETUP_WINDOWS.md), copy `.env.example` to `.env`, set your own secrets, and run:

```bash
docker compose up -d --build
docker compose ps
```

On Windows you can instead run `start.bat` after starting Docker Desktop. The demo gateway normally uses `http://localhost:8000`; the dashboard uses `http://localhost:8080`. Do **not** expose this research prototype directly to the public internet. The supplied ZIP intentionally excludes the machine-specific `.env` file.

## Current research comparison: revision v5

The comparison evaluates AccessSentinel and six **adapted** research methods on the same synthetic access-event data and chronological splits:

| Display name | Reference | Notes |
|---|---|---|
| AccessSentinel | Proposed method | TCN individual detection; native TGN collective evaluation where applicable |
| DynAD [14] | [14] | Dynamic-graph adaptation |
| PG-AID [17] | [17] | Interaction-provenance approximation |
| ACOBE [21] | [21] | Cohort/group-behavior adaptation |
| FGDT [22] | [22] | Co-access group adaptation |
| User-resource embedding [24] | [24] | Individual user-resource anomaly baseline |
| DEEPRO [25] | [25] | Interaction-provenance approximation |

**Methodological warning:** These are common-data adaptations, not guaranteed faithful reproductions of the original papers or their reported numbers. In particular, reference event-anomaly scores projected onto collusion anchors are **not native group detectors**. Data are synthetic. Record hardware, training budget, prediction coverage, seeds, and metric definitions before citing any figures.

### Install and verify

```bash
python -m pip install -r requirements/evaluation.txt
python -m pytest -q research_comparison/tests
python -m compileall -q research_comparison evaluation
```

### Preflight: validate generated datasets without expensive training

```bash
python -m research_comparison.run_evaluation --preflight --figures 2,3,4,5,6,7,8,9 --seeds 0,1,2,3,4 --device cpu
```

### Quick engineering smoke test (NOT paper results)

```bash
python -m research_comparison.run_evaluation --quick --figures 2,3,4,5,6,7,8,9 --seeds 0 --tgn-epochs 2 --load-duration 0.6 --device cpu
```

### Full synthetic five-seed evaluation (compute-intensive)

Use `--device cuda` only if `torch.cuda.is_available()` is true and the PyTorch installation supports the GPU:

```bash
python -m research_comparison.run_evaluation --figures 2,3,4,5,6,7,8,9 --seeds 0,1,2,3,4 --tgn-epochs 6 --load-duration 2 --device cuda
```

Run from the repository root, so Python can import `research_comparison` and `evaluation`. **Do not confuse a preflight pass or quick run with a completed five-seed result.** Revision v5 uses `research_comparison/results/evaluation_v5/` for full results and `evaluation_v5_smoke/` for quick results. These output directories are generated when experiments run; they are not shipped as verified full results.

### Figure index

| Figure | Subject |
|---|---|
| Fig. 2 | Precision, Recall, F1 |
| Fig. 3 | ROC-AUC and PR-AUC |
| Fig. 4 | Attack detection coverage |
| Fig. 5 | Attack detection delay (conditional on detection) |
| Fig. 6 | Collusion detection performance; AccessSentinel vs adapted references |
| Fig. 7 | Throughput |
| Fig. 8 | Component latency (local measurements, not full end-to-end) |
| Fig. 9 | Ablation study |
| Fig. 10 | Independent Experiment B |
| Fig. 11 | Independent Experiment C |
| Fig. 12 | Independent Experiment F |

Figures 2–9 are generated by the v5 comparison runner from measured experiment outputs. Figures 10–12 are regenerated **separately** from the recorded independent Experiment B/C/F results:

```bash
python -m evaluation.figures b c f
```

Do not merge B/C/F data into the v5 common-data comparison or interpret local component latency as gateway-to-enforcement latency.

### Outputs and research integrity

The comparison runner writes `raw/` measurement JSON, `cache/` model/prediction artifacts, `tables/` CSV summaries, `figures/` PNG charts, and `evaluation_report.json` under its output directory. Inspect missing scores, detected denominators, dataset fingerprints, model training budgets, and failures. `paper_ready` remains false pending independent methods review. The current v5 revision changed synthetic collusion generation and must be rerun for all five seeds before drawing new comparative conclusions.

For detailed rationale and limitations, see [`research_comparison/REVISION_V5_NOTES.md`](research_comparison/REVISION_V5_NOTES.md), [`research_comparison/IMPLEMENTATION_STATUS.md`](research_comparison/IMPLEMENTATION_STATUS.md), and [`research_comparison/reproduction_manifest.csv`](research_comparison/reproduction_manifest.csv). The separate experiment suite is documented in [`evaluation/README.md`](evaluation/README.md).

## Running remotely (Kaggle / Colab)

This archive is suitable as a **starting point** for a remote notebook, but it does **not yet include a configured Kaggle notebook**. Upload the cleaned project or an appropriate source-only subset, install `requirements/evaluation.txt`, check CUDA, run preflight and quick mode, then perform full runs with persistent checkpoint/output storage. GPU sessions have quotas and can terminate; do not assume multi-seed training will fit one session. Keep each seed's results and record the exact hardware and software environment.

## Safety and scope

- Use only with systems and data you are authorized to test.
- Synthetic detections and simulated enforcement are not proof of real-world attack prevention.
- Never publish `.env`, credentials, session tokens, production access logs, or analyst data in shared ZIP files.
- Existing research raw data and standalone experiment reports are retained for reproducibility; archived v2–v4 comparison outputs and superseded comparison README files were removed from this distribution.
