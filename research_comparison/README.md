# AccessSentinel related-work comparison

## Purpose and scope

This benchmark runs the six selected paper-method cores against AccessSentinel on a common generated AccessSentinel dataset and the same chronological evaluation-window split. It is a **common-data reimplementation/adaptation**, not a reproduction of the original papers' reported datasets or published metrics.

Selected references: **[24], [14], [17], [21], [22], and [25]**. See `IMPLEMENTATION_STATUS.md` and `reproduction_manifest.csv` for the method mapping and limitations.

## One-command run

From the repository root:

```powershell
python -m research_comparison.run_all
```

Windows users can also double-click `run_all_comparisons.bat`.

The default run uses seeds `0,1,2,3,4`, trains AccessSentinel TCN-v3 on the same generated data for each seed, runs all six reference adaptations, and generates aggregate tables and figures. Select a device if needed:

```powershell
python -m research_comparison.run_all --device cuda
```

For a smaller engineering smoke test:

```powershell
python -m research_comparison.run_all --quick --skip-accesssentinel-training --seeds 0
```

Quick mode deliberately uses a smaller synthetic dataset and does not train the large TCN by default. It is **not reportable** as a research result. To include TCN in a quick run, add `--train-accesssentinel`.

## Methods and adaptations

| Reference | Core | Common-data adapter |
|---|---|---|
| [24] User-resource graph embedding | Regularized bipartite spectral embedding; nearest prior user on resource | User/tenant/application/resource interaction graph; training-period fit |
| [14] DynAD | Evolving GCN, GRU-adapted weights, local attention, graph pooling, edge scoring | Temporal user-resource interaction snapshots; train labels supervise training edges |
| [17] PG-AID | Cache snapshots, interaction meta-paths, TF-IDF path selection, Doc2Vec, Transformer prediction | User/resource paths approximate process/resource provenance; Doc2Vec is fit on training snapshots |
| [21] ACOBE | Long-term weighted deviations, individual/cohort compound matrices, aspect autoencoders, rank critic | Access-event aspects; role is a cohort proxy; vocabulary and cohorts are train-fitted |
| [22] FGDT | Candidate groups, HIN-RNN, temporal collaboration RNN, GCN, outlier filtering, group classifier | Resource co-access groups and event vectors replace review/reviewer data; group candidates are split by time |
| [25] DEEPRO | Heterogeneous meta-path attention (MAGNN), process-like classification, campaign-neighborhood recovery | User/resource interaction-provenance graph; deterministic cap on meta-path fan-out |

The [17] and [25] adapters are **interaction-provenance approximations**, not OS-level process/file/socket audit provenance. [21] uses role as a cohort proxy. [22] maps review-group detection to co-access groups. These are methodological adaptations and must be described as such in the paper.

## Evaluation protocol

- Every method is evaluated on the same AccessSentinel `build_v3` chronological windows for a given seed.
- Models fit only on the training period. Validation data is used for threshold calibration; test labels are never used for threshold selection.
- ROC-AUC and PR-AUC use continuous anomaly scores. Precision, recall, and F1 use a threshold calibrated from nominal validation scores.
- Per-method test scores are mapped to the exact evaluation-window event IDs before metrics are calculated. User-level methods broadcast a user score to that user's evaluation windows.
- The target is the individual anomaly label used by AccessSentinel Exp.A; collusion labels are not silently mixed into this task.
- No original-paper reported result is copied into the benchmark.

## Outputs

- `results/raw/`: per-method, per-seed JSON records including adaptation notes and timings.
- `results/tables/all_seed_metrics.csv`: per-seed metrics.
- `results/tables/mean_std_summary.csv`: mean and standard deviation across seeds.
- `results/figures/roc_auc_mean_std.png`: ROC-AUC comparison.
- `results/figures/pr_auc_mean_std.png`: PR-AUC comparison.
- `results/figures/runtime_mean_std.png`: adapter runtime comparison.
- `results/run_report.json`: run configuration, failures, and comparability limitations.

## Important cautions

- The supplied AccessSentinel dataset is synthetic. Results must be identified as synthetic-data results.
- A smoke run validates execution only; it is not research evidence.
- Runtime comparisons are meaningful only when device, hardware, thread count, and timing boundaries are reported. `fit_and_score_seconds` excludes shared dataset generation and shared `build_v3` window-index construction, but includes method-specific feature/graph construction, fitting, and scoring. It is not a hardware-independent algorithm complexity measure.
- A method that fails or cannot score a sample is not silently replaced with another model. Failures and unscored samples are recorded.
- Reference [24] is not itself a group-collusion or policy-enforcement method. Capability dimensions outside a paper's scope should be marked not applicable, not zero.

## Current measured evaluation suite (Figures 2–9)

**The v5 evaluation suite** is described in [`REVISION_V5_NOTES.md`](REVISION_V5_NOTES.md). The older [`EVALUATION_V4.md`](EVALUATION_V4.md) is retained as history.
It trains TCN-v3 and TGN-v3 on the common synthetic dataset, runs the six
reference adapters, measures local inference latency, performs a real paced
component load test, and replays all four escalation paths for the six ablations.

```powershell
python -m pip install -r requirements/evaluation.txt
python -m pytest -q research_comparison/tests/test_measured_experiments.py
python -m research_comparison.run_evaluation --quick --seeds 0 --tgn-epochs 2 --load-duration 0.6
python -m research_comparison.run_evaluation --figures 2,3,4,5,6,7,8,9 --seeds 0,1,2,3,4 --device cuda
```

Full and quick v5 results are isolated in `evaluation_v5/` and
`evaluation_v5_smoke/` respectively. Figures 10–12 are generated separately with
`python -m evaluation.figures b c f` from the independent experiment raw data.
See the revision notes for precise measurement boundaries and limitations.
