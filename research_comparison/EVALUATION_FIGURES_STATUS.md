# Evaluation Figures 2–7: implementation and verification plan

The evaluation suite is implemented in `run_evaluation.py`. The first pass is deliberately staged: it creates the full evaluation workflow and measurement schemas before the expensive five-seed benchmark is run.

## What is implemented now

| Figure | Code status | Current evidence boundary |
|---|---|---|
| Fig. 2 | Implemented | Precision/Recall/F1 grouped bars from common test predictions; AccessSentinel + six adapters. |
| Fig. 3 | Implemented | ROC-AUC/PR-AUC grouped bars from the same cached predictions as Fig. 2. No second model run is needed. |
| Fig. 4 | Partially implemented | Attack time-to-detect per attack instance is calculated from chronological test event scores. Event-trigger-to-decision latency in ms is not available from batch adapters and is explicitly marked NOT_MEASURED. |
| Fig. 5 | Exploratory proxy implemented | Collusion-labelled event/member metrics are calculated using each method's anomaly score. This is not native group-level output for every method. Ref. [24] is excluded from the plotted group-method comparison. |
| Fig. 6 | Measurement schema + plotting implemented | Creates workload/component CSV template for 5/20/50/100 events/s and plots measured throughput. Actual component-level load instrumentation is still required; blank values are not results. |
| Fig. 7 | Measurement schema + plotting implemented | Creates the six-configuration ablation CSV and plots measured F1. Controlled model/configuration ablation runs are still required; blank values are not results. |

## Dataset rule

Full runs use `SynthConfig.tcn_v3_eval` (~506k events). Within each seed, the exact same generated dataset object and chronological split are supplied to every compared method. The script verifies that the full dataset contains 500,000–510,000 events. Quick mode uses a smaller engineering dataset and is not paper-reportable.

## Staged workflow

1. Create the Fig. 6/7 templates without training models:

   ```powershell
   python -m research_comparison.run_evaluation --figures 6,7 --prepare-templates
   ```

2. Check Figures 2–5 with one quick seed (not paper results):

   ```powershell
   python -m research_comparison.run_evaluation --figures 2,3,4,5 --quick --seeds 0 --skip-accesssentinel-training
   ```

3. After the first expensive seed has been run, regenerate figures/tables without retraining:

   ```powershell
   python -m research_comparison.run_evaluation --figures 2,3,4,5 --seeds 0 --reuse-cache
   ```

4. Populate and validate the Fig. 6/7 templates from measured experiments, then rerun with `--figures 6,7` to create their charts.

5. Only after all six figure definitions and outputs are approved, run the final five-seed benchmark:

   ```powershell
   python -m research_comparison.run_evaluation --figures 2,3,4,5 --seeds 0,1,2,3,4 --device cuda
   ```

   Figures 2 and 3 are computed from one model execution per method/seed. ROC-AUC and PR-AUC are metric calculations on cached continuous scores, not separate training runs.

## Output

`research_comparison/results/evaluation_v3/`

- `raw/`: per-method/seed metadata and scalar metrics
- `cache/`: compressed validation/test scores and event IDs, for plotting without retraining
- `tables/`: per-seed and per-figure CSV results
- `figures/`: generated figures
- `templates/`: Fig. 6 and Fig. 7 measurement input CSVs
- `evaluation_report.json`: protocol, dataset fingerprints, failures, and limitations

## Important scientific limitations

- The six reference methods are common-data adaptations, not exact reproductions on their original paper datasets.
- Reference [17] and [25] use interaction-provenance approximations; [21] uses role cohorts; [22] adapts co-access groups.
- Fig. 4's two requested timing quantities are different. Only attack time-to-detect is available from the current batch outputs; true event-to-decision latency requires online instrumentation.
- Fig. 5's current metric is a member/event-level proxy and must not be described as a direct comparison of native group-level detectors.
- Fig. 6 and Fig. 7 require real measurements from controlled component load tests and ablation runs. The code intentionally does not fill these with estimated values.
