# AccessSentinel evaluation revision v5 — changes and evidence boundary

This patch contains **only changed source/test files** and three presentation-only re-plots of existing Experiment B/C/F measurements. It is NOT a complete project and does NOT contain new five-seed accuracy measurements. Extract the ZIP **at the AccessSentinel project root** while preserving relative directories.

## Why the earlier full run failed

- `SynthConfig.tcn_v3_eval()` inherited the original Phase-5 collusion emitter (`collusion_v2=False`). It did not record `bursts`, which the TGN collusion anchor sampler (`Corpus.evaluation_set()`) requires. The evaluator therefore saw zero or few positive anchors, often no positive test groups.
- The original 25%-per-role attacker allocation was used after planning all seven individual attack types. It often ran out of eligible users before constructing all validation/test collusion groups. This yielded missing groups and an undefined single-class Equation 19 fit.
- A JSON `NaN` serialization issue was separately fixed previously by mapping non-finite floats to `null`, not to zero.

## Changed evaluation protocol — requires a new full run

- The **common-data evaluation preset only** (`tcn_v3_eval`) now enables `collusion_v2`, benign team co-access hard negatives, and an explicitly higher **0.42 per-role allocation ceiling**. This ceiling is a synthetic-dataset design compromise needed with 400 users and the existing number of individual attack instances. It is a *maximum*, not the measured realized attacker fraction; disclose this setting and limitations in the paper. This revision does not change `collusion_study` or `collusion_v3_eval` (the separate Experiment B/E presets).
- The generator fails if a v2 collusion group cannot be formed at its requested size, rather than silently using an incomplete group.
- TGN now validates collusion group count, burst metadata, labeled collusion edges, and **positive and negative anchor classes in each split BEFORE training**.
- Equation 19 training member labels use the causal, independently labeled anchor sample, restricted to replay-visible feature-bearing members; diagnostics report true positive/negative member counts. No guessed positive labels are inserted.
- Native TGN group credit requires a detected anchor belonging to *that same group*. Member attribution includes missed group members as false negatives.
- The collusion and ablation figures (originally numbered 5/7, now 6/9) refuse datasets without both collusion classes; one-class metrics are `null` rather than misleading `0`. Score coverage and unscored positives are reported. Figure 6 correctly includes AccessSentinel's **native collective** row and all six reference event-score proxies; these proxies are **not native group detectors and must not be claimed equivalent**.
- Figure 9 refuses incomplete/non-finite evaluation matrices. Figure 5 shows measured `0 s` median detection delays explicitly rather than empty-looking bars.
- Figures 2–3 now require every requested seed for every method; stale incomplete PNGs are removed.
- Quick smoke uses 200 users instead of 120 so the revised two-class group generator remains feasible. Quick outputs are never paper evidence.
- The updated runner now writes fresh outputs into `research_comparison/results/evaluation_v5` (or `evaluation_v5_smoke`), **preserving the old v4 outputs**.

## Verified without claiming new full-model accuracy

Full-data seed preflight was executed across seeds 0, 1, 2, 3, 4 using the revised generator and the actual `Corpus.evaluation_set()` anchor sampler. Each had 5 training, 2 validation, and 4 test collusion groups, with both label classes in all anchor splits.

| Seed | Events | Test anchors positive | Test anchors negative |
|---|---:|---:|---:|
| 0 | 506,438 | 41 | 645 |
| 1 | 510,856 | 33 | 659 |
| 2 | 478,558 | 34 | 648 |
| 3 | 500,830 | 35 | 643 |
| 4 | 484,507 | 33 | 647 |

A one-epoch CPU **quick smoke test only** trained TGN, replayed graph memory, fitted Eq. 19 with 38 positive member instances, and evaluated 21 positive test anchors. This is a functional check, **not** a scientific performance result.

The Python revision tests passed (12 tests) and Experiment B/C/F graph regeneration succeeded from their previously recorded raw files. Experiment B's replot now shows denominators (20, 20, 20, 15 pooled groups); C explicitly labels stacked medians as non-end-to-end; F explicitly calls its result **admission**, not detection, with denominators (15 groups, 4 individual instances). These separate experiments use their **original** datasets/results and should not be merged into the new common-data v5 metrics.

## Windows PowerShell commands (one line each)

Activate the correct project environment, from the project root. For GPU checks use `.venv-gpu\Scripts\python.exe`; replace with `.venv\Scripts\python.exe` if CUDA is unavailable.

1. **First verify all full-data seeds WITHOUT GPU training:**

   `.venv-gpu\Scripts\python.exe -m research_comparison.run_evaluation --preflight --figures 2,3,4,5,6,7,8,9 --seeds 0,1,2,3,4 --device cuda`

2. **Then run a one-seed quick ENGINEERING test:**

   `.venv-gpu\Scripts\python.exe -m research_comparison.run_evaluation --figures 2,3,4,5,6,7,8,9 --quick --seeds 0 --tgn-epochs 2 --load-duration 0.6 --device cuda`

3. **Only if the quick run completes, run the five-seed FULL synthetic evaluation:**

   `.venv-gpu\Scripts\python.exe -m research_comparison.run_evaluation --figures 2,3,4,5,6,7,8,9 --seeds 0,1,2,3,4 --tgn-epochs 6 --load-duration 60 --device cuda`

4. **Replot the independent Experiment B/C/F outputs (without retraining):**

   `.venv-gpu\Scripts\python.exe -m evaluation.figures b c e`

## What remains unverified

- A new five-seed full training/evaluation on v5 has **not** been performed by this patch. Old v4 results (including the previously shared precision, PR-AUC and detection coverage) cannot be treated as v5 results because the dataset fingerprints changed.
- The single-epoch quick smoke proves fitting and scoring work for one reduced-data seed, not that all full-data seeds achieve good F1/PR-AUC.
- Figures 7 and 8 are still **in-process load**, not a Docker/Kafka/Redis/HDFS end-to-end deployment benchmark. Experiment C is a distinct live-stack experiment.
- The reference methods are common-data adaptations, and the Figure 6 proxies do not supply faithful native group-detection outputs.
- `paper_ready` remains `false` until an independent methodological review; a completed command is not evidence of published-quality validity.
- If the exact experiment protocol requires the original 25% cap, a different population/attack-allocation design must be evaluated and disclosed rather than treating the revised 42% ceiling as identical.

A copy of this environment’s five-seed dataset validation results is included as `research_comparison/REVISION_V5_PREFLIGHT.json`. This is a preflight audit, **not** model accuracy evidence.

## Figure layout update — numbered Figures 2–12 (presentation only)

This additional figure-layout change uses the latest `AccessSentinel(4).zip` files.
It **does not retrain models or alter their scores**. A new full run has not been
performed merely by changing figure-generation code.

| Figure | Plot | Output file |
|---|---|---|
| 2 | Precision, Recall, F1 (7 methods) | `fig2_precision_recall_f1.png` |
| 3 | ROC-AUC, PR-AUC (7 methods) | `fig3_roc_auc_pr_auc.png` |
| 4 | Attack Detection Coverage | `fig4_detection_coverage.png` |
| 5 | Attack Detection Delay | `fig5_detection_delay.png` |
| 6 | Collusion Detection Performance (native + six proxies) | `fig6_collusion_detection.png` |
| 7 | Throughput | `fig7_throughput.png` |
| 8 | Component Latency | `fig8_component_latency.png` |
| 9 | Ablation Study | `fig9_ablation.png` |
| 10 | Group Detection vs. Group Size (independent Experiment B) | `fig10_group_detection_by_size.png` |
| 11 | Individual-Path Stage Latency (independent Experiment C) | `fig11_individual_path_latency.png` |
| 12 | Escalation-Path Ablation (independent Experiment F) | `fig12_escalation_path_ablation.png` |

Figures 2–9 go to `research_comparison/results/evaluation_v5/figures/`
(or `evaluation_v5_smoke/figures/` with `--quick`); Figures 10–12 go to
`evaluation/results/figures/` and are generated separately by `python -m evaluation.figures b c f`.

The six reference X-axis labels now include citation brackets:
DynAD [14], PG-AID [17], ACOBE [21], FGDT [22],
User-resource embedding [24], DEEPRO [25]. Their **internal model IDs remain unchanged**.

The older JSON raw inputs `fig5_collusion_seed*.json`,
`fig6_scalability_seed*.json`, and `fig7_ablation_seed*.json` are intentionally
retained to allow existing measured v5 cache records to be read without
renaming or editing measurements. The displayed figure numbers and PNG
filenames are updated. The independent Experiment B/C/F raw records are also
unchanged; only their graph renders were renamed.

To run all comparative plots after preflight and quick test, select
`--figures 2,3,4,5,6,7,8,9`; Figures 10–12 are NOT generated by that command.

The TCN-only ablation created by Experiment A is an **additional supplementary
plot**, not Figure 12. Its title is adjusted to avoid creating two Figure 12s.
