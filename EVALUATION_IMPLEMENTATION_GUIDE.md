# Complete measured comparison implementation — research suite v4

**Start here:** [`research_comparison/EVALUATION_V4.md`](research_comparison/EVALUATION_V4.md)

This archive contains the AccessSentinel source code and the complete executable
research-comparison evaluation implementation for Figures **2–7**. It does
**not** contain invented graph values, example scientific results, or a completed
five-seed publication benchmark. The pre-existing older evaluation files are
retained for provenance, but the v4 runner writes to new directories.

## Quick start (Windows PowerShell, from this AccessSentinel directory)

```powershell
python -m pip install -r requirements/evaluation.txt
python -m pytest -q research_comparison/tests/test_measured_experiments.py
python -m research_comparison.run_evaluation --figures 2,3,4,5,6,7 --quick --seeds 0 --tgn-epochs 2 --load-duration 0.6 --device cpu
```

Quick mode is **not paper evidence**. For the complete ~506k-event-per-seed
five-seed evaluation, after verifying GPU-capable PyTorch is installed:

```powershell
python -m research_comparison.run_evaluation --figures 2,3,4,5,6,7 --seeds 0,1,2,3,4 --device cuda --tgn-epochs 6 --load-duration 2
```

Use `--device cpu` if CUDA is unavailable. The larger run is computationally
expensive, particularly TCN-v3 training, baseline adapters, TGN replay, and
full-event scoring. `--reuse-cache` reuses complete trained weights/scores
without generating new predictions; `--skip-load` reuses existing measured
Fig. 6 JSON. See the detailed v4 document for correct command combinations.

## Implemented outputs

- **Fig. 2:** seven-method precision / recall / F1 grouped bar chart.
- **Fig. 3:** ROC-AUC / PR-AUC, reusing the very same predictions as Fig. 2.
- **Fig. 4:** time-to-detect by real labelled attack instance, and separately
  measured *local TCN* window-to-score-and-threshold decision latency.
- **Fig. 5:** native TGN collusion scores against reference event-score proxies,
  projected to identical test anchors and marked as not fully equivalent.
- **Fig. 6:** actual in-process wall-clock load test at 5, 20, 50 and 100
  offered requests/s, executing TCN, TGN and escalation callbacks; component
  P95 latency and attained throughput.
- **Fig. 7:** six real ablations, replaying the existing four-path Escalator
  with independent state and actual TCN/TGN scores.

The per-seed JSON files include dataset fingerprints, timing definitions,
limitations and measured values. Missing component measurements make Fig. 6/7
**fail closed** rather than produce illustrative or partial figures.

## Verification performed before packaging

`python -m compileall -q research_comparison accesssentinel/pdp_engine.py`
completed successfully; `python -m pytest -q
research_comparison/tests/test_measured_experiments.py` passed **6 tests**.
Those tests use tiny, engineering-only fixtures; randomly initialized models
within the isolated load-test unit fixture are **not** scientific models and
are **not** put in the results directory. One attempt at training through the
larger end-to-end quick suite exceeded the available local CPU time budget.
Therefore a finished full-run evaluation, performance numbers and figures
**have not been verified here**. They must be produced by the full command
above on suitable hardware, then inspected for scientific quality.

## Scientific boundaries

Fig. 4 measures local model-callback latency, not gateway-to-enforcement time.
Fig. 6 is an in-process component benchmark, not Kafka / Redis / HDFS
production throughput. Fig. 7 uses the Escalator's sliding-resource `on_event`
path, not the alternative finalized Kafka resource-window replay; their
out-of-order semantics differ. The six references are **common-data
adaptations**, not exact original-paper reproductions. An automatically
completed execution is a candidate for paper review, not a certification of
accuracy, statistical significance, or publication readiness.
