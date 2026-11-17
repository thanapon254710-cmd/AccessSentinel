# Selected related-paper implementation status

This status separates algorithm-core construction, AccessSentinel schema adaptation, and research-grade evaluation.

| Ref. | Method | Core | Common-data adapter | Smoke verification | Main limitation |
|---|---|---|---|---|---|
| [24] | User-resource graph embedding | Implemented | Implemented | Passed | Fixed embedding dimension/cosine distance are documented choices; cold-start events may be unscored |
| [14] | DynAD | Implemented | Implemented | Passed | User-resource temporal snapshots replace the paper's original dynamic-network datasets |
| [17] | PG-AID | Implemented | Implemented | Passed | Interaction-provenance approximation; Doc2Vec/Transformer adaptation |
| [21] | ACOBE | Implemented | Implemented | Passed | Role is a cohort proxy; event fields form behavior aspects |
| [22] | FGDT | Implemented | Implemented | Passed | Co-access groups and event vectors replace review/reviewer inputs |
| [25] | DEEPRO | Implemented | Implemented | Passed | Interaction graph is not OS-level provenance; bounded meta-path fan-out |

## Shared benchmark implementation

- `selected_adapters.py` connects all six selected cores to the AccessSentinel synthetic event generator.
- `run_all.py` evaluates the six references and, for full runs, AccessSentinel TCN-v3 on the same dataset seed and same `build_v3` evaluation windows.
- All scores are aligned by `idx.event_ids` before ROC-AUC and PR-AUC are calculated.
- Validation thresholds use nominal validation windows only. Test labels are used only for final metrics.
- The evaluation target is the AccessSentinel individual anomaly label. Collusion is not mixed into the individual-detection task.
- Reference [17] and [25] use interaction-provenance approximations. Reference [21] and [22] use explicitly documented task mappings.
- `ref25_deepro.py` bounds per-node meta-path instances to avoid unbounded fan-out on dense shared-resource graphs; this cap is an implementation adaptation and must be reported.

## Run commands

```powershell
python -m pytest -q tests/test_reference_cores.py
python -m compileall -q research_comparison
python -m research_comparison.run_all
```

Quick engineering smoke check (not research-reportable):

```powershell
python -m research_comparison.run_all --quick --skip-accesssentinel-training --seeds 0
```

Full run trains AccessSentinel TCN-v3 on the same generated dataset. If running without an available GPU, expect the TCN training stage to take substantially longer.

## Verification boundary

The six method adapters completed a one-seed quick smoke run, and the core unit tests passed. This does **not** establish convergence, paper-faithful reproduction, or final comparative performance. The full five-seed research run has not yet been completed. The AccessSentinel TCN-v3 adapter is implemented but its full training path has not been successfully smoke-tested in this CPU-limited environment; use a CUDA device for the full run where available. No quick-run metric should be inserted into the paper as a final result.
