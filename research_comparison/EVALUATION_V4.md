# AccessSentinel: measured evaluation suite v4 (Figures 2–7)

**Scope:** synthetic-data, same-seed, common-data **adaptations** of selected reference papers. Figures are generated **only** from the actual output of executable model, online-callback, load-test, and escalation experiments. Quick-mode results are engineering smoke tests, **not paper results**. The original archived `evaluation_v2` and `evaluation_v3` results are **not** reused by the v4 runner.

## Install and run (Windows PowerShell, repository root)

Use a supported 64-bit Python with PyTorch (GPU installation depends on your CUDA version). Install the evaluation requirements:

```powershell
python -m pip install -r requirements/evaluation.txt
```

Verify the local tests before long training:

```powershell
python -m pytest -q research_comparison/tests/test_measured_experiments.py
python -m compileall -q research_comparison
```

First engineering pass (small, **nonreportable** dataset, trained models, all six figures):

```powershell
python -m research_comparison.run_evaluation --figures 2,3,4,5,6,7 --quick --seeds 0 --tgn-epochs 2 --load-duration 0.6 --device cpu
```

Full five-seed experiment (compute-heavy; needs significant RAM/GPU resources):

```powershell
python -m research_comparison.run_evaluation --figures 2,3,4,5,6,7 --seeds 0,1,2,3,4 --tgn-epochs 6 --load-duration 2 --device cuda
```

The same command runs all requested experiments once per seed. The two AUC values and precision/recall/F1 reuse the same cached predictions. `--tgn-epochs 6` is an **explicit reduced training budget** (the default model config elsewhere allows up to 50 epochs); select/report the intended training budget consistently. If the computer has no CUDA-capable PyTorch installation, specify `--device cpu`, but allow substantially greater execution time.

Replot already computed Figures 2–5/7 (no refitting), using exactly the same `--tgn-epochs` and seed selection:

```powershell
python -m research_comparison.run_evaluation --quick --figures 2,3,4,5,7 --seeds 0 --tgn-epochs 2 --reuse-cache
```

Figure 6 measures **real** service times of a new load test on each run. To reuse existing Fig. 6 JSON and replot without generating new timed samples:

```powershell
python -m research_comparison.run_evaluation --quick --figures 6 --seeds 0 --tgn-epochs 2 --reuse-cache --skip-load
```

The output is `research_comparison/results/evaluation_v4/` for the full run or `research_comparison/results/evaluation_v4_smoke/` for quick runs. Useful contents:

- `raw/` — per-seed per-model JSON; Fig. 4 local timing; native Fig. 5 anchored group results; Fig. 6 actual paced workload measurements; and Fig. 7 ablation results **with path-by-path trigger/admission metadata**.
- `cache/` — per-method prediction NPZs, the TCN/TGN trained state dicts, TGN native test-anchor records, and all-event TCN predictions for escalation.
- `tables/` — per-method mean/std and evaluated attack/collusion/latency tables.
- `figures/` — only figures generated from existing valid measurements. A missing/partial Fig. 6 or Fig. 7 measurement matrix is an **error**, not an invitation to draw an illustrative chart.
- `evaluation_report.json` — dataset identity, conditions, measured coverage, missing components, and limitations. Its `candidate_protocol_complete` flag records a complete five-seed execution; `paper_ready` remains false until independent methodological review.

## Exact figure meaning

| Figure | Real measurement | Comparability notes |
|---|---|---|
| 2 | Precision, recall and F1 on common chronologically held-out individual-anomaly windows, nominal-validation threshold | TCN-v3 plus six paper-core common-data adaptations; some references cannot score cold-start users. |
| 3 | ROC-AUC and PR-AUC on **exactly the same score arrays** as Fig. 2 | No retraining or second prediction pass needed. |
| 4 | Individual-attack start to first flagged test event, **including attack instances never detected** in table; local TCN window-to-decision p50/p95 time | Reference methods have no online inference callback, so their event-to-decision latencies are **NOT MEASURED**. Local TCN latency excludes gateway, Kafka, network, consumer queues, PDP and enforcement; must not be called full pipeline latency. |
| 5 | **Native AccessSentinel TGN-v3 collusion score** from real TGN training, memory replay and test anchors; reference predictions projected to most recent preceding same-user test event at those **identical anchors** | Reference methods are *individual event-anomaly proxies* on the shared anchor labels, not original-paper-native group detectors. Ref. 24 (not a group detector) is excluded from the main comparison figure. `raw` includes proxy coverage. |
| 6 | A real wall-clock paced producer, a worker with **actual TCN and TGN inference**, plus the actual escalation `on_event`, background/sweep callbacks, at offered 5,20,50,100 requests/s | In-process microbenchmark. TGN is cold-started for each rate and called every request as a component-capacity stress test; HDFS/Redis/Kafka remote latency is not included. The chart compares **attained total processed event throughput** with offered load and plots P95 local service times for the five components. Background operation rates are not equated to event rates. |
| 7 | Six configurations using the **same trained TCN/TGN** and true TGN-native anchor scores; individual TCN alerts are always on. For path removal, the *real `Escalator`* (including its supported sliding-window `on_event` resource path, rather than the separate Kafka finalized-resource-window view) is re-run with independent local state and disabled paths before gating group alerts | Units: common collusion-labelled test anchors. `TCN+TGN` is the **ungated, full-graph-compute upper bound**. Other TGN variants only classify a group when the seed had an admitted escalation in the previous TGN window. Replay preserves cooldown and budget admission. |

## Synthetic dataset / statistical protocol

- `SynthConfig.tcn_v3_eval(seed=23+seed)` produces about **506,000** events for each full-run seed. Event count varies by seed (measured: seed 0 506,161; 1 505,010; 2 476,904; 3 497,405; 4 480,288), so the full runner only refuses counts outside **450,000–560,000** (a guard against a quick or mis-configured dataset); the actual count per seed is recorded. The same seeded `ds` event generator is used for the TCN, TGN, and six reference methods. The event-ID fingerprint is recorded; different seeds are expected to have **different** fingerprints and should be aggregated as replicates, not rejected as inconsistent datasets.
- TCN windows are built with `build_v3`: chronological train/validation/test, per-user gap, train-only encoder fitting. Every reference's test event IDs are aligned to that evaluation index. Per-method validation thresholds are derived from nominal validation examples, not test labels.
- The native TGN calibrates its collective classifier on **train** anchors, and theta_C/theta_A on nominal **validation** anchors; its own test anchor labels are only for final evaluation. It is trained on the same shared synthetic data. Cached model weights are saved separately for every seed, and caches validate dataset identity.
- Real collusion scores may differ from individual anomaly scores, hence Fig. 5 explicitly identifies the proxy comparison rather than claiming an equivalent original-paper collusion reproduction.
- A full research report should include mean and standard deviation across **five seeds**, actual hardware, model budgets, evaluation coverage, and synthetic-data limitations. These are **candidate results** requiring scientific review, not claims of generalization to real access-control incidents.

## Known operational boundaries

The Figure 7 offline Path B replay calls `Escalator.on_event` over a sliding window; the deployed v3 streaming path can instead call `on_resource_window` after shard aggregation and finalization. These are **not timing- or semantics-equivalent** under late/out-of-order events. Figure 7 is an algorithmic policy ablation, not a deployment-fidelity claim.

The local Redis subset (`memory_redis.py`) is intentionally single-process, does not implement Redis TTL eviction or transactions, and is only used to run the **unchanged application Escalator** deterministically in offline experiments. Deployed distributed availability, queues, HDFS, Redis server performance, network and Kafka processing are **not** measured by this suite. Full system-to-enforcement latency requires infrastructure-connected tracing and is not invented from batch runtime.

The TGN `evaluate_v3` anchor sampling and train/validation calibration are inherited from the current AccessSentinel implementation. If the generated quick dataset does not contain enough labelled training group members for calibration, the TGN raises an explicit exception instead of silently creating surrogate scores. Tune data generation/evaluation scientifically rather than substituting fake values.

If a model fails, inspect `evaluation_report.json` and the error logs; never copy partial smoke-mode figures into the research paper. No original article's published numbers were used as measured observations in this package.
