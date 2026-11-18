# Evaluation suite v3 — Figure 6/7 output patch

Run from the AccessSentinel project root:

```powershell
python -m research_comparison.run_evaluation --figures 6,7 --seeds 0
```

The command writes `fig6_scalability.png` and `fig7_ablation.png` under
`research_comparison/results/evaluation_v3/figures/`. It no longer creates
empty CSV templates. If actual measurement JSON files are not present, it
writes explicitly-labelled status images instead of fake charts.

See `FIGURES_6_7_OUTPUT.md` for the distinction between graph generation and
actual experiment execution.
