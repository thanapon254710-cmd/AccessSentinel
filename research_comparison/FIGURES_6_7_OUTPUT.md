# Figure 6 and Figure 7 output behavior

The evaluation runner no longer creates blank CSV measurement templates.

- Figure 6 output: `results/evaluation_v3/figures/fig6_scalability.png`
- Figure 7 output: `results/evaluation_v3/figures/fig7_ablation.png`
- Raw experiment records are JSON under `results/evaluation_v3/raw/`.

The plotter reads `fig6_scalability_seed<seed>.json` and
`fig7_ablation_seed<seed>.json`. The expected schema is documented in
`results/evaluation_v3/raw/FIG6_7_JSON_FORMAT.md` after the runner starts.

If no measured JSON exists, the runner writes a status PNG saying that the
experiment data is missing. That image is deliberately labelled as **not an
experimental result**. It does not invent scores or performance measurements.

**Important implementation status:** this update changes the output workflow
from blank CSV templates to PNG figures backed by raw JSON. It does not yet
implement the actual Fig. 6 workload replay or the six Fig. 7 component
ablation executions. Those experiment runners must be connected to the real
AccessSentinel component APIs before the PNGs can contain research results.
