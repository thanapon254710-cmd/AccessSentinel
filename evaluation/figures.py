"""Figures and tables for Deliverable 4, generated ONLY from evaluation/results/raw/ (nothing typed by hand).

    .venv/Scripts/python -m evaluation.figures [a b c e f g h]

Output: evaluation/results/figures/*.png (300 dpi) + *.pdf, evaluation/results/tables/*.md and summary.json.
Colour: the validated reference palette (dataviz skill), light mode for print; categorical slots in fixed order,
never cycled; every figure has a table with the same numbers (contrast relief for the three light slots).
An experiment whose raw files are missing is skipped and listed as Not Run in summary.json.
"""
import glob
import json
import os
import sys

import numpy as np

RAW = os.environ.get("AS_EVAL_RAW", "evaluation/results/raw")
FIG = os.environ.get("AS_EVAL_FIG", "evaluation/results/figures")
TAB = os.environ.get("AS_EVAL_TAB", "evaluation/results/tables")
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
SURFACE, TEXT, TEXT2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e6e5e1"
BLUES = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
MODEL_NAMES = {"tcn-v3": "TCN v3", "tcn-sup": "TCN (supervised only)", "lstm": "LSTM", "iforest": "Isolation Forest",
               "rule": "Rule (D3)", "tcn-d3": "TCN (D3)", "tcn-noz": "TCN v3 − z_u", "tcn-noaux": "TCN v3 − aux heads"}
MAIN_A = ["tcn-v3", "tcn-sup", "lstm", "tcn-d3", "iforest", "rule"]
COLOR_OF = {m: SERIES[i] for i, m in enumerate(MAIN_A)}                # colour follows the model, everywhere


def _plt():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({"figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
                         "axes.edgecolor": GRID, "axes.labelcolor": TEXT2, "xtick.color": TEXT2, "ytick.color": TEXT2,
                         "text.color": TEXT, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
                         "axes.axisbelow": True, "axes.spines.top": False, "axes.spines.right": False,
                         "font.size": 9, "axes.titlesize": 10, "axes.titleweight": "bold", "legend.frameon": False,
                         "lines.linewidth": 2.0, "font.family": "DejaVu Sans"})
    return plt


def _save(fig, name):
    os.makedirs(FIG, exist_ok=True)
    legacy = {
        "fig10_group_detection_by_size": "exp_b_fig1_by_size",
        "fig11_individual_path_latency": "exp_c_fig_stages",
        "fig12_escalation_path_ablation": "exp_f_fig_paths",
    }
    for ext in ("png", "pdf"):
        fig.savefig(os.path.join(FIG, f"{name}.{ext}"), dpi=300, bbox_inches="tight")
        if name in legacy:
            old = os.path.join(FIG, f"{legacy[name]}.{ext}")
            if os.path.exists(old):
                os.unlink(old)  # remove superseded render only, never raw measurement files
    import matplotlib.pyplot as plt
    plt.close(fig)
    return f"{FIG}/{name}.png"


def _table(name, header, rows, note=""):
    os.makedirs(TAB, exist_ok=True)
    L = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    L += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    txt = ("\n".join(L) + "\n" + (f"\n{note}\n" if note else ""))
    open(os.path.join(TAB, f"{name}.md"), "w", encoding="utf-8").write(txt)
    return f"{TAB}/{name}.md"


def _ms(vals, nd=3):
    v = [x for x in vals if x is not None and x == x]
    if not v:
        return "n/a"
    return f"{np.mean(v):.{nd}f} ± {np.std(v):.{nd}f}" if len(v) > 1 else f"{v[0]:.{nd}f}"


def _ci(vals, n_boot=2000, seed=0):
    v = np.array([x for x in vals if x is not None and x == x], float)
    if len(v) < 2:
        return None
    rng = np.random.default_rng(seed)
    b = rng.choice(v, (n_boot, len(v))).mean(1)
    return [float(np.percentile(b, 2.5)), float(np.percentile(b, 97.5))]


def _legend_top(ax, ncol, fontsize=8):
    """Legend in its own band above the plot area, so it never covers a bar or a value label."""
    ax.legend(ncol=ncol, loc="lower left", bbox_to_anchor=(0, 1.0), fontsize=fontsize, frameon=False,
              borderaxespad=0.2, handlelength=1.2)


def _bars(ax, groups, series, values, errs=None, colors=None, labels=True, fmt="{:.2f}"):
    """Grouped bars: groups on x, one bar per series; 2 px surface gap between bars."""
    n = len(series)
    w = 0.8 / n
    x = np.arange(len(groups))
    for j, s in enumerate(series):
        v = [values[g][s] for g in groups]
        e = [errs[g][s] for g in groups] if errs else None
        ax.bar(x + (j - (n - 1) / 2) * w, v, w, yerr=e, color=(colors or SERIES)[j], edgecolor=SURFACE, linewidth=1.5,
               label=s, error_kw={"ecolor": TEXT2, "elinewidth": 0.8, "capsize": 2})
        if labels:                                          # label above the error-bar cap, never on it
            for xi, vi, ei in zip(x + (j - (n - 1) / 2) * w, v, e or [0] * len(v)):
                if vi == vi:
                    ax.text(xi, vi + (ei if ei == ei else 0) + 0.012, fmt.format(vi), ha="center", va="bottom",
                            fontsize=6, color=TEXT2)
    ax.set_xticks(x)
    import textwrap
    ax.set_xticklabels([chr(10).join(textwrap.wrap(str(g), 14)) for g in groups])
    ax.grid(axis="x", visible=False)                       # recessive grid: value lines only


# ============================================================================================ Experiment A
def exp_a(summary):
    files = sorted(f for f in glob.glob(f"{RAW}/exp_a/seed*.json") if "quick" not in os.path.basename(f))
    if not files:
        summary["A"] = "Not Run"; return
    runs = [json.load(open(f, encoding="utf-8")) for f in files]
    models = [m for m in MAIN_A + ["tcn-noz", "tcn-noaux"] if all(m in r["models"] for r in runs)]
    g = lambda r, m, *ks: _get(r["models"][m], ks)
    rows, agg = [], {}
    for m in models:
        vals = {k: [g(r, m, *path) for r in runs] for k, path in {
            "roc_auc": ("event_level", "roc_auc"), "pr_auc": ("event_level", "pr_auc"),
            "p_high": ("event_level", "at_theta_high", "precision"), "r_high": ("event_level", "at_theta_high", "recall"),
            "f1_high": ("event_level", "at_theta_high", "f1"), "fpr_high": ("event_level", "at_theta_high", "fpr"),
            "p_B": ("event_level", "at_theta_B", "precision"), "r_B": ("event_level", "at_theta_B", "recall"),
            "fpr_B": ("event_level", "at_theta_B", "fpr"),
            "days_flagged": ("normal_user_days_flagged_at_theta_high", "rate")}.items()}
        inst = [(g(r, m, "attack_instances_at_theta_high", "detected"), g(r, m, "attack_instances_at_theta_high", "instances")) for r in runs]
        ttd = [g(r, m, "attack_instances_at_theta_high", "median_minutes_to_detect") for r in runs]
        cpu = [g(r, m, "latency", "cpu", "batch_1_ms_median") for r in runs]
        gpu = [g(r, m, "latency", "cuda", "batch_1_ms_median") for r in runs]
        agg[m] = {k: {"mean": float(np.mean([x for x in v if x is not None])) if any(x is not None for x in v) else None,
                      "std": float(np.std([x for x in v if x is not None])) if any(x is not None for x in v) else None,
                      "ci95": _ci(v), "values": v} for k, v in vals.items()}
        agg[m]["instances"] = inst; agg[m]["cpu_ms"] = cpu; agg[m]["cuda_ms"] = gpu
        rows.append([MODEL_NAMES[m], _ms(vals["roc_auc"]), _ms(vals["pr_auc"]), _ms(vals["p_high"]), _ms(vals["r_high"]),
                     _ms(vals["f1_high"]), _ms(vals["fpr_high"], 4), _ms(vals["p_B"]), _ms(vals["r_B"]), _ms(vals["fpr_B"], 4),
                     f"{sum(a for a, _ in inst)}/{sum(b for _, b in inst)}", _ms(ttd, 1), _ms(vals["days_flagged"], 3),
                     _ms(cpu, 2), _ms(gpu, 2)])
    note = (f"Synthetic data (SynthConfig.tcn_v3_eval, data seeds {[r['data']['synth_config']['seed'] for r in runs]}), "
            f"{len(runs)} seeds, mean ± std. Each model calibrated by Eq. 8 on its own nominal-validation scores "
            "(theta_high = 99th, theta_B = 99.9th percentile). Same evaluable test windows for every model.")
    _table("exp_a_detection", ["Model", "ROC-AUC", "PR-AUC", "P@θhigh", "R@θhigh", "F1@θhigh", "FPR@θhigh", "P@θB", "R@θB",
                               "FPR@θB", "instances detected", "median min to detect", "normal user-days flagged",
                               "CPU ms (batch 1)", "CUDA ms (batch 1)"], rows, note)
    plt = _plt()
    main = [m for m in MAIN_A if m in models]
    # Fig A1: P / R / F1 at theta_high
    fig, ax = plt.subplots(figsize=(7.2, 3.0))
    groups = [MODEL_NAMES[m] for m in main]
    vals = {MODEL_NAMES[m]: {"Precision": agg[m]["p_high"]["mean"], "Recall": agg[m]["r_high"]["mean"], "F1": agg[m]["f1_high"]["mean"]} for m in main}
    errs = {MODEL_NAMES[m]: {"Precision": agg[m]["p_high"]["std"], "Recall": agg[m]["r_high"]["std"], "F1": agg[m]["f1_high"]["std"]} for m in main}
    _bars(ax, groups, ["Precision", "Recall", "F1"], vals, errs)
    ax.set_ylim(0, 1.08); ax.set_ylabel("at calibrated θ_high")
    ax.set_title(f"Experiment A — individual detection at θ_high (synthetic, {_n(len(runs), 'seed')}, mean ± std)", loc="left", pad=22)
    _legend_top(ax, 3)
    plt.setp(ax.get_xticklabels(), fontsize=7.5)
    summary.setdefault("figures", []).append(_save(fig, "exp_a_fig1_prf"))
    # Fig A2: ROC and PR curves (seed 0 scores; threshold-free)
    from sklearn.metrics import precision_recall_curve, roc_curve
    npz = np.load(files[0].replace(".json", "-scores.npz"))
    y = npz["y"]
    # six curves in one panel fail the all-pairs colour check (dataviz validator), so facet: each column compares
    # TCN v3 with at most two other models; every model keeps its colour in every panel
    facets = [[m for m in ("tcn-v3", "tcn-sup", "lstm") if m in main], [m for m in ("tcn-v3", "tcn-d3") if m in main],
              [m for m in ("tcn-v3", "iforest", "rule") if m in main]]
    facets = [f for f in facets if len(f) > 1] or [main]
    fig, axs = plt.subplots(2, len(facets), figsize=(7.2, 4.8), sharex="row", sharey="row", squeeze=False)
    curves = {}
    for m in main:
        sc = npz[f"s_{m}"]
        f_, t_, _ = roc_curve(y, sc); p_, r_, _ = precision_recall_curve(y, sc)
        curves[m] = (f_, t_, r_, p_)
    for j, ms in enumerate(facets):
        a1, a2 = axs[0, j], axs[1, j]
        for m in ms:
            f_, t_, r_, p_ = curves[m]
            a1.plot(f_, t_, color=COLOR_OF[m], label=MODEL_NAMES[m], lw=1.6)
            a2.plot(r_, p_, color=COLOR_OF[m], label=MODEL_NAMES[m], lw=1.6)
        a1.plot([0, 1], [0, 1], color=GRID, lw=1, ls="--")
        a1.legend(fontsize=6.5, loc="lower right", frameon=False)
        a1.set_xlabel("false positive rate", fontsize=7.5); a2.set_xlabel("recall", fontsize=7.5)
    axs[0, 0].set_ylabel("true positive rate"); axs[1, 0].set_ylabel("precision")
    axs[0, 0].set_title(f"ROC (top) and precision–recall (bottom), seed 0", loc="left")
    fig.subplots_adjust(hspace=0.42, wspace=0.12)
    summary["figures"].append(_save(fig, "exp_a_fig2_roc_pr"))
    # Fig A3: confusion matrices at theta_high, summed over seeds (TCN v3 and the best other model by mean PR-AUC)
    best = max((m for m in main if m != "tcn-v3"), key=lambda m: agg[m]["pr_auc"]["mean"] or 0)
    fig, axs = plt.subplots(1, 2, figsize=(6.0, 2.7))
    for ax_, m in zip(axs, ["tcn-v3", best]):
        c = {k: sum(g(r, m, "event_level", "at_theta_high", k) for r in runs) for k in ("tn", "fp", "fn", "tp")}
        M = np.array([[c["tn"], c["fp"]], [c["fn"], c["tp"]]], float)
        Mn = M / M.sum(1, keepdims=True)
        ax_.imshow(Mn, cmap=_cmap(), vmin=0, vmax=1)
        for i in range(2):
            for j in range(2):
                ax_.text(j, i, f"{int(M[i, j]):,}\n({Mn[i, j]:.1%})", ha="center", va="center", fontsize=7,
                         color="white" if Mn[i, j] > 0.55 else TEXT)
        ax_.set_xticks([0, 1]); ax_.set_xticklabels(["normal", "attack"]); ax_.set_yticks([0, 1]); ax_.set_yticklabels(["normal", "attack"])
        ax_.set_xlabel("predicted"); ax_.grid(False); ax_.set_title(MODEL_NAMES[m], loc="left", fontsize=9)
    axs[0].set_ylabel("actual")
    fig.subplots_adjust(wspace=0.3, top=0.80)
    fig.suptitle(f"Confusion at θ_high, test windows summed over {len(runs)} seeds (row-normalised)", x=0.02, y=0.99,
                 ha="left", fontsize=9)
    summary["figures"].append(_save(fig, "exp_a_fig3_confusion"))
    # Fig A4: recall per attack scenario at theta_high (models x scenarios), sequential blue
    scen = sorted({k for r in runs for k, v in r["models"]["tcn-v3"]["by_scenario_at_theta_high"].items() if "recall" in v})
    M = np.array([[np.mean([r["models"][m]["by_scenario_at_theta_high"].get(sc, {}).get("recall", np.nan) for r in runs]) for sc in scen] for m in main])
    fig, ax = plt.subplots(figsize=(7.2, 2.8))
    ax.imshow(M, cmap=_cmap(), vmin=0, vmax=1, aspect="auto")
    for i in range(M.shape[0]):
        for j in range(M.shape[1]):
            ax.text(j, i, f"{M[i, j]:.2f}", ha="center", va="center", fontsize=6.5, color="white" if M[i, j] > 0.55 else TEXT)
    ax.set_xticks(range(len(scen))); ax.set_xticklabels([s.replace("_", " ") for s in scen], rotation=30, ha="right", fontsize=7)
    ax.set_yticks(range(len(main))); ax.set_yticklabels([MODEL_NAMES[m] for m in main], fontsize=7); ax.grid(False)
    ax.set_title("Recall per attack scenario at θ_high (mean over seeds)", loc="left")
    summary["figures"].append(_save(fig, "exp_a_fig4_scenarios"))
    _table("exp_a_scenarios", ["Model"] + scen, [[MODEL_NAMES[m]] + [f"{v:.3f}" for v in M[i]] for i, m in enumerate(main)],
           "Recall of attack events per scenario at each model's calibrated theta_high; mean over seeds.")
    # benign hard negatives: flag rate per benign scenario
    ben = sorted({k for r in runs for k, v in r["models"]["tcn-v3"]["by_scenario_at_theta_high"].items() if "flag_rate" in v})
    _table("exp_a_benign", ["Model"] + ben, [[MODEL_NAMES[m]] + [f"{np.mean([r['models'][m]['by_scenario_at_theta_high'].get(b, {}).get('flag_rate', np.nan) for r in runs]):.4f}" for b in ben] for m in main],
           "Flag rate (false alarms) of benign / hard-negative events at theta_high; mean over seeds.")
    # Fig A5: inference latency (batch 1, median), CPU and CUDA, for the neural models
    neural = [m for m in main if agg[m]["cpu_ms"][0] is not None]
    fig, ax = plt.subplots(figsize=(6.0, 2.6))
    vals = {MODEL_NAMES[m]: {"CPU": np.mean([x for x in agg[m]["cpu_ms"] if x]), "CUDA": np.mean([x for x in agg[m]["cuda_ms"] if x]) if any(agg[m]["cuda_ms"]) else np.nan} for m in neural}
    _bars(ax, [MODEL_NAMES[m] for m in neural], ["CPU", "CUDA"], vals, fmt="{:.1f}")
    ax.set_ylabel("ms per window (batch 1, median)"); ax.legend(fontsize=8)
    ax.set_title("Inference latency of one scoring window", loc="left")
    summary["figures"].append(_save(fig, "exp_a_fig5_latency"))
    summary["A"] = {"seeds": len(runs), "models": {m: {k: v for k, v in a.items() if k in ("roc_auc", "pr_auc", "p_high", "r_high", "f1_high", "fpr_high", "fpr_B", "r_B", "p_B")} for m, a in agg.items()},
                    "data_fingerprints": [r["data"]["fingerprint"] for r in runs]}
    # Experiment F (part): TCN ablation
    abl = [m for m in ("tcn-v3", "tcn-noz", "tcn-noaux", "tcn-sup") if m in models]
    if len(abl) > 1:
        _table("exp_f_tcn_ablation", ["Variant", "ROC-AUC", "PR-AUC", "R@θhigh", "FPR@θhigh"],
               [[MODEL_NAMES[m], _ms(agg[m]["roc_auc"]["values"]), _ms(agg[m]["pr_auc"]["values"]), _ms(agg[m]["r_high"]["values"]),
                 _ms(agg[m]["fpr_high"]["values"], 4)] for m in abl],
               "Ablation of the v3 TCN additions on Experiment A data (same seeds, same calibration).")
        fig, ax = plt.subplots(figsize=(5.0, 2.6))
        x = np.arange(len(abl))
        ax.bar(x, [agg[m]["pr_auc"]["mean"] for m in abl], 0.6, yerr=[agg[m]["pr_auc"]["std"] for m in abl], color=SERIES[0],
               edgecolor=SURFACE, linewidth=1.5, error_kw={"ecolor": TEXT2, "elinewidth": 0.8, "capsize": 2})
        for xi, m in zip(x, abl):
            ax.text(xi, agg[m]["pr_auc"]["mean"] + agg[m]["pr_auc"]["std"] + 0.015, f"{agg[m]['pr_auc']['mean']:.3f}",
                    ha="center", va="bottom", fontsize=7, color=TEXT2)
        import textwrap
        ax.set_xticks(x); ax.set_xticklabels([chr(10).join(textwrap.wrap(MODEL_NAMES[m], 14)) for m in abl], fontsize=7)
        ax.set_ylim(0, 1.05); ax.grid(axis="x", visible=False)
        ax.set_ylabel("test PR-AUC"); ax.set_title("Supplementary — TCN ablation", loc="left")
        summary["figures"].append(_save(fig, "exp_f_fig_tcn_ablation"))


def _n(k, word):
    return f"{k} {word}{'s' if k != 1 else ''}"


def _cmap():
    from matplotlib.colors import LinearSegmentedColormap
    return LinearSegmentedColormap.from_list("blues", [SURFACE] + BLUES)


def _get(d, ks):
    for k in ks:
        if not isinstance(d, dict) or k not in d:
            return None
        d = d[k]
    return d


# ============================================================================================ Experiment B
def exp_b(summary):
    files = sorted(f for f in glob.glob(f"{RAW}/exp_b/seed*.json") if not f.endswith(("-anchors.json", "-tcnonly.json")))
    if not files:
        summary["B"] = "Not Run"; return
    runs = [json.load(open(f, encoding="utf-8")) for f in files]
    models = ["tgn-v3", "rule-shared", "rule-uncommon"]
    rows = []
    for m in models:
        sl = [r["models"][m]["seed_level"] for r in runs]
        rows.append([m, _ms([x["roc_auc"] for x in sl]), _ms([x["pr_auc"] for x in sl]), _ms([x["precision"] for x in sl]),
                     _ms([x["recall"] for x in sl]), _ms([x["fpr"] for x in sl], 4)])
    _table("exp_b_seed_level", ["Model", "seed ROC-AUC", "seed PR-AUC", "precision", "recall", "FPR"], rows,
           f"Escalation-like test anchors: is the escalated seed in a colluding group? {len(runs)} seeds, mean ± std. "
           "tgn-v3 at theta_C (nominal-validation quantile); rules at 0.5.")
    # per group: detected by TGN / TCN-only (own events, any in span, matched chance rate) / either, by size and kind
    post = {}
    for f in files:
        pf = f.replace(".json", "-tcnonly.json")
        if os.path.exists(pf):
            post[f] = json.load(open(pf, encoding="utf-8"))["per_group"]
    by_size, by_kind = {}, {}
    for f, r in zip(files, runs):
        for i, v in r["models"]["tgn-v3"]["per_group"].items():
            c = r["combined_per_group"][i]
            ph = post.get(f, {}).get(i)
            own = bool(ph["on_group_events"]) if ph else None
            for key, d in ((v["size"], by_size), (v["kind"], by_kind)):
                e = d.setdefault(key, {"groups": 0, "tgn": 0, "tcn_own": 0, "tcn_span": 0, "null_span": [], "either_own": 0,
                                       "rule-shared": 0, "ttd": [], "posthoc": 0})
                e["groups"] += 1; e["tgn"] += int(c["tgn"]); e["tcn_span"] += int(bool(c["tcn_only"]))
                e["rule-shared"] += int(r["models"]["rule-shared"]["per_group"][i]["detected"])
                if ph:
                    e["posthoc"] += 1; e["tcn_own"] += int(own); e["null_span"].append(ph["null_any_in_span"])
                    e["either_own"] += int(c["tgn"] or own)
                if v["minutes_to_detect"] is not None:
                    e["ttd"].append(v["minutes_to_detect"])
    fr = lambda e, k, n="groups": f"{e[k]}/{e[n]} ({e[k] / e[n]:.0%})" if e[n] else "n/a"
    row = lambda k, e: [k, e["groups"], fr(e, "tgn"), fr(e, "tcn_own", "posthoc"), fr(e, "either_own", "posthoc"),
                        fr(e, "tcn_span"), f"{np.mean(e['null_span']):.0%}" if e["null_span"] else "n/a", fr(e, "rule-shared"),
                        f"{np.median(e['ttd']) / 60:.1f}" if e["ttd"] else "n/a"]
    hdr = ["groups", "TGN v3", "TCN-only on the group's own events", "TGN or TCN (own events)",
           "TCN any event in span", "chance rate of 'any in span' (matched random users)", "rule (shared)",
           "TGN median hours from first group access to detection"]
    note = ("Test groups pooled over seeds. TCN-only uses the promoted v3 TCN at its calibrated theta_high. 'Any event in span' "
            "fires by chance on ordinary activity over a multi-day span; its chance rate (random non-attacker users, same "
            "size and span) shows it is not evidence of detection, so the comparison uses the group's own events.")
    _table("exp_b_groups_by_size", ["size"] + hdr, [row(k, e) for k, e in sorted(by_size.items())], note)
    _table("exp_b_groups_by_kind", ["kind"] + hdr, [row(k, e) for k, e in sorted(by_kind.items())], note)
    plt = _plt()
    fig, ax = plt.subplots(figsize=(6.0, 2.8))
    sizes = sorted(by_size)
    # The old fallback silently plotted multi-day TCN *span* alarms as if
    # they were the group's own attack events, and duplicated the TCN bar as
    # the union. Refuse an incomplete posthoc series rather than mislabel it.
    missing_posthoc = {k: (by_size[k]['posthoc'], by_size[k]['groups'])
                       for k in sizes if by_size[k]['posthoc'] != by_size[k]['groups']}
    if missing_posthoc:
        raise ValueError(f'Experiment B needs own-event TCN posthoc for every test group: {missing_posthoc}')
    vals = {str(k): {"TGN v3": by_size[k]["tgn"] / by_size[k]["groups"],
                     "TCN-only": by_size[k]["tcn_own"] / by_size[k]["groups"],
                     "TGN or TCN": by_size[k]["either_own"] / by_size[k]["groups"]} for k in sizes}
    # Put denominators on the x axis; pooled group count is not 5 independent observations.
    labels = [f'{k}\n(n={by_size[k]["groups"]})' for k in sizes]
    _bars(ax, [str(k) for k in sizes], ["TGN v3", "TCN-only", "TGN or TCN"], vals, fmt="{:.2f}")
    ax.set_xticklabels(labels)
    ax.set_xlabel("group size (members; number of test groups)")
    ax.set_ylabel("share of test groups detected"); ax.set_ylim(0, 1.1)
    _legend_top(ax, 3)
    ax.set_title(f"Figure 10 — Group Detection vs. Group Size ({_n(len(runs), 'seed')} pooled)", loc="left", pad=22)
    summary.setdefault("figures", []).append(_save(fig, "fig10_group_detection_by_size"))
    # hard negatives / anchor kinds: flag rate (rule keys collapsed exactly like evaluate_v3's)
    norm = lambda k: k.split(":")[0] + (":" + k.split(":")[1] if k.startswith(("hard", "collusion")) else "")

    def kind_counts(r, m):
        out = {}
        for k, v in r["models"][m]["by_anchor_kind"].items():
            d = out.setdefault(norm(k), {"anchors": 0, "flagged": 0}); d["anchors"] += v["anchors"]; d["flagged"] += v["flagged"]
        return out
    kc = {m: [kind_counts(r, m) for r in runs] for m in models}
    kinds = sorted({k for x in kc["tgn-v3"] for k in x})
    rate = lambda m, k: np.mean([x[k]["flagged"] / x[k]["anchors"] for x in kc[m] if k in x and x[k]["anchors"]])
    _table("exp_b_anchor_kinds", ["anchor kind", "anchors (all seeds)"] + models,
           [[k, sum(x.get(k, {}).get("anchors", 0) for x in kc["tgn-v3"])] + [f"{rate(m, k):.3f}" for m in models] for k in kinds],
           "Flag rate per anchor kind (collusion kinds: recall; hard / pathB / pathD: false-alarm rate); mean over seeds.")
    below = [(v["members_below_theta_high"], len(v["member_max_s_ind"])) for r in runs if r["tcn_only"].get("available")
             for v in r["tcn_only"]["per_group"].values()]
    summary["B"] = {"seeds": len(runs), "groups_by_size": {str(k): {kk: vv for kk, vv in v.items() if kk != "ttd"} for k, v in by_size.items()},
                    "groups_by_kind": {k: {kk: vv for kk, vv in v.items() if kk != "ttd"} for k, v in by_kind.items()},
                    "members_below_theta_high": [sum(a for a, _ in below), sum(b for _, b in below)],
                    "seed_level": {m: [r["models"][m]["seed_level"] for r in runs] for m in models}}


# ============================================================================================ Experiments E / F
def exp_e(summary):
    files = sorted(glob.glob(f"{RAW}/exp_e/*-sim*.json"))
    if not files:
        summary["E"] = "Not Run"; return
    runs = [json.load(open(f, encoding="utf-8")) for f in files]
    by = {}
    for r in runs:
        by.setdefault(r["config"], []).append(r["result"])
    allf = f"{RAW}/exp_e/all-estimate.json"
    rows = []
    order = ["high", "union"] + sorted([k for k in by if k.startswith("admit-")], key=lambda k: -float(k.split("-")[1])) + \
            sorted(k for k in by if k.startswith("union-no-"))
    for k in order:
        if k not in by:
            continue
        v = by[k]
        rows.append([k, len(v), _ms([x["group_coverage"] for x in v]), _ms([x["individual_coverage"] for x in v]),
                     _ms([x["work_ms_per_window_mean"] for x in v], 1), _ms([x["work_ms_per_window_p99"] for x in v], 1),
                     f"{v[0]['budget_per_window']:.0f}" if v[0]["budget_per_window"] < 1e12 else "unbounded",
                     sum(x["windows_over_budget"] for x in v), _ms([x["admitted"] for x in v], 0),
                     json.dumps({kk: vv for kk, vv in v[0]["engine_stats"].items() if kk.split(":")[0] in ("high_pdp_alone", "sample_dropped", "sweep_deferred", "resource_expired")})])
    if os.path.exists(allf):
        a = json.load(open(allf, encoding="utf-8"))["result"]
        rows.insert(0, ["all (estimate)", a["windows_sampled"], "1 (by construction)", "1 (by construction)",
                        f"{a['work_ms_per_window_mean']:.1f}", f"{a['work_ms_per_window_p99']:.1f}", "unbounded", "-", "-", "-"])
    _table("exp_e_strategies", ["strategy", "runs", "group admission coverage", "individual admission coverage",
                                "work / window mean (ms)", "work / window p99 (ms)", "budget B_W (ms)", "windows over budget",
                                "admitted (test period)", "overflow counters (run 1)"], rows,
           "Admission coverage = >= 1 user of the instance admitted to graph analysis during its activity (+1 h). "
           "Not detection. Work = sum of Eq. 26 costs (measured coefficients) of admitted users.")
    plt = _plt()
    adm = [k for k in order if k.startswith("admit-") and k in by]
    if adm:
        xs = [float(k.split("-")[1]) for k in adm]
        fig, ax = plt.subplots(figsize=(5.4, 2.8))
        for j, (key, lab) in enumerate((("group_coverage", "groups"), ("individual_coverage", "individual instances"))):
            ys = [np.mean([x[key] for x in by[k]]) for k in adm]
            ax.plot(xs, ys, marker="o", ms=5, color=SERIES[j], label=lab)     # legend names the series
        ax.set_xscale("log"); ax.set_xlabel("budget as a fraction of the deployed B_W"); ax.set_ylabel("admission coverage")
        ax.set_ylim(0, 1.05); ax.legend(fontsize=8); ax.set_title("Experiment E — coverage vs compute budget", loc="left")
        summary.setdefault("figures", []).append(_save(fig, "exp_e_fig_budget"))
    abl = [k for k in order if k.startswith("union") and k in by]
    if abl:
        fig, ax = plt.subplots(figsize=(6.0, 2.6))
        group_counts = {len(run['groups']) for k in abl for run in by[k]}
        individual_counts = {len(run['individual']) for k in abl for run in by[k]}
        if len(group_counts)!=1 or len(individual_counts)!=1 or not min(*group_counts,*individual_counts):
            raise ValueError('Experiment F has inconsistent or empty evaluation denominators')
        n_g, n_i = next(iter(group_counts)), next(iter(individual_counts))
        vals = {k.replace("union-no-", "− "): {"groups": np.mean([x["group_coverage"] for x in by[k]]),
                                                "individual": np.mean([x["individual_coverage"] for x in by[k]])} for k in abl}
        _bars(ax, list(vals), ["groups", "individual"], vals)
        ax.set_ylim(0, 1.1); ax.set_ylabel("admission coverage (NOT detection)"); _legend_top(ax, 2)
        ax.set_title(f"Figure 12 — Escalation-Path Ablation (group n={n_g}, individual n={n_i})", loc="left", pad=22)
        fig.text(0.5,-0.035,'Admission = at least one member processed; this is not a group-detection metric.',
                 ha='center',va='top',fontsize=7,color=TEXT2)
        summary.setdefault("figures", []).append(_save(fig, "fig12_escalation_path_ablation"))
    summary["E"] = {k: {"group_coverage": [x["group_coverage"] for x in v], "individual_coverage": [x["individual_coverage"] for x in v],
                        "work_ms_per_window_mean": [x["work_ms_per_window_mean"] for x in v],
                        "windows_over_budget": [x["windows_over_budget"] for x in v]} for k, v in by.items()}


# ============================================================================================ Experiments C / D
STAGES = [("gw", "gateway"), ("kafka", "Kafka"), ("reorder", "reorder"), ("tcn_q", "TCN queue"), ("tcn", "TCN")]


def exp_c(summary):
    files = sorted(f for f in glob.glob(f"{RAW}/exp_c/rate*-run*.json")                # INVALID runs: gateway down
                   if "INVALID" not in f and json.load(open(f, encoding="utf-8")).get("valid", True))
    if not files:
        summary["C"] = "Not Run"; return
    runs = [json.load(open(f, encoding="utf-8")) for f in files]
    by, raw = {}, {}
    for r in runs:
        by.setdefault(r["rate"], []).append(r["result"])
        raw.setdefault(r["rate"], []).append(r)
    rates = sorted(by)
    rows = []

    def _coll(att):
        """Collective path, per decided attacker: escalated / TGN-scored counts, then medians (n shown)."""
        esc = [a["lat_esc_ms"] for a in att if a.get("lat_esc_ms") is not None]
        grp = [a["lat_coll_to_group_score_ms"] for a in att if a.get("lat_coll_to_group_score_ms") is not None]
        med = lambda xs: f"{np.median(xs):.0f}" if xs else "n/a"
        return f"{len(esc)} / {len(grp)} of {len(att)}; {med(esc)} / {med(grp)}"
    for rt in rates:
        v = by[rt]
        att = [a for x in v for a in x["per_attacker"].values() if a.get("decided")]
        und = sum(1 for x in v for a in x["per_attacker"].values() if not a.get("decided"))
        q = lambda key: np.percentile([a[key] for a in att if a.get(key) is not None], [50, 95, 99]) if any(a.get(key) is not None for a in att) else [np.nan] * 3
        e2e, obs = q("lat_ind_decision_ms"), q("lat_ind_observed_ms")
        gws = [x["stages_ms"]["gw_to_score"] for x in v if x["stages_ms"]["gw_to_score"]]
        rows.append([f"{rt:g}", len(v), f"{np.mean([x['throughput']['scores_per_s'] for x in v]):.1f}",
                     f"{np.median([g['p50'] for g in gws]):.0f} / {np.median([g['p99'] for g in gws]):.0f}",
                     f"{len(att)} (+{und} not enforced)", f"{e2e[0]:.0f} / {e2e[1]:.0f} / {e2e[2]:.0f}",
                     f"{obs[0]:.0f} / {obs[1]:.0f} / {obs[2]:.0f}",
                     f"{np.median([a['lat_pdp_ms'] for a in att]):.0f}" if att else "n/a",
                     f"{np.mean([a['n_exposed'] for a in att]):.1f}" if att else "n/a",
                     " / ".join(f"{np.mean([a['lat_ind_decision_ms'] <= x * 1000 for a in att]):.2f}" for x in (0.5, 1, 2, 5)) if att else "n/a",
                     _coll(att)])
    _table("exp_c_latency", ["background rate (req/s)", "runs", "scores/s", "gateway→score ms p50 / p99 (all events)",
                             "attack instances", "Lat_ind decision-ready ms p50/p95/p99", "Lat_ind observed ms p50/p95/p99",
                             "Lat_PDP ms median", "E[N_exposed] (λu = 5/s)", "P(Lat_ind ≤ 0.5/1/2/5 s)",
                             "collective: attackers escalated / group-scored of decided; median ms score→escalation / trigger→group score"], rows,
           "Live stack on one host; end-to-end values come from one trace per attack, never from summed stage percentiles. "
           "v3 defines no numeric Lat_SLO, so P(Lat_ind <= x) is reported for several x.")
    plt = _plt()
    fig, ax = plt.subplots(figsize=(6.0, 2.8))
    bottom = np.zeros(len(rates))
    med = {k: np.array([np.median([x["stages_ms"][k]["p50"] for x in by[rt] if x["stages_ms"].get(k)]) for rt in rates])
           for k, _ in STAGES}
    shown = [(k, lab) for k, lab in STAGES if np.nanmax(med[k]) > 0]          # an all-zero stage gets no colour
    hidden = [lab for k, lab in STAGES if (k, lab) not in shown]
    # fixed stage order = pipeline order; colours are the first palette slots in that order (adjacent pairs pass
    # the dataviz validator; orange next to amber did not)
    for j, (k, lab) in enumerate(shown):
        ax.bar([f"{r:g}" for r in rates], med[k], 0.55, bottom=bottom, color=SERIES[:4][j % 4], edgecolor=SURFACE,
               linewidth=1.5, label=lab)
        bottom += med[k]
    ax.set_xlabel("background load (requests/s)")
    ax.set_ylabel("sum of per-stage p50 values (ms)")
    _legend_top(ax, len(shown), fontsize=7)
    ax.set_title("Figure 11 — Individual-Path Stage Latency (NOT end-to-end p50)", loc="left", pad=22)
    if hidden:
        ax.text(0.01, 0.97, f"{', '.join(hidden)}: 0 ms in every run", transform=ax.transAxes, ha="left", va="top",
                fontsize=6.5, color=TEXT2)
    fig.text(0.5,-0.035,'Summed stage medians are descriptive; end-to-end latency must come from full request traces.',
             ha='center',va='top',fontsize=7,color=TEXT2)
    summary.setdefault("figures", []).append(_save(fig, "fig11_individual_path_latency"))
    stage_rows = [[f"{rt:g}"] + [f"{np.median([x['stages_ms'][k]['p50'] for x in by[rt] if x['stages_ms'].get(k)]):.1f} / "
                                 f"{np.median([x['stages_ms'][k]['p99'] for x in by[rt] if x['stages_ms'].get(k)]):.1f}" for k, _ in STAGES] for rt in rates]
    _table("exp_c_stages", ["rate"] + [f"{lab} p50 / p99 ms" for _, lab in STAGES], stage_rows, "Median over runs of the per-run percentiles.")
    # Experiment D: throughput and backlog on the same runs (lag sampled every 5 s by exp_c)
    LAG_GROUPS = ["sequencer", "tcn-v3", "pdp", "escalation", "cmu-user", "graph-index", "hdfs-sink"]
    d_rows = []
    for rt in rates:
        runs_ = by[rt]
        th = [x["throughput"] for x in runs_]
        mix = {}
        for t in th:
            for k_, v_ in t["gateway_status_mix"].items():
                mix[k_] = mix.get(k_, 0) + v_
        lag_max = {g: max((smp[1].get(g) or 0) for x in raw[rt] for smp in x["lag_samples"]) for g in LAG_GROUPS}
        d_rows.append([f"{rt:g}", len(runs_), f"{np.mean([t['gateway_requests_per_s'] for t in th]):.1f}",
                       f"{np.mean([t['scores_per_s'] for t in th]):.1f}",
                       f"{np.median([t['gateway_response_ms']['p50'] for t in th]):.1f} / "
                       f"{np.median([t['gateway_response_ms']['p99'] for t in th]):.1f}",
                       json.dumps(dict(sorted(mix.items())))] + [str(lag_max[g]) for g in LAG_GROUPS])
    _table("exp_d_throughput", ["background rate (req/s)", "runs", "gateway req/s", "scores/s", "gateway response ms p50 / p99",
                                "status mix (all runs)"] + [f"max lag {g}" for g in LAG_GROUPS], d_rows,
           "Same runs as Experiment C. Max lag = largest consumer-group backlog (records) in any 5 s sample of any run. "
           "Container CPU and memory: table exp_d_resources.")
    # Experiment D: container resources over all C runs (docker stats sampled about every 10 s by the host)
    ds = f"{RAW}/exp_c/docker-stats.jsonl"
    if os.path.exists(ds):
        import re
        unit = {"B": 1 / 2 ** 20, "KiB": 1 / 1024, "MiB": 1.0, "GiB": 1024.0}

        def mib(x):
            m = re.match(r"([\d.]+)\s*([KMG]?i?B)", x.strip())
            return float(m.group(1)) * unit[m.group(2)] if m else float("nan")
        per = {}
        for line in open(ds, encoding="utf-8"):
            try:
                d = json.loads(line)
            except ValueError:
                continue
            name = d["name"].replace("accesssentinel-", "")
            if name.startswith("tools-run"):
                continue
            used, lim = d["mem"].split("/")
            e = per.setdefault(name, {"mem": [], "lim": mib(lim), "cpu": []})
            e["mem"].append(mib(used)); e["cpu"].append(float(d["cpu"].rstrip("%") or 0))
        rows_r = sorted(([n, len(e["mem"]), f"{max(e['mem']):.0f}", f"{e['lim']:.0f}", f"{100 * max(e['mem']) / e['lim']:.0f} %",
                          f"{np.mean(e['cpu']):.1f}", f"{np.percentile(e['cpu'], 95):.1f}"] for n, e in per.items()),
                        key=lambda r: -float(r[4].rstrip(" %")))
        _table("exp_d_resources", ["container", "samples", "peak memory (MiB)", "limit (MiB)", "peak / limit",
                                   "CPU % mean", "CPU % p95"], rows_r,
               "docker stats over all Experiment C runs (5, 20 and 50 req/s), sampled about every 10 s; CPU % is of one "
               "core (Docker Desktop VM, 20 vCPU). Peaks between samples are not seen.")
    summary["C"] = {str(rt): by[rt] for rt in rates}


# ============================================================================================ Experiments G / H
def exp_g(summary):
    d = f"{RAW}/exp_g"
    if not os.path.isdir(d):
        summary["G"] = "Not Run"; return
    L = lambda n: json.load(open(f"{d}/{n}.json", encoding="utf-8"))["result"] if os.path.exists(f"{d}/{n}.json") else None
    eq, psi, ttl, pol, fb = L("eq21"), L("psi"), L("ttl"), L("policy"), L("fallback-probe")
    rows = []
    if eq:
        rows.append(["Eq. 21 live = pure mapping", f"{eq['match']}/{eq['cases']}", json.dumps(eq["by_expected_action"])])
        p = eq["propagation_ms"]
        rows.append(["score → decision cache (ms)", f"p50 {p['p50']:.1f} / p95 {p['p95']:.1f} / p99 {p['p99']:.1f} / max {p['max']:.1f}", f"n = {p['n']}"])
    if psi:
        rows.append(["stale decision (ψ ≤ ψcur) rejected", f"{psi['rejected']}/{psi['trials']}", ""])
    if ttl:
        rows.append(["TTL 60 s: Allow decision after expires_at (s)", f"{ttl['expire_decision_after_expiry_s']:.1f}",
                     f"login allowed {ttl['gateway_login_after_expiry_s']:.1f} s after expiry" if ttl["gateway_login_after_expiry_s"] is not None else ""])
    if pol:
        rows.append(["policy precedence (Block over Terminate, release refused, lift restores)", str(pol["all_ok"]), f"{len(pol['runs'])} runs"])
    if fb:
        r = fb["rows"]
        hf = f"{d}/fallback-host-times.txt"
        on = next((x["t"] for x in r if x["s1_fallback"]), None)
        off = next((x["t"] for x in r if on and x["t"] > on and not x["s1_fallback"]), None)
        rows.append(["fallback samples (s=1 refused while PDP down)", f"{sum(x['s1_fallback'] for x in r)}/{len(r)}",
                     f"fallback lasted {off - on:.1f} s" if on and off else ""])
        rows.append(["s=0 allowed during fallback", f"{sum(1 for x in r if x['s1_fallback'] and x['s0'] == 200)}/{sum(x['s1_fallback'] for x in r)}", ""])
        if os.path.exists(hf):
            ht = dict(line.split() for line in open(hf).read().strip().splitlines())
            stop, start = float(ht["pdp_stop"]), float(ht["pdp_start"])
            note = "host (Windows) clock vs probe (Docker VM) clock: approximate to their offset"
            if on:
                rows.append(["fallback onset after the PDP stop (s; decision TTL 30 s)", f"{on - stop:.1f}", note])
            back = next((x["t"] for x in r if x["t"] > start and x["s1"] == 200), None)
            if back:
                rows.append(["s=1 served again after the PDP start (s)", f"{back - start:.1f}", note])
    _table("exp_g_enforcement", ["check", "result", "detail"], rows)
    summary["G"] = {"eq21": eq and {k: eq[k] for k in ("cases", "match", "by_expected_action", "propagation_ms")},
                    "psi": psi and {k: psi[k] for k in ("trials", "rejected")}, "ttl": ttl, "policy_all_ok": pol and pol["all_ok"]}


def exp_h(summary):
    d = f"{RAW}/exp_h"
    if not os.path.isdir(d):
        summary["H"] = "Not Run"; return
    rows = []
    for f in sorted(glob.glob(f"{d}/*-check.json")):
        c = json.load(open(f, encoding="utf-8"))
        for st in ("sequencer", "tcn"):
            v = c["stages"][st]
            rows.append([c["tag"], st, f"{v['out_unique']}/{v['sent']}", v["lost"], v["duplicates"],
                         f"{v['max_gap_s']:.1f}" if v["max_gap_s"] is not None else "n/a"])
    _table("exp_h_crash", ["run", "stage", "unique out / sent", "lost", "duplicates", "largest output gap (s)"], rows,
           "SIGKILL of the named container 12 s into a 30 s burst (3,000 events at 100/s), restarted 5 s later. "
           "The largest gap between consecutive outputs approximates the outage seen by that stage.")
    prow = []
    for f in sorted(glob.glob(f"{d}/pause-*-probe.json")):
        p = json.load(open(f, encoding="utf-8"))
        prow.append([os.path.basename(f).replace("-probe.json", ""), p["user_requests"], json.dumps(p["status_mix"]),
                     p["allowed_requests"], p["events_on_kafka"]])
    _table("exp_h_outage", ["run", "requests", "status mix", "allowed", "events on Kafka for the user"], prow,
           "docker pause of the named service for 20 s during an 80 s probe (one request every 0.2 s).")
    dup = f"{d}/dup.json"
    summary["H"] = {"crash_rows": rows, "outage_rows": prow, "dup": json.load(open(dup, encoding="utf-8")) if os.path.exists(dup) else None}


def main(argv=None) -> int:
    which = (argv if argv is not None else sys.argv[1:]) or ["a", "b", "c", "e", "g", "h"]
    summary = {}
    for k in which:
        {"a": exp_a, "b": exp_b, "c": exp_c, "e": exp_e, "f": exp_e, "g": exp_g, "h": exp_h}[k](summary)
    os.makedirs(TAB, exist_ok=True)
    path = f"{TAB}/summary.json"
    old = json.load(open(path, encoding="utf-8")) if os.path.exists(path) else {}
    old.update(summary)
    json.dump(old, open(path, "w", encoding="utf-8"), indent=1, default=str)
    print(json.dumps({k: (v if isinstance(v, str) else "ok") for k, v in summary.items()}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
