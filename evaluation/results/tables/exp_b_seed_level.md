| Model | seed ROC-AUC | seed PR-AUC | precision | recall | FPR |
|---|---|---|---|---|---|
| tgn-v3 | 0.626 ± 0.049 | 0.397 ± 0.066 | 0.862 ± 0.043 | 0.135 ± 0.040 | 0.0036 ± 0.0007 |
| rule-shared | 0.639 ± 0.042 | 0.242 ± 0.023 | 0.178 ± 0.027 | 0.521 ± 0.095 | 0.4316 ± 0.0037 |
| rule-uncommon | 0.499 ± 0.008 | 0.155 ± 0.007 | 0.121 ± 0.142 | 0.012 ± 0.015 | 0.0131 ± 0.0032 |

Escalation-like test anchors: is the escalated seed in a colluding group? 5 seeds, mean ± std. tgn-v3 at theta_C (nominal-validation quantile); rules at 0.5.
