| background rate (req/s) | runs | scores/s | gateway→score ms p50 / p99 (all events) | attack instances | Lat_ind decision-ready ms p50/p95/p99 | Lat_ind observed ms p50/p95/p99 | Lat_PDP ms median | E[N_exposed] (λu = 5/s) | P(Lat_ind ≤ 0.5/1/2/5 s) | collective: attackers escalated / group-scored of decided; median ms score→escalation / trigger→group score |
|---|---|---|---|---|---|---|---|---|---|---|
| 5 | 3 | 4.3 | 25 / 132 | 15 (+0 not enforced) | 26 / 161 / 357 | 203 / 335 / 554 | 3 | 0.1 | 1.00 / 1.00 / 1.00 / 1.00 | 15 / 15 of 15; 5067 / 5144 |
| 20 | 3 | 18.7 | 30 / 89 | 15 (+0 not enforced) | 45 / 59 / 59 | 208 / 220 / 222 | 2 | 0.0 | 1.00 / 1.00 / 1.00 / 1.00 | 15 / 15 of 15; 5023 / 5073 |
| 50 | 3 | 47.8 | 27 / 183 | 15 (+0 not enforced) | 25 / 177 / 378 | 204 / 330 / 553 | 2 | 0.1 | 1.00 / 1.00 / 1.00 / 1.00 | 15 / 15 of 15; 5008 / 5058 |

Live stack on one host; end-to-end values come from one trace per attack, never from summed stage percentiles. v3 defines no numeric Lat_SLO, so P(Lat_ind <= x) is reported for several x.
