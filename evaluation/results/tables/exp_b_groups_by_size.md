| size | groups | TGN v3 | TCN-only on the group's own events | TGN or TCN (own events) | TCN any event in span | chance rate of 'any in span' (matched random users) | rule (shared) | TGN median hours from first group access to detection |
|---|---|---|---|---|---|---|---|---|
| 2 | 20 | 9/20 (45%) | 9/20 (45%) | 15/20 (75%) | 13/20 (65%) | 35% | 17/20 (85%) | 73.1 |
| 3 | 20 | 13/20 (65%) | 12/20 (60%) | 17/20 (85%) | 14/20 (70%) | 49% | 16/20 (80%) | 71.7 |
| 5 | 20 | 10/20 (50%) | 14/20 (70%) | 17/20 (85%) | 17/20 (85%) | 64% | 18/20 (90%) | 81.6 |
| 8 | 15 | 7/15 (47%) | 11/15 (73%) | 12/15 (80%) | 15/15 (100%) | 81% | 12/15 (80%) | 96.2 |

Test groups pooled over seeds. TCN-only uses the promoted v3 TCN at its calibrated theta_high. 'Any event in span' fires by chance on ordinary activity over a multi-day span; its chance rate (random non-attacker users, same size and span) shows it is not evidence of detection, so the comparison uses the group's own events.
