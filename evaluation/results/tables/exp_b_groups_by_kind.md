| kind | groups | TGN v3 | TCN-only on the group's own events | TGN or TCN (own events) | TCN any event in span | chance rate of 'any in span' (matched random users) | rule (shared) | TGN median hours from first group access to detection |
|---|---|---|---|---|---|---|---|---|
| coordinated_collection | 20 | 8/20 (40%) | 11/20 (55%) | 17/20 (85%) | 17/20 (85%) | 54% | 20/20 (100%) | 96.6 |
| cross_app_collection | 15 | 10/15 (67%) | 9/15 (60%) | 13/15 (87%) | 11/15 (73%) | 48% | 15/15 (100%) | 96.6 |
| low_slow_group | 20 | 4/20 (20%) | 9/20 (45%) | 12/20 (60%) | 14/20 (70%) | 66% | 15/20 (75%) | 148.6 |
| split_exfiltration | 20 | 17/20 (85%) | 17/20 (85%) | 19/20 (95%) | 17/20 (85%) | 52% | 13/20 (65%) | 48.7 |

Test groups pooled over seeds. TCN-only uses the promoted v3 TCN at its calibrated theta_high. 'Any event in span' fires by chance on ordinary activity over a multi-day span; its chance rate (random non-attacker users, same size and span) shows it is not evidence of detection, so the comparison uses the group's own events.
