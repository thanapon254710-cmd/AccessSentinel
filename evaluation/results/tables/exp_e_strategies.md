| strategy | runs | group admission coverage | individual admission coverage | work / window mean (ms) | work / window p99 (ms) | budget B_W (ms) | windows over budget | admitted (test period) | overflow counters (run 1) |
|---|---|---|---|---|---|---|---|---|---|
| all (estimate) | 300 | 1 (by construction) | 1 (by construction) | 965.1 | 5107.7 | unbounded | - | - | - |
| high | 1 | 0.667 | 1.000 | 18.6 | 264.2 | 84000 | 0 | 2434 | {} |
| union | 3 | 1.000 ± 0.000 | 1.000 ± 0.000 | 171.0 ± 0.1 | 7277.5 ± 24.1 | unbounded | 0 | 21474 ± 17 | {} |
| admit-1 | 3 | 1.000 ± 0.000 | 1.000 ± 0.000 | 171.0 ± 0.1 | 7277.5 ± 24.1 | 84000 | 0 | 21474 ± 17 | {} |
| admit-0.1 | 1 | 1.000 | 1.000 | 170.6 | 7311.1 | 8399 | 0 | 21392 | {"sample_dropped:background": 33, "high_pdp_alone:realtime": 41} |
| admit-0.01 | 1 | 1.000 | 1.000 | 159.5 | 837.9 | 839 | 0 | 19469 | {"high_pdp_alone:realtime": 1793, "sample_dropped:background": 957, "sweep_deferred:background": 1799} |
| admit-0.001 | 1 | 1.000 | 0.750 | 29.9 | 83.7 | 84 | 0 | 4943 | {"sweep_deferred:background": 247164, "high_pdp_alone:realtime": 3067, "sample_dropped:background": 2161, "resource_expired:realtime": 8922} |
| union-no-high | 1 | 1.000 | 0.500 | 159.8 | 7498.9 | unbounded | 0 | 19877 | {} |
| union-no-resource | 1 | 1.000 | 1.000 | 44.9 | 408.1 | unbounded | 0 | 6862 | {} |
| union-no-sample | 1 | 1.000 | 1.000 | 160.1 | 7255.7 | unbounded | 0 | 19859 | {} |
| union-no-sweep | 1 | 1.000 | 1.000 | 163.7 | 7311.1 | unbounded | 0 | 19580 | {} |

Admission coverage = >= 1 user of the instance admitted to graph analysis during its activity (+1 h). Not detection. Work = sum of Eq. 26 costs (measured coefficients) of admitted users.
