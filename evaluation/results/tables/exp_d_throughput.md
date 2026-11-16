| background rate (req/s) | runs | gateway req/s | scores/s | gateway response ms p50 / p99 | status mix (all runs) | max lag sequencer | max lag tcn-v3 | max lag pdp | max lag escalation | max lag cmu-user | max lag graph-index | max lag hdfs-sink |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 5 | 3 | 4.2 | 4.3 | 8.1 / 38.4 | {"200": 1939} | 1 | 10 | 3 | 3 | 10 | 10 | 349 |
| 20 | 3 | 18.7 | 18.7 | 10.0 / 43.4 | {"200": 7215} | 2 | 13 | 2 | 1 | 14 | 12 | 1367 |
| 50 | 3 | 47.7 | 47.8 | 7.7 / 43.7 | {"200": 17590} | 3 | 222 | 16 | 2 | 14 | 12 | 2255 |

Same runs as Experiment C. Max lag = largest consumer-group backlog (records) in any 5 s sample of any run. Container CPU and memory: table exp_d_resources.
