| container | samples | peak memory (MiB) | limit (MiB) | peak / limit | CPU % mean | CPU % p95 |
|---|---|---|---|---|---|---|
| kafka-1 | 176 | 896 | 1024 | 88 % | 57.7 | 196.4 |
| cmu-user-1 | 176 | 282 | 384 | 74 % | 23.6 | 70.3 |
| cmu-resource-1 | 176 | 278 | 384 | 72 % | 23.5 | 70.5 |
| datanode-1 | 176 | 520 | 768 | 68 % | 2.1 | 9.3 |
| namenode-1 | 176 | 470 | 768 | 61 % | 2.5 | 9.3 |
| gateway-1 | 176 | 211 | 384 | 55 % | 26.1 | 42.6 |
| hdfs-sink-1 | 176 | 204 | 384 | 53 % | 3.9 | 13.8 |
| redis-1 | 176 | 185 | 384 | 48 % | 8.9 | 25.1 |
| tgn-worker-bg-1 | 176 | 346 | 768 | 45 % | 1.4 | 10.2 |
| tgn-worker-rt-1 | 176 | 331 | 768 | 43 % | 1.0 | 9.0 |
| escalation-1 | 176 | 153 | 384 | 40 % | 3.7 | 14.5 |
| tcn-worker-1 | 176 | 280 | 768 | 37 % | 23.4 | 79.1 |
| incidents-1 | 176 | 83 | 256 | 33 % | 1.6 | 12.8 |
| pdp-1 | 176 | 81 | 256 | 32 % | 6.7 | 34.1 |
| tcn-worker-2 | 176 | 249 | 768 | 32 % | 24.0 | 79.6 |
| app-hr-1 | 176 | 70 | 256 | 27 % | 3.6 | 22.5 |
| resource-windows-1 | 176 | 61 | 256 | 24 % | 6.1 | 19.5 |
| app-docs-1 | 176 | 60 | 256 | 23 % | 4.2 | 22.5 |
| app-finance-1 | 176 | 57 | 256 | 22 % | 3.3 | 22.5 |
| api-1 | 176 | 163 | 768 | 21 % | 8.4 | 24.2 |
| graph-index-1 | 176 | 44 | 256 | 17 % | 4.2 | 19.2 |
| sequencer-1 | 176 | 40 | 256 | 16 % | 5.5 | 19.2 |
| repartitioner-1 | 176 | 29 | 256 | 11 % | 4.9 | 14.1 |
| postgres-1 | 176 | 56 | 512 | 11 % | 1.3 | 5.7 |
| kafka-ui-1 | 176 | 301 | 7799 | 4 % | 0.6 | 0.8 |

docker stats over all Experiment C runs (5, 20 and 50 req/s), sampled about every 10 s; CPU % is of one core (Docker Desktop VM, 20 vCPU). Peaks between samples are not seen.
