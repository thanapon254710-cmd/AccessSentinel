| check | result | detail |
|---|---|---|
| Eq. 21 live = pure mapping | 200/200 | {"Terminate": {"cases": 70, "match": 70}, "Restrict": {"cases": 30, "match": 30}, "Throttle": {"cases": 42, "match": 42}, "StepUp": {"cases": 15, "match": 15}, "Allow": {"cases": 43, "match": 43}} |
| score → decision cache (ms) | p50 12.6 / p95 103.5 / p99 793.4 / max 2085.6 | n = 157 |
| stale decision (ψ ≤ ψcur) rejected | 20/20 |  |
| TTL 60 s: Allow decision after expires_at (s) | 1.0 | login allowed 1.1 s after expiry |
| policy precedence (Block over Terminate, release refused, lift restores) | True | 3 runs |
| fallback samples (s=1 refused while PDP down) | 73/220 | fallback lasted 39.7 s |
| s=0 allowed during fallback | 73/73 |  |
| fallback onset after the PDP stop (s; decision TTL 30 s) | 31.0 | host (Windows) clock vs probe (Docker VM) clock: approximate to their offset |
| s=1 served again after the PDP start (s) | 7.3 | host (Windows) clock vs probe (Docker VM) clock: approximate to their offset |
