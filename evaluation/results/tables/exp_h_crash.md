| run | stage | unique out / sent | lost | duplicates | largest output gap (s) |
|---|---|---|---|---|---|
| baseline | sequencer | 3000/3000 | 0 | 0 | 2.4 |
| baseline | tcn | 3000/3000 | 0 | 0 | 0.7 |
| kill-cmu-user-1 | sequencer | 3000/3000 | 0 | 0 | 0.2 |
| kill-cmu-user-1 | tcn | 3000/3000 | 0 | 0 | 0.2 |
| kill-cmu-user-2 | sequencer | 3000/3000 | 0 | 0 | 0.2 |
| kill-cmu-user-2 | tcn | 3000/3000 | 0 | 0 | 0.2 |
| kill-cmu-user-3 | sequencer | 3000/3000 | 0 | 0 | 0.2 |
| kill-cmu-user-3 | tcn | 3000/3000 | 0 | 0 | 0.2 |
| kill-sequencer-1 | sequencer | 3000/3000 | 0 | 0 | 7.8 |
| kill-sequencer-1 | tcn | 3000/3000 | 0 | 0 | 8.0 |
| kill-sequencer-2 | sequencer | 3000/3000 | 0 | 0 | 7.4 |
| kill-sequencer-2 | tcn | 3000/3000 | 0 | 0 | 7.7 |
| kill-sequencer-3 | sequencer | 3000/3000 | 0 | 0 | 7.4 |
| kill-sequencer-3 | tcn | 3000/3000 | 0 | 0 | 7.7 |
| kill-tcn-worker-1 | sequencer | 3000/3000 | 0 | 0 | 0.2 |
| kill-tcn-worker-1 | tcn | 3000/3000 | 0 | 0 | 35.3 |
| kill-tcn-worker-2 | sequencer | 3000/3000 | 0 | 0 | 0.2 |
| kill-tcn-worker-2 | tcn | 3000/3000 | 0 | 0 | 33.7 |
| kill-tcn-worker-3 | sequencer | 3000/3000 | 0 | 0 | 0.2 |
| kill-tcn-worker-3 | tcn | 3000/3000 | 0 | 0 | 34.9 |

SIGKILL of the named container 12 s into a 30 s burst (3,000 events at 100/s), restarted 5 s later. The largest gap between consecutive outputs approximates the outage seen by that stage.
