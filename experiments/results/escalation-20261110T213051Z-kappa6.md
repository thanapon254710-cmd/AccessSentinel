# Escalation policy evaluation (synthetic) — model tcn-20261110T194520Z-beeb9b

**Data: SYNTHETIC (accesssentinel.datasets.synth v2)**, fingerprint `beeb9bffdb1d7160`, 132,881 events. Synthetic results only.

Configuration: theta_high 0.6, kappa 6, delta_res 900 s, T_sweep 7200 s, gamma 0.02, W 60 s, real-time window 5 s, cooldowns 300/3600 s, caps 100/50.

## Which paths reach which attacks

| Set | instances | Path A high | Path B resource | Path C sweep | Path D sample | escalated | users never scored High | of those, escalated via B/C/D |
|---|---|---|---|---|---|---|---|---|
| individual_test | 13 | 13 | 7 | 7 | 1 | 13 | 0/13 | 0 |
| individual_all | 39 | 39 | 21 | 23 | 5 | 39 | 0/39 | 0 |
| collusion_all | 4 | 4 | 4 | 4 | 4 | 4 | 3/11 | 3 |

Paths are counted when they triggered for the instance's users during the attack (plus T_sweep + W); 'users never scored High' = users for whom Path A never fired during that interval.

## Per instance

| Instance | split | users | events | TCN high events | paths triggered | users escalated | minutes to first escalation |
|---|---|---|---|---|---|---|---|
| bac_probe-test-0 | test | 1 | 62 | 56 | high, sweep | 1/1 | 2.3 |
| bac_probe-test-1 | test | 1 | 40 | 35 | high, resource, sweep | 1/1 | 2.4 |
| bac_probe-test-2 | test | 1 | 54 | 52 | high, resource, sweep | 1/1 | 0.1 |
| bac_probe-train-0 | train | 1 | 69 | 68 | high, resource, sweep | 1/1 | 0.1 |
| bac_probe-train-1 | train | 1 | 47 | 47 | high, resource, sweep | 1/1 | 0.2 |
| bac_probe-train-2 | train | 1 | 66 | 66 | high, resource, sweep | 1/1 | 0.0 |
| bac_probe-train-3 | train | 1 | 55 | 55 | high, sweep | 1/1 | 0.2 |
| bac_probe-train-4 | train | 1 | 33 | 33 | high, resource, sweep | 1/1 | 0.2 |
| bac_probe-val-0 | val | 1 | 51 | 50 | high, sweep | 1/1 | 0.3 |
| bac_probe-val-1 | val | 1 | 43 | 38 | high, resource, sweep | 1/1 | 1.3 |
| compromised_account-test-0 | test | 1 | 121 | 121 | high | 1/1 | 0.5 |
| compromised_account-test-1 | test | 1 | 108 | 107 | high | 1/1 | 0.4 |
| compromised_account-test-2 | test | 1 | 85 | 85 | high | 1/1 | 0.0 |
| compromised_account-train-0 | train | 1 | 77 | 77 | high | 1/1 | 0.0 |
| compromised_account-train-1 | train | 1 | 101 | 100 | high | 1/1 | 0.2 |
| compromised_account-train-2 | train | 1 | 67 | 67 | high | 1/1 | 0.6 |
| compromised_account-train-3 | train | 1 | 74 | 74 | high | 1/1 | 0.8 |
| compromised_account-train-4 | train | 1 | 120 | 120 | high | 1/1 | 0.4 |
| compromised_account-val-0 | val | 1 | 107 | 107 | high | 1/1 | 1.2 |
| compromised_account-val-1 | val | 1 | 97 | 97 | high | 1/1 | 0.7 |
| coordinated_collection-train-0 | train | 3 | 30 | 3 | high, resource, sample, sweep | 3/3 | 7.5 |
| coordinated_collection-train-1 | train | 3 | 22 | 9 | high, resource, sample, sweep | 3/3 | 0.2 |
| coordinated_collection-train-2 | train | 2 | 17 | 1 | high, resource, sample, sweep | 2/2 | 0.4 |
| insider_exfiltration-test-0 | test | 1 | 107 | 88 | high, sweep | 1/1 | 2.6 |
| insider_exfiltration-test-1 | test | 1 | 52 | 48 | high | 1/1 | 1.2 |
| insider_exfiltration-test-2 | test | 1 | 93 | 92 | high, resource, sweep | 1/1 | 0.1 |
| insider_exfiltration-test-3 | test | 1 | 58 | 54 | high, resource | 1/1 | 0.1 |
| insider_exfiltration-train-0 | train | 1 | 81 | 81 | high, resource, sweep | 1/1 | 0.2 |
| insider_exfiltration-train-1 | train | 1 | 75 | 75 | high, resource, sweep | 1/1 | 0.1 |
| insider_exfiltration-train-2 | train | 1 | 56 | 56 | high | 1/1 | 0.1 |
| insider_exfiltration-train-3 | train | 1 | 93 | 93 | high, sweep | 1/1 | 0.2 |
| insider_exfiltration-train-4 | train | 1 | 93 | 93 | high, resource, sweep | 1/1 | 0.1 |
| insider_exfiltration-val-0 | val | 1 | 92 | 90 | high, resource | 1/1 | 0.8 |
| insider_exfiltration-val-1 | val | 1 | 56 | 50 | high | 1/1 | 1.0 |
| low_and_slow-test-0 | test | 1 | 28 | 3 | high, resource | 1/1 | 293.3 |
| low_and_slow-test-1 | test | 1 | 24 | 24 | high, resource, sample, sweep | 1/1 | 0.3 |
| low_and_slow-test-2 | test | 1 | 22 | 22 | high, resource, sweep | 1/1 | 0.1 |
| low_and_slow-train-0 | train | 1 | 30 | 30 | high, resource, sample, sweep | 1/1 | 0.2 |
| low_and_slow-train-1 | train | 1 | 38 | 38 | high, resource, sample, sweep | 1/1 | 0.2 |
| low_and_slow-train-2 | train | 1 | 28 | 28 | high, resource, sample, sweep | 1/1 | 0.3 |
| low_and_slow-train-3 | train | 1 | 30 | 29 | high, resource, sweep | 1/1 | 2.2 |
| low_and_slow-val-0 | val | 1 | 19 | 19 | high, resource, sample, sweep | 1/1 | 0.4 |
| split_exfiltration-train-0 | train | 3 | 23 | 7 | high, resource, sample, sweep | 3/3 | 0.6 |

## Load (test period)

- Escalations of non-attackers: 1507 (251.2/day) over 176 of 210 normal users; by path {'sample': 228, 'high': 88, 'resource': 1172, 'sweep': 46}.
- Unique escalations per W = 60 s: mean 0.242, p95 1, max 17 (738 of 8640 windows non-empty).
- Eq. (11) terms per window: |U_high| 0.076 + n_res 0.562 + B_sweep/T_sweep*W 0.417 + gamma*N_low 0.047 = 1.101 (upper bound; measured 0.242 after union and cooldowns).
- Totals: {'escalations': 9429, 'by_priority': {'background': 2948, 'realtime': 6481}, 'triggers_by_path': {'sample': 1789, 'sweep': 2221, 'resource': 27355, 'high': 3177}, 'sweeps': 216}
- Wall time 55.1 s.
