| Model | bac_probe | compromised_account | horizontal_abuse | insider_exfiltration | low_and_slow | unusual_sequence | unusual_time |
|---|---|---|---|---|---|---|---|
| TCN v3 | 0.958 | 0.990 | 0.632 | 0.816 | 0.197 | 0.130 | 0.160 |
| TCN (supervised only) | 0.974 | 0.999 | 0.565 | 0.898 | 0.593 | 0.562 | 0.892 |
| LSTM | 0.982 | 1.000 | 0.672 | 0.878 | 0.640 | 0.425 | 0.813 |
| TCN (D3) | 0.988 | 1.000 | 0.960 | 0.959 | 0.847 | 0.929 | 0.942 |
| Isolation Forest | 0.747 | 1.000 | 0.084 | 0.160 | 0.490 | 0.001 | 0.026 |
| Rule (D3) | 0.898 | 0.978 | 0.989 | 0.808 | 0.512 | 0.001 | 0.025 |

Recall of attack events per scenario at each model's calibrated theta_high; mean over seeds.
