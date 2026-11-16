| Model | benign_flash_crowd | benign_month_end | benign_new_device | benign_overtime | benign_travel | normal |
|---|---|---|---|---|---|---|
| TCN v3 | 0.0000 | 0.2140 | 0.0116 | 0.0121 | 0.0202 | 0.0127 |
| TCN (supervised only) | 0.0491 | 0.0398 | 0.0160 | 0.0133 | 0.0027 | 0.0093 |
| LSTM | 0.1261 | 0.0804 | 0.0188 | 0.0112 | 0.0081 | 0.0095 |
| TCN (D3) | 0.1162 | 0.0256 | 0.0098 | 0.0096 | 0.0063 | 0.0067 |
| Isolation Forest | 0.0648 | 0.0287 | 0.0343 | 0.0253 | 0.0903 | 0.0148 |
| Rule (D3) | 0.0000 | 0.0054 | 0.1301 | 0.1053 | 0.1170 | 0.1063 |

Flag rate (false alarms) of benign / hard-negative events at theta_high; mean over seeds.
