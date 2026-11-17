# Common-data adapters

The runnable adapters are implemented in `../selected_adapters.py` and orchestrated by `../run_all.py`.

Every adapter receives the same generated AccessSentinel dataset seed, uses the shared chronological `build_v3` window IDs for final evaluation, and returns continuous anomaly scores. The target label is the individual anomaly label. Preprocessing and model fitting use training-period data; thresholds are calibrated on nominal validation windows; test labels are used only for final metrics.

The method-specific mappings and limitations are recorded in `../README.md` and `../IMPLEMENTATION_STATUS.md`. In particular, PG-AID and DEEPRO use interaction-provenance approximations, ACOBE uses role cohorts, and FGDT uses co-access groups. These adaptations are not exact replications of original datasets.
