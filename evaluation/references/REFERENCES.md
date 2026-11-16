# Reference verification log (Deliverable 4)

Every number used from a published paper must have: the full citation, the page/table it comes from, the
dataset and conditions, and the label **Reported by Reference Paper**. Unverified references are not used.

| Ref | Citation (as verified) | Verified how / when | Full text available here | Usable for numbers |
|---|---|---|---|---|
| [1] | A. Mezina, R. Burget, C. M. Travieso-González, "Network Anomaly Detection With Temporal Convolutional Network and U-Net Model," IEEE Access 9, 143608–143622, 2021, doi:10.1109/ACCESS.2021.3121998 (CC BY 4.0) | Title/venue/DOI via dblp and repository listings, 2026-11-15 | Open access, but every copy tried returned HTTP 403 to this environment (ULPGC, VUT, DOAJ); not read | **No** (numbers only after the team reads the paper; it evaluates network traffic on KDD99 / CSE-CIC-IDS2018, so its results are Not Comparable with access-event detection in any case) |
| [2] | J. Zhang, G. Chen, H. Zhao, Y. Ye, Proc. IEEE ICCC 2022, 590–598 | Citation in v3 PDF | Not yet retrieved | After retrieval |
| [4] | T. Al-Shehari, M. Al-Razgan, T. Alfakih, R. A. Alsowail, S. Pandiaraj, "Insider Threat Detection Model Using Anomaly-Based Isolation Forest Algorithm," IEEE Access 11, 118170–118185, 2023, doi:10.1109/ACCESS.2023.3326750 | Title/venue/DOI via dblp, 2026-11-15 | Not retrieved | **No** (CERT dataset: Not Comparable with the synthetic data here; our Isolation Forest baseline is the same method family, NOT a reproduction of [4]) |
| [14] | D. Zhu, Y. Ma, Y. Liu, IEEE TrustCom 2020, 870–875 | Citation in v3 PDF | Not yet retrieved | After retrieval |
| [18] | F. Skopik et al., IEEE TDSC 20(4), 3158–3175, 2023 | Citation in v3 PDF | Not yet retrieved | After retrieval |
| [21] | L.-P. Yuan et al., IEEE/IFIP DSN 2021, 250–262 | Citation in v3 PDF | Not yet retrieved | After retrieval |
| [22] | S. Shehnepoor et al., IEEE TNNLS 35(5), 6628–6642, 2024 | Citation in v3 PDF | Not yet retrieved | After retrieval |
| [23] | E. Rossi et al., "Temporal Graph Networks for Deep Learning on Dynamic Graphs," ICML 2020 GRL Workshop (arXiv:2006.10637) | Citation in v3 PDF | Open access | Model definition only |
| [24] | **Placeholder in v3** ("[TODO: add full reference]"). Candidate: A. Modell, J. Larson, M. Turcotte, A. Bertiger, "A Graph Embedding Approach to User Behavior Anomaly Detection," GTA³ 2.0 workshop, IEEE BigData 2021 | Web search 2026-11-15; Microsoft Research publication page | Candidate's PDF linked from that page, not reviewed | **No: team must confirm it is the intended work** |
| [25] | **Placeholder in v3.** Found: X. Li, L. Cao, J. Bai, H. Lv, X. Du, "Prov-Trust: Zero-trust dynamic access control technology based on anomaly detection of provenance graph," Springer journal, 2026, doi:10.1007/s44443-026-00731-5 | Web search 2026-11-15 (search snippet; publisher page requires sign-in, journal name and volume not confirmed) | No (paywalled) | **No: full text needed** |

Sources consulted:
- https://link.springer.com/article/10.1007/s44443-026-00731-5
- https://www.microsoft.com/en-us/research/publication/a-graph-embedding-approach-to-user-behavior-anomaly-detection/
- CERT Insider Threat Test Dataset, CMU SEI, doi:10.1184/R1/12841247 (KiltHub; record not yet opened)

- https://dblp.dagstuhl.de/rec/journals/access/MezinaBT21.html
- https://dblp.dagstuhl.de/rec/journals/access/AlShehariAAAP23.html
