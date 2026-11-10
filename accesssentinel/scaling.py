"""Step 10: capacity-planning equations (8)-(12)."""
import math


def lambda_recheck(n_medium, t_recheck): return n_medium / t_recheck                              # Eq. 9
def lambda_eff(lambda_event, n_medium, t_recheck): return lambda_event + lambda_recheck(n_medium, t_recheck)   # Eq. 8
def n_tcn_workers(lam_eff, rho, mu): return math.ceil(lam_eff / (rho * mu))                       # Eq. 10
def expected_escalated(u_high, n_res, b_sweep, t_sweep, gamma, n_low, window=1.0):
    """Eq. 11. D3 writes B_sweep / T_sweep (a rate); multiply by `window` (same time unit) to get a count per window."""
    return u_high + n_res + (b_sweep / t_sweep) * window + gamma * n_low
def n_tgn_workers(e_u, eta, rho_p, mu_p): return math.ceil(e_u * eta / (rho_p * mu_p))            # Eq. 12
