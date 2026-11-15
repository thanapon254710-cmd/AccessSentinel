# AccessSentinel — Project Summary

*A plain-language overview. For technical details see `README.md`, `docs/PROGRESS.md` and `docs/SETUP_WINDOWS.md`.*

---

## What is AccessSentinel?

AccessSentinel is a **security system that watches how people use company applications** and stops accounts
that behave suspiciously, while they are still active.

It looks for two kinds of threat:

| Threat | Example | Which AI model finds it |
|---|---|---|
| **One risky user** | A stolen account suddenly downloads payroll files at 3 a.m. | **TCN**, which learns each user's normal habits over time |
| **Users working together** | Several employees each copy a small piece of sensitive data so no single person looks suspicious | **TGN**, which looks at the network of who accessed what, and when |

When the risk is high enough, the system **blocks the account within seconds** and **logs the user out**. A
security analyst then reviews the case in a web dashboard. The analyst's verdict is used to **retrain the AI
models**, so they improve over time.

This is a **working research prototype** of the design in *Deliverable 3*. Everything runs on your own computer
in Docker.

---

## How it works (in one picture)

```
  Employee ──► Access Gateway ──► Protected apps (HR, Finance, Docs)
                     │
                     ▼  every request becomes an "access event"
                   Kafka  (event stream) ──────────────► HDFS (long-term history)
                     │
                     ▼
        TCN: "Is this user acting strangely?"   → risk score 0–1 (Low / Medium / High)
                     │
                     ▼
        Escalation: 4 ways to pick users for a closer look
          A. High risk   B. Many users on one sensitive file   C. Regular sweep   D. Random sample
                     │
                     ▼
        TGN: "Is this user part of a group working together?"
                     │
                     ▼
        Policy Decision Point → Allow or Block → gateway enforces it, active sessions are ended
                     │
                     ▼
        Dashboard: an analyst reviews the incident → verdict → saved to HDFS → used for retraining
```

---

## What has been built

| Phase | What it delivers | Status |
|---|---|---|
| 1 | Review of the design and the existing code, plan of work | ✅ Done |
| 2 | Infrastructure: Kafka, HDFS, PostgreSQL, Redis in Docker | ✅ Done |
| 3 | Access Gateway (secure login, permissions) + 3 demo apps (HR, Finance, Docs) | ✅ Done |
| 4 | Reliable event streaming to Kafka, history stored in HDFS | ✅ Done |
| 5 | TCN model: trained, tested and running live | ✅ Done |
| 6 | The four escalation paths (A–D) | ✅ Done |
| 7 | TGN model for collusion: trained, tested and running live | ✅ Done |
| 8 | Blocking and enforcement (Allow / Block, session termination) | ✅ Done |
| 9 | Analyst dashboard, incident review, feedback and retraining | 🟡 Almost done (see "What's left") |
| 10 | Full integration tests and final experiments | ⬜ Not started |

---

## Key results

> ⚠️ **All results come from synthetic (computer-generated) data**, not real company logs. They show that the
> system works as designed. They do not prove how well it would do in a real organisation.

**TCN (one risky user)**, tested on attackers it had never seen:
- Detected **13 out of 13** attacks.
- Correctly flagged **92 %** of attack activity at the blocking threshold.
- Wrongly flagged about **6 %** of normal user-days (a simple baseline: 9 %).

**TGN (users working together):**
- Found **8 out of 8** colluding groups in the test period.
- Fewer false alarms than simpler rules. For example, it wrongly flagged 12 % of normal team meetings; a simple
  rule flagged 41 %.
- Group membership is not exact: about half of the users it puts in a group are real colluders. Analysts review
  every case.

**Live system:**
- A probing attack through the real gateway was **blocked after 7 requests, 1.4 seconds after it began**. The
  attacker's open session was ended and new logins were refused.
- **A brand-new account** (created during the test, no history) worked normally without being blocked, then
  was **blocked after 11 attack requests (2.3 s)** when it started probing forbidden pages. Step-by-step demo:
  `PRESENTATION_DEMO.md`.
- If the AI models stop, **blocked users stay blocked**: security does not depend on the models being available.

---

## The analyst dashboard (Phase 9)

Open **http://localhost:8080** and sign in as a security user:

| Account | Role | Can do |
|---|---|---|
| `analyst1` | Security analyst | View everything, review incidents, give verdicts, release or block users |
| `secadmin` | Security admin | All of the above, plus change settings, edit which resources are sensitive, register apps, switch model versions |

The password for both is `AS_ANALYST_PASSWORD` in your `.env` file. Normal employees (alice, bob, …) cannot
sign in to the dashboard.

| Page | What you see |
|---|---|
| **Overview** | Number of users, live event rate, risk levels, open incidents, blocked users |
| **Access Activity** | Every access request, live |
| **Individual Risk** | Each user's risk score over time, their recent accesses, Release / Block buttons |
| **Collusion Detection** | Interactive graph of users and the resources they touched; suspicious links in red |
| **Security Incidents** | Cases to review; choose *Confirmed malicious*, *False positive*, *Benign* or *Needs more info* |
| **System Monitoring** | Health of every service, processing delays, model speed, storage use |
| **User Accounts** | Employees being monitored; the admin creates, deactivates or resets accounts here (no self-registration) |
| **Settings** | Risk thresholds, escalation settings, sensitive resources, apps, model versions |

Anything from test data is marked with a purple **synthetic** label.

**How feedback improves the models:** only an analyst's verdict counts as a training label. The models never
learn from their own guesses. A retrained model is compared with the current one, and it is only switched in if
it is at least as good.

---

## How to run it

You need **Docker Desktop** with about 8 GB of memory for Docker (the default on a 16 GB PC). Run these from the project folder:

```powershell
copy .env.example .env               # then replace every "change-me" with a random value
docker compose up -d --build         # start everything (first time takes a while)
```

Then open:
- **Demo apps through the gateway:** http://localhost:8000. Demo users such as `alice` use `AS_DEMO_USER_PASSWORD`.
- **Analyst dashboard:** http://localhost:8080.
- **HDFS file browser:** http://localhost:9870.

To stop: `docker compose down` (your data is kept).

**Easiest on Windows:** start Docker Desktop, then double-click **`start.bat`**. It creates the settings file with random passwords, starts everything and opens the dashboard; `stop.bat` stops it.

Full step-by-step guide: `docs/SETUP_WINDOWS.md`. **On another computer:** clone the repository, create `.env`, `docker compose up -d --build`. The trained models are included, so no training is needed (details: `README.md`, *Run on another computer*).

---

## Security safeguards

- Passwords are stored only as secure hashes (Argon2). No secrets are written in the code or the logs.
- Blocking is enforced at the gateway itself. A block stays in force even if the AI is down.
- The dashboard is for security staff only, with separate analyst and admin roles.
- Dashboard sign-in is protected against cross-site tricks (CSRF) and against password guessing (account
  lockout).
- Every important action is written to an audit log: logins, blocks, releases, verdicts, settings changes.

---

## What's left

1. **Finish Phase 9:** an analyst confirms a live incident in the dashboard (for example ivan's), then the TCN is
   retrained on that verdict and the model switch is tested.
2. **Phase 10:** full end-to-end integration tests and the final experiments.

**Known limits:**
- Results are from synthetic data only.
- No single sign-on or two-factor login for the dashboard.
- Retraining is started by hand, not on a schedule.
- Blocking is all-or-nothing: no "ask for extra verification" step yet.

---

## Where to find more

| Document | Contents |
|---|---|
| `README.md` | Technical overview: which code implements which part of the design |
| `docs/SETUP_WINDOWS.md` | Installation, commands, troubleshooting |
| `docs/PROGRESS.md` | Detailed log of every phase, test results and issues found |
| `docs/PHASE1_AUDIT.md` | Original review of the design and the roadmap |
| `experiments/results/` | Full model evaluation reports |
