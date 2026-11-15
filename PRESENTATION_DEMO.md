# Presentation demo: "Create a new user and attack. Does AccessSentinel detect it?"

A step-by-step script for the live demo, with what to click, what to say, and what to do if something goes wrong.

---

## The short answer (for your opening)

> "Yes. AccessSentinel has no list of known attackers. It scores **behaviour**. I will create an account that has
> never existed, use it normally, which is not blocked, then attack with it. Within seconds the system blocks it,
> logs it out and opens an incident for the analyst."

---

## Before the presentation

**The day before:**
1. Start everything: `docker compose up -d` and wait about 2 minutes.
2. Check all services are healthy:
   `docker compose run --rm -T tools python scripts/smoke_dashboard.py` should end with `30/30 checks passed`.
3. Do one full rehearsal with the steps below.

**30 minutes before:**
1. Make sure Docker Desktop is running and `docker compose ps` shows everything `healthy`.
2. **Do not run test attacks in the hour before the demo** (see Q&A: *"Why does it say collusion?"*).
3. Open three browser windows:

| Window | URL | Signed in as |
|---|---|---|
| A: Admin | http://localhost:8080 | `secadmin` |
| B: Attacker | http://localhost:8000 | *(nobody yet)*. Use a **private/incognito** window so the cookies stay separate |
| C: Analyst | http://localhost:8080 | `analyst1` (another private window or another browser) |

The `secadmin` / `analyst1` password is `AS_ANALYST_PASSWORD` in `.env`.

---

## The demo (about 5 minutes)

### Step 1: The admin creates a brand-new user (Window A)
- Click **User Accounts** in the menu.
- Fill in: User ID `mallory`, Full name `Mallory Test`, Department `Sales`, Role `staff`. Leave the password empty.
- Click **Create user**. A **one-time password** appears. Click **Copy**.

> 🗣 "This account has never existed. The AI has **no history** for it, so it can't recognise it. It can only
> judge what it does."

Point at the grey line under the form: *staff may use: directory, handbook, policies*. Everything else is denied.

*(If `mallory` already exists from a rehearsal, use `mallory2`, `mallory3`, …)*

### Step 2: The new user works normally (Window B)
- Sign in as `mallory` with the copied password.
- Open a few allowed pages, slowly, like a normal employee: **HR → Directory**, **Docs → Handbook**,
  **Docs → Policies**, a directory entry or two. Spend about 30–60 seconds.
- In Window C (analyst), open **Individual Risk** and click `mallory`: the risk score stays **Low**.

> 🗣 "Normal work is not punished. Risk is low, and nothing is blocked."

### Step 3: The attack (Window B)
In the address bar, quickly open pages a staff member is **not** allowed to see, one after another:

```
http://localhost:8000/apps/hr/payroll
http://localhost:8000/apps/finance/payments
http://localhost:8000/apps/hr/admin
http://localhost:8000/apps/finance/vendor-banking
http://localhost:8000/apps/docs/board-minutes
http://localhost:8000/apps/hr/reviews
```

Each one is denied ("forbidden"). Keep going quickly. After a few attempts the page changes to
**"Access is suspended by the continuous-monitoring risk policy"**, or you are sent back to the login page.

> 🗣 "The gateway sent every request to Kafka, the TCN scored the behaviour, the risk crossed the threshold, and
> the Policy Decision Point blocked the account. It also **ended the session**."

- Try to sign in again as `mallory`: **refused** ("Access is suspended…").

In the rehearsal, a person clicking one forbidden page every 2–3 seconds was blocked after about 4 attempts
(10 s). Going faster gets caught sooner. If anything stalls, use the backup script below: it is the same
attack, automated.

### Step 4: What the analyst sees (Window C)
1. **Overview**: blocked users went up by 1, and an open incident appeared.
2. **Individual Risk → mallory**: the score timeline jumps from Low to **High** (above the red θ_high line);
   status **Blocked**.
3. **Security Incidents**: open *"High individual risk: mallory"* to show the evidence (score, model version,
   enforcement).
4. Choose **Confirmed malicious**, add a note, click **Submit verdict**.

> 🗣 "Only this human verdict becomes training data. The model never trains on its own guesses."

5. Optional: on the **Supervised retraining set** card at the bottom of the page, the new label appears within
   about a minute.

### Step 5 (optional): Collusion graph
**Collusion Detection** → type `mallory` → **Show graph**: the user and every resource they touched; red links
are ones the TGN considers suspicious.

---

## Backup: run the whole scenario automatically

If anything goes wrong live, or for a second run, this does all of the steps above on the real system and
prints the results:

```powershell
docker compose run --rm -T tools python scripts/demo_new_user.py --user mallory-auto
```

Example output from the rehearsal (2026-11-11, brand-new account, synthetic-trained model):

```
  PASS  new user signs in at the gateway
  PASS  normal work is NOT blocked           [8 requests; tier low]
  PASS  attack detected and the account blocked  [after 11 attack requests, 2.3 s]
  PASS  open session ended
  PASS  new login refused while blocked
  PASS  PDP decision                          [reason=individual, S_ind=0.63 >= theta_high 0.6]
  PASS  incident opened for the analyst
8/8 checks passed
```

Other variants: `--attack enumerate` (scraping records the user *is* allowed to read, at machine speed; it was
caught after about 25 s in the rehearsal) and `--attack-pause 2.5` (one attempt every 2.5 s, like a person
clicking).

---

## Measured in the rehearsal (brand-new accounts, live system)

| Attack | Speed | Result |
|---|---|---|
| Probing forbidden pages | 5 per second | Blocked after 8–11 attempts, about 2 s |
| Enumerating allowed records | 20 per second | Blocked after about 430 requests, 25 s |
| Probing forbidden pages | 1 every 2.5 s (by hand), no other test activity | Blocked after **4 attempts, 10 s**, by individual risk (S_ind 0.94) |
| Probing forbidden pages | 1 every 2.5 s, right after other test accounts | Blocked after 3 attempts, 7.5 s, by **collusion** detection (see Q&A) |
| Normal work | 1 every 4 s | Never blocked (risk stayed Low) |

---

## Questions your professor may ask

**"Isn't this a pre-recorded attack the model memorised?"**
No. There is no list of attackers. The model was trained on synthetic users and has never seen this account.
It scores signals such as denied requests, unknown pages, request speed, stepping through record IDs, exports
and odd hours, relative to what that user normally does.

**"Why did it say *collusion* instead of individual risk?"**
If several new accounts probed the same forbidden pages within the last hour, for example rehearsal runs, the
TGN correctly sees them as a coordinated group and blocks the whole group. That is the second detector working.
For a clean "individual" result, don't run test attacks in the hour before the demo.

**"What would it miss?"**
A careful insider who only uses what their role allows, slowly and at normal hours, looks like normal work.
That is a known limit of behaviour-based detection; collusion detection and analyst review help. New accounts
also have no personal history at first (a cold start), so detection relies on general signals until about 30
requests have been seen.

**"What if the AI is down?"**
Blocks are enforced by the gateway from a cached decision. If the models stop, blocked users stay blocked. This
was tested in Phase 8.

**"How well does it work on real data?"**
Unknown: all training and evaluation used synthetic data (TCN: 13/13 test attacks detected; TGN: 8/8 groups).
The numbers show the system works as designed, not real-world accuracy.

**"Can users register themselves?"**
No, deliberately. In a company, IT or security creates accounts. Self-registration would let anyone into the
protected applications.

---

## After the demo

- Unblock `mallory`: Individual Risk → mallory → note → **Release**. Blocks also expire by themselves after
  24 hours.
- Or remove the account: **User Accounts** → **Deactivate**.
