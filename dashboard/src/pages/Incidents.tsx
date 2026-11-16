import { FormEvent, Fragment, useState } from "react";
import { api, usePoll } from "../api";
import type { Me, Nav } from "../App";
import { Badge, Card, Empty, ErrorLine, fmtNum, fmtTime, SourceBadge, User } from "../ui";

const VERDICTS: [string, string][] = [
  ["confirmed_malicious", "Confirmed malicious"],
  ["false_positive", "False positive"],
  ["benign_explained", "Benign (explained)"],
  ["needs_more_info", "Needs more information"],
];

export default function Incidents({ nav, id, me }: { nav: Nav; id?: string; me: Me }) {
  const [status, setStatus] = useState("open");
  const [kind, setKind] = useState("");
  const [source, setSource] = useState("");
  const list = usePoll<any>(`/api/incidents?status=${status}&kind=${kind}&source=${source}`, 5000);
  return (
    <>
      <div className="page-h">
        <div>
          <h2>Security Incidents</h2>
          <p>Incidents are opened from PDP enforcement decisions (Throttle … Block, Eq. 21) and TGN groups above θ_C. Analyst verdicts are the only labels used for retraining.</p>
        </div>
      </div>
      <div className="split">
        <Card
          title="Incidents"
          actions={
            <div className="row">
              <select value={status} onChange={(e) => setStatus(e.target.value)}>
                <option value="">all</option><option value="open">open</option><option value="triaged">triaged</option><option value="closed">closed</option>
              </select>
              <select value={kind} onChange={(e) => setKind(e.target.value)}>
                <option value="">all kinds</option><option value="individual">individual</option><option value="collective">collective</option>
              </select>
              <select value={source} onChange={(e) => setSource(e.target.value)}>
                <option value="">all sources</option><option value="live">live</option><option value="synthetic-replay">synthetic</option>
              </select>
            </div>
          }
        >
          <ErrorLine error={list.error} />
          {(list.data?.incidents ?? []).length === 0 ? <Empty>No incidents match.</Empty> : (
            <div className="scroll">
              <table>
                <thead><tr><th>Opened</th><th>Severity</th><th>Incident</th><th>Status</th><th>Verdict</th></tr></thead>
                <tbody>
                  {list.data.incidents.map((i: any) => (
                    <tr key={i.id} className={"click" + (i.id === id ? " sel" : "")} onClick={() => nav("incidents", i.id)}>
                      <td className="mono">{fmtTime(i.created_at)}</td>
                      <td><Badge tone={"sev-" + i.severity}>{i.severity}</Badge></td>
                      <td>{i.title} <SourceBadge source={i.source} /><div className="muted" style={{ fontSize: 12 }}>{i.users.join(", ")}</div></td>
                      <td>{i.status}{i.assigned_to ? <div className="muted" style={{ fontSize: 12 }}>{i.assigned_to}</div> : null}</td>
                      <td>{i.verdict ?? "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Card>
        {id ? <IncidentDetail id={id} nav={nav} me={me} onChange={list.reload} /> : <Card><Empty>Select an incident to review it.</Empty></Card>}
      </div>
      <div style={{ height: 14 }} />
      <div className="grid">
        <MissedDetection />
        <TrainingSet />
      </div>
    </>
  );
}

function IncidentDetail({ id, nav, me, onChange }: { id: string; nav: Nav; me: Me; onChange: () => void }) {
  const { data, error, reload } = usePoll<any>(`/api/incidents/${encodeURIComponent(id)}`, 10000);
  const [verdict, setVerdict] = useState("");
  const [note, setNote] = useState("");
  const [release, setRelease] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setMsg(null); setErr(null);
    if (!verdict) { setErr("Choose a verdict."); return; }
    setBusy(true);
    try {
      const r = await api<any>(`/api/incidents/${encodeURIComponent(id)}/feedback`, "POST", { verdict, note, release });
      setMsg(`Feedback ${r.feedback.id.slice(0, 8)} recorded and published to analyst-feedback (Kafka → HDFS).` +
        (r.released.length ? ` Released: ${r.released.join(", ")}.` : ""));
      setVerdict(""); setNote(""); setRelease(false);
      reload(); onChange();
    } catch (e2) {
      setErr((e2 as Error).message);
    } finally {
      setBusy(false);
    }
  };
  const assign = async () => {
    try { await api(`/api/incidents/${encodeURIComponent(id)}/assign`, "POST", {}); reload(); onChange(); }
    catch (e) { setErr((e as Error).message); }
  };
  if (error) return <Card><ErrorLine error={error} /></Card>;
  if (!data) return <Card><Empty>Loading…</Empty></Card>;
  const i = data;
  return (
    <Card
      title={<>{i.title} <SourceBadge source={i.source} /></>}
      actions={i.status !== "closed" && i.assigned_to !== me.user ? <button className="ghost small" onClick={assign}>Assign to me</button> : null}
    >
      {i.source === "synthetic-replay" && <div className="notice">This incident involves synthetic replay users (test data), not real people.</div>}
      <dl className="kv">
        <dt>Kind / severity</dt><dd>{i.kind} · <Badge tone={"sev-" + i.severity}>{i.severity}</Badge></dd>
        <dt>Status</dt><dd>{i.status}{i.verdict ? ` · verdict: ${i.verdict}` : ""}{i.assigned_to ? ` · ${i.assigned_to}` : ""}</dd>
        <dt>Activity window</dt><dd>{fmtTime(i.first_seen)} → {fmtTime(i.last_seen)}</dd>
        <dt>Max S_ind / S_coll</dt><dd>{fmtNum(i.max_s_ind, 3)} / {fmtNum(i.s_coll, 3)}</dd>
        {i.group_id && (<><dt>TGN group</dt><dd className="mono">{i.group_id}</dd></>)}
        <dt>Escalation paths</dt><dd>{i.paths.length ? i.paths.map((p: string) => <Badge key={p}>{p}</Badge>) : "—"}</dd>
      </dl>
      <h3 style={{ margin: "12px 0 6px" }}>Users and enforcement</h3>
      <table>
        <thead><tr><th>User</th><th>Enforcement</th><th></th></tr></thead>
        <tbody>
          {i.users.map((u: string) => (
            <tr key={u}>
              <td><User u={u} onClick={(x) => nav("risk", x)} /></td>
              <td>{i.enforcement?.[u] ?? "—"}</td>
              <td><button className="link" onClick={() => nav("collusion", u)}>graph</button></td>
            </tr>
          ))}
        </tbody>
      </table>
      <h3 style={{ margin: "12px 0 6px" }}>Evidence ({i.evidence.length})</h3>
      <div className="scroll" style={{ maxHeight: 220 }}>
        <table>
          <thead><tr><th>Time</th><th>Kind</th><th>Details</th></tr></thead>
          <tbody>
            {i.evidence.map((e: any) => (
              <tr key={e.id}>
                <td className="mono">{fmtTime(e.t)}</td>
                <td>{e.kind}</td>
                <td className="mono" style={{ fontSize: 12 }}>
                  {[e.user && `user=${e.user}`, e.s_ind !== undefined && e.s_ind !== null && `S_ind=${fmtNum(e.s_ind, 3)}`,
                    e.s_coll !== undefined && e.s_coll !== null && `S_coll=${fmtNum(e.s_coll, 3)}`, e.members && `members=${e.members.join(",")}`,
                    e.resources && `resources=${e.resources.join(",")}`, e.basis && `basis=${e.basis}`, e.actor && `by=${e.actor}`]
                    .filter(Boolean).join("  ")}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {i.feedback.length > 0 && (
        <>
          <h3 style={{ margin: "12px 0 6px" }}>Analyst feedback</h3>
          {i.feedback.map((f: any) => (
            <div key={f.id} className="muted" style={{ fontSize: 13 }}>
              {fmtTime(f.created_at)} · <b>{f.analyst}</b>: {f.verdict}{f.note ? ` — ${f.note}` : ""}
            </div>
          ))}
        </>
      )}
      <form onSubmit={submit} style={{ marginTop: 14, display: "flex", flexDirection: "column", gap: 8 }}>
        <h3>Review</h3>
        <div className="row">
          {VERDICTS.map(([v, label]) => (
            <label key={v} className="inline"><input type="radio" name="verdict" checked={verdict === v} onChange={() => setVerdict(v)} /> {label}</label>
          ))}
        </div>
        <textarea placeholder="Notes (investigation summary)" value={note} onChange={(e) => setNote(e.target.value)} maxLength={2000} />
        {(verdict === "false_positive" || verdict === "benign_explained") && (
          <label className="inline"><input type="checkbox" checked={release} onChange={(e) => setRelease(e.target.checked)} /> also release the blocked user(s)</label>
        )}
        <div><button type="submit" disabled={busy}>{busy ? "Saving…" : "Submit verdict"}</button></div>
        {msg && <div className="success">{msg}</div>}
        <ErrorLine error={err} />
      </form>
    </Card>
  );
}

function MissedDetection() {
  const [users, setUsers] = useState("");
  const [scope, setScope] = useState("individual");
  const [from, setFrom] = useState("");
  const [to, setTo] = useState("");
  const [note, setNote] = useState("");
  const [msg, setMsg] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setMsg(null); setErr(null);
    const t0 = Date.parse(from) / 1000, t1 = Date.parse(to) / 1000;
    if (!users.trim() || Number.isNaN(t0) || Number.isNaN(t1)) { setErr("Users and a time window are required."); return; }
    try {
      const r = await api<any>("/api/feedback/missed", "POST", {
        users: users.split(/[\s,]+/).filter(Boolean), scope, window_t0: t0, window_t1: t1, note,
      });
      setMsg(`Missed detection ${r.feedback.id.slice(0, 8)} recorded as a positive label.`);
      setUsers(""); setNote("");
    } catch (e2) {
      setErr((e2 as Error).message);
    }
  };
  return (
    <Card title="Report a missed detection (false negative)">
      <form onSubmit={submit} style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        <div className="row">
          <label style={{ flex: 1 }}>Users (comma separated)<input value={users} onChange={(e) => setUsers(e.target.value)} /></label>
          <label>Scope
            <select value={scope} onChange={(e) => setScope(e.target.value)}><option value="individual">individual</option><option value="collective">collective</option></select>
          </label>
        </div>
        <div className="row">
          <label>From<input type="datetime-local" value={from} onChange={(e) => setFrom(e.target.value)} /></label>
          <label>To<input type="datetime-local" value={to} onChange={(e) => setTo(e.target.value)} /></label>
        </div>
        <textarea placeholder="What happened" value={note} onChange={(e) => setNote(e.target.value)} />
        <div><button type="submit">Record</button></div>
        {msg && <div className="success">{msg}</div>}
        <ErrorLine error={err} />
      </form>
    </Card>
  );
}

function TrainingSet() {
  const ds = usePoll<any>("/api/retraining/dataset", 30000);
  const fb = usePoll<any>("/api/feedback?limit=20", 15000);
  return (
    <Card title="Supervised retraining set">
      <p className="muted" style={{ marginTop: 0 }}>
        Read from the HDFS <span className="mono">analyst-feedback</span> dataset. Only analyst verdicts with a label
        (confirmed malicious → 1; false positive / benign → 0) are included; model outputs never are.
      </p>
      <ErrorLine error={ds.error} />
      {ds.data && (
        <dl className="kv">
          <dt>Labelled feedback</dt><dd>{ds.data.feedback}</dd>
          {Object.entries(ds.data.by_scope_verdict ?? {}).map(([k, v]) => (<Fragment key={k + "k"}><dt>{k}</dt><dd>{v as number}</dd></Fragment>))}
        </dl>
      )}
      <h3 style={{ margin: "12px 0 6px" }}>Latest feedback (PostgreSQL)</h3>
      {(fb.data?.feedback ?? []).length === 0 ? <Empty>No feedback yet.</Empty> : (
        <table>
          <thead><tr><th>Time</th><th>Analyst</th><th>Scope</th><th>Verdict</th><th>Users</th></tr></thead>
          <tbody>
            {fb.data.feedback.map((f: any) => (
              <tr key={f.id}>
                <td className="mono">{fmtTime(f.created_at)}</td><td>{f.analyst}</td><td>{f.scope}</td><td>{f.verdict}</td>
                <td>{f.users.join(", ")} <SourceBadge source={f.source} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Card>
  );
}
