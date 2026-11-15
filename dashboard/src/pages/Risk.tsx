import { useState } from "react";
import { CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { api, usePoll } from "../api";
import type { Me, Nav } from "../App";
import { Badge, Card, Empty, ErrorLine, fmtClock, fmtNum, fmtTime, SourceBadge, Tier } from "../ui";

export default function Risk({ nav, user, me }: { nav: Nav; user?: string; me: Me }) {
  const [tier, setTier] = useState("");
  const [source, setSource] = useState("");
  const list = usePoll<any>(`/api/risk/users?tier=${tier}&source=${source}&limit=500`, 10000);
  const settings = usePoll<any>("/api/settings", 0);
  const th = Object.fromEntries((settings.data?.settings ?? []).map((s: any) => [s.key, s.value]));
  return (
    <>
      <div className="page-h">
        <div>
          <h2>Individual Risk</h2>
          <p>Current TCN risk score S_ind per user (Redis) and its history (HDFS risk-scores). Model: trained TCN from the registry.</p>
        </div>
      </div>
      <div className="split">
        <Card
          title="Users by S_ind"
          actions={
            <div className="row">
              <select value={tier} onChange={(e) => setTier(e.target.value)}>
                <option value="">all tiers</option><option>high</option><option>medium</option><option>low</option>
              </select>
              <select value={source} onChange={(e) => setSource(e.target.value)}>
                <option value="">all</option><option value="live">live</option><option value="synthetic-replay">synthetic</option>
              </select>
            </div>
          }
        >
          <ErrorLine error={list.error} />
          {(list.data?.users ?? []).length === 0 ? (
            <Empty>No scored users.</Empty>
          ) : (
            <div className="scroll">
              <table>
                <thead><tr><th>User</th><th className="num">S_ind</th><th>Tier</th><th>Scored</th><th>Trigger</th></tr></thead>
                <tbody>
                  {list.data.users.map((r: any) => (
                    <tr key={r.u} className={"click" + (r.u === user ? " sel" : "")} onClick={() => nav("risk", r.u)}>
                      <td><span className="user">{r.u}<SourceBadge u={r.u} /></span></td>
                      <td className="num">{fmtNum(r.s_ind, 3)}</td>
                      <td><Tier tier={r.tier} /></td>
                      <td className="mono">{fmtTime(r.scored_at || r.t)}</td>
                      <td className="muted">{r.trigger}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Card>
        {user ? <UserDetail u={user} me={me} nav={nav} thLow={th["risk.theta_low"]} thHigh={th["risk.theta_high"]} /> : (
          <Card><Empty>Select a user to see the score timeline, recent accesses and enforcement status.</Empty></Card>
        )}
      </div>
    </>
  );
}

function UserDetail({ u, me, nav, thLow, thHigh }: { u: string; me: Me; nav: Nav; thLow?: number; thHigh?: number }) {
  const [hours, setHours] = useState(24);
  const { data, error, reload } = usePoll<any>(`/api/risk/users/${encodeURIComponent(u)}?hours=${hours}`, 15000);
  const [note, setNote] = useState("");
  const [msg, setMsg] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const act = async (what: "release" | "block") => {
    setMsg(null); setErr(null);
    if (!note.trim()) { setErr("A note is required for the audit log."); return; }
    try {
      await api(`/api/users/${encodeURIComponent(u)}/${what}`, "POST", { note });
      setMsg(what === "release" ? `${u} released (grace period applies).` : `${u} blocked by ${me.user}.`);
      setNote("");
      reload();
    } catch (e) {
      setErr((e as Error).message);
    }
  };
  const scores = (data?.scores ?? []).map((s: any) => ({ t: s.t, label: fmtClock(s.t), s: s.s_ind }));
  const d = data?.decision;
  return (
    <Card
      title={<>User {u} <SourceBadge u={u} /></>}
      actions={
        <>
          <select value={hours} onChange={(e) => setHours(Number(e.target.value))}>
            <option value={1}>1 h</option><option value={24}>24 h</option><option value={168}>7 d</option><option value={720}>30 d</option>
          </select>
          <button className="ghost small" onClick={() => nav("collusion", u)}>Collusion graph</button>
        </>
      }
    >
      <ErrorLine error={error} />
      {d && (
        <dl className="kv">
          <dt>Gateway decision</dt>
          <dd><Badge tone={d.blocked ? "bad" : (d.action ?? "Allow") === "Allow" ? "ok" : "warn"}>{d.action ?? (d.blocked ? "Blocked" : "Allow")}</Badge> {d.mode !== "enforce" && <Badge tone="warn">{d.mode}</Badge>}</dd>
          <dt>Reason</dt><dd>{d.reason ?? "—"} {d.actor ? `(by ${d.actor})` : ""}</dd>
          <dt>Expires</dt><dd>{fmtTime(d.expires_at)}</dd>
          {d.override_until && (<><dt>Release grace until</dt><dd>{fmtTime(d.override_until)}</dd></>)}
        </dl>
      )}
      <div style={{ marginTop: 10 }}>
        {scores.length === 0 ? <Empty>No risk scores in this window.</Empty> : (
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={scores}>
              <CartesianGrid stroke="#273341" strokeDasharray="3 3" />
              <XAxis dataKey="label" stroke="#8a99aa" fontSize={11} minTickGap={40} />
              <YAxis domain={[0, 1]} stroke="#8a99aa" fontSize={11} />
              <Tooltip contentStyle={{ background: "#161d25", border: "1px solid #273341" }} />
              {thLow !== undefined && <ReferenceLine y={thLow} stroke="#e0a43a" strokeDasharray="4 4" label={{ value: "θ_low", fill: "#e0a43a", fontSize: 10 }} />}
              {thHigh !== undefined && <ReferenceLine y={thHigh} stroke="#e5534b" strokeDasharray="4 4" label={{ value: "θ_high", fill: "#e5534b", fontSize: 10 }} />}
              <Line dataKey="s" stroke="#4ea1ff" dot={{ r: 2 }} isAnimationActive={false} name="S_ind" />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
      <div className="row" style={{ marginTop: 10 }}>
        <input style={{ flex: 1, minWidth: 200 }} placeholder="Note for the audit log (required)" value={note} onChange={(e) => setNote(e.target.value)} />
        <button className="ok" onClick={() => act("release")}>Release</button>
        <button className="danger" onClick={() => act("block")}>Block</button>
      </div>
      {msg && <div className="success">{msg}</div>}
      <ErrorLine error={err} />
      <h3 style={{ margin: "14px 0 6px" }}>Recent accesses (HDFS history)</h3>
      {(data?.events ?? []).length === 0 ? <Empty>No accesses in this window.</Empty> : (
        <div className="scroll" style={{ maxHeight: 300 }}>
          <table>
            <thead><tr><th>Time</th><th>Resource</th><th>Op</th><th>Outcome</th><th>Reason</th></tr></thead>
            <tbody>
              {[...data.events].reverse().slice(0, 200).map((e: any, i: number) => (
                <tr key={i} className={e.y === 0 ? "denied" : ""}>
                  <td className="mono">{fmtTime(e.t)}</td>
                  <td className="mono">{e.a}:{e.r} {e.s ? <Badge tone="warn">S</Badge> : null}</td>
                  <td>{e.o}</td>
                  <td>{e.y ? "allowed" : "denied"}</td>
                  <td className="muted">{e.reason}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <div className="muted" style={{ marginTop: 8, fontSize: 12 }}>
        Model versions in window: {[...new Set((data?.scores ?? []).map((s: any) => s.model_version))].join(", ") || "—"}
      </div>
    </Card>
  );
}
