import { Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { usePoll } from "../api";
import type { Nav } from "../App";
import { Badge, Card, Empty, ErrorLine, fmtClock, fmtNum, Stat, User } from "../ui";

const TIERS = ["low", "medium", "high"];

export default function Overview({ nav }: { nav: Nav }) {
  const { data, error } = usePoll<any>("/api/overview", 5000);
  const tiers = data?.tiers ?? {};
  const tierData = TIERS.map((t) => ({ tier: t, live: tiers[`live:${t}`] ?? 0, synthetic: tiers[`synthetic-replay:${t}`] ?? 0 }));
  const rate = (data?.rate_history ?? []).map((r: any) => ({ t: fmtClock(r.t), events: Number(r.events.toFixed(2)) }));
  const openInc = data?.open_incidents ?? {};
  return (
    <>
      <div className="page-h">
        <div>
          <h2>Overview</h2>
          <p>Live state of the deployment. Counts come from Redis, PostgreSQL and Kafka; synthetic replay data is counted separately.</p>
        </div>
      </div>
      <ErrorLine error={error} />
      <div className="stats">
        <Stat label="Monitored users (scored)" value={data?.monitored_users?.live ?? "—"} sub={`+ ${data?.monitored_users?.["synthetic-replay"] ?? 0} synthetic`} />
        <Stat label="Protected applications" value={data?.applications ?? "—"} />
        <Stat label="Access events / s" value={data ? fmtNum(data.events_per_second, 2) : "—"} sub="Kafka access-events, last 5 s" />
        <Stat label="Open incidents" value={openInc.live ?? 0} sub={`+ ${openInc["synthetic-replay"] ?? 0} synthetic`} tone={(openInc.live ?? 0) > 0 ? "warn" : undefined} />
        <Stat label="Blocked users" value={data?.blocked_users?.live ?? "—"} sub={`+ ${data?.blocked_users?.["synthetic-replay"] ?? 0} synthetic`} tone={(data?.blocked_users?.live ?? 0) > 0 ? "bad" : undefined} />
        <Stat label="Sessions revoked (24 h)" value={data?.sessions_revoked_24h ?? "—"} />
        <Stat label="PDP mode" value={data?.pdp_mode ?? "—"} tone={data?.pdp_mode === "enforce" ? "ok" : "warn"} />
      </div>
      <div className="grid">
        <Card title="Access event rate (measured from Kafka offsets)">
          {rate.length < 2 ? (
            <Empty>Collecting samples (every 5 s)…</Empty>
          ) : (
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={rate}>
                <CartesianGrid stroke="#273341" strokeDasharray="3 3" />
                <XAxis dataKey="t" stroke="#8a99aa" fontSize={11} minTickGap={40} />
                <YAxis stroke="#8a99aa" fontSize={11} />
                <Tooltip contentStyle={{ background: "#161d25", border: "1px solid #273341" }} />
                <Line type="monotone" dataKey="events" stroke="#4ea1ff" dot={false} isAnimationActive={false} name="events/s" />
              </LineChart>
            </ResponsiveContainer>
          )}
        </Card>
        <Card title="Users by current risk tier (TCN S_ind)">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={tierData}>
              <CartesianGrid stroke="#273341" strokeDasharray="3 3" />
              <XAxis dataKey="tier" stroke="#8a99aa" fontSize={11} />
              <YAxis stroke="#8a99aa" fontSize={11} allowDecimals={false} />
              <Tooltip contentStyle={{ background: "#161d25", border: "1px solid #273341" }} />
              <Legend />
              <Bar dataKey="live" stackId="a" fill="#4ea1ff" isAnimationActive={false} />
              <Bar dataKey="synthetic" stackId="a" fill="#a77bf3" isAnimationActive={false} />
            </BarChart>
          </ResponsiveContainer>
        </Card>
        <Card title="Recent PDP decisions (since this API started)" wide>
          {(data?.recent_decisions ?? []).length === 0 ? (
            <Empty>No decisions observed yet.</Empty>
          ) : (
            <table>
              <thead>
                <tr><th>Time</th><th>User</th><th>Decision</th><th>Transition</th><th>Reason</th><th>Mode</th><th className="num">S_ind</th><th className="num">S_coll</th><th>Actor</th></tr>
              </thead>
              <tbody>
                {data.recent_decisions.map((d: any) => (
                  <tr key={d.decision_id}>
                    <td>{fmtClock(d.t)}</td>
                    <td><User u={d.u} onClick={(u) => nav("risk", u)} /></td>
                    <td><Badge tone={d.decision === "Block" ? "bad" : "ok"}>{d.decision}</Badge></td>
                    <td>{d.transition}</td>
                    <td>{d.reason}</td>
                    <td>{d.mode}{d.enforced ? "" : " (not enforced)"}</td>
                    <td className="num">{fmtNum(d.s_ind)}</td>
                    <td className="num">{fmtNum(d.s_coll)}</td>
                    <td>{d.actor}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Card>
      </div>
    </>
  );
}
