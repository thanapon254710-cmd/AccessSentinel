import { useMemo, useState } from "react";
import { useLive, usePoll } from "../api";
import type { Nav } from "../App";
import { Badge, Card, Empty, ErrorLine, fmtClock, User } from "../ui";

export default function Activity({ nav }: { nav: Nav }) {
  const [user, setUser] = useState("");
  const [app, setApp] = useState("");
  const [denied, setDenied] = useState(false);
  const [source, setSource] = useState("");
  const q = new URLSearchParams({ limit: "300", user, app_id: app, denied: String(denied), source }).toString();
  const { data, error } = usePoll<any>(`/api/activity/recent?${q}`, 0);
  const live = useLive(["event", "escalation"], 400);

  const events = useMemo(() => {
    const seen = new Set<string>();
    const fromWs = live.items.filter((m) => m.type === "event").map((m) => m.data);
    const out: any[] = [];
    for (const e of [...fromWs, ...(data?.events ?? [])]) {
      if (seen.has(e.event_id)) continue;
      seen.add(e.event_id);
      if (user && e.u !== user) continue;
      if (app && e.a !== app) continue;
      if (denied && e.y !== 0) continue;
      const src = e.meta?.source ?? "live";
      if (source && !src.startsWith(source.slice(0, 5))) continue;
      out.push(e);
    }
    return out.sort((a, b) => b.t - a.t).slice(0, 500);
  }, [live.items, data, user, app, denied, source]);
  const escalations = live.items.filter((m) => m.type === "escalation").map((m) => m.data).slice(0, 50);

  return (
    <>
      <div className="page-h">
        <div>
          <h2>Access Activity</h2>
          <p>Access events published by the gateway to Kafka, streamed live over WebSocket.</p>
        </div>
        <div className="muted">
          <span className={"dot " + (live.connected ? "on" : "off")} />
          {live.connected ? "live" : "disconnected"}
        </div>
      </div>
      <ErrorLine error={error} />
      <Card
        title={`Events (${events.length})`}
        actions={
          <div className="row">
            <input placeholder="user" value={user} onChange={(e) => setUser(e.target.value.trim())} />
            <input placeholder="app" value={app} onChange={(e) => setApp(e.target.value.trim())} />
            <select value={source} onChange={(e) => setSource(e.target.value)}>
              <option value="">all sources</option>
              <option value="live">live only</option>
              <option value="synthetic-replay">synthetic only</option>
            </select>
            <label className="inline"><input type="checkbox" checked={denied} onChange={(e) => setDenied(e.target.checked)} /> denied only</label>
          </div>
        }
      >
        {events.length === 0 ? (
          <Empty>No events yet. The buffer starts when the API starts; generate traffic through the gateway.</Empty>
        ) : (
          <div className="scroll">
            <table>
              <thead>
                <tr><th>Time</th><th>User</th><th>App</th><th>Resource</th><th>Sens.</th><th>Operation</th><th>Outcome</th><th>Reason</th><th>Client IP</th></tr>
              </thead>
              <tbody>
                {events.map((e) => (
                  <tr key={e.event_id} className={e.y === 0 ? "denied" : ""}>
                    <td className="mono">{fmtClock(e.t)}</td>
                    <td><User u={e.u} onClick={(u) => nav("risk", u)} /></td>
                    <td>{e.a}</td>
                    <td className="mono">{e.r}</td>
                    <td>{e.s ? <Badge tone="warn">sensitive</Badge> : ""}</td>
                    <td>{e.o}</td>
                    <td>{e.y ? <Badge tone="ok">allowed</Badge> : <Badge tone="bad">denied</Badge>}</td>
                    <td className="muted">{e.meta?.reason ?? ""}</td>
                    <td className="mono">{e.c?.ip ?? ""}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
      <div style={{ height: 14 }} />
      <Card title="Escalations (live, TCN → TGN)">
        {escalations.length === 0 ? (
          <Empty>No escalations since this page opened.</Empty>
        ) : (
          <table>
            <thead><tr><th>Time</th><th>User</th><th>Queue</th><th>Paths</th></tr></thead>
            <tbody>
              {escalations.map((x) => (
                <tr key={x.escalation_id}>
                  <td className="mono">{fmtClock(x.t)}</td>
                  <td><User u={x.u} onClick={(u) => nav("collusion", u)} /></td>
                  <td>{x.priority}</td>
                  <td>{(x.paths ?? []).map((p: string) => <Badge key={p}>{p}</Badge>)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Card>
    </>
  );
}
