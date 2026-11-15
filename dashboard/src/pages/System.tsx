import { Fragment } from "react";
import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { usePoll } from "../api";
import { Card, Empty, ErrorLine, fmtBytes, fmtClock, fmtNum, Health } from "../ui";

const TOPIC_COLORS: Record<string, string> = {
  "access-events": "#4ea1ff", "risk-scores": "#3fb68b", "escalations.realtime": "#e5534b",
  "escalations.background": "#e0a43a", "group-scores": "#a77bf3", "pdp-decisions": "#dfe6ee",
};

export default function SystemPage() {
  const { data, error } = usePoll<any>("/api/system", 10000);
  const rates = (data?.rates ?? []).map((r: any) => ({ ...r, label: fmtClock(r.t) }));
  const esc = data?.escalation;
  return (
    <>
      <div className="page-h">
        <div>
          <h2>System Monitoring</h2>
          <p>Measured from the running stack: Kafka consumer lag and group membership, worker counters in Redis, HDFS usage and service reachability.</p>
        </div>
        <div className="muted">{data ? `snapshot ${fmtClock(data.t)}` : ""}</div>
      </div>
      <ErrorLine error={error} />
      <div className="stats">
        {Object.entries(data?.health ?? {}).map(([k, v]) => (
          <div key={k} className="stat"><div className="stat-label">{k}</div><div style={{ marginTop: 6 }}><Health value={v as string} /></div></div>
        ))}
        <div className="stat"><div className="stat-label">hdfs</div><div style={{ marginTop: 6 }}><Health value={data?.hdfs?.reachable ? "ok" : data ? "down" : undefined} /></div></div>
      </div>
      <div className="grid">
        <Card title="Stream processing: consumer groups">
          <table>
            <thead><tr><th>Group</th><th>State</th><th className="num">Members</th><th className="num">Lag</th></tr></thead>
            <tbody>
              {Object.entries(data?.lag ?? {}).map(([g, l]: [string, any]) => {
                const grp = data?.groups?.[g];
                return (
                  <tr key={g}>
                    <td className="mono">{g}</td>
                    <td>{grp?.state ?? "—"}</td>
                    <td className="num">{grp?.members ?? "—"}</td>
                    <td className="num" style={{ color: (l.total ?? 0) > 1000 ? "#e0a43a" : undefined }}>{l.error ? l.error : l.total}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </Card>
        <Card title="Topic throughput (messages / s)">
          {rates.length < 2 ? <Empty>Collecting samples…</Empty> : (
            <ResponsiveContainer width="100%" height={240}>
              <LineChart data={rates}>
                <CartesianGrid stroke="#273341" strokeDasharray="3 3" />
                <XAxis dataKey="label" stroke="#8a99aa" fontSize={11} minTickGap={40} />
                <YAxis stroke="#8a99aa" fontSize={11} />
                <Tooltip contentStyle={{ background: "#161d25", border: "1px solid #273341" }} formatter={(v: number) => v.toFixed(2)} />
                <Legend wrapperStyle={{ fontSize: 11 }} />
                {Object.entries(TOPIC_COLORS).map(([t, c]) => (
                  <Line key={t} dataKey={t} stroke={c} dot={false} isAnimationActive={false} />
                ))}
              </LineChart>
            </ResponsiveContainer>
          )}
        </Card>
        <Card title="TCN workers (individual risk)">
          {(data?.tcn_workers ?? []).length === 0 ? <Empty>No TCN worker heartbeat.</Empty> : (
            <table>
              <thead><tr><th>Worker</th><th>Model</th><th className="num">Scored</th><th className="num">Rechecks</th><th className="num">Last batch ms</th><th>Updated</th></tr></thead>
              <tbody>
                {data.tcn_workers.map((w: any) => (
                  <tr key={w.worker}>
                    <td className="mono">{w.worker}</td><td className="mono muted">{w.model_version}</td>
                    <td className="num">{w.scored}</td><td className="num">{w.rechecks}</td>
                    <td className="num">{fmtNum(w.last_batch_ms, 1)}</td><td className="mono">{fmtClock(w.updated_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Card>
        <Card title="TGN (collusion) latency, recent group scores">
          <dl className="kv">
            <dt>Samples</dt><dd>{data?.tgn_latency_ms?.n ?? "—"}</dd>
            <dt>Median ms</dt><dd>{fmtNum(data?.tgn_latency_ms?.median, 1)}</dd>
            <dt>p95 ms</dt><dd>{fmtNum(data?.tgn_latency_ms?.p95, 1)}</dd>
          </dl>
        </Card>
        <Card title="Escalation (last hour)">
          {esc ? (
            <dl className="kv">
              <dt>Analysis windows</dt><dd>{esc.last_hour.windows}</dd>
              <dt>Users escalated</dt><dd>{esc.last_hour.escalated}</dd>
              {Object.entries(esc.last_hour.by_path).map(([p, n]) => (<Fragment key={p}><dt>Path {p}</dt><dd>{n as number}</dd></Fragment>))}
              {Object.entries(esc.stats ?? {}).map(([k, v]) => (<Fragment key={k}><dt className="mono">{k}</dt><dd>{v as string}</dd></Fragment>))}
            </dl>
          ) : <Empty>—</Empty>}
        </Card>
        <Card title="Policy decision point">
          <dl className="kv">
            {Object.entries(data?.pdp ?? {}).map(([k, v]) => (<Fragment key={k}><dt className="mono">{k}</dt><dd>{v as string}</dd></Fragment>))}
          </dl>
          {Object.keys(data?.pdp ?? {}).length === 0 && <Empty>No PDP counters yet.</Empty>}
        </Card>
        <Card title="HDFS history lake">
          {data?.hdfs?.reachable ? (
            <dl className="kv">
              <dt>Base</dt><dd className="mono">{data.hdfs.base}</dd>
              <dt>Size</dt><dd>{fmtBytes(data.hdfs.bytes)}</dd>
              <dt>Files</dt><dd>{data.hdfs.files ?? "—"}</dd>
              <dt>Directories</dt><dd>{data.hdfs.directories ?? "—"}</dd>
              <dt>Event partitions</dt><dd>{data.hdfs.event_partitions}</dd>
            </dl>
          ) : <Empty>{data?.hdfs?.error ?? "unreachable"}</Empty>}
        </Card>
      </div>
    </>
  );
}
