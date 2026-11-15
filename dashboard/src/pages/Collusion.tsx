import cytoscape from "cytoscape";
import { FormEvent, useEffect, useRef, useState } from "react";
import { usePoll } from "../api";
import type { Nav } from "../App";
import { Badge, Card, Empty, ErrorLine, fmtClock, fmtNum, fmtTime, SourceBadge, User } from "../ui";

export default function Collusion({ nav, user }: { nav: Nav; user?: string }) {
  const [onlyGroups, setOnlyGroups] = useState(true);
  const groups = usePoll<any>(`/api/collusion/groups?only_groups=${onlyGroups}&limit=200`, 5000);
  const [input, setInput] = useState(user ?? "");
  useEffect(() => setInput(user ?? ""), [user]);
  const go = (e: FormEvent) => {
    e.preventDefault();
    if (input.trim()) nav("collusion", input.trim());
  };
  return (
    <>
      <div className="page-h">
        <div>
          <h2>Collusion Detection</h2>
          <p>Group scores from the trained TGN for users escalated by the four paths, plus each user's user–resource graph from the live edge index.</p>
        </div>
        <form onSubmit={go} className="row">
          <input placeholder="user id" value={input} onChange={(e) => setInput(e.target.value)} />
          <button type="submit">Show graph</button>
        </form>
      </div>
      {user && <GraphView u={user} nav={nav} />}
      <div style={{ height: 14 }} />
      <Card
        title="TGN group scores (live, since API start)"
        actions={<label className="inline"><input type="checkbox" checked={onlyGroups} onChange={(e) => setOnlyGroups(e.target.checked)} /> coupled groups only</label>}
      >
        <ErrorLine error={groups.error} />
        {(groups.data?.group_scores ?? []).length === 0 ? <Empty>No group scores observed yet.</Empty> : (
          <div className="scroll">
            <table>
              <thead><tr><th>Time</th><th>Seed</th><th>Members</th><th className="num">S_coll</th><th className="num">θ_coll</th><th>Paths</th><th className="num">Inference ms</th><th>Model</th></tr></thead>
              <tbody>
                {groups.data.group_scores.map((g: any) => (
                  <tr key={g.score_id} className="click" onClick={() => nav("collusion", g.u)}>
                    <td className="mono">{fmtClock(g.t)}</td>
                    <td><User u={g.u} /></td>
                    <td>{(g.members ?? []).join(", ")}</td>
                    <td className="num">{g.s_coll === null || g.s_coll === undefined ? "—" : <span style={{ color: g.above_theta_coll ? "#e5534b" : undefined }}>{fmtNum(g.s_coll, 3)}</span>}</td>
                    <td className="num">{fmtNum(g.theta_coll)}</td>
                    <td>{(g.paths ?? []).map((p: string) => <Badge key={p}>{p}</Badge>)}</td>
                    <td className="num">{fmtNum(g.inference_ms, 1)}</td>
                    <td className="mono muted">{g.model_version}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </>
  );
}

function GraphView({ u, nav }: { u: string; nav: Nav }) {
  const { data, error } = usePoll<any>(`/api/collusion/graph?u=${encodeURIComponent(u)}`, 0);
  const ref = useRef<HTMLDivElement>(null);
  const [sel, setSel] = useState<any>(null);
  const [showCo, setShowCo] = useState(true);
  useEffect(() => {
    if (!ref.current || !data) return;
    const tau = data.tau_edge ?? 0.5;
    const elements: cytoscape.ElementDefinition[] = [
      ...data.nodes.map((n: any) => ({ data: { ...n, synthetic: n.type === "user" && n.label.startsWith("syn-") ? 1 : 0 } })),
      ...data.edges
        .filter((e: any) => showCo || e.type !== "co-access")
        .map((e: any, i: number) => ({ data: { id: `e${i}`, ...e, hot: e.type === "access" && e.score >= tau ? 1 : 0 } })),
    ];
    const cy = cytoscape({
      container: ref.current,
      elements,
      wheelSensitivity: 0.3,
      style: [
        { selector: "node", style: { label: "data(label)", color: "#dfe6ee", "font-size": 9, "text-valign": "bottom", "text-margin-y": 3 } },
        { selector: "node[type='user']", style: { "background-color": "#4ea1ff", width: 18, height: 18 } },
        { selector: "node[synthetic=1]", style: { "background-color": "#a77bf3" } },
        { selector: "node[?seed]", style: { "border-width": 3, "border-color": "#ffffff", width: 24, height: 24 } },
        { selector: "node[type='resource']", style: { shape: "round-rectangle", "background-color": "#3a4756", width: 14, height: 14 } },
        { selector: "node[type='application']", style: { shape: "hexagon", "background-color": "#e0a43a", width: 20, height: 20 } },
        { selector: "edge", style: { width: 1, "line-color": "#3b4a5c", "curve-style": "bezier", opacity: 0.8 } },
        { selector: "edge[type='access']", style: { width: "mapData(count, 1, 30, 1, 5)" as any } },
        { selector: "edge[sensitive=1]", style: { "line-color": "#6c7d90" } },
        { selector: "edge[hot=1]", style: { "line-color": "#e5534b", width: 3 } },
        { selector: "edge[type='co-access']", style: { "line-style": "dashed", "line-color": "#a77bf3", opacity: 0.5 } },
        { selector: "edge[type='membership']", style: { "line-color": "#2a3542", width: 1, opacity: 0.5 } },
        { selector: ":selected", style: { "overlay-color": "#4ea1ff", "overlay-opacity": 0.25 } },
      ],
      layout: { name: "cose", animate: false, nodeRepulsion: () => 9000, idealEdgeLength: () => 60 } as any,
    });
    cy.on("tap", "node, edge", (ev) => setSel(ev.target.data()));
    cy.on("dbltap", "node[type='user']", (ev) => nav("collusion", ev.target.data("label")));
    return () => cy.destroy();
  }, [data, showCo, nav]);

  const a = data?.analysis;
  return (
    <div className="split">
      <Card
        title={<>User–resource graph around {u} <SourceBadge u={u} /></>}
        actions={<label className="inline"><input type="checkbox" checked={showCo} onChange={(e) => setShowCo(e.target.checked)} /> co-access edges</label>}
      >
        <ErrorLine error={error} />
        {data && data.nodes.length <= 1 ? <Empty>No accesses by {u} in the TGN window.</Empty> : <div ref={ref} className="graph" />}
        <div className="legend">
          <span><i style={{ background: "#4ea1ff" }} />user</span>
          <span><i style={{ background: "#a77bf3" }} />synthetic user</span>
          <span><i style={{ background: "#3a4756" }} />resource</span>
          <span><i style={{ background: "#e0a43a" }} />application</span>
          <span><i style={{ background: "#e5534b" }} />access edge with TGN score ≥ τ_edge ({fmtNum(data?.tau_edge)})</span>
          <span>double-click a user to re-centre</span>
        </div>
      </Card>
      <Card title="TGN analysis">
        {data && (
          <dl className="kv">
            <dt>Window</dt><dd>{fmtTime(data.window?.[0])} → {fmtTime(data.window?.[1])}</dd>
            <dt>Model</dt><dd className="mono">{data.model_version ?? "no trained TGN"}</dd>
            <dt>Nodes / edges</dt><dd>{data.nodes.length} / {data.edges.length}</dd>
            <dt>Group</dt><dd>{a ? (a.members ?? []).join(", ") : "—"}</dd>
            <dt>S_coll</dt><dd>{a?.s_coll === null || a?.s_coll === undefined ? (a?.reason ?? "—") : fmtNum(a.s_coll, 3)}</dd>
            {a?.resources && (<><dt>Shared resources</dt><dd className="mono">{a.resources.join(", ")}</dd></>)}
          </dl>
        )}
        {a?.member_scores && (
          <table style={{ marginTop: 10 }}>
            <thead><tr><th>Member</th><th className="num">max edge score</th></tr></thead>
            <tbody>
              {Object.entries(a.member_scores).map(([m, s]) => (
                <tr key={m}><td><User u={m} onClick={(x) => nav("risk", x)} /></td><td className="num">{fmtNum(s as number, 3)}</td></tr>
              ))}
            </tbody>
          </table>
        )}
        <h3 style={{ margin: "14px 0 6px" }}>Selection</h3>
        {sel ? <pre className="json">{JSON.stringify(sel, null, 1)}</pre> : <Empty>Click a node or edge.</Empty>}
      </Card>
    </div>
  );
}
