import { FormEvent, useEffect, useState } from "react";
import { api, usePoll } from "../api";
import type { Me } from "../App";
import { Badge, Card, Empty, ErrorLine, fmtTime } from "../ui";

export default function SettingsPage({ me }: { me: Me }) {
  const admin = me.role === "security_admin";
  return (
    <>
      <div className="page-h">
        <div>
          <h2>Settings</h2>
          <p>
            Runtime parameters are stored in Redis and picked up by the running TCN, escalation and PDP services within seconds.
            {admin ? "" : " Read-only: changing settings requires the security_admin role."}
          </p>
        </div>
      </div>
      <div className="grid">
        <RuntimeSettings admin={admin} />
        <Models admin={admin} />
        <Catalog admin={admin} />
        <RegisterApp admin={admin} />
      </div>
    </>
  );
}

function RuntimeSettings({ admin }: { admin: boolean }) {
  const { data, error, reload } = usePoll<any>("/api/settings", 0);
  const hist = usePoll<any>("/api/settings/history?limit=30", 0);
  const [draft, setDraft] = useState<Record<string, string>>({});
  const [msg, setMsg] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  useEffect(() => {
    if (data) setDraft(Object.fromEntries(data.settings.map((s: any) => [s.key, s.value === null ? "" : String(s.value)])));
  }, [data]);
  const changed = (data?.settings ?? []).filter((s: any) => draft[s.key] !== (s.value === null ? "" : String(s.value)));
  const save = async (e: FormEvent) => {
    e.preventDefault();
    setMsg(null); setErr(null);
    const body = Object.fromEntries(changed.map((s: any) => [s.key, draft[s.key] === "" ? null : Number(draft[s.key])]));
    try {
      const r = await api<any>("/api/settings", "PUT", body);
      setMsg(`Saved (version ${r.version}); services apply it within ~${r.applied_within_seconds} s.`);
      reload(); hist.reload();
    } catch (e2) {
      setErr((e2 as Error).message);
    }
  };
  const groups: Record<string, any[]> = {};
  for (const s of data?.settings ?? []) (groups[s.key.split(".")[0]] ??= []).push(s);
  return (
    <Card title="Detection and enforcement parameters" wide>
      <ErrorLine error={error} />
      <form onSubmit={save}>
        {Object.entries(groups).map(([g, items]) => (
          <div key={g} style={{ marginBottom: 12 }}>
            <h3 style={{ textTransform: "capitalize", marginBottom: 6 }}>{g === "risk" ? "Risk thresholds (TCN tiers)" : g === "pdp" ? "Policy decision point" : "Escalation (paths B–D)"}</h3>
            <table>
              <thead><tr><th>Parameter</th><th>Value</th><th>Default</th><th>Range</th><th>Description</th></tr></thead>
              <tbody>
                {items.map((s) => (
                  <tr key={s.key}>
                    <td className="mono">{s.key} {s.explicit && <Badge tone="warn">set</Badge>}</td>
                    <td>
                      <input style={{ width: 110 }} disabled={!admin} value={draft[s.key] ?? ""} onChange={(e) => setDraft({ ...draft, [s.key]: e.target.value })}
                        placeholder={s.key === "pdp.theta_coll" ? "model" : ""} />
                    </td>
                    <td className="mono muted">{s.default ?? "model"}</td>
                    <td className="mono muted">{s.min}–{s.max}</td>
                    <td className="muted">{s.description}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ))}
        {admin && (
          <div className="row">
            <button type="submit" disabled={changed.length === 0}>Save {changed.length ? `(${changed.length})` : ""}</button>
            {changed.length > 0 && <button type="button" className="ghost" onClick={() => reload()}>Discard</button>}
          </div>
        )}
        {msg && <div className="success">{msg}</div>}
        <ErrorLine error={err} />
      </form>
      <h3 style={{ margin: "14px 0 6px" }}>Change history</h3>
      {(hist.data?.history ?? []).length === 0 ? <Empty>No changes yet; research defaults (D3) in effect.</Empty> : (
        <table>
          <thead><tr><th>Time</th><th>By</th><th>Parameter</th><th>Old</th><th>New</th></tr></thead>
          <tbody>
            {hist.data.history.map((h: any, i: number) => (
              <tr key={i}><td className="mono">{fmtTime(h.changed_at)}</td><td>{h.changed_by}</td><td className="mono">{h.key}</td>
                <td className="mono">{String(h.old ?? "—")}</td><td className="mono">{String(h.new ?? "—")}</td></tr>
            ))}
          </tbody>
        </table>
      )}
    </Card>
  );
}

function Models({ admin }: { admin: boolean }) {
  const { data, error, reload } = usePoll<any>("/api/models", 0);
  const [msg, setMsg] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const promote = async (kind: string, version: string) => {
    if (!confirm(`Promote ${version} to the live ${kind.toUpperCase()} model?`)) return;
    setMsg(null); setErr(null);
    try {
      const r = await api<any>(`/api/models/${kind}/promote`, "POST", { version });
      setMsg(`${version} promoted; workers hot-reload within ~${r.workers_reload_within_seconds} s.`);
      reload();
    } catch (e) {
      setErr((e as Error).message);
    }
  };
  return (
    <Card title="Model versions (registry)" wide>
      <ErrorLine error={error} />
      {msg && <div className="success">{msg}</div>}
      <ErrorLine error={err} />
      {["tcn", "tgn"].map((kind) => (
        <div key={kind} style={{ marginBottom: 12 }}>
          <h3 style={{ marginBottom: 6 }}>{kind === "tcn" ? "TCN (individual risk)" : "TGN (collusion)"}</h3>
          {(data?.[kind]?.versions ?? []).length === 0 ? <Empty>No trained versions.</Empty> : (
            <table>
              <thead><tr><th>Version</th><th>Created</th><th>Parent</th><th>Training data</th><th>Retraining gate</th><th></th></tr></thead>
              <tbody>
                {[...data[kind].versions].reverse().map((v: any) => (
                  <tr key={v.version}>
                    <td className="mono">{v.version} {v.current && <Badge tone="ok">live</Badge>}</td>
                    <td className="mono">{fmtTime(v.created_at)}</td>
                    <td className="mono muted">{v.parent_version ?? "—"}</td>
                    <td className="muted">{v.data ?? "—"}</td>
                    <td>{v.gate === undefined || v.gate === null ? "—" : v.gate ? <Badge tone="ok">passed</Badge> : <Badge tone="bad">failed</Badge>}</td>
                    <td>{admin && !v.current && <button className="small ghost" onClick={() => promote(kind, v.version)}>Promote</button>}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      ))}
      <p className="muted" style={{ fontSize: 12 }}>
        Candidates are produced by <span className="mono">python -m accesssentinel.ml.retrain tcn|tgn</span> from analyst-verified feedback
        and are only promoted automatically with <span className="mono">--promote-if-better</span> when the gate passes.
      </p>
    </Card>
  );
}

function Catalog({ admin }: { admin: boolean }) {
  const { data, error, reload } = usePoll<any>("/api/catalog", 0);
  const [err, setErr] = useState<string | null>(null);
  const [app, setApp] = useState("");
  const toggle = async (a: string, r: string, s: number) => {
    setErr(null);
    try {
      await api(`/api/catalog/resources/${encodeURIComponent(a)}/${encodeURIComponent(r)}`, "PUT", { sensitivity: s ? 0 : 1 });
      reload();
    } catch (e) {
      setErr((e as Error).message);
    }
  };
  const res = (data?.resources ?? []).filter((r: any) => !app || r.app === app);
  return (
    <Card
      title="Resource sensitivity catalog"
      actions={
        <select value={app} onChange={(e) => setApp(e.target.value)}>
          <option value="">all applications</option>
          {(data?.applications ?? []).map((a: any) => <option key={a.id} value={a.id}>{a.name}</option>)}
        </select>
      }
    >
      <ErrorLine error={error || err} />
      <p className="muted" style={{ marginTop: 0, fontSize: 12 }}>
        Sensitivity s feeds the TCN features and Path B; the gateway reloads the catalog every few seconds. {data ? `${data.acl_rules} ACL rules.` : ""}
      </p>
      <div className="scroll" style={{ maxHeight: 380 }}>
        <table>
          <thead><tr><th>Application</th><th>Resource</th><th>Sensitivity</th><th></th></tr></thead>
          <tbody>
            {res.map((r: any) => (
              <tr key={r.app + r.resource}>
                <td>{r.app}</td><td className="mono">{r.resource}</td>
                <td>{r.sensitivity ? <Badge tone="warn">sensitive</Badge> : <Badge>normal</Badge>}</td>
                <td>{admin && <button className="small ghost" onClick={() => toggle(r.app, r.resource, r.sensitivity)}>Mark {r.sensitivity ? "normal" : "sensitive"}</button>}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

function RegisterApp({ admin }: { admin: boolean }) {
  const [id, setId] = useState("");
  const [name, setName] = useState("");
  const [url, setUrl] = useState("");
  const [resources, setResources] = useState("");
  const [msg, setMsg] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setMsg(null); setErr(null);
    const rs = resources.split(/[\n,]+/).map((x) => x.trim()).filter(Boolean).map((x) => {
      const [key, s] = x.split(":");
      return { key: key.trim(), sensitivity: s?.trim() === "1" ? 1 : 0 };
    });
    try {
      const r = await api<any>("/api/catalog/apps", "POST", { id, name, base_url: url, resources: rs });
      setMsg(`Registered ${r.registered}. ${r.note}`);
      setId(""); setName(""); setUrl(""); setResources("");
    } catch (e2) {
      setErr((e2 as Error).message);
    }
  };
  return (
    <Card title="Register a protected application">
      {!admin ? <Empty>Requires the security_admin role.</Empty> : (
        <form onSubmit={submit} style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          <div className="row">
            <label>ID<input value={id} onChange={(e) => setId(e.target.value)} placeholder="crm" /></label>
            <label style={{ flex: 1 }}>Name<input value={name} onChange={(e) => setName(e.target.value)} placeholder="Customer CRM" /></label>
          </div>
          <label>Internal base URL<input value={url} onChange={(e) => setUrl(e.target.value)} placeholder="http://crm:8000" /></label>
          <label>Resources (key:sensitivity, one per line)<textarea value={resources} onChange={(e) => setResources(e.target.value)} placeholder={"accounts:1\nnotes:0"} /></label>
          <div><button type="submit">Register</button></div>
          {msg && <div className="success">{msg}</div>}
          <ErrorLine error={err} />
        </form>
      )}
    </Card>
  );
}
