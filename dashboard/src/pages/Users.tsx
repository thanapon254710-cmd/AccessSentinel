import { FormEvent, useState } from "react";
import { api, usePoll } from "../api";
import type { Me, Nav } from "../App";
import { Badge, Card, Empty, ErrorLine, fmtNum, fmtTime, SourceBadge, Tier } from "../ui";

export default function Users({ nav, me }: { nav: Nav; me: Me }) {
  const admin = me.role === "security_admin";
  const { data, error, reload } = usePoll<any>("/api/accounts", 10000);
  const [shown, setShown] = useState<{ user: string; password: string } | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [filter, setFilter] = useState("");

  const act = async (u: string, what: "deactivate" | "activate" | "reset-password") => {
    setErr(null);
    setShown(null);
    if (what !== "activate" && !confirm(`${what === "deactivate" ? "Deactivate" : "Reset the password of"} ${u}? Their open sessions will be ended.`)) return;
    try {
      const r = await api<any>(`/api/accounts/${encodeURIComponent(u)}/${what}`, "POST", {});
      if (r.initial_password) setShown({ user: u, password: r.initial_password });
      reload();
    } catch (e) {
      setErr((e as Error).message);
    }
  };

  const users = (data?.users ?? []).filter((u: any) => !filter || u.id.includes(filter) || u.role.includes(filter));
  return (
    <>
      <div className="page-h">
        <div>
          <h2>User Accounts</h2>
          <p>
            Employees who use the protected applications, and are monitored by AccessSentinel. Accounts are provisioned by a
            security admin (no self-registration); a new account starts with no history.
          </p>
        </div>
        <input placeholder="filter by id or role" value={filter} onChange={(e) => setFilter(e.target.value)} />
      </div>
      {admin ? <CreateUser roles={data?.roles ?? []} grants={data?.grants ?? {}} onCreated={(u, p) => { setShown(p ? { user: u, password: p } : null); reload(); }} /> : (
        <div className="notice">Read-only: creating and changing accounts requires the security_admin role.</div>
      )}
      {shown && <PasswordOnce user={shown.user} password={shown.password} onClose={() => setShown(null)} />}
      <ErrorLine error={error || err} />
      <div style={{ height: 14 }} />
      <Card title={`Monitored users (${users.length})`}>
        {users.length === 0 ? <Empty>No users.</Empty> : (
          <div className="scroll">
            <table>
              <thead>
                <tr><th>User</th><th>Name</th><th>Role</th><th>Department</th><th>Status</th><th>Risk tier</th><th className="num">S_ind</th><th>Created</th>{admin && <th></th>}</tr>
              </thead>
              <tbody>
                {users.map((u: any) => (
                  <tr key={u.id}>
                    <td><button className="link" onClick={() => nav("risk", u.id)}>{u.id}</button><SourceBadge u={u.id} /></td>
                    <td>{u.name}</td>
                    <td className="mono">{u.role}</td>
                    <td className="muted">{u.department}</td>
                    <td>{u.active ? <Badge tone="ok">active</Badge> : <Badge tone="bad">deactivated</Badge>}</td>
                    <td>{u.tier ? <Tier tier={u.tier} /> : <span className="muted">not scored yet</span>}</td>
                    <td className="num">{fmtNum(u.s_ind, 3)}</td>
                    <td className="mono muted">{fmtTime(u.created_at)}</td>
                    {admin && (
                      <td style={{ whiteSpace: "nowrap" }}>
                        {u.active
                          ? <button className="small ghost" onClick={() => act(u.id, "deactivate")}>Deactivate</button>
                          : <button className="small ghost" onClick={() => act(u.id, "activate")}>Activate</button>}{" "}
                        <button className="small ghost" onClick={() => act(u.id, "reset-password")}>Reset password</button>
                      </td>
                    )}
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

function CreateUser({ roles, grants, onCreated }: { roles: string[]; grants: Record<string, string[]>; onCreated: (u: string, pw: string | null) => void }) {
  const [id, setId] = useState("");
  const [name, setName] = useState("");
  const [dept, setDept] = useState("");
  const [role, setRole] = useState("staff");
  const [password, setPassword] = useState("");
  const [err, setErr] = useState<string | null>(null);
  const [msg, setMsg] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setErr(null); setMsg(null); setBusy(true);
    try {
      const r = await api<any>("/api/accounts", "POST", { id, name, department: dept, role, password });
      setMsg(`Created ${r.created} (${r.role}). ${r.note}`);
      onCreated(r.created, r.initial_password);
      setId(""); setName(""); setDept(""); setPassword("");
    } catch (e2) {
      setErr((e2 as Error).message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <Card title="Create a user account">
      <form onSubmit={submit} style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        <div className="row">
          <label>User ID<input value={id} onChange={(e) => setId(e.target.value.trim().toLowerCase())} placeholder="mallory" required /></label>
          <label style={{ flex: 1 }}>Full name<input value={name} onChange={(e) => setName(e.target.value)} placeholder="Mallory Test" required /></label>
          <label>Department<input value={dept} onChange={(e) => setDept(e.target.value)} placeholder="Sales" /></label>
          <label>Role
            <select value={role} onChange={(e) => setRole(e.target.value)}>
              {roles.map((r) => <option key={r} value={r}>{r}</option>)}
            </select>
          </label>
        </div>
        <label>Initial password (12+ characters; leave empty to generate one)
          <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} autoComplete="new-password" />
        </label>
        {grants[role] && (
          <div className="muted" style={{ fontSize: 12 }}>
            <b>{role}</b> may use: {grants[role].join(", ") || "nothing"}. Everything else is denied by the gateway.
          </div>
        )}
        <div><button type="submit" disabled={busy}>{busy ? "Creating…" : "Create user"}</button></div>
        {msg && <div className="success">{msg}</div>}
        <ErrorLine error={err} />
      </form>
    </Card>
  );
}

function PasswordOnce({ user, password, onClose }: { user: string; password: string; onClose: () => void }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="notice" style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap", marginTop: 10 }}>
      <span>Initial password for <b>{user}</b> (shown once, not stored anywhere readable):</span>
      <code className="mono" style={{ fontSize: 14, color: "var(--text)" }}>{password}</code>
      <button className="small ghost" type="button" onClick={async () => { try { await navigator.clipboard.writeText(password); setCopied(true); } catch { /* clipboard blocked */ } }}>
        {copied ? "Copied" : "Copy"}
      </button>
      <button className="small ghost" type="button" onClick={onClose}>Hide</button>
    </div>
  );
}
