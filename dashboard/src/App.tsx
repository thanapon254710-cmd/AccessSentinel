import { FormEvent, useCallback, useEffect, useState } from "react";
import { api, ApiError } from "./api";
import Overview from "./pages/Overview";
import Activity from "./pages/Activity";
import Risk from "./pages/Risk";
import Collusion from "./pages/Collusion";
import Incidents from "./pages/Incidents";
import SystemPage from "./pages/System";
import SettingsPage from "./pages/Settings";
import Users from "./pages/Users";

export type Me = { user: string; role: "security_analyst" | "security_admin"; pdp_mode: string };
export type Nav = (page: string, arg?: string) => void;

const PAGES: [string, string][] = [
  ["overview", "Overview"],
  ["activity", "Access Activity"],
  ["risk", "Individual Risk"],
  ["collusion", "Collusion Detection"],
  ["incidents", "Security Incidents"],
  ["system", "System Monitoring"],
  ["users", "User Accounts"],
  ["settings", "Settings"],
];

function parseHash(): [string, string | undefined] {
  const [p, a] = location.hash.replace(/^#\/?/, "").split("/");
  return [PAGES.some(([k]) => k === p) ? p : "overview", a ? decodeURIComponent(a) : undefined];
}

export default function App() {
  const [me, setMe] = useState<Me | null | undefined>(undefined);
  const [[page, arg], setRoute] = useState(parseHash());

  useEffect(() => {
    api<Me>("/api/auth/me").then(setMe).catch(() => setMe(null));
    const onHash = () => setRoute(parseHash());
    const onUnauth = () => setMe(null);
    window.addEventListener("hashchange", onHash);
    window.addEventListener("as-unauthorized", onUnauth);
    return () => {
      window.removeEventListener("hashchange", onHash);
      window.removeEventListener("as-unauthorized", onUnauth);
    };
  }, []);

  const nav: Nav = useCallback((p, a) => {
    location.hash = `#/${p}${a ? "/" + encodeURIComponent(a) : ""}`;
  }, []);

  if (me === undefined) return <div className="boot">Loading…</div>;
  if (me === null) return <Login onLogin={setMe} />;

  const logout = async () => {
    await api("/api/auth/logout", "POST").catch(() => undefined);
    setMe(null);
  };

  return (
    <div className="shell">
      <aside className="side">
        <div className="brand">
          <span className="logo">◆</span> AccessSentinel
        </div>
        <nav>
          {PAGES.map(([k, label]) => (
            <a key={k} href={`#/${k}`} className={k === page ? "active" : ""}>
              {label}
            </a>
          ))}
        </nav>
        <div className="side-foot">
          <div className={"mode " + me.pdp_mode}>PDP: {me.pdp_mode}</div>
          <div className="who">
            {me.user}
            <small>{me.role === "security_admin" ? "Security admin" : "Security analyst"}</small>
          </div>
          <button className="ghost" onClick={logout}>
            Sign out
          </button>
        </div>
      </aside>
      <main className="main">
        {page === "overview" && <Overview nav={nav} />}
        {page === "activity" && <Activity nav={nav} />}
        {page === "risk" && <Risk nav={nav} user={arg} me={me} />}
        {page === "collusion" && <Collusion nav={nav} user={arg} />}
        {page === "incidents" && <Incidents nav={nav} id={arg} me={me} />}
        {page === "system" && <SystemPage />}
        {page === "users" && <Users nav={nav} me={me} />}
        {page === "settings" && <SettingsPage me={me} />}
      </main>
    </div>
  );
}

function Login({ onLogin }: { onLogin: (m: Me) => void }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await api("/api/auth/login", "POST", { username, password });
      setPassword("");
      onLogin(await api<Me>("/api/auth/me"));
    } catch (err) {
      setError(err instanceof ApiError && err.status === 429 ? "Too many failed attempts; try again later." : "Sign-in failed.");
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="login">
      <form onSubmit={submit} className="login-box">
        <h1>
          <span className="logo">◆</span> AccessSentinel
        </h1>
        <p className="muted">Security operations console. Security analyst or security admin accounts only.</p>
        <label>
          User ID
          <input value={username} onChange={(e) => setUsername(e.target.value)} autoComplete="username" required />
        </label>
        <label>
          Password
          <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} autoComplete="current-password" required />
        </label>
        {error && <div className="error">{error}</div>}
        <button disabled={busy} type="submit">
          {busy ? "Signing in…" : "Sign in"}
        </button>
      </form>
    </div>
  );
}
