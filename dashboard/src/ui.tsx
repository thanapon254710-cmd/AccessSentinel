import { ReactNode } from "react";

export const fmtTime = (t?: number | null) =>
  t ? new Date(t * 1000).toLocaleString(undefined, { hour12: false }) : "—";
export const fmtClock = (t?: number | null) =>
  t ? new Date(t * 1000).toLocaleTimeString(undefined, { hour12: false }) : "—";
export const fmtNum = (x?: number | null, d = 2) =>
  x === null || x === undefined || Number.isNaN(x) ? "—" : Number(x).toFixed(d);
export const fmtBytes = (b?: number | null) => {
  if (b === null || b === undefined) return "—";
  const u = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let x = b;
  while (x >= 1024 && i < u.length - 1) {
    x /= 1024;
    i++;
  }
  return `${x.toFixed(1)} ${u[i]}`;
};

export const isSynthetic = (u?: string, source?: string) =>
  source === "synthetic-replay" || (u ?? "").startsWith("syn-");

export function Card({ title, children, actions, wide }: { title?: ReactNode; children: ReactNode; actions?: ReactNode; wide?: boolean }) {
  return (
    <section className={"card" + (wide ? " wide" : "")}>
      {(title || actions) && (
        <header className="card-h">
          <h3>{title}</h3>
          <div className="card-actions">{actions}</div>
        </header>
      )}
      {children}
    </section>
  );
}

export function Stat({ label, value, sub, tone }: { label: string; value: ReactNode; sub?: ReactNode; tone?: "bad" | "warn" | "ok" }) {
  return (
    <div className={"stat " + (tone ?? "")}>
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
      {sub && <div className="stat-sub">{sub}</div>}
    </div>
  );
}

export function SourceBadge({ u, source }: { u?: string; source?: string }) {
  return isSynthetic(u, source) ? (
    <span className="badge synth" title="Synthetic replay / test data, not a real user">synthetic</span>
  ) : null;
}

export function Tier({ tier }: { tier?: string | null }) {
  return <span className={"badge tier-" + (tier ?? "none")}>{tier ?? "—"}</span>;
}

export function Badge({ children, tone }: { children: ReactNode; tone?: string }) {
  return <span className={"badge " + (tone ?? "")}>{children}</span>;
}

export function User({ u, onClick }: { u: string; onClick?: (u: string) => void }) {
  return (
    <span className="user">
      {onClick ? (
        <button className="link" onClick={() => onClick(u)}>
          {u}
        </button>
      ) : (
        u
      )}
      <SourceBadge u={u} />
    </span>
  );
}

export function ErrorLine({ error }: { error?: string | null }) {
  return error ? <div className="error">⚠ {error}</div> : null;
}

export function Empty({ children }: { children: ReactNode }) {
  return <div className="empty">{children}</div>;
}

export function Health({ value }: { value?: string }) {
  const tone = value === "ok" ? "ok" : value === "degraded" ? "warn" : "bad";
  return <span className={"badge h-" + tone}>{value ?? "unknown"}</span>;
}
