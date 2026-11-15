// Thin client for the dashboard API. Every mutation carries the CSRF header the backend requires;
// the session lives in an HttpOnly cookie the page script cannot read.
import { useCallback, useEffect, useRef, useState } from "react";

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

export async function api<T = any>(path: string, method = "GET", body?: unknown): Promise<T> {
  const headers: Record<string, string> = { "X-Requested-With": "accesssentinel" };
  if (body !== undefined) headers["Content-Type"] = "application/json";
  const r = await fetch(path, {
    method,
    credentials: "same-origin",
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (r.status === 401 && !path.startsWith("/api/auth/")) window.dispatchEvent(new Event("as-unauthorized"));
  if (!r.ok) {
    let msg = r.statusText;
    try {
      const j = await r.json();
      msg = typeof j.detail === "string" ? j.detail : j.error || JSON.stringify(j.detail ?? j);
    } catch {
      /* not JSON */
    }
    throw new ApiError(r.status, msg);
  }
  return r.json();
}

/** Poll a GET endpoint; data always comes from the backend (no client-side fabrication). */
export function usePoll<T = any>(path: string | null, ms = 5000) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const alive = useRef(true);
  const load = useCallback(async () => {
    if (!path) return;
    setLoading(true);
    try {
      const d = await api<T>(path);
      if (alive.current) {
        setData(d);
        setError(null);
      }
    } catch (e) {
      if (alive.current) setError((e as Error).message);
    } finally {
      if (alive.current) setLoading(false);
    }
  }, [path]);
  useEffect(() => {
    alive.current = true;
    setData(null);
    load();
    const id = ms > 0 ? window.setInterval(load, ms) : undefined;
    return () => {
      alive.current = false;
      if (id) window.clearInterval(id);
    };
  }, [load, ms]);
  return { data, error, loading, reload: load };
}

export type LiveMsg = { type: "event" | "score" | "escalation" | "group" | "decision"; data: any };

/** Live records from Kafka via the API's WebSocket (/ws/live). */
export function useLive(types: LiveMsg["type"][], cap = 300) {
  const [items, setItems] = useState<LiveMsg[]>([]);
  const [connected, setConnected] = useState(false);
  const key = types.join(",");
  useEffect(() => {
    let ws: WebSocket | null = null;
    let stopped = false;
    let retry: number | undefined;
    const want = new Set(key.split(","));
    const connect = () => {
      const proto = location.protocol === "https:" ? "wss" : "ws";
      ws = new WebSocket(`${proto}://${location.host}/ws/live`);
      ws.onopen = () => setConnected(true);
      ws.onclose = () => {
        setConnected(false);
        if (!stopped) retry = window.setTimeout(connect, 3000);
      };
      ws.onmessage = (m) => {
        try {
          const msg = JSON.parse(m.data) as LiveMsg;
          if (want.has(msg.type)) setItems((prev) => [msg, ...prev].slice(0, cap));
        } catch {
          /* ignore malformed */
        }
      };
    };
    connect();
    return () => {
      stopped = true;
      if (retry) window.clearTimeout(retry);
      ws?.close();
    };
  }, [key, cap]);
  return { items, connected };
}
