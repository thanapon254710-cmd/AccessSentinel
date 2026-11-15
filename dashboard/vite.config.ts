import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev server proxies the dashboard API (docker compose `api` service on :8080).
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": "http://localhost:8080",
      "/ws": { target: "ws://localhost:8080", ws: true },
    },
  },
  build: { outDir: "dist", sourcemap: false, chunkSizeWarningLimit: 1500 },
});
