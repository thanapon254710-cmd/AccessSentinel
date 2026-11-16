#!/usr/bin/env bash
# Deliverable 4 LIVE experiments C, D (latency / throughput), G (enforcement) and H (failure modes).
# Run from the repository root on an otherwise IDLE machine (no training running), with the stack up:
#     bash evaluation/run_live.sh [c|g|h|all]
# Raw results: evaluation/results/raw/exp_{c,g,h}/ ; log: evaluation/results/logs/live-<part>.log
set -u
cd "$(dirname "$0")/.."
PART="${1:-all}"
export MSYS_NO_PATHCONV=1
T() { docker compose run --rm -T tools python -m "$@" 2>&1 | grep -v "Container "; }
stamp() { date -u +%Y-%m-%dT%H:%M:%SZ; }
mkdir -p evaluation/results/logs
# Pre-flight before each part: every service up and the gateway ready. The start time of the gateway and the PDP is
# logged before and after each part; a change means a restart DURING the part, and that part must be rerun
# (on 2026-11-15 the gateway stopped 4 min into Experiment C and silently invalidated most of C and G).
started() { docker inspect -f '{{.State.StartedAt}}' accesssentinel-gateway-1 accesssentinel-pdp-1 2>/dev/null | tr -s '[:space:]' ' '; }
preflight() {
  docker compose up -d >/dev/null 2>&1
  for i in $(seq 1 60); do
    curl -fsS -m 5 http://localhost:8000/readyz >/dev/null 2>&1 && { echo "[$(stamp)] preflight $1: ready; started $(started)"; return 0; }
    sleep 2
  done
  echo "[$(stamp)] preflight $1: gateway NOT ready - aborting"; exit 2
}
postcheck() {
  local now; now="$(started)"
  if [ "$now" != "$2" ]; then echo "[$(stamp)] WARNING $1: gateway/PDP restarted during the part ($2 -> $now); rerun it"
  else echo "[$(stamp)] $1: no gateway/PDP restart during the part"; fi
}

if [ "$PART" = c ] || [ "$PART" = all ]; then
  preflight C; s0="$(started)"
  echo "[$(stamp)] Experiment C/D"
  T evaluation.exp_c --rates 5 20 50 --runs 3 --duration 120 | tee -a evaluation/results/logs/live-c.log
  postcheck C "$s0" | tee -a evaluation/results/logs/live-c.log
fi

if [ "$PART" = g ] || [ "$PART" = all ]; then
  preflight G; s0="$(started)"
  echo "[$(stamp)] Experiment G"
  { T evaluation.exp_g eq21 --n 200
    T evaluation.exp_g psi --n 20
    T evaluation.exp_g policy --runs 3
    T evaluation.exp_g ttl
    postcheck "G (before the fallback, which restarts the PDP on purpose)" "$s0"
    # fallback: probe 120 s; the PDP is stopped at +20 s and started again at +80 s (host clock; the probe
    # records its own timestamps, the stop/start times are written next to it for the figure)
    T evaluation.exp_g fallback-probe --seconds 120 &
    sleep 20; echo "pdp_stop $(date +%s.%N)" > evaluation/results/raw/exp_g/fallback-host-times.txt
    docker compose stop pdp >/dev/null 2>&1
    sleep 60; echo "pdp_start $(date +%s.%N)" >> evaluation/results/raw/exp_g/fallback-host-times.txt
    docker compose start pdp >/dev/null 2>&1
    wait
  } | tee -a evaluation/results/logs/live-g.log
fi

if [ "$PART" = h ] || [ "$PART" = all ]; then
  preflight H
  echo "[$(stamp)] Experiment H"
  {
    T evaluation.exp_h dup --n 200 --copies 3
    T evaluation.exp_h burst --tag baseline --n 3000 --rate 100
    T evaluation.exp_h check --tag baseline
    for target in sequencer tcn-worker-1 cmu-user-1; do
      svc="${target%-1}"
      for k in 1 2 3; do
        tag="kill-${svc}-${k}"
        T evaluation.exp_h burst --tag "$tag" --n 3000 --rate 100 &
        sleep 12; docker kill -s KILL "accesssentinel-${svc}-1" >/dev/null 2>&1
        sleep 5; docker start "accesssentinel-${svc}-1" >/dev/null 2>&1
        wait
        T evaluation.exp_h check --tag "$tag"
      done
    done
    for svc in kafka redis; do
      for k in 1 2 3; do
        tag="pause-${svc}-${k}"
        T evaluation.exp_h probe --tag "$tag" --seconds 80 &
        sleep 20; docker compose pause "$svc" >/dev/null 2>&1
        sleep 20; docker compose unpause "$svc" >/dev/null 2>&1
        wait
      done
    done
  } | tee -a evaluation/results/logs/live-h.log
fi
echo "[$(stamp)] done"
