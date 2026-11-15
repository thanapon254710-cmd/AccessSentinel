#!/usr/bin/env sh
# AccessSentinel - start on macOS / Linux:  sh start.sh
# First run: creates .env from .env.example with random passwords, then builds and starts everything in Docker.
set -e
cd "$(dirname "$0")"

if ! docker info >/dev/null 2>&1; then
  echo "Docker is not running. Install Docker Desktop, start it, then run: sh start.sh"; exit 1
fi

if [ ! -f .env ]; then
  [ -f .env.example ] || { echo ".env.example is missing - the folder is incomplete."; exit 1; }
  tmp=$(mktemp)
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in
      *=change-me) key=${line%%=*}
                   secret=$(LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 24)
                   printf '%s=%s\n' "$key" "$secret" >>"$tmp" ;;
      *) printf '%s\n' "$line" >>"$tmp" ;;
    esac
  done < .env.example
  mv "$tmp" .env
  echo "Created .env with new random passwords."
elif grep -q '=change-me' .env; then
  echo ".env still contains 'change-me'. Delete .env and run sh start.sh again"
  echo "(if you already started with it, first run: docker compose down -v)."; exit 1
fi

echo "Building and starting the containers (first time 10-20 minutes)..."
docker compose up -d --build

printf "Waiting for all services to become healthy"
i=0
while [ $i -lt 60 ]; do
  total=$(docker compose ps --format '{{.Service}} {{.Status}}' | grep -c . || true)
  bad=$(docker compose ps --format '{{.Service}} {{.Status}}' | grep -vc '(healthy)' || true)
  if [ "$total" -gt 0 ] && [ "$bad" -eq 0 ]; then break; fi
  printf "."; sleep 10; i=$((i + 1))
done
echo
[ "$bad" -eq 0 ] || { docker compose ps; echo "Some services are not healthy (see above)."; exit 1; }

echo "AccessSentinel is running."
echo "  Dashboard:      http://localhost:8080  (secadmin / analyst1, password AS_ANALYST_PASSWORD in .env)"
echo "  Protected apps: http://localhost:8000  (alice, bob, ..., password AS_DEMO_USER_PASSWORD in .env)"
echo "  Stop: docker compose down"
