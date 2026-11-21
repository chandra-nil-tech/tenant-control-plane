#!/usr/bin/env sh
# Block until the API reports ready (DB + broker reachable), max ~90s.
set -eu
URL="${1:-http://localhost:8000/readyz}"
printf "waiting for %s " "$URL"
i=0
until curl -fsS "$URL" >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -gt 90 ]; then
    echo " timed out"; docker compose ps; docker compose logs --tail=50 api; exit 1
  fi
  printf "."; sleep 1
done
echo " ready"
