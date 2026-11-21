#!/usr/bin/env sh
# Happy path, end to end: create a tenant, watch the worker drive it to `active`.
set -eu
API="${API:-http://localhost:8000}"
SLUG="${1:-demo-$(date +%s)}"

echo "==> POST /v1/tenants  slug=$SLUG"
RESP=$(curl -fsS -X POST "$API/v1/tenants" -H 'content-type: application/json' \
  -d "{\"slug\":\"$SLUG\",\"name\":\"Demo tenant\"}")
echo "$RESP"
TENANT_ID=$(echo "$RESP" | sed -E 's/.*"tenant":\{"id":"([^"]+)".*/\1/')

echo "==> polling GET /v1/tenants/$TENANT_ID"
for _ in $(seq 1 30); do
  STATUS=$(curl -fsS "$API/v1/tenants/$TENANT_ID" | sed -E 's/.*"status":"([^"]+)".*/\1/')
  echo "    tenant status: $STATUS"
  case "$STATUS" in active|failed) break ;; esac
  sleep 1
done

echo "==> GET /v1/tenants/$TENANT_ID"
curl -fsS "$API/v1/tenants/$TENANT_ID"; echo
echo "==> GET /v1/tenants/$TENANT_ID/tasks"
curl -fsS "$API/v1/tenants/$TENANT_ID/tasks"; echo
