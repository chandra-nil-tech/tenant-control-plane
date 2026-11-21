#!/usr/bin/env sh
# Publish a task update straight onto the broker, bypassing the worker, using
# RabbitMQ's own management HTTP API. Use it to reproduce duplicates,
# out-of-order deliveries and poison messages by hand.
#
#   scripts/publish_update.sh <task_id> <in_progress|done|failed> [message_id]
#   scripts/publish_update.sh --raw '<any payload, e.g. not json>'
#
# Re-running with the same message_id = a duplicate delivery.
set -eu
MGMT="${RABBITMQ_MGMT:-http://localhost:15672}"
AUTH="${RABBITMQ_AUTH:-guest:guest}"
EXCHANGE="tenant.task-updates"

if [ "${1:-}" = "--raw" ]; then
  PAYLOAD="$2"; ROUTING_KEY="task_update.done"
else
  TASK_ID="$1"; STATUS="$2"
  MESSAGE_ID="${3:-manual-$TASK_ID-$STATUS}"
  PAYLOAD="{\"message_id\":\"$MESSAGE_ID\",\"task_id\":\"$TASK_ID\",\"status\":\"$STATUS\"}"
  ROUTING_KEY="task_update.$STATUS"
fi

# The management API wants the payload as a JSON string: escape \ and ".
ESCAPED=$(printf '%s' "$PAYLOAD" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g')
BODY="{\"properties\":{\"delivery_mode\":2,\"content_type\":\"application/json\"},\"routing_key\":\"$ROUTING_KEY\",\"payload\":\"$ESCAPED\",\"payload_encoding\":\"string\"}"

echo "publishing to $EXCHANGE ($ROUTING_KEY): $PAYLOAD"
curl -fsS -u "$AUTH" -H 'content-type: application/json' \
  -X POST "$MGMT/api/exchanges/%2F/$EXCHANGE/publish" -d "$BODY"
echo
