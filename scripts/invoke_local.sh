#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# invoke_local.sh — Invoke a Lambda function locally via SAM CLI
# Usage: bash scripts/invoke_local.sh <function-name> '<json-payload>'
#
# Examples:
#   bash scripts/invoke_local.sh producer '{"order_id":"ORD-002","sku":"WIDGET-SM","quantity":1}'
#   bash scripts/invoke_local.sh consumer  # (uses sample SQS event)
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

FUNCTION_NAME="${1:-producer}"
PAYLOAD="${2:-}"

# Map friendly names → SAM logical IDs
case "$FUNCTION_NAME" in
    producer|ProducerFunction)
        LOGICAL_ID="ProducerFunction"
        ;;
    consumer|ConsumerFunction)
        LOGICAL_ID="ConsumerFunction"
        ;;
    *)
        echo "❌ Unknown function: $FUNCTION_NAME"
        echo "   Available: producer, consumer"
        exit 1
        ;;
esac

# Build the event JSON
if [ -n "$PAYLOAD" ]; then
    # Wrap raw payload in API Gateway proxy format for the producer
    if [ "$LOGICAL_ID" = "ProducerFunction" ]; then
        EVENT=$(cat <<EOF
{
  "httpMethod": "POST",
  "path": "/orders",
  "headers": {"Content-Type": "application/json"},
  "body": $(echo "$PAYLOAD" | python -c "import sys,json; print(json.dumps(sys.stdin.read()))")
}
EOF
)
    else
        # Wrap in SQS event format for the consumer
        EVENT=$(cat <<EOF
{
  "Records": [
    {
      "messageId": "local-test-$(date +%s)",
      "body": $(echo "$PAYLOAD" | python -c "import sys,json; print(json.dumps(sys.stdin.read()))"),
      "attributes": {"ApproximateReceiveCount": "1"},
      "eventSource": "aws:sqs"
    }
  ]
}
EOF
)
    fi
else
    # Default test events
    if [ "$LOGICAL_ID" = "ProducerFunction" ]; then
        EVENT='{
  "httpMethod": "POST",
  "path": "/orders",
  "headers": {"Content-Type": "application/json"},
  "body": "{\"order_id\":\"ORD-TEST\",\"sku\":\"WIDGET-XL\",\"quantity\":2,\"price\":29.99}"
}'
    else
        EVENT='{
  "Records": [
    {
      "messageId": "test-msg-001",
      "body": "{\"order_id\":\"ORD-TEST\",\"sku\":\"WIDGET-XL\",\"quantity\":2,\"price\":29.99}",
      "attributes": {"ApproximateReceiveCount": "1"},
      "eventSource": "aws:sqs"
    }
  ]
}'
    fi
fi

echo "🚀 Invoking $LOGICAL_ID..."
echo "   Event: $(echo "$EVENT" | head -c 200)..."
echo ""

# Write event to temp file and invoke
TMPFILE=$(mktemp /tmp/sam-event-XXXXXX.json)
echo "$EVENT" > "$TMPFILE"

sam local invoke "$LOGICAL_ID" \
    --event "$TMPFILE" \
    --docker-network localstack-order-pipeline \
    --env-vars <(cat <<EOF
{
  "$LOGICAL_ID": {
    "AWS_ENDPOINT_URL": "http://host.docker.internal:4566",
    "SQS_QUEUE_URL": "http://host.docker.internal:4566/000000000000/orders-queue",
    "ORDERS_TABLE_NAME": "OrdersTable",
    "INVENTORY_TABLE_NAME": "InventoryTable",
    "SNS_TOPIC_ARN": "arn:aws:sns:us-east-1:000000000000:order-pipeline-alerts",
    "AWS_DEFAULT_REGION": "us-east-1"
  }
}
EOF
)

rm -f "$TMPFILE"
echo ""
echo "✅ Invocation complete."
