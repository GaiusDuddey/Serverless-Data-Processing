#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# setup_localstack.sh — Provision AWS resources inside LocalStack
# Run after `docker-compose up -d`
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

ENDPOINT="http://localhost:4566"
REGION="us-east-1"
ACCOUNT_ID="000000000000"

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=$REGION

echo "🔧 Setting up LocalStack resources..."

# ── 1. Dead-Letter Queue ─────────────────────────────────────────────────────
echo "  📬 Creating orders-dlq..."
aws --endpoint-url=$ENDPOINT sqs create-queue \
    --queue-name orders-dlq \
    --region $REGION \
    --output text > /dev/null

DLQ_ARN="arn:aws:sqs:${REGION}:${ACCOUNT_ID}:orders-dlq"

# ── 2. Main Orders Queue (with redrive to DLQ) ──────────────────────────────
echo "  📬 Creating orders-queue (DLQ: maxReceiveCount=3)..."
aws --endpoint-url=$ENDPOINT sqs create-queue \
    --queue-name orders-queue \
    --attributes '{
        "RedrivePolicy": "{\"deadLetterTargetArn\":\"'"$DLQ_ARN"'\",\"maxReceiveCount\":\"3\"}",
        "VisibilityTimeout": "60"
    }' \
    --region $REGION \
    --output text > /dev/null

# ── 3. DynamoDB — OrdersTable ────────────────────────────────────────────────
echo "  🗄️  Creating OrdersTable..."
aws --endpoint-url=$ENDPOINT dynamodb create-table \
    --table-name OrdersTable \
    --attribute-definitions AttributeName=order_id,AttributeType=S \
    --key-schema AttributeName=order_id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region $REGION \
    --output text > /dev/null

# ── 4. DynamoDB — InventoryTable ─────────────────────────────────────────────
echo "  🗄️  Creating InventoryTable..."
aws --endpoint-url=$ENDPOINT dynamodb create-table \
    --table-name InventoryTable \
    --attribute-definitions AttributeName=sku,AttributeType=S \
    --key-schema AttributeName=sku,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region $REGION \
    --output text > /dev/null

# ── 5. SNS — Alerts Topic ───────────────────────────────────────────────────
echo "  🔔 Creating SNS topic: order-pipeline-alerts..."
TOPIC_ARN=$(aws --endpoint-url=$ENDPOINT sns create-topic \
    --name order-pipeline-alerts \
    --region $REGION \
    --query 'TopicArn' --output text)
echo "     Topic ARN: $TOPIC_ARN"

# ── 6. S3 — CSV Upload Bucket ───────────────────────────────────────────────
echo "  📦 Creating S3 bucket: order-pipeline-csv-uploads..."
aws --endpoint-url=$ENDPOINT s3 mb \
    s3://order-pipeline-csv-uploads \
    --region $REGION 2>/dev/null || true

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "✅ LocalStack setup complete!"
echo ""
echo "   SQS Queues:"
echo "     - orders-queue  → $ENDPOINT/$ACCOUNT_ID/orders-queue"
echo "     - orders-dlq    → $ENDPOINT/$ACCOUNT_ID/orders-dlq"
echo ""
echo "   DynamoDB Tables:"
echo "     - OrdersTable    (partition key: order_id)"
echo "     - InventoryTable (partition key: sku)"
echo ""
echo "   SNS Topic:"
echo "     - $TOPIC_ARN"
echo ""
echo "   S3 Bucket:"
echo "     - order-pipeline-csv-uploads"
echo ""
