"""
Consumer Lambda — Order Processing & Inventory Management
Reads SQS messages, validates orders, writes to DynamoDB (OrdersTable +
InventoryTable), and publishes SNS alerts on failures or low-stock thresholds.
"""

import json
import logging
import os
import time
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── Configuration ────────────────────────────────────────────────────────────
ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL") or None
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
ORDERS_TABLE = os.environ.get("ORDERS_TABLE_NAME", "OrdersTable")
INVENTORY_TABLE = os.environ.get("INVENTORY_TABLE_NAME", "InventoryTable")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
LOW_STOCK_THRESHOLD = int(os.environ.get("LOW_STOCK_THRESHOLD", "10"))

# ── AWS Clients ──────────────────────────────────────────────────────────────
dynamodb = boto3.resource("dynamodb", region_name=REGION, endpoint_url=ENDPOINT_URL)
sns = boto3.client("sns", region_name=REGION, endpoint_url=ENDPOINT_URL)

orders_table = dynamodb.Table(ORDERS_TABLE)
inventory_table = dynamodb.Table(INVENTORY_TABLE)

# ── Required order fields ────────────────────────────────────────────────────
REQUIRED_FIELDS = {"order_id", "sku", "quantity"}

# ── Metrics counters (logged per invocation) ─────────────────────────────────
metrics = {"processed": 0, "failed": 0, "retried": 0}


def _reset_metrics():
    metrics["processed"] = 0
    metrics["failed"] = 0
    metrics["retried"] = 0


def _validate_order(order: dict) -> list[str]:
    """Return a list of validation errors (empty == valid)."""
    errors = []
    missing = REQUIRED_FIELDS - set(order.keys())
    if missing:
        errors.append(f"Missing required fields: {', '.join(sorted(missing))}")

    if "quantity" in order:
        try:
            qty = int(order["quantity"])
            if qty <= 0:
                errors.append("quantity must be a positive integer")
        except (TypeError, ValueError):
            errors.append("quantity must be a valid integer")

    return errors


def _write_order(order: dict):
    """Write the order record to DynamoDB OrdersTable."""
    item = {
        "order_id": order["order_id"],
        "customer_id": order.get("customer_id", "UNKNOWN"),
        "sku": order["sku"],
        "quantity": int(order["quantity"]),
        "price": Decimal(str(order.get("price", 0))),
        "status": "processed",
        "created_at": order.get("created_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
        "processed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    orders_table.put_item(Item=item)
    logger.info("Order %s written to %s", order["order_id"], ORDERS_TABLE)


def _update_inventory(sku: str, quantity: int):
    """
    Decrement inventory for the given SKU. If the SKU doesn't exist yet,
    initialise it with stock 100 and then decrement.
    Returns the new stock level.
    """
    try:
        response = inventory_table.update_item(
            Key={"sku": sku},
            UpdateExpression=(
                "SET stock = if_not_exists(stock, :init) - :qty, "
                "last_updated = :ts"
            ),
            ExpressionAttributeValues={
                ":qty": quantity,
                ":init": 100,
                ":ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
            ReturnValues="ALL_NEW",
        )
        new_stock = int(response["Attributes"]["stock"])
        logger.info("Inventory for %s updated — new stock: %d", sku, new_stock)
        return new_stock
    except ClientError as exc:
        logger.error("Inventory update failed for SKU %s: %s", sku, exc)
        raise


def _publish_alert(subject: str, message: str):
    """Publish an alert to the SNS topic (if configured)."""
    if not SNS_TOPIC_ARN:
        logger.debug("SNS_TOPIC_ARN not configured — skipping alert")
        return
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],  # SNS subject max 100 chars
            Message=message,
        )
        logger.info("SNS alert published: %s", subject)
    except ClientError as exc:
        logger.error("SNS publish failed: %s", exc)


def _process_record(record: dict):
    """Process a single SQS record."""
    start = time.time()

    # Parse message body
    try:
        order = json.loads(record["body"])
    except (json.JSONDecodeError, KeyError) as exc:
        logger.error("Malformed SQS message: %s", exc)
        metrics["failed"] += 1
        _publish_alert(
            "Order Pipeline — Malformed Message",
            f"Failed to parse SQS message: {exc}\nRaw: {record.get('body', 'N/A')}",
        )
        raise

    order_id = order.get("order_id", "UNKNOWN")

    # Check approximate receive count for retry tracking
    receive_count = int(
        record.get("attributes", {}).get("ApproximateReceiveCount", 1)
    )
    if receive_count > 1:
        metrics["retried"] += 1
        logger.warning(
            "Retrying order %s (attempt %d)", order_id, receive_count
        )

    # Validate
    errors = _validate_order(order)
    if errors:
        logger.error("Validation failed for order %s: %s", order_id, errors)
        metrics["failed"] += 1
        _publish_alert(
            f"Order Pipeline — Validation Failed [{order_id}]",
            f"Order {order_id} failed validation:\n" + "\n".join(errors),
        )
        raise ValueError(f"Validation failed: {errors}")

    # Write to DynamoDB
    _write_order(order)

    # Update inventory
    new_stock = _update_inventory(order["sku"], int(order["quantity"]))

    # Alert on low stock
    if new_stock <= LOW_STOCK_THRESHOLD:
        _publish_alert(
            f"Low Stock Alert — {order['sku']}",
            f"SKU {order['sku']} stock is critically low: {new_stock} units remaining.",
        )

    latency_ms = round((time.time() - start) * 1000, 2)
    metrics["processed"] += 1

    logger.info(
        json.dumps(
            {
                "action": "order_processed",
                "order_id": order_id,
                "sku": order["sku"],
                "latency_ms": latency_ms,
                "status": "success",
            }
        )
    )


def lambda_handler(event, context):
    """
    SQS event handler.

    Processes a batch of SQS records. Each record contains an order JSON.
    Failed records raise exceptions so SQS can retry (up to maxReceiveCount)
    before routing to the DLQ.
    """
    _reset_metrics()
    records = event.get("Records", [])
    logger.info("Consumer invoked — processing %d record(s)", len(records))

    batch_item_failures = []

    for record in records:
        try:
            _process_record(record)
        except Exception as exc:
            logger.error(
                "Record processing failed (messageId=%s): %s",
                record.get("messageId", "?"),
                exc,
            )
            batch_item_failures.append(
                {"itemIdentifier": record.get("messageId")}
            )

    # Log invocation metrics
    logger.info(
        json.dumps(
            {
                "action": "batch_complete",
                "total": len(records),
                "processed": metrics["processed"],
                "failed": metrics["failed"],
                "retried": metrics["retried"],
            }
        )
    )

    # Return partial batch failure response so only failed messages are retried
    return {"batchItemFailures": batch_item_failures}
