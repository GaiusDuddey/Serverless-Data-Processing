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
ORDERS_TABLE = os.environ.get("ORDERS_TABLE_NAME", "OrdersTable")
INVENTORY_TABLE = os.environ.get("INVENTORY_TABLE_NAME", "InventoryTable")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
LOW_STOCK_THRESHOLD = int(os.environ.get("LOW_STOCK_THRESHOLD", "10"))

# ── AWS Clients (lazy — resolved at invoke time so env vars are set) ─────────
_orders_table = None
_inventory_table = None
_sns = None


def _get_clients():
    global _orders_table, _inventory_table, _sns
    if _orders_table is None:
        endpoint = os.environ.get("AWS_ENDPOINT_URL") or None
        region = "us-east-1"
        dynamodb = boto3.resource("dynamodb", region_name=region, endpoint_url=endpoint)
        _sns = boto3.client("sns", region_name=region, endpoint_url=endpoint)
        _orders_table = dynamodb.Table(os.environ.get("ORDERS_TABLE_NAME", "OrdersTable"))
        _inventory_table = dynamodb.Table(os.environ.get("INVENTORY_TABLE_NAME", "InventoryTable"))
    return _orders_table, _inventory_table, _sns


# ── Required order fields ────────────────────────────────────────────────────
REQUIRED_FIELDS = {"order_id", "sku", "quantity"}

# ── Metrics counters (logged per invocation) ─────────────────────────────────
metrics = {"processed": 0, "failed": 0, "retried": 0}


def _reset_metrics():
    metrics["processed"] = 0
    metrics["failed"] = 0
    metrics["retried"] = 0


def _validate_order(order: dict) -> list[str]:
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
    orders_table, _, _ = _get_clients()
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
    _, inventory_table, _ = _get_clients()
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
    _, _, sns = _get_clients()
    if not SNS_TOPIC_ARN:
        logger.debug("SNS_TOPIC_ARN not configured — skipping alert")
        return
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],
            Message=message,
        )
        logger.info("SNS alert published: %s", subject)
    except ClientError as exc:
        logger.error("SNS publish failed: %s", exc)


def _process_record(record: dict):
    start = time.time()
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
    receive_count = int(record.get("attributes", {}).get("ApproximateReceiveCount", 1))
    if receive_count > 1:
        metrics["retried"] += 1
        logger.warning("Retrying order %s (attempt %d)", order_id, receive_count)

    errors = _validate_order(order)
    if errors:
        logger.error("Validation failed for order %s: %s", order_id, errors)
        metrics["failed"] += 1
        _publish_alert(
            f"Order Pipeline — Validation Failed [{order_id}]",
            f"Order {order_id} failed validation:\n" + "\n".join(errors),
        )
        raise ValueError(f"Validation failed: {errors}")

    _write_order(order)
    new_stock = _update_inventory(order["sku"], int(order["quantity"]))

    if new_stock <= LOW_STOCK_THRESHOLD:
        _publish_alert(
            f"Low Stock Alert — {order['sku']}",
            f"SKU {order['sku']} stock is critically low: {new_stock} units remaining.",
        )

    latency_ms = round((time.time() - start) * 1000, 2)
    metrics["processed"] += 1
    logger.info(json.dumps({
        "action": "order_processed",
        "order_id": order_id,
        "sku": order["sku"],
        "latency_ms": latency_ms,
        "status": "success",
    }))


def lambda_handler(event, context):
    _reset_metrics()
    _get_clients()  # force init with correct env vars
    records = event.get("Records", [])
    logger.info("Consumer invoked — processing %d record(s)", len(records))

    batch_item_failures = []
    for record in records:
        try:
            _process_record(record)
        except Exception as exc:
            logger.error("Record processing failed (messageId=%s): %s", record.get("messageId", "?"), exc)
            batch_item_failures.append({"itemIdentifier": record.get("messageId")})

    logger.info(json.dumps({
        "action": "batch_complete",
        "total": len(records),
        "processed": metrics["processed"],
        "failed": metrics["failed"],
        "retried": metrics["retried"],
    }))

    return {"batchItemFailures": batch_item_failures}