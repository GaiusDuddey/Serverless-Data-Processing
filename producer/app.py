"""
Producer Lambda — Order Ingestion
Receives order events via API Gateway, validates required fields,
and publishes them to the SQS orders queue.
"""

import json
import logging
import os
import time
import uuid

import boto3
from botocore.exceptions import ClientError

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── AWS Clients ──────────────────────────────────────────────────────────────
ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL") or None
SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

sqs = boto3.client("sqs", region_name=REGION, endpoint_url=ENDPOINT_URL)

# ── Required order fields ────────────────────────────────────────────────────
REQUIRED_FIELDS = {"order_id", "sku", "quantity"}


def _build_response(status_code: int, body: dict) -> dict:
    """Build a standard API Gateway proxy response."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }


def _validate_order(order: dict) -> list[str]:
    """Return a list of validation error messages (empty == valid)."""
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
            errors.append("quantity must be a positive integer")

    if "price" in order:
        try:
            price = float(order["price"])
            if price < 0:
                errors.append("price must be non-negative")
        except (TypeError, ValueError):
            errors.append("price must be a valid number")

    return errors


def lambda_handler(event, context):
    """
    API Gateway proxy handler.

    Accepts a JSON order payload, validates it, and publishes to SQS.
    Returns 200 on success, 400 on validation errors, 500 on SQS failures.
    """
    start_time = time.time()

    # ── Parse body ───────────────────────────────────────────────────────
    try:
        body = event.get("body")
        if isinstance(body, str):
            order = json.loads(body)
        elif isinstance(body, dict):
            order = body
        else:
            # Direct invocation (not via API Gateway)
            order = event
    except (json.JSONDecodeError, TypeError) as exc:
        logger.error("Invalid JSON payload: %s", exc)
        return _build_response(400, {"error": "Invalid JSON payload"})

    # ── Validate ─────────────────────────────────────────────────────────
    errors = _validate_order(order)
    if errors:
        logger.warning("Validation failed for order: %s — %s", order, errors)
        return _build_response(400, {"error": "Validation failed", "details": errors})

    # ── Ensure order_id exists ───────────────────────────────────────────
    if "order_id" not in order or not order["order_id"]:
        order["order_id"] = f"ORD-{uuid.uuid4().hex[:8].upper()}"

    # ── Enrich with metadata ─────────────────────────────────────────────
    order.setdefault("customer_id", "UNKNOWN")
    order.setdefault("price", 0.0)
    order["status"] = "pending"
    order["created_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # ── Publish to SQS ───────────────────────────────────────────────────
    try:
        message_body = json.dumps(order)
        response = sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=message_body,
            MessageAttributes={
                "order_id": {
                    "StringValue": order["order_id"],
                    "DataType": "String",
                },
                "sku": {
                    "StringValue": order["sku"],
                    "DataType": "String",
                },
            },
        )

        latency_ms = round((time.time() - start_time) * 1000, 2)
        logger.info(
            json.dumps(
                {
                    "action": "order_published",
                    "order_id": order["order_id"],
                    "message_id": response["MessageId"],
                    "latency_ms": latency_ms,
                    "status": "success",
                }
            )
        )

        return _build_response(
            200,
            {
                "message": "Order accepted",
                "order_id": order["order_id"],
                "message_id": response["MessageId"],
            },
        )

    except ClientError as exc:
        logger.error("SQS send failed for order %s: %s", order.get("order_id"), exc)
        return _build_response(
            500,
            {"error": "Failed to queue order", "details": str(exc)},
        )
