"""
test_consumer.py — Unit tests for the Consumer Lambda.

Tests cover:
  - Successful order processing → DynamoDB write
  - Inventory decrement and low-stock alert
  - Validation failure handling
  - Malformed message handling
  - Retry tracking (ApproximateReceiveCount)
  - Partial batch failure response
"""

import json
import importlib
from decimal import Decimal
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws


def _invoke_consumer(event, orders_table, inventory_table, sns_topic_arn=""):
    """Helper: import & invoke the consumer handler with patched env and clients."""
    with patch.dict(
        "os.environ",
        {
            "AWS_ENDPOINT_URL": "",
            "AWS_DEFAULT_REGION": "us-east-1",
            "ORDERS_TABLE_NAME": "OrdersTable",
            "INVENTORY_TABLE_NAME": "InventoryTable",
            "SNS_TOPIC_ARN": sns_topic_arn,
            "LOW_STOCK_THRESHOLD": "10",
        },
    ):
        import consumer.app as consumer_module

        # Reload to pick up patched env vars (ENDPOINT_URL becomes None via `or None`)
        importlib.reload(consumer_module)

        # Point the module's DynamoDB / SNS at the moto mock
        resource = boto3.resource("dynamodb", region_name="us-east-1")
        consumer_module.dynamodb = resource
        consumer_module.orders_table = resource.Table("OrdersTable")
        consumer_module.inventory_table = resource.Table("InventoryTable")
        consumer_module.sns = boto3.client("sns", region_name="us-east-1")
        consumer_module.SNS_TOPIC_ARN = sns_topic_arn

        return consumer_module.lambda_handler(event, None)


class TestConsumerSuccess:
    """Test successful order processing."""

    def test_order_written_to_dynamodb(
        self, orders_table, inventory_table, sample_order, make_sqs_event
    ):
        """A valid order should be written to OrdersTable."""
        event = make_sqs_event(sample_order)
        result = _invoke_consumer(event, orders_table, inventory_table)

        assert result["batchItemFailures"] == []

        item = orders_table.get_item(Key={"order_id": "ORD-TEST-001"})
        assert "Item" in item
        assert item["Item"]["sku"] == "WIDGET-XL"
        assert item["Item"]["status"] == "processed"

    def test_inventory_decremented(
        self, orders_table, inventory_table, sample_order, make_sqs_event
    ):
        """Processing an order should decrement inventory stock."""
        event = make_sqs_event(sample_order)
        _invoke_consumer(event, orders_table, inventory_table)

        item = inventory_table.get_item(Key={"sku": "WIDGET-XL"})
        assert "Item" in item
        # Initial stock is 100 (set by if_not_exists), minus quantity 3
        assert item["Item"]["stock"] == 97

    def test_multiple_orders_decrement_correctly(
        self, orders_table, inventory_table, make_sqs_event
    ):
        """Two orders for the same SKU should decrement stock cumulatively."""
        order1 = {
            "order_id": "ORD-A",
            "sku": "WIDGET-XL",
            "quantity": 10,
            "price": 29.99,
        }
        order2 = {
            "order_id": "ORD-B",
            "sku": "WIDGET-XL",
            "quantity": 5,
            "price": 29.99,
        }

        _invoke_consumer(
            make_sqs_event(order1), orders_table, inventory_table
        )
        _invoke_consumer(
            make_sqs_event(order2), orders_table, inventory_table
        )

        item = inventory_table.get_item(Key={"sku": "WIDGET-XL"})
        # 100 - 10 - 5 = 85
        assert item["Item"]["stock"] == 85


class TestConsumerValidation:
    """Test validation and error handling."""

    def test_missing_fields_causes_failure(
        self, orders_table, inventory_table, make_sqs_event
    ):
        """An order missing 'sku' should fail and be in batchItemFailures."""
        bad_order = {"order_id": "ORD-BAD", "quantity": 1}
        event = make_sqs_event(bad_order)
        result = _invoke_consumer(event, orders_table, inventory_table)

        assert len(result["batchItemFailures"]) == 1
        assert result["batchItemFailures"][0]["itemIdentifier"] == "test-msg-001"

    def test_invalid_quantity_causes_failure(
        self, orders_table, inventory_table, make_sqs_event
    ):
        """Negative quantity should fail validation."""
        bad_order = {
            "order_id": "ORD-BAD",
            "sku": "W-1",
            "quantity": -3,
        }
        event = make_sqs_event(bad_order)
        result = _invoke_consumer(event, orders_table, inventory_table)
        assert len(result["batchItemFailures"]) == 1

    def test_malformed_json_causes_failure(
        self, orders_table, inventory_table
    ):
        """A message with invalid JSON body should fail gracefully."""
        event = {
            "Records": [
                {
                    "messageId": "bad-msg-001",
                    "body": "NOT-VALID-JSON",
                    "attributes": {"ApproximateReceiveCount": "1"},
                    "eventSource": "aws:sqs",
                }
            ]
        }
        result = _invoke_consumer(event, orders_table, inventory_table)
        assert len(result["batchItemFailures"]) == 1


class TestConsumerAlerts:
    """Test SNS alert publishing."""

    def test_low_stock_triggers_sns_alert(
        self, orders_table, inventory_table, sns_topic, make_sqs_event
    ):
        """When stock drops below threshold, an SNS alert should be published."""
        # Order quantity 95 → stock goes from 100 to 5 (below threshold 10)
        big_order = {
            "order_id": "ORD-BIG",
            "sku": "RARE-ITEM",
            "quantity": 95,
            "price": 999.99,
        }
        event = make_sqs_event(big_order)
        result = _invoke_consumer(
            event, orders_table, inventory_table, sns_topic_arn=sns_topic
        )

        assert result["batchItemFailures"] == []
        inv = inventory_table.get_item(Key={"sku": "RARE-ITEM"})
        assert inv["Item"]["stock"] == 5


class TestConsumerRetry:
    """Test retry / receive-count tracking."""

    def test_retry_logged_on_second_receive(
        self, orders_table, inventory_table, sample_order, make_sqs_event
    ):
        """A message with ApproximateReceiveCount > 1 should succeed but be logged as retry."""
        event = make_sqs_event(sample_order, receive_count=2)
        result = _invoke_consumer(event, orders_table, inventory_table)
        assert result["batchItemFailures"] == []

        item = orders_table.get_item(Key={"order_id": "ORD-TEST-001"})
        assert item["Item"]["status"] == "processed"


class TestConsumerPartialBatchFailure:
    """Test mixed batch (some succeed, some fail)."""

    def test_partial_batch_failure(
        self, orders_table, inventory_table
    ):
        """A batch with one good and one bad order should only fail the bad one."""
        event = {
            "Records": [
                {
                    "messageId": "good-msg",
                    "body": json.dumps(
                        {
                            "order_id": "ORD-GOOD",
                            "sku": "W-1",
                            "quantity": 1,
                            "price": 5.0,
                        }
                    ),
                    "attributes": {"ApproximateReceiveCount": "1"},
                    "eventSource": "aws:sqs",
                },
                {
                    "messageId": "bad-msg",
                    "body": "BROKEN",
                    "attributes": {"ApproximateReceiveCount": "1"},
                    "eventSource": "aws:sqs",
                },
            ]
        }
        result = _invoke_consumer(event, orders_table, inventory_table)

        # Only the bad record should be in failures
        assert len(result["batchItemFailures"]) == 1
        assert result["batchItemFailures"][0]["itemIdentifier"] == "bad-msg"

        # Good order should exist in DynamoDB
        item = orders_table.get_item(Key={"order_id": "ORD-GOOD"})
        assert "Item" in item
