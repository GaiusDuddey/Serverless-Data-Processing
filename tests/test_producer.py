"""
test_producer.py — Unit tests for the Producer Lambda.

Tests cover:
  - Successful order validation & SQS publish
  - Validation errors (missing fields, bad types) → 400
  - SQS send failure → 500
  - Auto-generated order_id when missing
"""

import json
import importlib
from unittest.mock import patch, MagicMock

import boto3
import pytest
from moto import mock_aws


def _invoke_producer(event, sqs_queue_url):
    """Helper: import (or reload) & invoke the producer handler with patched env."""
    with patch.dict(
        "os.environ",
        {
            "AWS_ENDPOINT_URL": "",
            "SQS_QUEUE_URL": sqs_queue_url,
            "AWS_DEFAULT_REGION": "us-east-1",
        },
    ):
        import producer.app as producer_module

        # Reload to pick up patched env vars
        importlib.reload(producer_module)

        # Override the SQS client/queue URL to use the moto mock
        producer_module.sqs = boto3.client("sqs", region_name="us-east-1")
        producer_module.SQS_QUEUE_URL = sqs_queue_url
        return producer_module.lambda_handler(event, None)


class TestProducerValidation:
    """Test input validation in the producer Lambda."""

    def test_missing_required_fields_returns_400(self, sqs_queue):
        """Order without 'sku' should fail validation."""
        event = {"body": json.dumps({"order_id": "ORD-1", "quantity": 2})}
        result = _invoke_producer(event, sqs_queue)
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "Validation failed" in body["error"]
        assert any("sku" in d for d in body["details"])

    def test_invalid_quantity_returns_400(self, sqs_queue):
        """Negative quantity should fail validation."""
        event = {
            "body": json.dumps(
                {"order_id": "ORD-1", "sku": "W-1", "quantity": -5}
            )
        }
        result = _invoke_producer(event, sqs_queue)
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert any("positive" in d for d in body["details"])

    def test_invalid_price_returns_400(self, sqs_queue):
        """Non-numeric price should fail validation."""
        event = {
            "body": json.dumps(
                {
                    "order_id": "ORD-1",
                    "sku": "W-1",
                    "quantity": 1,
                    "price": "free",
                }
            )
        }
        result = _invoke_producer(event, sqs_queue)
        assert result["statusCode"] == 400

    def test_invalid_json_returns_400(self, sqs_queue):
        """Malformed JSON body should return 400."""
        event = {"body": "not-json!!!"}
        result = _invoke_producer(event, sqs_queue)
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "Invalid JSON" in body["error"]


class TestProducerSuccess:
    """Test successful order publishing."""

    def test_valid_order_returns_200(self, sqs_queue, sample_order):
        """A valid order should return 200 with message_id."""
        event = {"body": json.dumps(sample_order)}
        result = _invoke_producer(event, sqs_queue)
        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["message"] == "Order accepted"
        assert "message_id" in body
        assert body["order_id"] == sample_order["order_id"]

    def test_message_appears_in_sqs(self, sqs_client, sqs_queue, sample_order):
        """After publishing, the message should be in the SQS queue."""
        event = {"body": json.dumps(sample_order)}
        _invoke_producer(event, sqs_queue)

        messages = sqs_client.receive_message(
            QueueUrl=sqs_queue, MaxNumberOfMessages=1
        )
        assert "Messages" in messages
        msg_body = json.loads(messages["Messages"][0]["Body"])
        assert msg_body["order_id"] == sample_order["order_id"]
        assert msg_body["status"] == "pending"

    def test_direct_invocation_without_body_wrapper(self, sqs_queue, sample_order):
        """Direct Lambda invocation (no 'body' wrapper) should also work."""
        result = _invoke_producer(sample_order, sqs_queue)
        assert result["statusCode"] == 200

    def test_auto_generated_order_id(self, sqs_queue):
        """If order_id is empty, producer should auto-generate one."""
        event = {
            "body": json.dumps(
                {"order_id": "", "sku": "W-1", "quantity": 1}
            )
        }
        result = _invoke_producer(event, sqs_queue)
        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["order_id"].startswith("ORD-")


class TestProducerSQSFailure:
    """Test SQS error handling."""

    def test_sqs_send_failure_returns_500(self, sqs_queue, sample_order):
        """If SQS send fails, producer should return 500."""
        with patch.dict(
            "os.environ",
            {
                "AWS_ENDPOINT_URL": "",
                "SQS_QUEUE_URL": sqs_queue,
                "AWS_DEFAULT_REGION": "us-east-1",
            },
        ):
            import producer.app as producer_module

            importlib.reload(producer_module)

            from botocore.exceptions import ClientError

            mock_sqs = MagicMock()
            mock_sqs.send_message.side_effect = ClientError(
                {"Error": {"Code": "500", "Message": "SQS down"}},
                "SendMessage",
            )
            producer_module.sqs = mock_sqs
            producer_module.SQS_QUEUE_URL = sqs_queue

            event = {"body": json.dumps(sample_order)}
            result = producer_module.lambda_handler(event, None)
            assert result["statusCode"] == 500
            body = json.loads(result["body"])
            assert "Failed to queue order" in body["error"]
