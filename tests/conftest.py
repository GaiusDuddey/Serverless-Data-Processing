"""
conftest.py — Shared pytest fixtures for the order/inventory pipeline tests.

Uses the `moto` library to mock AWS services (SQS, DynamoDB, SNS, S3) so
tests run without Docker or LocalStack.
"""

import json
import os

import boto3
import pytest
from moto import mock_aws


# ── Environment setup (before any AWS clients are created) ───────────────────
@pytest.fixture(autouse=True)
def _aws_env(monkeypatch):
    """Set fake AWS credentials and region for every test."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    # Ensure Lambdas don't try to hit a real/local endpoint
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)


# ── Mocked AWS environment ──────────────────────────────────────────────────
@pytest.fixture
def aws_mock():
    """Start moto mocking for all AWS services."""
    with mock_aws():
        yield


@pytest.fixture
def sqs_client(aws_mock):
    """Return a boto3 SQS client inside a moto mock."""
    return boto3.client("sqs", region_name="us-east-1")


@pytest.fixture
def sqs_queue(sqs_client):
    """Create the orders-queue and return its URL."""
    resp = sqs_client.create_queue(QueueName="orders-queue")
    return resp["QueueUrl"]


@pytest.fixture
def sqs_dlq(sqs_client):
    """Create the dead-letter queue and return its URL."""
    resp = sqs_client.create_queue(QueueName="orders-dlq")
    return resp["QueueUrl"]


@pytest.fixture
def dynamodb_resource(aws_mock):
    """Return a boto3 DynamoDB resource inside a moto mock."""
    return boto3.resource("dynamodb", region_name="us-east-1")


@pytest.fixture
def orders_table(dynamodb_resource):
    """Create the OrdersTable and return the Table object."""
    table = dynamodb_resource.create_table(
        TableName="OrdersTable",
        KeySchema=[{"AttributeName": "order_id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "order_id", "AttributeType": "S"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()
    return table


@pytest.fixture
def inventory_table(dynamodb_resource):
    """Create the InventoryTable and return the Table object."""
    table = dynamodb_resource.create_table(
        TableName="InventoryTable",
        KeySchema=[{"AttributeName": "sku", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "sku", "AttributeType": "S"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()
    return table


@pytest.fixture
def sns_client(aws_mock):
    """Return a boto3 SNS client inside a moto mock."""
    return boto3.client("sns", region_name="us-east-1")


@pytest.fixture
def sns_topic(sns_client):
    """Create the alerts SNS topic and return its ARN."""
    resp = sns_client.create_topic(Name="order-pipeline-alerts")
    return resp["TopicArn"]


@pytest.fixture
def s3_client(aws_mock):
    """Return a boto3 S3 client inside a moto mock."""
    return boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def s3_bucket(s3_client):
    """Create the CSV-upload S3 bucket and return the name."""
    bucket_name = "order-pipeline-csv-uploads"
    s3_client.create_bucket(Bucket=bucket_name)
    return bucket_name


# ── Helper: build a sample order dict ────────────────────────────────────────
@pytest.fixture
def sample_order():
    """Return a valid order dictionary."""
    return {
        "order_id": "ORD-TEST-001",
        "customer_id": "CUST-42",
        "sku": "WIDGET-XL",
        "quantity": 3,
        "price": 29.99,
    }


# ── Helper: build a sample SQS event ────────────────────────────────────────
@pytest.fixture
def make_sqs_event():
    """Factory fixture — returns a function that wraps an order dict in an SQS event."""

    def _make(order: dict, receive_count: int = 1) -> dict:
        return {
            "Records": [
                {
                    "messageId": "test-msg-001",
                    "body": json.dumps(order),
                    "attributes": {
                        "ApproximateReceiveCount": str(receive_count)
                    },
                    "eventSource": "aws:sqs",
                }
            ]
        }

    return _make
