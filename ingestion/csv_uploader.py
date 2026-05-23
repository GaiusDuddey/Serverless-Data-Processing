"""
CSV Uploader — Batch Order Ingestion
Reads a CSV file of orders, uploads it to S3, and optionally sends each
row as an individual SQS message into the processing pipeline.
"""

import argparse
import csv
import json
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError

# ── Configuration ────────────────────────────────────────────────────────────
ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "order-pipeline-csv-uploads")
SQS_QUEUE_URL = os.environ.get(
    "SQS_QUEUE_URL",
    "http://localhost:4566/000000000000/orders-queue",
)

s3 = boto3.client("s3", region_name=REGION, endpoint_url=ENDPOINT_URL)
sqs = boto3.client("sqs", region_name=REGION, endpoint_url=ENDPOINT_URL)


def upload_to_s3(filepath: str) -> str:
    """Upload the CSV file to S3 and return the S3 key."""
    filename = os.path.basename(filepath)
    key = f"uploads/{int(time.time())}_{filename}"
    try:
        s3.upload_file(filepath, S3_BUCKET, key)
        print(f"✅ Uploaded {filepath} → s3://{S3_BUCKET}/{key}")
        return key
    except ClientError as exc:
        print(f"❌ S3 upload failed: {exc}")
        sys.exit(1)


def send_orders_to_sqs(filepath: str) -> int:
    """Read each CSV row and send as an SQS message. Returns count sent."""
    sent = 0
    with open(filepath, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Normalise field types
            order = {
                "order_id": row.get("order_id", f"CSV-{sent+1:04d}"),
                "customer_id": row.get("customer_id", "UNKNOWN"),
                "sku": row.get("sku", ""),
                "quantity": int(row.get("quantity", 1)),
                "price": float(row.get("price", 0.0)),
                "source": "csv_upload",
                "created_at": time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                ),
            }
            try:
                sqs.send_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MessageBody=json.dumps(order),
                )
                sent += 1
                print(f"  📨 Sent order {order['order_id']} to SQS")
            except ClientError as exc:
                print(f"  ❌ Failed to send {order['order_id']}: {exc}")

    return sent


def main():
    parser = argparse.ArgumentParser(
        description="Upload a CSV of orders and feed them into the pipeline"
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Path to the CSV file (e.g. ingestion/sample_orders.csv)",
    )
    parser.add_argument(
        "--skip-s3",
        action="store_true",
        help="Skip S3 upload, only send rows to SQS",
    )
    args = parser.parse_args()

    filepath = args.file
    if not os.path.isfile(filepath):
        print(f"❌ File not found: {filepath}")
        sys.exit(1)

    print(f"\n🔄 Processing {filepath}...\n")

    # 1. Upload to S3 (unless skipped)
    if not args.skip_s3:
        upload_to_s3(filepath)

    # 2. Send each row to SQS
    count = send_orders_to_sqs(filepath)

    print(f"\n✅ Done — {count} order(s) sent to SQS.\n")


if __name__ == "__main__":
    main()
