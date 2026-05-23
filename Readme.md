# 🛒 LocalStack Serverless Order/Inventory Pipeline

A **local-first, event-driven order and inventory processing pipeline** built with Python, AWS SAM, LocalStack, SQS, Lambda, and DynamoDB. No paid AWS account required — everything runs locally via Docker.

This project demonstrates async event processing, queue-based decoupling, retry handling, and cloud-style observability — patterns directly applicable to production serverless architectures.

---

## 📐 Architecture Overview

```
┌─────────────────┐       ┌─────────────┐       ┌──────────────────┐       ┌────────────────┐
│  Producer       │──────▶│  SQS Queue  │──────▶│ Consumer Lambda  │──────▶│   DynamoDB     │
│  (API Gateway / │       │             │       │ (validate +      │       │   (orders /    │
│   CSV Upload)   │       │  + DLQ      │       │  transform)      │       │   inventory)   │
└─────────────────┘       └─────────────┘       └──────────────────┘       └────────────────┘
                                                         │
                                                         ▼
                                                 ┌──────────────┐
                                                 │  SNS Alerts  │
                                                 │  (failures / │
                                                 │  thresholds) │
                                                 └──────────────┘
```

All AWS services are emulated locally via **LocalStack** running in Docker.

---

## 🧰 Tech Stack

| Layer         | Technology                          |
|---------------|-------------------------------------|
| Language      | Python 3.11                         |
| Infra-as-Code | AWS SAM (`template.yaml`)           |
| Cloud Emulator| LocalStack (Docker)                 |
| Queue         | Amazon SQS (+ Dead Letter Queue)    |
| Compute       | AWS Lambda                          |
| Database      | Amazon DynamoDB                     |
| Input         | API Gateway (local) / S3 CSV upload |
| Alerts        | Amazon SNS (optional)               |
| Testing       | pytest + Postman                    |
| Runtime       | Docker                              |

---

## 📁 Project Structure

```
order-inventory-pipeline/
├── producer/
│   ├── app.py                  # Lambda: receives event, pushes to SQS
│   └── requirements.txt
├── consumer/
│   ├── app.py                  # Lambda: reads SQS, validates, writes DynamoDB
│   └── requirements.txt
├── ingestion/
│   ├── csv_uploader.py         # Optional: uploads CSV to S3, triggers pipeline
│   └── sample_orders.csv
├── tests/
│   ├── test_producer.py
│   ├── test_consumer.py
│   └── conftest.py
├── scripts/
│   ├── setup_localstack.sh     # Creates SQS queues, DynamoDB tables locally
│   └── invoke_local.sh         # Helper to invoke Lambdas via SAM CLI
├── template.yaml               # AWS SAM infrastructure definition
├── samconfig.toml              # SAM deploy config (LocalStack endpoint)
├── docker-compose.yml          # Spins up LocalStack
├── requirements-dev.txt        # Dev/test dependencies
└── README.md
```

---

## ⚙️ Prerequisites

Install the following before getting started:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (required for LocalStack)
- [Python 3.11+](https://www.python.org/downloads/)
- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)
- [LocalStack CLI](https://docs.localstack.cloud/getting-started/installation/) (`pip install localstack`)
- [AWS CLI](https://aws.amazon.com/cli/) (used with fake creds for LocalStack)
- [Postman](https://www.postman.com/downloads/) (optional, for API testing)

---

## 🚀 Getting Started

### 1. Clone the repo

```bash
git clone https://github.com/your-username/order-inventory-pipeline.git
cd order-inventory-pipeline
```

### 2. Set up a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
```

### 3. Start LocalStack via Docker

```bash
docker-compose up -d
```

Verify LocalStack is running:

```bash
curl http://localhost:4566/_localstack/health
```

### 4. Configure fake AWS credentials (LocalStack ignores real ones)

```bash
aws configure
# AWS Access Key ID:     test
# AWS Secret Access Key: test
# Default region:        us-east-1
# Output format:         json
```

### 5. Create SQS queues and DynamoDB tables

```bash
bash scripts/setup_localstack.sh
```

This script creates:
- `orders-queue` — main SQS queue
- `orders-dlq` — dead-letter queue for failed messages
- `OrdersTable` — DynamoDB table (partition key: `order_id`)
- `InventoryTable` — DynamoDB table (partition key: `sku`)

### 6. Build and deploy with SAM (to LocalStack)

```bash
sam build
sam deploy --config-file samconfig.toml
```

---

## 🔄 Running the Pipeline

### Send an order event (via API Gateway)

```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "order_id": "ORD-001",
    "customer_id": "CUST-42",
    "sku": "WIDGET-XL",
    "quantity": 3,
    "price": 29.99
  }'
```

### Or invoke the producer Lambda directly

```bash
bash scripts/invoke_local.sh producer '{"order_id":"ORD-002","sku":"WIDGET-SM","quantity":1}'
```

### Upload a CSV batch (optional S3 ingestion)

```bash
python ingestion/csv_uploader.py --file ingestion/sample_orders.csv
```

---

## 🧪 Running Tests

```bash
pytest tests/ -v
```

Tests cover:
- Producer: validates SQS message format and send success
- Consumer: validates DynamoDB write, retry logic, and DLQ routing
- Integration: end-to-end message flow with LocalStack

---

## 📊 Observability

The consumer Lambda logs the following to CloudWatch (emulated by LocalStack):

| Metric         | Description                              |
|----------------|------------------------------------------|
| `processed`    | Successfully written to DynamoDB         |
| `failed`       | Validation or write errors               |
| `retried`      | Messages reprocessed after initial fail  |
| `latency_ms`   | Time from SQS receive to DynamoDB write  |

View logs locally:

```bash
sam logs -n ConsumerFunction --stack-name order-pipeline --tail
```

---

## 🔁 Retry & Failure Handling

- The SQS queue has a **maxReceiveCount of 3** — messages are retried up to 3 times.
- After 3 failures, messages are routed to the **Dead Letter Queue (DLQ)**.
- An optional **SNS topic** publishes alerts when:
  - A message lands in the DLQ
  - Inventory quantity drops below a threshold

---

## 🛑 Stopping LocalStack

```bash
docker-compose down
```

---

## 🗺️ Build Order (Recommended)

1. ✅ Set up Docker, Python, SAM CLI, LocalStack
2. ✅ Build producer Lambda → sends to SQS
3. ✅ Build consumer Lambda → reads SQS, writes DynamoDB
4. ✅ Add DLQ + retry logic + structured logging
5. ⬜ (Optional) Add S3 CSV ingestion trigger
6. ⬜ (Optional) Add SNS alerts for failures/thresholds

---

## 🌐 Environment Variables

| Variable               | Default                        | Description                    |
|------------------------|--------------------------------|--------------------------------|
| `AWS_ENDPOINT_URL`     | `http://localhost:4566`        | LocalStack endpoint            |
| `SQS_QUEUE_URL`        | `http://localhost:4566/...`    | Full SQS queue URL             |
| `DYNAMODB_TABLE_NAME`  | `OrdersTable`                  | Target DynamoDB table          |
| `SNS_TOPIC_ARN`        | *(optional)*                   | ARN for alert notifications    |
| `AWS_DEFAULT_REGION`   | `us-east-1`                    | AWS region (fake for LocalStack)|

Set these in a `.env` file and load with `python-dotenv`, or export them directly in your shell.

---

## 📦 requirements-dev.txt

```
boto3>=1.34.0
pytest>=8.0.0
pytest-mock>=3.12.0
moto[sqs,dynamodb]>=5.0.0   # For unit tests without LocalStack
python-dotenv>=1.0.0
requests>=2.31.0
```

---

## 📝 Notes

- This project uses **LocalStack Community Edition** (free). SNS and some advanced CloudWatch features may require LocalStack Pro.
- `moto` is used in unit tests as a lightweight alternative to spinning up LocalStack for every test run.
- The SAM `samconfig.toml` overrides the endpoint to `http://localhost:4566` so all deploys target LocalStack, not real AWS.

---

## 📄 License

MIT — free to use, modify, and build on.