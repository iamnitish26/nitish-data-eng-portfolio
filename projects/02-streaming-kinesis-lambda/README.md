# 02 — Streaming: Kinesis → Lambda → S3 (Simulator)

**Goal:** Ingest clickstream events to Kinesis, process in Lambda, write hourly aggregates to S3.
Includes a local **producer simulator** and a Lambda handler you can deploy.

## Local simulator
```bash
python -m venv .venv && source .venv/bin/activate
pip install boto3
python src/producer_sim.py --stream demo-events --count 100 --region eu-west-2
```
(Requires AWS credentials if pointing to real Kinesis; otherwise see `src/producer_file.py` for local JSONL.)

## Lambda handler
- `src/lambda_handler.py` processes a Kinesis batch, aggregates events per hour/action, writes JSON to S3 with idempotent keys.
- See `infra/policy.json` for minimal IAM example.
