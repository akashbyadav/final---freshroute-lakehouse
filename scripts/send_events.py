"""Optional event sender, invoked manually in AWS CloudShell after deployment."""

import argparse
import json
import logging
import time
from pathlib import Path

import boto3
from botocore.config import Config


def send_batch(api, stream_name, records):
    """Retry failed entries without resending records already accepted by Firehose."""
    pending = [{"Data": row} for row in records]
    for attempt in range(8):
        response = api.put_record_batch(DeliveryStreamName=stream_name, Records=pending)
        # Firehose can accept the HTTP request while rejecting individual entries.
        # Its response order matches the request, so retain only failed records.
        pending = [
            record
            for record, result in zip(pending, response["RequestResponses"], strict=True)
            if "ErrorCode" in result
        ]
        if not pending:
            return
        if attempt < 7:
            time.sleep(min(2**attempt, 20))
    raise RuntimeError(f"Firehose rejected {len(pending)} records after retries")


def send_events(path: Path, environment: str):
    """Send existing JSONL events; leave S3 delivery and Airflow execution separate."""
    if environment not in ("dev", "prod"):
        raise ValueError("Environment must be dev or prod")
    # Validate the whole file before contacting AWS. Preserve each original JSON
    # envelope and newline so Firehose can concatenate them into readable JSONL.
    records = [line + b"\n" for line in path.read_bytes().splitlines()]
    if not records:
        raise ValueError("The event file is empty")
    for line in records:
        envelope = json.loads(line)
        if envelope.get("entity_type") != "events" or not envelope.get("record_id"):
            raise ValueError("Use an events JSONL file with entity_type and record_id")
        if not isinstance(json.loads(envelope["payload"]), dict):
            raise ValueError("Each payload must contain a JSON object")
        if len(line) > 1_000_000:
            raise ValueError("An event exceeds the sender's one-million-byte record limit")

    # CloudShell supplies the signed-in operator's credentials. No account ID,
    # bucket name, deployment package, or local credential setup is required.
    api = boto3.client(
        "firehose", region_name="us-east-1", config=Config(retries={"max_attempts": 8})
    )
    stream_name = f"freshroute-{environment}-events"
    batch, size = [], 0
    for record in records:
        # Bound both count and bytes, leaving headroom for API serialization.
        if batch and (len(batch) == 200 or size + len(record) > 3_000_000):
            send_batch(api, stream_name, batch)
            batch, size = [], 0
        batch.append(record)
        size += len(record)
    if batch:
        send_batch(api, stream_name, batch)
    # Acceptance is not S3 delivery. The setup guide explains how to confirm the
    # buffered objects and observe the independently scheduled Airflow run.
    logging.info(
        json.dumps({"event": "firehose.accepted", "records": len(records), "stream": stream_name})
    )
    return len(records)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", type=Path, help="Existing events.jsonl file uploaded to CloudShell")
    parser.add_argument("--environment", choices=("dev", "prod"), default="dev")
    arguments = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    send_events(arguments.file, arguments.environment)
