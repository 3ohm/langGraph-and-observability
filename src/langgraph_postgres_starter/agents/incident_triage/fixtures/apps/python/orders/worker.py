"""Consume orders.paid events from SQS and invoice each order."""

import json
import logging
import os

import boto3

from .service import build_invoice

log = logging.getLogger("orders.worker")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    queue_url = os.environ["ORDERS_PAID_QUEUE_URL"]
    sqs = boto3.client("sqs")
    log.info("polling %s", queue_url)
    while True:
        reply = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=20)
        for message in reply.get("Messages", []):
            order = json.loads(message["Body"])
            invoice = build_invoice(order)
            log.info(
                "invoiced order %s: %d lines, total %s",
                invoice.order_id,
                len(invoice.lines),
                invoice.total,
            )
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])


if __name__ == "__main__":
    main()
