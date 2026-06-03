"""
kafka/consumer.py
-----------------
Consumes transaction messages from the Kafka topic.
Deserializes each message and hands it to a pluggable handler.

Currently implements:
    - Console handler    (default) — prints each transaction
    - File handler       (--output) — writes JSONL to disk for inspection

In Phase 3, the handler will be replaced with the ML inference pipeline.

Usage:
    # Print transactions to console
    python kafka/consumer.py

    # Write to a JSONL file (good for verifying the pipeline)
    python kafka/consumer.py --output data/consumed.jsonl

    # Run a second consumer in the same group (parallel processing)
    python kafka/consumer.py --group fraud-workers
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# ── Config ─────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "transactions"
DEFAULT_GROUP_ID = "fraud-detection-group"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ── Consumer Setup ─────────────────────────────────────────────────────────────

def build_consumer(group_id: str, retries: int = 5, retry_delay: float = 3.0) -> KafkaConsumer:
    """
    Create a KafkaConsumer with JSON deserialization.
    Multiple consumers sharing the same group_id get different partitions
    automatically — horizontal scaling with zero config changes.
    """
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=group_id,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                key_deserializer=lambda b: b.decode("utf-8") if b else None,
                # Start from the earliest unread message on first join
                auto_offset_reset="earliest",
                # Commit offsets automatically every 5s
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                # How long to wait for new messages before returning empty poll
                consumer_timeout_ms=10_000,
                # Max messages returned per poll cycle
                max_poll_records=500,
            )
            logger.info(
                f"Consumer connected to Kafka — "
                f"topic='{KAFKA_TOPIC}', group='{group_id}'"
            )
            return consumer
        except NoBrokersAvailable:
            logger.warning(
                f"Kafka not reachable (attempt {attempt}/{retries}). "
                f"Retrying in {retry_delay}s ..."
            )
            time.sleep(retry_delay)

    raise RuntimeError(
        "Could not connect to Kafka after several attempts.\n"
        "Make sure Kafka is running: see setup/start_kafka.sh"
    )


# ── Handlers ───────────────────────────────────────────────────────────────────

class ConsoleHandler:
    """Prints each transaction. Highlights fraud events."""

    def __init__(self):
        self.count = 0
        self.fraud_count = 0

    def handle(self, tx: dict):
        self.count += 1
        if tx.get("is_fraud"):
            self.fraud_count += 1
            logger.warning(
                f"🚨 FRAUD [{tx.get('fraud_pattern', 'unknown')}] "
                f"| {tx['sender_id']} → {tx['receiver_id']} "
                f"| ${tx['amount']:,.2f}"
            )
        else:
            if self.count % 100 == 0:  # Don't flood the terminal
                logger.info(
                    f"[{self.count:,}] Normal tx "
                    f"| {tx['sender_id']} → {tx['receiver_id']} "
                    f"| ${tx['amount']:,.2f}"
                )

    def summary(self):
        logger.info(
            f"Consumed {self.count:,} transactions, "
            f"{self.fraud_count:,} fraud "
            f"({self.fraud_count / max(self.count, 1) * 100:.2f}%)"
        )


class FileHandler:
    """Writes every transaction as a JSON line to disk."""

    def __init__(self, output_path: str):
        self.path = Path(output_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.path, "a", encoding="utf-8")
        self.count = 0
        self.fraud_count = 0
        logger.info(f"Writing transactions to {self.path}")

    def handle(self, tx: dict):
        self.count += 1
        if tx.get("is_fraud"):
            self.fraud_count += 1
        self._file.write(json.dumps(tx) + "\n")
        if self.count % 500 == 0:
            self._file.flush()
            logger.info(f"Written {self.count:,} transactions to {self.path}")

    def summary(self):
        self._file.flush()
        self._file.close()
        logger.info(
            f"File closed — {self.count:,} transactions written "
            f"({self.fraud_count:,} fraud)"
        )


# ── Consume Loop ───────────────────────────────────────────────────────────────

def run_consumer(group_id: str, handler):
    consumer = build_consumer(group_id)

    logger.info("Waiting for messages ... (Ctrl+C to stop)")

    try:
        for message in consumer:
            tx = message.value

            # Attach Kafka metadata for traceability
            tx["_kafka_partition"] = message.partition
            tx["_kafka_offset"] = message.offset

            handler.handle(tx)

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    finally:
        consumer.close()
        handler.summary()


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fraud Detection Kafka Consumer")
    parser.add_argument(
        "--group",
        type=str,
        default=DEFAULT_GROUP_ID,
        help=f"Consumer group ID (default: {DEFAULT_GROUP_ID})",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="If set, write transactions to this JSONL file instead of console",
    )
    args = parser.parse_args()

    handler = FileHandler(args.output) if args.output else ConsoleHandler()
    run_consumer(args.group, handler)
