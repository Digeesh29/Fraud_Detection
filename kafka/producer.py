"""
kafka/producer.py
-----------------
Reads from either the PaySim loader or the Synthetic generator
and publishes every transaction to a Kafka topic as a JSON message.

Usage:
    # Stream synthetic data (default, no CSV needed)
    python kafka/producer.py

    # Stream from PaySim CSV
    python kafka/producer.py --source paysim --csv data/paysim.csv

    # Tune throughput
    python kafka/producer.py --source synthetic --rate 100
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Allow running from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generator.synthetic_generator import SyntheticGenerator
from generator.paysim_loader import PaySimLoader

# ── Config ─────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "transactions"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ── Producer Setup ─────────────────────────────────────────────────────────────

def build_producer(retries: int = 5, retry_delay: float = 3.0) -> KafkaProducer:
    """
    Create a KafkaProducer with JSON serialization.
    Retries on connection failure to handle slow broker startup.
    """
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # Key by sender_id so all transactions from one account
                # land on the same partition (preserves ordering per account)
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                # Batch small messages together for throughput
                linger_ms=10,
                batch_size=16384,
                # Retry on transient errors
                retries=3,
                acks="all",  # Wait for all replicas to confirm
            )
            logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
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


def on_send_error(exc):
    logger.error(f"Failed to deliver message: {exc}")


# ── Publish Loop ───────────────────────────────────────────────────────────────

def run_producer(source: str, csv_path: str | None, rate: float):
    producer = build_producer()

    if source == "paysim":
        if not csv_path:
            raise ValueError("--csv path required when --source paysim")
        stream = PaySimLoader(csv_path).stream(delay=1.0 / rate if rate > 0 else 0)
        logger.info(f"Streaming PaySim data → topic '{KAFKA_TOPIC}'")
    else:
        gen = SyntheticGenerator(num_accounts=500, fraud_rate=0.02)
        stream = gen.stream(rate_per_second=rate)
        logger.info(f"Streaming synthetic data → topic '{KAFKA_TOPIC}' at ~{rate} tx/s")

    sent = 0
    fraud_sent = 0

    try:
        for tx in stream:
            # Partition key = sender account ID
            key = tx.get("sender_id")

            producer.send(
                KAFKA_TOPIC,
                key=key,
                value=tx,
            ).add_errback(on_send_error)

            sent += 1
            if tx.get("is_fraud"):
                fraud_sent += 1

            # Periodic progress log
            if sent % 1000 == 0:
                logger.info(
                    f"Published {sent:,} transactions "
                    f"({fraud_sent:,} fraud, "
                    f"{fraud_sent / sent * 100:.2f}%)"
                )

    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    finally:
        producer.flush()  # Ensure all buffered messages are sent
        producer.close()
        logger.info(
            f"Producer closed — total sent: {sent:,} "
            f"({fraud_sent:,} fraud)"
        )


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fraud Detection Kafka Producer")
    parser.add_argument(
        "--source",
        choices=["synthetic", "paysim"],
        default="synthetic",
        help="Data source (default: synthetic)",
    )
    parser.add_argument(
        "--csv",
        type=str,
        default=None,
        help="Path to PaySim CSV (required if --source paysim)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=50.0,
        help="Target transactions per second (default: 50)",
    )
    args = parser.parse_args()
    run_producer(args.source, args.csv, args.rate)
