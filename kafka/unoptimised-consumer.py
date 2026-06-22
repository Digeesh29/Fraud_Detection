"""
kafka/consumer.py
-----------------
Consumes transactions from Kafka and runs the full pipeline:

    Kafka message
        → Preprocessor         (clean + normalize)
        → FeatureEngineer      (extract feature vector)
        → InferenceEngine      (ensemble ML prediction)
        → Neo4jWriter          (write to graph store)
        → PostgresWriter       (write to structured store)
        → MLInferenceHandler   (log + write fraud alerts)

Usage:
    python kafka/consumer.py
    python kafka/consumer.py --alerts data/alerts.jsonl
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

from processing.preprocessor import Preprocessor
from processing.feature_engineering import FeatureEngineer
from ml.inference import InferenceEngine
from graph.neo4j_writer import Neo4jWriter
from storage.sqlite_writer import SQLiteWriter

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC             = "transactions"
DEFAULT_GROUP_ID        = "fraud-detection-group"
DEFAULT_ALERTS_PATH     = "data/fraud_alerts.jsonl"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ── Consumer Setup ─────────────────────────────────────────────────────────────

def build_consumer(group_id: str, retries: int = 5, retry_delay: float = 3.0) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=group_id,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                key_deserializer=lambda b: b.decode("utf-8") if b else None,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                consumer_timeout_ms=10_000,
                max_poll_records=500,
            )
            logger.info(f"Kafka connected — topic='{KAFKA_TOPIC}', group='{group_id}'")
            return consumer
        except NoBrokersAvailable:
            logger.warning(f"Kafka not reachable (attempt {attempt}/{retries}). Retrying in {retry_delay}s ...")
            time.sleep(retry_delay)
    raise RuntimeError("Could not connect to Kafka. Run: ./setup/start_kafka.sh or docker compose up -d")


# ── Full Pipeline Handler ──────────────────────────────────────────────────────

class PipelineHandler:
    """
    Orchestrates all five pipeline stages per transaction.
    Writes every transaction to Neo4j + Postgres,
    and writes fraud alerts to JSONL file.
    """

    def __init__(self, alerts_path: str = DEFAULT_ALERTS_PATH):
        logger.info("Initializing full pipeline ...")

        self.preprocessor   = Preprocessor()
        self.engineer       = FeatureEngineer()
        self.engine         = InferenceEngine()
        self.neo4j          = Neo4jWriter()
        self.postgres       = SQLiteWriter()

        alerts_file = Path(alerts_path)
        alerts_file.parent.mkdir(parents=True, exist_ok=True)
        self._alert_file    = open(alerts_file, "a", encoding="utf-8")
        self.alerts_path    = alerts_file

        self.total           = 0
        self.fraud_flagged   = 0
        self.true_positives  = 0
        self.false_positives = 0

        logger.info("Pipeline ready ✅")

    def handle(self, tx: dict):
        self.total += 1

        # ── Stage 1: Preprocess ────────────────────────────────────────────────
        cleaned = self.preprocessor.process(tx)
        if cleaned is None:
            return

        # ── Stage 2: Feature engineering ──────────────────────────────────────
        features = self.engineer.extract(cleaned)

        # ── Stage 3: ML inference ──────────────────────────────────────────────
        result = self.engine.predict(features)
        is_fraud = result["is_fraud_predicted"]

        # ── Stage 4: Write to Neo4j ────────────────────────────────────────────
        try:
            self.neo4j.write_transaction(cleaned, is_fraud=is_fraud)
        except Exception as e:
            logger.error(f"Neo4j write failed: {e}")

        # ── Stage 5: Write to PostgreSQL ───────────────────────────────────────
        try:
            self.postgres.write_transaction(cleaned)
            if is_fraud:
                self.postgres.write_alert(cleaned, result)
        except Exception as e:
            logger.error(f"Postgres write failed: {e}")

        # ── Stage 6: Log + alert file ──────────────────────────────────────────
        if is_fraud:
            self.fraud_flagged += 1

            if cleaned.get("is_fraud") is True:
                self.true_positives += 1
            elif cleaned.get("is_fraud") is False:
                self.false_positives += 1

            logger.warning(
                f"🚨 FRAUD | tx={cleaned['transaction_id'][:8]} "
                f"| {cleaned['sender_id']} → {cleaned['receiver_id']} "
                f"| ${cleaned['amount']:,.2f} "
                f"| confidence={result['confidence']:.2f} "
                f"| {result['reason']}"
            )

            alert = {
                "transaction_id": cleaned["transaction_id"],
                "timestamp":      cleaned["timestamp"],
                "sender_id":      cleaned["sender_id"],
                "receiver_id":    cleaned["receiver_id"],
                "amount":         cleaned["amount"],
                "source":         cleaned["source"],
                "is_fraud_label": cleaned.get("is_fraud"),
                "fraud_pattern":  cleaned.get("fraud_pattern"),
                "prediction":     result,
            }
            self._alert_file.write(json.dumps(alert) + "\n")

        else:
            if self.total % 500 == 0:
                logger.info(
                    f"[{self.total:,}] Normal "
                    f"| {cleaned['sender_id']} → {cleaned['receiver_id']} "
                    f"| ${cleaned['amount']:,.2f} "
                    f"| RF={result['model_votes']['random_forest']:.3f} "
                    f"| XGB={result['model_votes']['xgboost']:.3f}"
                )

    def summary(self):
        self._alert_file.flush()
        self._alert_file.close()
        self.neo4j.close()
        self.postgres.close()

        logger.info(
            f"\n{'─' * 55}\n"
            f"  Pipeline Summary\n"
            f"  Total processed : {self.total:,}\n"
            f"  Fraud flagged   : {self.fraud_flagged:,} "
            f"({self.fraud_flagged / max(self.total,1) * 100:.2f}%)\n"
            f"  True positives  : {self.true_positives:,}\n"
            f"  False positives : {self.false_positives:,}\n"
            f"  Alerts written  : {self.alerts_path}\n"
            f"{'─' * 55}"
        )


# ── Consume Loop ───────────────────────────────────────────────────────────────

def run_consumer(group_id: str, alerts_path: str):
    consumer = build_consumer(group_id)
    handler  = PipelineHandler(alerts_path=alerts_path)

    logger.info("Waiting for messages ... (Ctrl+C to stop)")
    try:
        for message in consumer:
            tx = message.value
            tx["_kafka_partition"] = message.partition
            tx["_kafka_offset"]    = message.offset
            handler.handle(tx)
    except KeyboardInterrupt:
        logger.info("Consumer interrupted.")
    finally:
        consumer.close()
        handler.summary()


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fraud Detection Pipeline Consumer")
    parser.add_argument("--group",   type=str, default=DEFAULT_GROUP_ID)
    parser.add_argument("--alerts",  type=str, default=DEFAULT_ALERTS_PATH)
    args = parser.parse_args()
    run_consumer(args.group, args.alerts)
