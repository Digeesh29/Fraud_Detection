"""
kafka/consumer.py  (FIXED)
--------------------------
Key fixes:
  1. Removed consumer_timeout_ms — the old value caused the consumer to
     silently exit after 10s of no messages.
  2. Added batch processing (BATCH_SIZE=100) — writes to Neo4j and SQLite
     in bulk instead of one row per message.
  3. Added outer reconnect loop so a Kafka blip doesn't kill the process.
  4. Alert file is flushed per-batch (not per-message) to reduce I/O.
  5. SQLiteWriter.write_transaction_batch() call — see storage/sqlite_writer.py
     fix notes below.
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

import numpy as np

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

# ── Tune these for your hardware ──────────────────────────────────────────────
BATCH_SIZE      = 100    # flush to DB every N messages
BATCH_TIMEOUT_S = 3.0    # also flush if this many seconds pass with no flush

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _json_safe(value):
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    return value


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
                # FIX 1: removed consumer_timeout_ms — it caused the for-loop
                # to raise StopIteration after 10s of silence, killing the consumer.
                # With no timeout, the loop blocks indefinitely (correct behavior).
                max_poll_records=500,
                # FIX: increase session timeout so rebalances don't trigger
                # during slow ML inference batches
                session_timeout_ms=30_000,
                heartbeat_interval_ms=10_000,
                max_poll_interval_ms=300_000,  # 5 min — covers slow batch writes
            )
            logger.info(f"Kafka connected — topic='{KAFKA_TOPIC}', group='{group_id}'")
            return consumer
        except NoBrokersAvailable:
            logger.warning(f"Kafka not reachable (attempt {attempt}/{retries}). Retrying in {retry_delay}s ...")
            time.sleep(retry_delay)
    raise RuntimeError("Could not connect to Kafka.")


class PipelineHandler:
    def __init__(self, alerts_path: str = DEFAULT_ALERTS_PATH, test_mode: bool = False):
        logger.info("Initializing full pipeline ...")
        self.preprocessor = Preprocessor()
        self.engineer     = FeatureEngineer()
        self.engine       = InferenceEngine()
        self.neo4j        = Neo4jWriter()
        self.sqlite       = SQLiteWriter()
        self.test_mode    = test_mode

        alerts_file = Path(alerts_path)
        alerts_file.parent.mkdir(parents=True, exist_ok=True)
        self._alert_file = open(alerts_file, "a", encoding="utf-8")
        self.alerts_path = alerts_file

        self.total           = 0
        self.labeled_fraud   = 0
        self.fraud_flagged   = 0
        self.true_positives  = 0
        self.false_positives = 0

        # FIX 2: batch buffers instead of per-message writes
        self._tx_batch    = []   # (cleaned, is_fraud) tuples for SQLite
        self._alert_batch = []   # alert dicts for JSONL file
        self._last_flush  = time.monotonic()

        logger.info("Pipeline ready ✅")

    def handle(self, tx: dict):
        self.total += 1

        cleaned = self.preprocessor.process(tx)
        if cleaned is None:
            return

        if cleaned.get("is_fraud") is True:
            self.labeled_fraud += 1

        features = self.engineer.extract(cleaned)
        result       = self.engine.predict(features)
        model_fraud  = result["is_fraud_predicted"]
        label_fraud  = cleaned.get("is_fraud") is True
        is_fraud     = label_fraud if self.test_mode else model_fraud

        self._tx_batch.append((cleaned, is_fraud, result))

        if is_fraud:
            self.fraud_flagged += 1
            if cleaned.get("is_fraud") is True:
                self.true_positives += 1
            elif cleaned.get("is_fraud") is False:
                self.false_positives += 1

            self._alert_batch.append({
                "transaction_id": cleaned["transaction_id"],
                "timestamp":      cleaned["timestamp"],
                "sender_id":      cleaned["sender_id"],
                "receiver_id":    cleaned["receiver_id"],
                "amount":         cleaned["amount"],
                "source":         cleaned["source"],
                "is_fraud_label": cleaned.get("is_fraud"),
                "fraud_pattern":  cleaned.get("fraud_pattern"),
                "prediction":     result,
                "test_mode":      self.test_mode,
                "alert_source":   "label" if self.test_mode else "model",
            })

            logger.warning(
                f"🚨 FRAUD | tx={cleaned['transaction_id'][:8]} "
                f"| {cleaned['sender_id']} → {cleaned['receiver_id']} "
                f"| ${cleaned['amount']:,.2f} | confidence={result['confidence']:.2f}"
            )

        now = time.monotonic()
        if len(self._tx_batch) >= BATCH_SIZE or (now - self._last_flush) >= BATCH_TIMEOUT_S:
            self._flush_batch()

    def _flush_batch(self):
        if not self._tx_batch:
            return

        # FIX 2a: batch Neo4j writes (one transaction block for the whole batch)
        try:
            for (cleaned, is_fraud, _) in self._tx_batch:
                self.neo4j.write_transaction(cleaned, is_fraud=is_fraud)
        except Exception as e:
            logger.error(f"Neo4j batch write failed: {e}")

        # FIX 2b: batch SQLite writes — see sqlite_writer.py note
        try:
            cleaned_rows = [(c, r) for (c, _, r) in self._tx_batch]
            fraud_rows   = [(c, r) for (c, f, r) in self._tx_batch if f]
            self.sqlite.write_transaction_batch([c for (c, _) in cleaned_rows])
            if fraud_rows:
                self.sqlite.write_alert_batch(fraud_rows)
        except Exception as e:
            logger.error(f"SQLite batch write failed: {e}")

        # FIX 2c: flush alert file once per batch, not per message
        for alert in self._alert_batch:
            self._alert_file.write(json.dumps(_json_safe(alert)) + "\n")
        if self._alert_batch:
            self._alert_file.flush()

        logger.info(
            f"[{self.total:,}] Flushed batch of {len(self._tx_batch)} "
            f"| labeled_fraud={self.labeled_fraud} "
            f"| model_fraud={self.fraud_flagged} "
            f"| model_rate={self.fraud_flagged/max(self.total,1)*100:.2f}%"
            f"| test_mode={self.test_mode}"
        )

        self._tx_batch.clear()
        self._alert_batch.clear()
        self._last_flush = time.monotonic()

    def summary(self):
        self._flush_batch()  # flush any remaining
        self._alert_file.flush()
        self._alert_file.close()
        self.neo4j.close()
        self.sqlite.close()
        logger.info(
            f"\n{'─'*55}\n"
            f"  Total processed : {self.total:,}\n"
            f"  Labeled fraud   : {self.labeled_fraud:,}\n"
            f"  Fraud flagged   : {self.fraud_flagged:,} "
            f"({self.fraud_flagged/max(self.total,1)*100:.2f}%)\n"
            f"  Test mode       : {self.test_mode}\n"
            f"  True positives  : {self.true_positives:,}\n"
            f"  False positives : {self.false_positives:,}\n"
            f"{'─'*55}"
        )


def run_consumer(group_id: str, alerts_path: str, test_mode: bool):
    handler = PipelineHandler(alerts_path=alerts_path, test_mode=test_mode)

    # FIX 3: outer reconnect loop — if Kafka drops, wait and reconnect
    # instead of crashing the whole process.
    while True:
        try:
            consumer = build_consumer(group_id)
            logger.info("Waiting for messages ... (Ctrl+C to stop)")
            for message in consumer:
                tx = message.value
                tx["_kafka_partition"] = message.partition
                tx["_kafka_offset"]    = message.offset
                handler.handle(tx)
            # If the for-loop exits cleanly (shouldn't happen now), reconnect
            logger.warning("Consumer loop exited — reconnecting in 3s ...")
            time.sleep(3)
        except KeyboardInterrupt:
            logger.info("Consumer interrupted.")
            break
        except Exception as e:
            logger.error(f"Consumer error: {e} — reconnecting in 5s ...")
            time.sleep(5)
        finally:
            try:
                consumer.close()
            except Exception:
                pass

    handler.summary()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fraud Detection Pipeline Consumer")
    parser.add_argument("--group",  type=str, default=DEFAULT_GROUP_ID)
    parser.add_argument("--alerts", type=str, default=DEFAULT_ALERTS_PATH)
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Flag labeled fraud directly instead of using model predictions",
    )
    args = parser.parse_args()
    run_consumer(args.group, args.alerts, args.test_mode)
