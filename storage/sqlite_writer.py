"""
storage/sqlite_writer.py
-------------------------
Writes transactions and fraud alerts to a local SQLite database.
Zero setup — SQLite is built into Python, no server needed.

Database file: data/fraud_pipeline.db
Two tables:
    transactions  — every processed transaction
    fraud_alerts  — only flagged transactions with model scores
"""

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional


import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH
    
logger = logging.getLogger(__name__)


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=10000")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.row_factory = sqlite3.Row
    return conn


def _init_tables():
    """Create tables if they don't exist. Runs once at startup."""
    conn = _connect()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS transactions (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id          TEXT NOT NULL UNIQUE,
                timestamp               TEXT,
                type                    TEXT,
                amount                  REAL,
                sender_id               TEXT,
                sender_balance_before   REAL,
                sender_balance_after    REAL,
                receiver_id             TEXT,
                receiver_balance_before REAL,
                receiver_balance_after  REAL,
                source                  TEXT,
                kafka_partition         INTEGER,
                kafka_offset            INTEGER,
                created_at              TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_tx_sender    ON transactions(sender_id);
            CREATE INDEX IF NOT EXISTS idx_tx_receiver  ON transactions(receiver_id);
            CREATE INDEX IF NOT EXISTS idx_tx_timestamp ON transactions(timestamp);

            CREATE TABLE IF NOT EXISTS fraud_alerts (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id    TEXT NOT NULL UNIQUE,
                timestamp         TEXT,
                sender_id         TEXT,
                receiver_id       TEXT,
                amount            REAL,
                confidence        REAL,
                reason            TEXT,
                rf_score          REAL,
                xgb_score         REAL,
                iso_flag          INTEGER,
                is_fraud_label    INTEGER,
                fraud_pattern     TEXT,
                graph_pattern     TEXT,
                created_at        TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_alert_sender    ON fraud_alerts(sender_id);
            CREATE INDEX IF NOT EXISTS idx_alert_timestamp ON fraud_alerts(timestamp);
            CREATE INDEX IF NOT EXISTS idx_alert_confidence ON fraud_alerts(confidence);

            CREATE TABLE IF NOT EXISTS scan_log (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                cycles_found      INTEGER DEFAULT 0,
                communities_found INTEGER DEFAULT 0,
                bursts_found      INTEGER DEFAULT 0,
                hubs_found        INTEGER DEFAULT 0,
                scanned_at        TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.commit()
        logger.info(f"SQLite database ready at {DB_PATH.resolve()}")
    finally:
        conn.close()


class SQLiteWriter:
    """
    Writes pipeline data to SQLite.
    Thread-safe — each write opens and closes its own connection.
    """

    def __init__(self):
        _init_tables()
        logger.info("SQLite writer ready ✅")

    def write_transaction(self, tx: dict):
        """Insert a transaction. Skips silently on duplicate transaction_id."""
        sql = """
            INSERT OR IGNORE INTO transactions (
                transaction_id, timestamp, type, amount,
                sender_id, sender_balance_before, sender_balance_after,
                receiver_id, receiver_balance_before, receiver_balance_after,
                source, kafka_partition, kafka_offset
            ) VALUES (
                :transaction_id, :timestamp, :type, :amount,
                :sender_id, :sender_balance_before, :sender_balance_after,
                :receiver_id, :receiver_balance_before, :receiver_balance_after,
                :source, :kafka_partition, :kafka_offset
            )
        """
        params = {
            "transaction_id":          tx["transaction_id"],
            "timestamp":               tx.get("timestamp"),
            "type":                    tx.get("source", "unknown"),
            "amount":                  tx["amount"],
            "sender_id":               tx["sender_id"],
            "sender_balance_before":   tx.get("sender_bal_before"),
            "sender_balance_after":    tx.get("sender_bal_after"),
            "receiver_id":             tx["receiver_id"],
            "receiver_balance_before": tx.get("receiver_bal_before"),
            "receiver_balance_after":  tx.get("receiver_bal_after"),
            "source":                  tx.get("source"),
            "kafka_partition":         tx.get("_kafka_partition"),
            "kafka_offset":            tx.get("_kafka_offset"),
        }
        self._execute(sql, params)

    def write_transaction_batch(self, transactions: list[dict]):
        if not transactions:
            return
        rows = [
            (
                tx["transaction_id"],
                tx.get("timestamp"),
                tx.get("source", tx.get("type")),
                tx.get("amount"),
                tx.get("sender_id"),
                tx.get("sender_bal_before"),
                tx.get("sender_bal_after"),
                tx.get("receiver_id"),
                tx.get("receiver_bal_before"),
                tx.get("receiver_bal_after"),
                tx.get("source"),
                tx.get("_kafka_partition"),
                tx.get("_kafka_offset"),
            )
            for tx in transactions
        ]
        sql = """
            INSERT OR IGNORE INTO transactions
                (transaction_id, timestamp, type, amount,
                 sender_id, sender_balance_before, sender_balance_after,
                 receiver_id, receiver_balance_before, receiver_balance_after,
                 source, kafka_partition, kafka_offset)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        conn = _connect()
        try:
            with conn:
                conn.executemany(sql, rows)
        finally:
            conn.close()

    def write_alert(self, tx: dict, prediction: dict, graph_pattern: Optional[str] = None):
        """Insert a fraud alert."""
        sql = """
            INSERT OR IGNORE INTO fraud_alerts (
                transaction_id, timestamp, sender_id, receiver_id, amount,
                confidence, reason, rf_score, xgb_score, iso_flag,
                is_fraud_label, fraud_pattern, graph_pattern
            ) VALUES (
                :transaction_id, :timestamp, :sender_id, :receiver_id, :amount,
                :confidence, :reason, :rf_score, :xgb_score, :iso_flag,
                :is_fraud_label, :fraud_pattern, :graph_pattern
            )
        """
        votes = prediction.get("model_votes", {})
        params = {
            "transaction_id": tx["transaction_id"],
            "timestamp":      tx.get("timestamp"),
            "sender_id":      tx["sender_id"],
            "receiver_id":    tx["receiver_id"],
            "amount":         tx["amount"],
            "confidence":     prediction.get("confidence"),
            "reason":         prediction.get("reason"),
            "rf_score":       votes.get("random_forest"),
            "xgb_score":      votes.get("xgboost"),
            "iso_flag":       1 if votes.get("isolation_forest") else 0,
            "is_fraud_label": 1 if tx.get("is_fraud") else (0 if tx.get("is_fraud") is False else None),
            "fraud_pattern":  tx.get("fraud_pattern"),
            "graph_pattern":  graph_pattern,
        }
        self._execute(sql, params)

    def write_alert_batch(self, fraud_pairs: list[tuple[dict, dict]]):
        if not fraud_pairs:
            return
        rows = []
        for tx, pred in fraud_pairs:
            votes = pred.get("model_votes", {})
            rows.append((
                tx["transaction_id"],
                tx.get("timestamp"),
                tx.get("sender_id"),
                tx.get("receiver_id"),
                tx.get("amount"),
                pred.get("confidence"),
                pred.get("reason"),
                votes.get("random_forest"),
                votes.get("xgboost"),
                1 if votes.get("isolation_forest") else 0,
                1 if tx.get("is_fraud") else (0 if tx.get("is_fraud") is False else None),
                tx.get("fraud_pattern"),
                None,  # graph_pattern
            ))
        sql = """
            INSERT OR IGNORE INTO fraud_alerts
                (transaction_id, timestamp, sender_id, receiver_id, amount,
                 confidence, reason, rf_score, xgb_score, iso_flag,
                 is_fraud_label, fraud_pattern, graph_pattern)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        conn = _connect()
        try:
            with conn:
                conn.executemany(sql, rows)
        finally:
            conn.close()

    def write_scan_log(self, cycles: int, communities: int, bursts: int, hubs: int):
        """Write a graph scan summary row."""
        sql = """
            INSERT INTO scan_log (cycles_found, communities_found, bursts_found, hubs_found)
            VALUES (:cycles, :communities, :bursts, :hubs)
        """
        self._execute(sql, {
            "cycles": cycles, "communities": communities,
            "bursts": bursts, "hubs": hubs,
        })

    def update_graph_pattern(self, transaction_id: str, pattern: str):
        """Update graph_pattern on an existing alert."""
        sql = """
            UPDATE fraud_alerts SET graph_pattern = :pattern
            WHERE transaction_id = :transaction_id AND graph_pattern IS NULL
        """
        self._execute(sql, {"pattern": pattern, "transaction_id": transaction_id})

    def _execute(self, sql: str, params: dict):
        conn = _connect()
        try:
            conn.execute(sql, params)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"SQLite write failed: {e}")
        finally:
            conn.close()

    def close(self):
        pass  # no persistent connection to close
