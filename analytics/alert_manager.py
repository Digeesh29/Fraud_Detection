"""
analytics/alert_manager.py
---------------------------
Reads from SQLite and produces a unified alert feed for the dashboard.
"""

import logging
import sqlite3
from pathlib import Path
from typing import Optional

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH

logger = logging.getLogger(__name__)

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _rows_to_dicts(rows) -> list:
    return [dict(r) for r in rows]


class AlertManager:

    def get_summary_stats(self) -> dict:
        conn = _connect()
        try:
            cur = conn.cursor()

            cur.execute("SELECT COUNT(*) FROM transactions")
            total_tx = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM fraud_alerts")
            total_alerts = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM fraud_alerts
                WHERE created_at >= datetime('now', '-1 hour')
            """)
            alerts_last_hour = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM fraud_alerts
                WHERE created_at >= datetime('now', '-1 minute')
            """)
            alerts_last_minute = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM transactions
                WHERE created_at >= datetime('now', '-1 minute')
            """)
            tx_last_minute = cur.fetchone()[0]

            return {
                "total_transactions":  total_tx,
                "total_alerts":        total_alerts,
                "fraud_rate_pct":      round(total_alerts / max(total_tx, 1) * 100, 3),
                "alerts_last_hour":    alerts_last_hour,
                "alerts_last_minute":  alerts_last_minute,
                "tx_per_minute":       tx_last_minute,
            }
        finally:
            conn.close()

    def get_recent_alerts(self, limit: int = 20) -> list:
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT transaction_id, timestamp, sender_id, receiver_id,
                       amount, confidence, reason, rf_score, xgb_score,
                       iso_flag, fraud_pattern, graph_pattern, created_at
                FROM fraud_alerts
                ORDER BY created_at DESC
                LIMIT ?
            """, (limit,))
            return _rows_to_dicts(cur.fetchall())
        finally:
            conn.close()

    def get_pattern_breakdown(self) -> list:
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    COALESCE(fraud_pattern, 'unknown') AS pattern,
                    COUNT(*) AS count,
                    AVG(confidence) AS avg_confidence,
                    SUM(amount) AS total_amount
                FROM fraud_alerts
                GROUP BY COALESCE(fraud_pattern, 'unknown')
                ORDER BY count DESC
            """)
            return _rows_to_dicts(cur.fetchall())
        finally:
            conn.close()

    def get_alerts_over_time(self, hours: int = 6) -> list:
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    strftime('%Y-%m-%dT%H:%M:00', created_at) AS bucket,
                    COUNT(*) AS alert_count
                FROM fraud_alerts
                WHERE created_at >= datetime('now', '-6 hours')
                GROUP BY bucket
                ORDER BY bucket ASC
            """)
            return _rows_to_dicts(cur.fetchall())
        finally:
            conn.close()

    def get_volume_over_time(self, hours: int = 6) -> list:
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    strftime('%Y-%m-%dT%H:%M:00', created_at) AS bucket,
                    COUNT(*) AS tx_count
                FROM transactions
                WHERE created_at >= datetime('now', '-6 hours')
                GROUP BY bucket
                ORDER BY bucket ASC
            """)
            return _rows_to_dicts(cur.fetchall())
        finally:
            conn.close()

    def get_model_performance(self) -> dict:
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    AVG(rf_score)   AS avg_rf_score,
                    AVG(xgb_score)  AS avg_xgb_score,
                    AVG(confidence) AS avg_confidence,
                    SUM(iso_flag)   AS iso_flags,
                    SUM(CASE WHEN is_fraud_label = 1 THEN 1 ELSE 0 END) AS true_positives,
                    SUM(CASE WHEN is_fraud_label = 0 THEN 1 ELSE 0 END) AS false_positives,
                    SUM(CASE WHEN is_fraud_label IS NULL THEN 1 ELSE 0 END) AS unlabeled,
                    COUNT(*) AS total
                FROM fraud_alerts
            """)
            row = cur.fetchone()
            return dict(row) if row else {}
        finally:
            conn.close()

    def get_last_scan(self) -> Optional[dict]:
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM scan_log ORDER BY scanned_at DESC LIMIT 1")
            row = cur.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_graph_confirmed_alerts(self) -> list:
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT transaction_id, sender_id, receiver_id,
                       amount, confidence, fraud_pattern, graph_pattern, created_at
                FROM fraud_alerts
                WHERE graph_pattern IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 50
            """)
            return _rows_to_dicts(cur.fetchall())
        finally:
            conn.close()
