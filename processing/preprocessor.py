"""
processing/preprocessor.py
---------------------------
Cleans and normalizes raw transaction dicts coming off the Kafka consumer.

Responsibilities:
    - Validate required fields are present
    - Coerce types (strings → floats, etc.)
    - Handle missing / null values
    - Normalize amount and balance fields
    - Encode transaction type as integer

This runs before feature engineering — garbage in, garbage out.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

REQUIRED_FIELDS = [
    "transaction_id",
    "type",
    "amount",
    "sender_id",
    "sender_balance_before",
    "sender_balance_after",
    "receiver_id",
    "receiver_balance_before",
    "receiver_balance_after",
]

# Consistent encoding so the model always sees the same integer for each type
TRANSACTION_TYPE_ENCODING = {
    "transfer":  0,
    "payment":   1,
    "cash_out":  2,
    "cash_in":   3,
    "debit":     4,
}

# Used for amount/balance normalization (based on PaySim dataset range)
# Keeps feature values in a reasonable scale for tree-based models
AMOUNT_SCALE = 1_000_000.0


# ── Validation ─────────────────────────────────────────────────────────────────

def _validate(tx: dict) -> Optional[str]:
    """
    Returns an error string if the transaction is invalid, else None.
    """
    for field in REQUIRED_FIELDS:
        if field not in tx or tx[field] is None:
            return f"Missing required field: '{field}'"

    if tx["amount"] < 0:
        return f"Negative amount: {tx['amount']}"

    if tx["type"] not in TRANSACTION_TYPE_ENCODING:
        return f"Unknown transaction type: '{tx['type']}'"

    return None


# ── Preprocessor ───────────────────────────────────────────────────────────────

class Preprocessor:
    """
    Cleans a raw transaction dict and returns a normalized version
    ready for feature engineering.

    Invalid transactions are dropped with a warning log.
    """

    def __init__(self):
        self.total = 0
        self.dropped = 0

    def process(self, tx: dict) -> Optional[dict]:
        """
        Clean and normalize a single transaction.

        Returns:
            Cleaned dict, or None if the transaction is invalid.
        """
        self.total += 1

        # ── Validate ───────────────────────────────────────────────────────────
        error = _validate(tx)
        if error:
            logger.warning(f"Dropping transaction {tx.get('transaction_id', '?')}: {error}")
            self.dropped += 1
            return None

        # ── Coerce types ───────────────────────────────────────────────────────
        try:
            amount                  = float(tx["amount"])
            sender_bal_before       = float(tx["sender_balance_before"])
            sender_bal_after        = float(tx["sender_balance_after"])
            receiver_bal_before     = float(tx["receiver_balance_before"])
            receiver_bal_after      = float(tx["receiver_balance_after"])
        except (ValueError, TypeError) as e:
            logger.warning(f"Type coercion failed for {tx.get('transaction_id', '?')}: {e}")
            self.dropped += 1
            return None

        # ── Encode transaction type ────────────────────────────────────────────
        type_encoded = TRANSACTION_TYPE_ENCODING[tx["type"]]

        # ── Normalize numeric fields ───────────────────────────────────────────
        # Dividing by AMOUNT_SCALE keeps values in [0, ~10] range.
        # Tree-based models don't strictly need this, but it helps
        # Isolation Forest which is distance-sensitive.
        return {
            # ── Identity (not used by ML, kept for traceability) ───────────────
            "transaction_id":       tx["transaction_id"],
            "sender_id":            tx["sender_id"],
            "receiver_id":          tx["receiver_id"],
            "timestamp":            tx.get("timestamp"),
            "source":               tx.get("source", "unknown"),

            # ── Cleaned numeric fields ─────────────────────────────────────────
            "type_encoded":         type_encoded,
            "amount":               amount,
            "amount_norm":          amount / AMOUNT_SCALE,
            "sender_bal_before":    sender_bal_before,
            "sender_bal_after":     sender_bal_after,
            "receiver_bal_before":  receiver_bal_before,
            "receiver_bal_after":   receiver_bal_after,

            # ── Ground truth label (present in PaySim, None in live stream) ────
            "is_fraud":             tx.get("is_fraud"),
            "fraud_pattern":        tx.get("fraud_pattern"),

            # ── Kafka metadata (for audit trail) ──────────────────────────────
            "_kafka_partition":     tx.get("_kafka_partition"),
            "_kafka_offset":        tx.get("_kafka_offset"),
        }

    def stats(self) -> dict:
        return {
            "total":   self.total,
            "dropped": self.dropped,
            "valid":   self.total - self.dropped,
        }
