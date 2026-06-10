"""
processing/feature_engineering.py
-----------------------------------
Derives ML-ready features from a cleaned (preprocessed) transaction dict.

Two categories of features:
    1. Transaction-level features  — derived from a single transaction
    2. Account-level features      — derived from running history per account
                                     (velocity, average amount, etc.)

The account-level features are the most powerful — they capture behavioral
patterns that a single transaction can't show on its own.

Output is always a fixed-length numpy array (FEATURE_VECTOR_SIZE,)
in the same column order the models were trained on.
"""

import logging
from collections import defaultdict, deque

import numpy as np

logger = logging.getLogger(__name__)

# ── Feature vector column order ────────────────────────────────────────────────
# IMPORTANT: This order must match exactly what was used during training.
# Any change here requires retraining all models.

FEATURE_COLUMNS = [
    # Transaction-level
    "type_encoded",             # 0-4 integer
    "amount_norm",              # amount / 1_000_000
    "sender_bal_before_norm",   # sender balance before / 1_000_000
    "sender_bal_after_norm",    # sender balance after / 1_000_000
    "receiver_bal_before_norm", # receiver balance before / 1_000_000
    "receiver_bal_after_norm",  # receiver balance after / 1_000_000

    # Derived transaction-level
    "balance_delta_sender",     # sender_after - sender_before (should be -amount for transfers)
    "balance_delta_receiver",   # receiver_after - receiver_before
    "sender_balance_drained",   # 1 if sender balance goes to 0, else 0 (strong fraud signal)
    "amount_to_balance_ratio",  # amount / (sender_bal_before + 1) — relative transaction size

    # Account-level velocity (last 10 transactions per account)
    "sender_tx_count",          # number of recent transactions by this sender
    "sender_avg_amount",        # average amount sent recently
    "sender_max_amount",        # max amount sent recently
    "sender_tx_frequency",      # tx count / time window — burst detection
]

FEATURE_VECTOR_SIZE = len(FEATURE_COLUMNS)

# Sliding window size for account history
ACCOUNT_HISTORY_WINDOW = 10

AMOUNT_SCALE = 1_000_000.0


# ── Account History Tracker ────────────────────────────────────────────────────

class AccountHistory:
    """
    Maintains a sliding window of recent transaction amounts per account.
    Used to compute velocity features without storing unbounded history.
    """

    def __init__(self, window: int = ACCOUNT_HISTORY_WINDOW):
        self.window = window
        # account_id → deque of (timestamp_epoch, amount)
        self._amounts: dict[str, deque] = defaultdict(lambda: deque(maxlen=window))
        self._timestamps: dict[str, deque] = defaultdict(lambda: deque(maxlen=window))

    def update(self, account_id: str, amount: float, timestamp_epoch: float):
        self._amounts[account_id].append(amount)
        self._timestamps[account_id].append(timestamp_epoch)

    def get_features(self, account_id: str, now: float) -> dict:
        """
        Returns velocity features for an account based on recent history.
        Called BEFORE updating history with the current transaction.
        """
        amounts = list(self._amounts[account_id])
        times   = list(self._timestamps[account_id])

        if not amounts:
            return {
                "tx_count":    0,
                "avg_amount":  0.0,
                "max_amount":  0.0,
                "tx_frequency": 0.0,
            }

        count = len(amounts)
        avg   = float(np.mean(amounts))
        mx    = float(np.max(amounts))

        # Frequency = transactions per minute over the observed window
        if len(times) >= 2 and now > times[0]:
            time_span_minutes = (now - times[0]) / 60.0
            frequency = count / max(time_span_minutes, 1e-6)
        else:
            frequency = 0.0

        return {
            "tx_count":     count,
            "avg_amount":   avg,
            "max_amount":   mx,
            "tx_frequency": frequency,
        }


# ── Feature Engineer ───────────────────────────────────────────────────────────

class FeatureEngineer:
    """
    Converts a preprocessed transaction dict into a numpy feature vector.
    Maintains running account history for velocity features.
    """

    def __init__(self):
        self.account_history = AccountHistory()

    def extract(self, tx: dict) -> np.ndarray:
        """
        Extract features from a preprocessed transaction.

        Args:
            tx: Output of Preprocessor.process()

        Returns:
            numpy array of shape (FEATURE_VECTOR_SIZE,)
        """
        # ── Parse timestamp ────────────────────────────────────────────────────
        timestamp_epoch = _parse_timestamp(tx.get("timestamp"))

        sender_id = tx["sender_id"]
        amount    = tx["amount"]

        # ── Account velocity features (before updating history) ────────────────
        velocity = self.account_history.get_features(sender_id, timestamp_epoch)

        # ── Transaction-level features ─────────────────────────────────────────
        sender_before = tx["sender_bal_before"]
        sender_after  = tx["sender_bal_after"]
        recv_before   = tx["receiver_bal_before"]
        recv_after    = tx["receiver_bal_after"]

        balance_delta_sender   = sender_after - sender_before
        balance_delta_receiver = recv_after - recv_before
        sender_balance_drained = 1.0 if sender_after == 0.0 and sender_before > 0.0 else 0.0
        amount_to_balance_ratio = amount / (sender_before + 1.0)

        # ── Assemble feature vector in FEATURE_COLUMNS order ──────────────────
        features = np.array([
            tx["type_encoded"],
            tx["amount_norm"],
            sender_before / AMOUNT_SCALE,
            sender_after  / AMOUNT_SCALE,
            recv_before   / AMOUNT_SCALE,
            recv_after    / AMOUNT_SCALE,
            balance_delta_sender   / AMOUNT_SCALE,
            balance_delta_receiver / AMOUNT_SCALE,
            sender_balance_drained,
            min(amount_to_balance_ratio, 10.0),  # cap at 10 to avoid outlier explosion

            float(velocity["tx_count"]),
            velocity["avg_amount"] / AMOUNT_SCALE,
            velocity["max_amount"] / AMOUNT_SCALE,
            min(velocity["tx_frequency"], 1000.0),  # cap burst frequency
        ], dtype=np.float32)

        # ── Update account history with current transaction ────────────────────
        self.account_history.update(sender_id, amount, timestamp_epoch)

        assert features.shape == (FEATURE_VECTOR_SIZE,), (
            f"Feature vector size mismatch: expected {FEATURE_VECTOR_SIZE}, "
            f"got {features.shape[0]}"
        )

        return features

    def extract_batch(self, transactions: list[dict]) -> np.ndarray:
        """
        Extract features for a list of transactions.
        Returns array of shape (n, FEATURE_VECTOR_SIZE).
        """
        return np.vstack([self.extract(tx) for tx in transactions])


# ── Helpers ────────────────────────────────────────────────────────────────────

def _parse_timestamp(ts) -> float:
    """
    Convert an ISO timestamp string or epoch float to epoch float.
    Falls back to 0.0 if parsing fails.
    """
    if ts is None:
        return 0.0
    if isinstance(ts, (int, float)):
        return float(ts)
    try:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(str(ts))
        return dt.timestamp()
    except Exception:
        return 0.0
