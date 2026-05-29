"""
paysim_loader.py
----------------
Loads the PaySim CSV dataset and streams rows as structured transaction events.

PaySim columns:
    step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig,
    nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud

Usage:
    from generator.paysim_loader import PaySimLoader

    loader = PaySimLoader("data/paysim.csv")
    for transaction in loader.stream(delay=0.01):
        print(transaction)
"""

import csv
import time
import logging
from pathlib import Path
from typing import Generator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# Maps PaySim type strings to normalized internal type codes
TRANSACTION_TYPE_MAP = {
    "CASH_IN":   "cash_in",
    "CASH_OUT":  "cash_out",
    "DEBIT":     "debit",
    "PAYMENT":   "payment",
    "TRANSFER":  "transfer",
}


def _parse_row(row: dict) -> dict:
    """Convert a raw CSV row into a clean, typed transaction dict."""
    return {
        "step":             int(row["step"]),
        "type":             TRANSACTION_TYPE_MAP.get(row["type"], row["type"].lower()),
        "amount":           float(row["amount"]),
        "sender_id":        row["nameOrig"],
        "sender_balance_before": float(row["oldbalanceOrg"]),
        "sender_balance_after":  float(row["newbalanceOrig"]),
        "receiver_id":      row["nameDest"],
        "receiver_balance_before": float(row["oldbalanceDest"]),
        "receiver_balance_after":  float(row["newbalanceDest"]),
        "is_fraud":         bool(int(row["isFraud"])),
        "is_flagged_fraud": bool(int(row["isFlaggedFraud"])),
        "source":           "paysim",
    }


class PaySimLoader:
    """
    Streams PaySim transactions from a CSV file.

    Args:
        filepath:   Path to the PaySim CSV file.
        skip_rows:  Number of rows to skip from the start (useful for resuming).
    """

    def __init__(self, filepath: str, skip_rows: int = 0):
        self.filepath = Path(filepath)
        self.skip_rows = skip_rows

        if not self.filepath.exists():
            raise FileNotFoundError(
                f"PaySim CSV not found at '{self.filepath}'.\n"
                f"Download it from: https://www.kaggle.com/datasets/ealaxi/paysim1"
            )

    def stream(self, delay: float = 0.0) -> Generator[dict, None, None]:
        """
        Yields one transaction dict per row.

        Args:
            delay:  Seconds to wait between rows (simulate real-time pacing).
                    Set to 0 for maximum speed (e.g., training).
        """
        logger.info(f"Opening PaySim dataset: {self.filepath}")
        row_count = 0
        fraud_count = 0

        with self.filepath.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for i, raw_row in enumerate(reader):
                if i < self.skip_rows:
                    continue

                try:
                    tx = _parse_row(raw_row)
                except (KeyError, ValueError) as e:
                    logger.warning(f"Skipping malformed row {i}: {e}")
                    continue

                row_count += 1
                if tx["is_fraud"]:
                    fraud_count += 1

                if delay > 0:
                    time.sleep(delay)

                yield tx

        logger.info(
            f"PaySim stream complete — {row_count:,} transactions, "
            f"{fraud_count:,} fraud ({fraud_count / row_count * 100:.2f}%)"
        )

    def sample(self, n: int = 5) -> list[dict]:
        """Return the first n transactions without streaming."""
        return [tx for i, tx in enumerate(self.stream()) if i < n]


# ── Quick sanity check ────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    csv_path = sys.argv[1] if len(sys.argv) > 1 else "data/paysim.csv"
    loader = PaySimLoader(csv_path)

    print("── First 3 transactions ──")
    for tx in loader.sample(3):
        print(tx)
