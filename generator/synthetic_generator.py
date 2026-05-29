"""
synthetic_generator.py
-----------------------
Generates a continuous stream of realistic synthetic transactions.
Injects controllable fraud patterns for testing the detection pipeline.

Fraud patterns supported:
    - CYCLE:       Money loops between a small set of accounts
    - RING:        Coordinated cluster of accounts transacting tightly
    - BURST:       One account fires many rapid transfers in a short window
    - NORMAL:      Benign background transaction (majority of traffic)

Usage:
    from generator.synthetic_generator import SyntheticGenerator

    gen = SyntheticGenerator(num_accounts=500, fraud_rate=0.02)
    for transaction in gen.stream(rate_per_second=50):
        print(transaction)
"""

import uuid
import random
import time
import logging
from datetime import datetime, timezone
from typing import Generator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

TRANSACTION_TYPES = ["transfer", "payment", "cash_out", "cash_in", "debit"]

FRAUD_PATTERNS = ["cycle", "ring", "burst"]

# Probability weights for each fraud pattern when a fraud event is triggered
FRAUD_PATTERN_WEIGHTS = [0.4, 0.35, 0.25]  # cycle, ring, burst


# ── Account Pool ───────────────────────────────────────────────────────────────

def _generate_account_pool(n: int) -> list[dict]:
    """Create a pool of synthetic account objects with random balances."""
    return [
        {
            "id": f"C{str(uuid.uuid4().int)[:10]}",
            "balance": round(random.uniform(100, 50_000), 2),
        }
        for _ in range(n)
    ]


# ── Transaction Builders ───────────────────────────────────────────────────────

def _make_transaction(
    sender: dict,
    receiver: dict,
    amount: float,
    tx_type: str,
    fraud_pattern: str | None,
) -> dict:
    """Assemble a transaction event dict."""
    amount = round(min(amount, sender["balance"]), 2)
    sender_before = sender["balance"]
    receiver_before = receiver["balance"]

    # Update balances in-place so state persists across the stream
    if tx_type in ("transfer", "payment", "cash_out", "debit"):
        sender["balance"] = round(max(sender["balance"] - amount, 0), 2)
        receiver["balance"] = round(receiver["balance"] + amount, 2)
    else:  # cash_in
        sender["balance"] = round(sender["balance"] + amount, 2)

    is_fraud = fraud_pattern is not None

    return {
        "transaction_id":   str(uuid.uuid4()),
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "type":             tx_type,
        "amount":           amount,
        "sender_id":        sender["id"],
        "sender_balance_before":   sender_before,
        "sender_balance_after":    sender["balance"],
        "receiver_id":      receiver["id"],
        "receiver_balance_before": receiver_before,
        "receiver_balance_after":  receiver["balance"],
        "is_fraud":         is_fraud,
        "fraud_pattern":    fraud_pattern,  # None for normal transactions
        "source":           "synthetic",
    }


# ── Fraud Pattern Generators ───────────────────────────────────────────────────

def _generate_cycle(accounts: list[dict], cycle_length: int = 3) -> list[dict]:
    """
    CYCLE fraud: money moves A→B→C→A in a loop.
    Classic money laundering signature.
    """
    pool = random.sample(accounts, min(cycle_length, len(accounts)))
    amount = round(random.uniform(500, 5000), 2)
    transactions = []

    for i in range(len(pool)):
        sender   = pool[i]
        receiver = pool[(i + 1) % len(pool)]
        transactions.append(
            _make_transaction(sender, receiver, amount, "transfer", "cycle")
        )

    return transactions


def _generate_ring(accounts: list[dict], ring_size: int = 6) -> list[dict]:
    """
    RING fraud: a tight cluster of accounts all transacting with each other.
    Simulates a coordinated fraud ring.
    """
    pool = random.sample(accounts, min(ring_size, len(accounts)))
    transactions = []
    num_txs = ring_size * 2  # Dense interaction

    for _ in range(num_txs):
        sender, receiver = random.sample(pool, 2)
        amount = round(random.uniform(100, 3000), 2)
        transactions.append(
            _make_transaction(sender, receiver, amount, "transfer", "ring")
        )

    return transactions


def _generate_burst(accounts: list[dict], burst_count: int = 20) -> list[dict]:
    """
    BURST fraud: one account fires many rapid transfers.
    Simulates account takeover or automated fraud bot.
    """
    sender = random.choice(accounts)
    transactions = []

    for _ in range(burst_count):
        receiver = random.choice([a for a in accounts if a["id"] != sender["id"]])
        amount = round(random.uniform(10, 500), 2)
        transactions.append(
            _make_transaction(sender, receiver, amount, "transfer", "burst")
        )

    return transactions


def _generate_normal(accounts: list[dict]) -> dict:
    """Generate a single benign background transaction."""
    sender, receiver = random.sample(accounts, 2)
    tx_type = random.choice(TRANSACTION_TYPES)
    amount = round(random.uniform(1, sender["balance"] * 0.3 + 1), 2)
    return _make_transaction(sender, receiver, amount, tx_type, None)


# ── Main Generator Class ───────────────────────────────────────────────────────

class SyntheticGenerator:
    """
    Generates a continuous stream of synthetic transactions.

    Args:
        num_accounts:    Size of the synthetic account pool.
        fraud_rate:      Fraction of time steps that inject a fraud pattern (0.0–1.0).
        seed:            Random seed for reproducibility (None = random).
    """

    def __init__(
        self,
        num_accounts: int = 500,
        fraud_rate: float = 0.02,
        seed: int | None = None,
    ):
        if not 0.0 <= fraud_rate <= 1.0:
            raise ValueError("fraud_rate must be between 0.0 and 1.0")

        if seed is not None:
            random.seed(seed)

        self.fraud_rate = fraud_rate
        self.accounts = _generate_account_pool(num_accounts)

        logger.info(
            f"SyntheticGenerator ready — {num_accounts} accounts, "
            f"fraud_rate={fraud_rate:.1%}"
        )

    def _next_batch(self) -> list[dict]:
        """
        Returns either a fraud event batch or a single normal transaction.
        Fraud events produce multiple related transactions (the pattern).
        """
        if random.random() < self.fraud_rate:
            pattern = random.choices(FRAUD_PATTERNS, weights=FRAUD_PATTERN_WEIGHTS, k=1)[0]
            if pattern == "cycle":
                return _generate_cycle(self.accounts)
            elif pattern == "ring":
                return _generate_ring(self.accounts)
            elif pattern == "burst":
                return _generate_burst(self.accounts)

        return [_generate_normal(self.accounts)]

    def stream(self, rate_per_second: float = 10.0) -> Generator[dict, None, None]:
        """
        Yields transactions continuously.

        Args:
            rate_per_second:  Target throughput. Controls sleep between batches.
                              Set to 0 for no throttling (max speed).
        """
        delay = 1.0 / rate_per_second if rate_per_second > 0 else 0
        total = 0
        fraud_total = 0

        logger.info(f"Streaming synthetic transactions at ~{rate_per_second}/s ...")

        try:
            while True:
                batch = self._next_batch()
                for tx in batch:
                    total += 1
                    if tx["is_fraud"]:
                        fraud_total += 1
                    yield tx

                if delay > 0:
                    time.sleep(delay)

        except KeyboardInterrupt:
            logger.info(
                f"Stream stopped — {total:,} transactions generated, "
                f"{fraud_total:,} fraud ({fraud_total / max(total, 1) * 100:.2f}%)"
            )


# ── Quick sanity check ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    gen = SyntheticGenerator(num_accounts=100, fraud_rate=0.1, seed=42)

    print("── Sample transactions (5 normal + fraud mix) ──")
    count = 0
    for tx in gen.stream(rate_per_second=0):
        print(tx)
        count += 1
        if count >= 10:
            break
