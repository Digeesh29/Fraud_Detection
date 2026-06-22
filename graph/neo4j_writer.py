"""
graph/neo4j_writer.py
----------------------
Writes accounts and transactions into Neo4j as a property graph.

Graph model:
    (:Account {id})  -[:SENT {amount, timestamp, ...}]->  (:Account {id})

Every transaction becomes a directed edge between two account nodes.
Neo4j's MERGE ensures accounts are created once and reused — no duplicates.

This is the graph that fraud_queries.py runs pattern detection on.

Usage:
    from graph.neo4j_writer import Neo4jWriter

    writer = Neo4jWriter()
    writer.write_transaction(cleaned_tx)
    writer.write_transaction(cleaned_tx, is_fraud=True)
    writer.close()
"""

import logging
from typing import Optional

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

logger = logging.getLogger(__name__)

# ── Connection config ──────────────────────────────────────────────────────────
# Matches docker-compose.yml

NEO4J_URI      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "fraudpassword"


class Neo4jWriter:
    """
    Manages a Neo4j driver and writes transactions as graph edges.
    The driver maintains an internal connection pool — instantiate once,
    reuse for every write.
    """

    def __init__(self):
        logger.info("Connecting to Neo4j ...")
        try:
            self._driver = GraphDatabase.driver(
                NEO4J_URI,
                auth=(NEO4J_USER, NEO4J_PASSWORD),
            )
            self._driver.verify_connectivity()
            logger.info("Neo4j connected ✅")
            self._create_constraints()
        except ServiceUnavailable as e:
            raise ConnectionError(
                f"Could not connect to Neo4j: {e}\n"
                f"Make sure it's running: docker compose up -d neo4j"
            )

    # ── Schema setup ───────────────────────────────────────────────────────────

    def _create_constraints(self):
        """
        Create a uniqueness constraint on Account.id.
        This also creates an index, making MERGE fast.
        Only runs once — Neo4j skips if constraint already exists.
        """
        with self._driver.session() as session:
            session.run("""
                CREATE CONSTRAINT account_id_unique IF NOT EXISTS
                FOR (a:Account) REQUIRE a.id IS UNIQUE
            """)
        logger.info("Neo4j constraints verified.")

    # ── Single transaction write ───────────────────────────────────────────────

    def write_transaction(self, tx: dict, is_fraud: bool = False):
        """
        Write a single transaction as a graph edge.

        MERGE on Account nodes means:
            - If the account already exists → reuse it
            - If it doesn't → create it
        This gives us a continuously growing graph where accounts
        accumulate all their transaction edges over time.

        Args:
            tx:       Cleaned transaction dict from the preprocessor.
            is_fraud: Whether this transaction was flagged as fraud.
        """
        cypher = """
            MERGE (sender:Account {id: $sender_id})
            MERGE (receiver:Account {id: $receiver_id})
            CREATE (sender)-[:SENT {
                transaction_id: $transaction_id,
                amount:         $amount,
                timestamp:      $timestamp,
                type:           $type,
                is_fraud:       $is_fraud,
                fraud_pattern:  $fraud_pattern,
                source:         $source
            }]->(receiver)
        """
        params = {
            "sender_id":      tx["sender_id"],
            "receiver_id":    tx["receiver_id"],
            "transaction_id": tx["transaction_id"],
            "amount":         tx["amount"],
            "timestamp":      tx.get("timestamp"),
            "type":           _decode_type(tx.get("type_encoded", 0)),
            "is_fraud":       is_fraud,
            "fraud_pattern":  tx.get("fraud_pattern"),
            "source":         tx.get("source", "unknown"),
        }
        with self._driver.session() as session:
            session.run(cypher, params)

    # ── Batch write ────────────────────────────────────────────────────────────

    def write_transaction_batch(self, transactions: list[dict], fraud_ids: set[str] = None):
        """
        Write a list of transactions in a single session.
        Much faster than calling write_transaction() in a loop.

        Args:
            transactions: List of cleaned transaction dicts.
            fraud_ids:    Set of transaction_ids flagged as fraud.
        """
        fraud_ids = fraud_ids or set()

        cypher = """
            UNWIND $rows AS row
            MERGE (sender:Account {id: row.sender_id})
            MERGE (receiver:Account {id: row.receiver_id})
            CREATE (sender)-[:SENT {
                transaction_id: row.transaction_id,
                amount:         row.amount,
                timestamp:      row.timestamp,
                type:           row.type,
                is_fraud:       row.is_fraud,
                fraud_pattern:  row.fraud_pattern,
                source:         row.source
            }]->(receiver)
        """
        rows = [
            {
                "sender_id":      tx["sender_id"],
                "receiver_id":    tx["receiver_id"],
                "transaction_id": tx["transaction_id"],
                "amount":         tx["amount"],
                "timestamp":      tx.get("timestamp"),
                "type":           _decode_type(tx.get("type_encoded", 0)),
                "is_fraud":       tx["transaction_id"] in fraud_ids,
                "fraud_pattern":  tx.get("fraud_pattern"),
                "source":         tx.get("source", "unknown"),
            }
            for tx in transactions
        ]

        with self._driver.session() as session:
            session.run(cypher, {"rows": rows})

        logger.info(f"Batch wrote {len(rows)} transactions to Neo4j.")

    # ── Mark fraud on existing edge ────────────────────────────────────────────

    def mark_fraud(self, transaction_id: str, pattern: Optional[str] = None):
        """
        Update an existing SENT edge to mark it as fraud.
        Used when Neo4j graph queries confirm a pattern after the fact.
        """
        cypher = """
            MATCH ()-[r:SENT {transaction_id: $transaction_id}]->()
            SET r.is_fraud = true,
                r.graph_pattern = $pattern
        """
        with self._driver.session() as session:
            session.run(cypher, {
                "transaction_id": transaction_id,
                "pattern": pattern,
            })

    def close(self):
        self._driver.close()
        logger.info("Neo4j driver closed.")


# ── Helpers ────────────────────────────────────────────────────────────────────

_TYPE_DECODE = {0: "transfer", 1: "payment", 2: "cash_out", 3: "cash_in", 4: "debit"}

def _decode_type(encoded: int) -> str:
    return _TYPE_DECODE.get(int(encoded), "unknown")
