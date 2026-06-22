"""
graph/fraud_queries.py  (FIXED)
---------------------------------
Runs graph-based fraud pattern detection queries against Neo4j.

Four detection techniques:
    1. Cycle Detection        — money looping between accounts (laundering)
    2. Community Detection    — tightly connected fraud rings (GDS required)
    3. Velocity Monitoring    — single account sending many transactions fast
    4. Graph Centrality       — accounts acting as hubs in suspicious networks

Fixes applied vs the original:
    1. Added a relationship index on SENT.timestamp — every query here
       filters or sorts by timestamp, and there was no index backing it.
    2. Fixed detect_velocity's "latest timestamp" bug — it relied on
       collect(r)[0] after a WITH, which does NOT guarantee the sort order
       from an earlier ORDER BY survives. Replaced with max(r.timestamp),
       a real aggregation that's correct regardless of row order.
    3. detect_cycles now takes a time window (since_hours) and filters
       edges by recency, so the traversal cost doesn't grow forever as
       your transaction graph grows. Also pushed the amount filter inline.
    4. run_all() now projects the GDS graph ONCE and shares it between
       detect_communities and detect_centrality, instead of dropping and
       rebuilding the whole in-memory projection twice per cycle.

Usage:
    from graph.fraud_queries import FraudQueryEngine

    engine = FraudQueryEngine()

    cycles      = engine.detect_cycles(max_depth=5, since_hours=24)
    communities = engine.detect_communities(min_size=4)
    bursts      = engine.detect_velocity(window_minutes=10, threshold=20)
    hubs        = engine.detect_centrality(top_n=10)

    # Or run everything in one pass (recommended — shares one GDS projection):
    results = engine.run_all()

    engine.close()
"""

import logging
from typing import Optional

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, ClientError

logger = logging.getLogger(__name__)

NEO4J_URI      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "fraudpassword"

# GDS graph projection name — created once, reused for all GDS queries
GDS_GRAPH_NAME = "fraud_graph"


class FraudQueryEngine:
    """
    Executes Cypher and GDS queries for fraud pattern detection.
    Designed to be called periodically (e.g., every 60 seconds) on the
    live graph rather than per-transaction.
    """

    def __init__(self):
        logger.info("Connecting to Neo4j for fraud queries ...")
        try:
            self._driver = GraphDatabase.driver(
                NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
            )
            self._driver.verify_connectivity()
            logger.info("FraudQueryEngine ready ✅")
            self._create_indexes()
        except ServiceUnavailable as e:
            raise ConnectionError(
                f"Could not connect to Neo4j: {e}\n"
                f"Make sure Neo4j is running: docker compose up -d neo4j"
            )

    # ── FIX 1: relationship index on timestamp ─────────────────────────────────

    def _create_indexes(self):
        """
        Create an index on SENT.timestamp if it doesn't already exist.
        Every query in this class filters or sorts by timestamp — without
        this index, Neo4j does a full relationship scan every single call.
        Safe to call repeatedly; Neo4j skips if it already exists.
        """
        with self._driver.session() as session:
            session.run("""
                CREATE INDEX sent_timestamp_idx IF NOT EXISTS
                FOR ()-[r:SENT]-() ON (r.timestamp)
            """)
        logger.info("Neo4j relationship index on SENT.timestamp verified.")

    # ── 1. Cycle Detection ─────────────────────────────────────────────────────

    def detect_cycles(
        self,
        max_depth: int = 5,
        min_amount: float = 0.0,
        since_hours: Optional[float] = 24.0,
    ) -> list[dict]:
        """
        Find accounts involved in money cycles: A→B→C→...→A

        How it works:
            Neo4j's variable-length path matching traverses the graph up to
            max_depth hops. The WHERE clause filters for paths that return
            to the starting node — that's the cycle signature.

        FIX 3: Added since_hours. Without a recency bound, this traversal
        gets slower every cycle as your transaction graph grows, because it
        has to consider every edge ever written. Pass since_hours=None to
        disable the bound and search the whole graph (slower, but useful
        for a one-off historical audit).

        Args:
            max_depth:   Maximum cycle length to search for (deeper = slower).
            min_amount:  Only report cycles where all edges exceed this amount.
            since_hours: Only consider edges newer than this many hours ago.
                         None disables the bound (searches entire graph).

        Returns:
            List of dicts, one per cycle found:
            {cycle_accounts, cycle_length, total_amount, transaction_ids}
        """
        params = {"min_amount": min_amount}

        if since_hours is not None:
            # Bound the traversal to recent edges. WITH computes the cutoff
            # once up front; the MATCH/WHERE below filters against it.
            cypher = f"""
                WITH datetime() - duration({{hours: $since_hours}}) AS since
                MATCH path = (start:Account)-[:SENT*2..{max_depth}]->(start)
                WHERE ALL(r IN relationships(path)
                          WHERE r.amount >= $min_amount
                          AND datetime(r.timestamp) > since)
                WITH nodes(path)         AS accounts,
                     relationships(path) AS rels,
                     length(path)        AS depth
                RETURN
                    [a IN accounts | a.id]                  AS cycle_accounts,
                    depth                                   AS cycle_length,
                    reduce(s=0.0, r IN rels | s + r.amount)  AS total_amount,
                    [r IN rels | r.transaction_id]           AS transaction_ids
                ORDER BY total_amount DESC
                LIMIT 50
            """
            params["since_hours"] = since_hours
        else:
            # No recency bound — searches the entire graph. Slower, but
            # useful for a one-off historical audit rather than a live scan.
            cypher = f"""
                MATCH path = (start:Account)-[:SENT*2..{max_depth}]->(start)
                WHERE ALL(r IN relationships(path) WHERE r.amount >= $min_amount)
                WITH nodes(path)         AS accounts,
                     relationships(path) AS rels,
                     length(path)        AS depth
                RETURN
                    [a IN accounts | a.id]                  AS cycle_accounts,
                    depth                                   AS cycle_length,
                    reduce(s=0.0, r IN rels | s + r.amount)  AS total_amount,
                    [r IN rels | r.transaction_id]           AS transaction_ids
                ORDER BY total_amount DESC
                LIMIT 50
            """

        results = []
        with self._driver.session() as session:
            records = session.run(cypher, params)
            for record in records:
                results.append({
                    "pattern":         "cycle",
                    "cycle_accounts":  list(record["cycle_accounts"]),
                    "cycle_length":    record["cycle_length"],
                    "total_amount":    record["total_amount"],
                    "transaction_ids": list(record["transaction_ids"]),
                })

        if results:
            logger.warning(f"🔄 Cycle detection: {len(results)} cycle(s) found")
        else:
            logger.info("Cycle detection: no cycles found")

        return results

    # ── 2. Community Detection (GDS) ───────────────────────────────────────────

    def detect_communities(
        self,
        min_community_size: int = 4,
        fraud_edge_ratio: float = 0.3,
        skip_projection: bool = False,
    ) -> list[dict]:
        """
        Use the Louvain algorithm (GDS) to find tightly connected account clusters.
        Flags communities where a significant fraction of edges are marked as fraud.

        How it works:
            GDS projects the graph into memory, runs Louvain community detection,
            and writes a communityId property back to each Account node.
            We then query for communities with high fraud edge density.

        FIX 4: skip_projection lets run_all() share a single projection
        across detect_communities and detect_centrality instead of paying
        the drop+rebuild cost twice per scan cycle.

        Args:
            min_community_size: Ignore communities smaller than this.
            fraud_edge_ratio:   Flag communities where fraud edges / total edges
                                exceeds this ratio.
            skip_projection:    If True, assumes the GDS graph is already
                                projected (caller is responsible). Used by
                                run_all() to avoid a redundant projection.

        Returns:
            List of dicts, one per suspicious community.
        """
        if not skip_projection:
            self._project_gds_graph()

        write_cypher = """
            CALL gds.louvain.write($graph_name, {
                writeProperty: 'communityId',
                relationshipWeightProperty: 'amount'
            })
            YIELD communityCount, modularity
            RETURN communityCount, modularity
        """
        with self._driver.session() as session:
            result = session.run(write_cypher, {"graph_name": GDS_GRAPH_NAME}).single()
            if result:
                logger.info(
                    f"Louvain: {result['communityCount']} communities, "
                    f"modularity={result['modularity']:.4f}"
                )

        query_cypher = """
            MATCH (a:Account)-[r:SENT]->(b:Account)
            WHERE a.communityId IS NOT NULL
              AND a.communityId = b.communityId
            WITH a.communityId AS community_id,
                 count(r)                                   AS total_edges,
                 sum(CASE WHEN r.is_fraud THEN 1 ELSE 0 END) AS fraud_edges,
                 count(DISTINCT a)                          AS member_count,
                 sum(r.amount)                              AS total_amount,
                                 collect(DISTINCT a.id)[..10]               AS sample_accounts,
                                 collect(DISTINCT r.transaction_id)[..50]   AS transaction_ids
            WHERE member_count >= $min_size
              AND total_edges > 0
              AND toFloat(fraud_edges) / total_edges >= $fraud_ratio
            RETURN community_id, total_edges, fraud_edges, member_count,
                                     total_amount, sample_accounts, transaction_ids,
                   toFloat(fraud_edges) / total_edges AS fraud_ratio
            ORDER BY fraud_ratio DESC
            LIMIT 20
        """
        results = []
        with self._driver.session() as session:
            records = session.run(query_cypher, {
                "min_size":    min_community_size,
                "fraud_ratio": fraud_edge_ratio,
            })
            for record in records:
                results.append({
                    "pattern":         "ring",
                    "community_id":    record["community_id"],
                    "member_count":    record["member_count"],
                    "total_edges":     record["total_edges"],
                    "fraud_edges":     record["fraud_edges"],
                    "fraud_ratio":     round(record["fraud_ratio"], 4),
                    "total_amount":    record["total_amount"],
                    "sample_accounts": list(record["sample_accounts"]),
                    "transaction_ids": list(record["transaction_ids"]),
                })

        if results:
            logger.warning(
                f"👥 Community detection: {len(results)} suspicious ring(s) found"
            )
        else:
            logger.info("Community detection: no suspicious communities found")

        return results

    # ── 3. Velocity Monitoring ─────────────────────────────────────────────────

    def detect_velocity(
        self,
        window_minutes: int = 10,
        threshold: int = 20,
    ) -> list[dict]:
        """
        Find accounts making an unusually high number of transactions
        in a short time window — burst/bot fraud signature.

        How it works:
            Pure Cypher — no GDS needed. Filters SENT edges by timestamp
            recency, groups by sender, counts transactions per sender.

        FIX 2: The original computed "latest timestamp" as
        collect(r)[0].timestamp after an ORDER BY in an earlier WITH block.
        Cypher does NOT guarantee that sort order survives across a WITH
        regrouping — collect(r)[0] could be any row, not the most recent.
        This silently broke the time-window filter: it was comparing against
        an arbitrary timestamp instead of "now" (or close to it).

        Fixed by using max(r.timestamp) directly — a real aggregation that
        Neo4j computes correctly regardless of row order, no ORDER BY needed.

        Args:
            window_minutes: Look back this many minutes from the latest timestamp.
            threshold:      Flag accounts with more transactions than this.

        Returns:
            List of dicts, one per high-velocity account.
        """
        cypher = """
            MATCH (a:Account)-[r:SENT]->()
            WHERE r.timestamp IS NOT NULL
            WITH a, collect(r) AS rels, max(r.timestamp) AS latest_ts

            WITH a, latest_ts,
                 [r IN rels
                  WHERE duration.between(
                      datetime(r.timestamp),
                      datetime(latest_ts)
                  ).minutes <= $window] AS window_rels

            WITH a,
                 size(window_rels)                              AS tx_count,
                 reduce(s=0.0, r IN window_rels | s + r.amount)  AS total_amount,
                 [r IN window_rels | r.transaction_id]           AS transaction_ids

            WHERE tx_count >= $threshold
            RETURN a.id AS account_id, tx_count, total_amount, transaction_ids
            ORDER BY tx_count DESC
            LIMIT 50
        """
        results = []
        with self._driver.session() as session:
            records = session.run(cypher, {
                "window":    window_minutes,
                "threshold": threshold,
            })
            for record in records:
                results.append({
                    "pattern":         "burst",
                    "account_id":      record["account_id"],
                    "tx_count":        record["tx_count"],
                    "total_amount":    record["total_amount"],
                    "transaction_ids": list(record["transaction_ids"]),
                    "window_minutes":  window_minutes,
                })

        if results:
            logger.warning(
                f"⚡ Velocity monitoring: {len(results)} high-velocity account(s)"
            )
        else:
            logger.info("Velocity monitoring: no bursts detected")

        return results

    # ── 4. Graph Centrality ────────────────────────────────────────────────────

    def detect_centrality(
        self,
        top_n: int = 10,
        skip_projection: bool = False,
    ) -> list[dict]:
        """
        Find accounts with unusually high PageRank in the transaction network.
        Hub accounts that many others route money through are a laundering signal.

        How it works:
            GDS PageRank treats transaction amounts as edge weights.
            High-PageRank accounts receive many weighted inflows — classic
            money mule or hub-account pattern.

        FIX 4: skip_projection — see detect_communities docstring. run_all()
        projects once and reuses it here instead of rebuilding from scratch.

        Args:
            top_n:           Return the top N accounts by PageRank score.
            skip_projection: If True, assumes the GDS graph is already
                              projected (caller is responsible).

        Returns:
            List of dicts with account_id and pagerank_score.
        """
        if not skip_projection:
            self._project_gds_graph()

        cypher = """
            CALL gds.pageRank.stream($graph_name, {
                relationshipWeightProperty: 'amount',
                dampingFactor: 0.85,
                maxIterations: 20
            })
            YIELD nodeId, score
            WITH gds.util.asNode(nodeId) AS account, score
            WHERE score > 0
            RETURN account.id AS account_id, score AS pagerank_score
            ORDER BY pagerank_score DESC
            LIMIT $top_n
        """
        results = []
        with self._driver.session() as session:
            records = session.run(cypher, {
                "graph_name": GDS_GRAPH_NAME,
                "top_n":      top_n,
            })
            for record in records:
                results.append({
                    "pattern":        "hub",
                    "account_id":     record["account_id"],
                    "pagerank_score": round(record["pagerank_score"], 6),
                })

        if results:
            logger.warning(
                f"🕸️  Centrality: top hub account = "
                f"{results[0]['account_id']} "
                f"(score={results[0]['pagerank_score']})"
            )

        return results

    # ── Run all detections ─────────────────────────────────────────────────────

    def run_all(
        self,
        cycle_depth: int = 5,
        cycle_since_hours: Optional[float] = 24.0,
        community_min_size: int = 4,
        velocity_window: int = 10,
        velocity_threshold: int = 20,
        centrality_top_n: int = 10,
    ) -> dict:
        """
        Run all four detection techniques and return combined results.
        Call this on a schedule (e.g., every 60 seconds).

        FIX 4: Projects the GDS graph ONCE here and passes skip_projection=True
        to detect_communities and detect_centrality, since both run against
        the same projection. The original called _project_gds_graph() inside
        each method independently — meaning every run_all() cycle dropped and
        rebuilt the entire in-memory graph twice instead of once.
        """
        logger.info("Running full graph fraud scan ...")

        # Project once, share across both GDS-based detections below.
        self._project_gds_graph()

        return {
            "cycles": self.detect_cycles(
                max_depth=cycle_depth,
                since_hours=cycle_since_hours,
            ),
            "communities": self.detect_communities(
                min_community_size=community_min_size,
                skip_projection=True,
            ),
            "bursts": self.detect_velocity(
                window_minutes=velocity_window,
                threshold=velocity_threshold,
            ),
            "hubs": self.detect_centrality(
                top_n=centrality_top_n,
                skip_projection=True,
            ),
        }

    # ── GDS graph projection ───────────────────────────────────────────────────

    def _project_gds_graph(self):
        """
        Project the Neo4j graph into GDS in-memory format.
        Required before any GDS algorithm call.
        Drops existing projection first to pick up latest data.
        """
        drop_cypher = "CALL gds.graph.drop($name, false) YIELD graphName"
        with self._driver.session() as session:
            try:
                session.run(drop_cypher, {"name": GDS_GRAPH_NAME})
            except ClientError:
                pass  # projection didn't exist yet

        project_cypher = """
            CALL gds.graph.project(
                $name,
                'Account',
                {
                    SENT: {
                        orientation: 'NATURAL',
                        properties: ['amount']
                    }
                }
            )
            YIELD graphName, nodeCount, relationshipCount
            RETURN graphName, nodeCount, relationshipCount
        """
        with self._driver.session() as session:
            result = session.run(project_cypher, {"name": GDS_GRAPH_NAME}).single()
            if result:
                logger.info(
                    f"GDS graph projected: {result['nodeCount']} accounts, "
                    f"{result['relationshipCount']} transactions"
                )

    def close(self):
        self._driver.close()
        logger.info("FraudQueryEngine closed.")