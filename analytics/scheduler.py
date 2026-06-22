"""
analytics/scheduler.py
-----------------------
Runs FraudQueryEngine.run_all() every 60 seconds.
Writes confirmed graph patterns back to SQLite.
"""

import logging
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from graph.fraud_queries import FraudQueryEngine
from storage.sqlite_writer import SQLiteWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SCAN_INTERVAL_SECONDS = 60


def _write_scan_results(writer: SQLiteWriter, results: dict):
    for cycle in results.get("cycles", []):
        for tx_id in cycle.get("transaction_ids", []):
            writer.update_graph_pattern(tx_id, "cycle")

    for community in results.get("communities", []):
        for tx_id in community.get("transaction_ids", []):
            writer.update_graph_pattern(tx_id, "ring")

    for burst in results.get("bursts", []):
        for tx_id in burst.get("transaction_ids", []):
            writer.update_graph_pattern(tx_id, "burst")

    writer.write_scan_log(
        cycles=len(results.get("cycles", [])),
        communities=len(results.get("communities", [])),
        bursts=len(results.get("bursts", [])),
        hubs=len(results.get("hubs", [])),
    )
    logger.info("Scan results written to SQLite.")


def run_scheduler():
    writer = SQLiteWriter()

    try:
        engine = FraudQueryEngine()
    except Exception as e:
        logger.error(f"Could not connect to Neo4j: {e}")
        return

    logger.info(f"Graph fraud scanner started — interval: {SCAN_INTERVAL_SECONDS}s")
    scan_count = 0

    try:
        while True:
            scan_count += 1
            logger.info(f"── Graph scan #{scan_count} ──")
            try:
                results = engine.run_all(
                    cycle_depth=5,
                    community_min_size=4,
                    velocity_window=10,
                    velocity_threshold=20,
                    centrality_top_n=10,
                )
                total = sum(len(v) for v in results.values())
                logger.info(
                    f"Scan #{scan_count} done — {total} findings | "
                    f"cycles={len(results['cycles'])} "
                    f"rings={len(results['communities'])} "
                    f"bursts={len(results['bursts'])} "
                    f"hubs={len(results['hubs'])}"
                )
                _write_scan_results(writer, results)
            except Exception as e:
                logger.error(f"Scan #{scan_count} failed: {e}")

            time.sleep(SCAN_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info(f"Scheduler stopped after {scan_count} scans.")
    finally:
        engine.close()


def start_scheduler():
    thread = threading.Thread(target=run_scheduler, daemon=True, name="graph-scheduler")
    thread.start()
    logger.info("Graph scheduler running in background.")
    return thread


if __name__ == "__main__":
    run_scheduler()
