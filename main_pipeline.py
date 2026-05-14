"""
=============================================================
STOCK PRICE ETL PIPELINE — MAIN ORCHESTRATOR
=============================================================
Purpose : Runs the full ETL pipeline end-to-end
          Extract → Transform → Load → Report

Run this file daily (manually or via scheduler):
    python src/main_pipeline.py

In production: replace this with Apache Airflow DAG
=============================================================
"""

import time
import logging
import os
import sys
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from extract   import extract_all_stocks
from transform import transform_all
from load      import load, get_connection, log_pipeline_run

# ── Logging ────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(f"logs/pipeline_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def run_pipeline():
    """
    Main ETL orchestration function.
    Runs Extract → Transform → Load with full error handling,
    logging and audit trail — production-grade pattern.
    """
    start_time = time.time()
    run_status = "SUCCESS"
    error_msg  = None
    total_inserted = 0
    total_failed   = 0
    symbols_fetched = []

    logger.info("=" * 60)
    logger.info("  STOCK PRICE ETL PIPELINE STARTED")
    logger.info(f"  Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    try:
        # ── STEP 1: EXTRACT ───────────────────────────────
        logger.info("📥 STEP 1/3 — EXTRACT: Fetching stock data from API...")
        raw_data = extract_all_stocks()

        if not raw_data:
            raise ValueError("Extract returned no data — check API key and network")

        symbols_fetched = [r["symbol"] for r in raw_data]
        logger.info(f"   Extracted: {symbols_fetched}")

        # ── STEP 2: TRANSFORM ─────────────────────────────
        logger.info("⚙️  STEP 2/3 — TRANSFORM: Cleaning and enriching data...")
        transformed_df = transform_all(raw_data)
        logger.info(f"   Transformed: {len(transformed_df)} rows, {transformed_df['symbol'].nunique()} symbols")

        # Save intermediate CSV (useful for debugging)
        transformed_df.to_csv("data/latest_transformed.csv", index=False)
        logger.info("   Saved transformed data to data/latest_transformed.csv")

        # ── STEP 3: LOAD ──────────────────────────────────
        logger.info("💾 STEP 3/3 — LOAD: Writing to database...")
        result = load(transformed_df)
        total_inserted = result["inserted"]
        total_failed   = result["failed"]
        logger.info(f"   Loaded: {total_inserted} rows, {total_failed} failed")

    except Exception as e:
        run_status = "FAILED"
        error_msg  = str(e)
        logger.error(f"❌ PIPELINE FAILED: {e}", exc_info=True)

    finally:
        duration = time.time() - start_time

        # Log pipeline run to audit table (always runs)
        try:
            conn = get_connection()
            log_pipeline_run(
                conn=conn,
                status=run_status,
                rows_inserted=total_inserted,
                rows_failed=total_failed,
                symbols=symbols_fetched,
                error_msg=error_msg,
                duration=duration
            )
            conn.close()
        except Exception as log_err:
            logger.error(f"Failed to log pipeline run: {log_err}")

        logger.info("=" * 60)
        logger.info(f"  PIPELINE {run_status}")
        logger.info(f"  Duration  : {duration:.2f} seconds")
        logger.info(f"  Rows In DB: {total_inserted}")
        logger.info(f"  Symbols   : {symbols_fetched}")
        logger.info("=" * 60)

    return run_status


if __name__ == "__main__":
    status = run_pipeline()
    sys.exit(0 if status == "SUCCESS" else 1)
