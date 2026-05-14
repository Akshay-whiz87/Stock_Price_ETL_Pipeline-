"""
=============================================================
STOCK PRICE ETL PIPELINE — LOAD MODULE
=============================================================
Purpose : Load cleaned & enriched stock data into SQLite DB
          (In production: replace SQLite with PostgreSQL/
           Snowflake/Redshift — same code, just change conn)
=============================================================
"""

import sqlite3
import pandas as pd
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

DB_PATH = "data/stock_pipeline.db"


def get_connection() -> sqlite3.Connection:
    """Create and return DB connection."""
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")  # better concurrency
    return conn


def create_tables(conn: sqlite3.Connection):
    """
    Create tables if they don't exist.
    Idempotent — safe to run multiple times.
    """
    conn.executescript("""
        -- Main stock prices table
        CREATE TABLE IF NOT EXISTS stock_prices (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol          TEXT    NOT NULL,
            date            DATE    NOT NULL,
            open            REAL    NOT NULL,
            high            REAL    NOT NULL,
            low             REAL    NOT NULL,
            close           REAL    NOT NULL,
            volume          INTEGER NOT NULL,
            daily_return_pct REAL,
            ma_7            REAL,
            ma_20           REAL,
            ma_50           REAL,
            volatility_20d  REAL,
            daily_range     REAL,
            typical_price   REAL,
            signal          TEXT,
            processed_at    TEXT,
            data_source     TEXT,
            UNIQUE(symbol, date)   -- prevent duplicates
        );

        -- Pipeline run log table (audit trail)
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at          TEXT    NOT NULL,
            status          TEXT    NOT NULL,
            rows_inserted   INTEGER DEFAULT 0,
            rows_updated    INTEGER DEFAULT 0,
            rows_failed     INTEGER DEFAULT 0,
            symbols         TEXT,
            error_message   TEXT,
            duration_secs   REAL
        );

        -- Daily summary table (aggregated view)
        CREATE TABLE IF NOT EXISTS daily_summary (
            summary_date    DATE    NOT NULL,
            symbol          TEXT    NOT NULL,
            close           REAL,
            daily_return_pct REAL,
            signal          TEXT,
            ma_7            REAL,
            ma_20           REAL,
            volatility_20d  REAL,
            created_at      TEXT,
            PRIMARY KEY (summary_date, symbol)
        );
    """)
    conn.commit()
    logger.info("Tables created / verified OK")


def upsert_stock_prices(conn: sqlite3.Connection, df: pd.DataFrame) -> dict:
    """
    Insert or update stock prices using INSERT OR REPLACE.
    This is the UPSERT pattern — common in data engineering.

    Args:
        conn: SQLite connection
        df: Transformed DataFrame

    Returns:
        dict with inserted/updated counts
    """
    inserted = 0
    failed   = 0

    for _, row in df.iterrows():
        try:
            conn.execute("""
                INSERT OR REPLACE INTO stock_prices
                (symbol, date, open, high, low, close, volume,
                 daily_return_pct, ma_7, ma_20, ma_50,
                 volatility_20d, daily_range, typical_price,
                 signal, processed_at, data_source)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                row["symbol"],
                str(row["date"].date()),
                row["open"], row["high"], row["low"],
                row["close"], row["volume"],
                row.get("daily_return_pct"),
                row.get("ma_7"), row.get("ma_20"), row.get("ma_50"),
                row.get("volatility_20d"), row.get("daily_range"),
                row.get("typical_price"), row.get("signal"),
                row.get("processed_at"), row.get("data_source")
            ))
            inserted += 1
        except Exception as e:
            logger.error(f"Row insert failed {row['symbol']} {row['date']}: {e}")
            failed += 1

    conn.commit()
    logger.info(f"Load complete — Inserted/Updated: {inserted}, Failed: {failed}")
    return {"inserted": inserted, "failed": failed}


def refresh_daily_summary(conn: sqlite3.Connection):
    """
    Refresh the daily summary table — latest day per symbol.
    This is like a materialized view refresh in production.
    """
    conn.execute("DELETE FROM daily_summary")
    conn.execute("""
        INSERT INTO daily_summary
        SELECT
            date            AS summary_date,
            symbol,
            close,
            daily_return_pct,
            signal,
            ma_7,
            ma_20,
            volatility_20d,
            datetime('now') AS created_at
        FROM stock_prices
        WHERE (symbol, date) IN (
            SELECT symbol, MAX(date)
            FROM stock_prices
            GROUP BY symbol
        )
    """)
    conn.commit()
    logger.info("Daily summary refreshed")


def log_pipeline_run(conn: sqlite3.Connection, status: str,
                     rows_inserted: int, rows_failed: int,
                     symbols: list, error_msg: str = None,
                     duration: float = 0.0):
    """Log every pipeline run for audit trail."""
    conn.execute("""
        INSERT INTO pipeline_runs
        (run_at, status, rows_inserted, rows_failed, symbols, error_message, duration_secs)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        status,
        rows_inserted,
        rows_failed,
        ",".join(symbols) if symbols else "",
        error_msg,
        round(duration, 2)
    ))
    conn.commit()


def load(df: pd.DataFrame) -> dict:
    """
    Master load function — runs full load pipeline.
    """
    conn = get_connection()
    create_tables(conn)
    result = upsert_stock_prices(conn, df)
    refresh_daily_summary(conn)
    conn.close()
    return result


if __name__ == "__main__":
    df = pd.read_csv("data/transformed_stocks.csv", parse_dates=["date"])
    result = load(df)
    print(f"Load result: {result}")

    # Quick verify
    conn = get_connection()
    print("\n--- Stock Prices (last 5 rows) ---")
    print(pd.read_sql("SELECT * FROM stock_prices ORDER BY date DESC LIMIT 5", conn))
    print("\n--- Daily Summary ---")
    print(pd.read_sql("SELECT * FROM daily_summary", conn))
    conn.close()
