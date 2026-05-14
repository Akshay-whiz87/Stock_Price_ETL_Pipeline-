"""
=============================================================
STOCK PRICE ETL PIPELINE — APACHE AIRFLOW DAG
=============================================================
Purpose : Schedule the ETL pipeline to run daily at 6 AM IST
          This is what a production data pipeline looks like

How to use:
1. Install Airflow: pip install apache-airflow
2. Copy this file to your ~/airflow/dags/ folder
3. Start Airflow: airflow standalone
4. Open: http://localhost:8080
5. Enable the DAG: stock_price_pipeline
=============================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email  import EmailOperator
import sys, os

# Add project src to path
sys.path.insert(0, "/path/to/your/stock_pipeline/src")

# ── Default Args (applies to all tasks) ───────────────────
default_args = {
    "owner"           : "data_engineering_team",
    "depends_on_past" : False,          # don't wait for yesterday's run
    "start_date"      : datetime(2025, 1, 1),
    "email"           : ["your_email@gmail.com"],
    "email_on_failure": True,           # alert on failure
    "email_on_retry"  : False,
    "retries"         : 2,              # retry twice before failing
    "retry_delay"     : timedelta(minutes=5),
}

# ── DAG Definition ─────────────────────────────────────────
dag = DAG(
    dag_id="stock_price_pipeline",
    default_args=default_args,
    description="Daily stock price ETL: Alpha Vantage → SQLite",
    schedule_interval="0 6 * * 1-5",   # 6 AM IST, Mon-Fri only
    catchup=False,                      # don't backfill missed runs
    tags=["finance", "etl", "daily"],
)

# ── Task Functions ─────────────────────────────────────────
def task_extract(**context):
    from extract import extract_all_stocks
    import json
    raw = extract_all_stocks()
    # Push data to XCom (Airflow's inter-task data sharing)
    context["ti"].xcom_push(key="raw_data", value=json.dumps([
        {"symbol": r["symbol"], "rows": len(r["data"])} for r in raw
    ]))
    return f"Extracted {len(raw)} symbols"


def task_transform(**context):
    from extract   import extract_all_stocks
    from transform import transform_all
    raw = extract_all_stocks()
    df  = transform_all(raw)
    df.to_csv("/tmp/stock_transformed.csv", index=False)
    return f"Transformed {len(df)} rows"


def task_load(**context):
    import pandas as pd
    from load import load
    df = pd.read_csv("/tmp/stock_transformed.csv", parse_dates=["date"])
    result = load(df)
    return f"Loaded: {result}"


def task_validate(**context):
    """Post-load data quality check."""
    from load import get_connection
    import pandas as pd
    conn = get_connection()
    count = pd.read_sql(
        "SELECT COUNT(*) as cnt FROM stock_prices WHERE date = DATE('now')", conn
    )
    conn.close()
    rows = count["cnt"].iloc[0]
    if rows == 0:
        raise ValueError("No data loaded for today — pipeline may have failed!")
    return f"Validation passed: {rows} rows for today"


# ── Task Definitions ───────────────────────────────────────
t1_extract = PythonOperator(
    task_id="extract_stock_data",
    python_callable=task_extract,
    dag=dag,
)

t2_transform = PythonOperator(
    task_id="transform_and_enrich",
    python_callable=task_transform,
    dag=dag,
)

t3_load = PythonOperator(
    task_id="load_to_database",
    python_callable=task_load,
    dag=dag,
)

t4_validate = PythonOperator(
    task_id="validate_loaded_data",
    python_callable=task_validate,
    dag=dag,
)

# ── Task Dependencies (defines the flow) ──────────────────
# Extract → Transform → Load → Validate
t1_extract >> t2_transform >> t3_load >> t4_validate
