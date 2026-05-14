# 📊 Stock Price ETL Pipeline
## Complete Project Synopsis — Interview Ready

---

## 🎯 Project Title
**Automated Financial Data Pipeline — Stock Price ETL System**

---

## 📋 One-Line Summary (Say this in interview)
> "I built an automated ETL pipeline that extracts daily stock price data from a financial API, cleans and enriches it with financial indicators like moving averages and volatility, loads it into a database, and schedules it to run every weekday automatically using Apache Airflow."

---

## 🏗️ Project Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   DAILY SCHEDULE (6 AM)                  │
│                   Apache Airflow DAG                     │
└──────────────────────┬──────────────────────────────────┘
                       │
          ┌────────────▼────────────┐
          │    EXTRACT              │
          │  Alpha Vantage API      │
          │  5 Stock Symbols        │
          │  100 days history each  │
          └────────────┬────────────┘
                       │
          ┌────────────▼────────────┐
          │    TRANSFORM            │
          │  • Data Validation      │
          │  • Clean null/bad rows  │
          │  • Add MA 7/20/50       │
          │  • Daily Returns %      │
          │  • Volatility 20d       │
          │  • BUY/SELL/HOLD Signal │
          └────────────┬────────────┘
                       │
          ┌────────────▼────────────┐
          │    LOAD                 │
          │  SQLite Database        │
          │  (PostgreSQL-ready)     │
          │  • stock_prices table   │
          │  • daily_summary table  │
          │  • pipeline_runs table  │
          └────────────┬────────────┘
                       │
          ┌────────────▼────────────┐
          │    VALIDATE             │
          │  Post-load data checks  │
          │  Audit trail logged     │
          └─────────────────────────┘
```

---

## 📁 Project Structure

```
stock_pipeline/
│
├── src/
│   ├── extract.py          # API calls, error handling
│   ├── transform.py        # Data cleaning + enrichment
│   ├── load.py             # Database operations, upsert
│   ├── main_pipeline.py    # Orchestrator — runs full ETL
│   └── airflow_dag.py      # Airflow scheduling DAG
│
├── sql/
│   └── analysis_queries.sql # 10 interview-ready SQL queries
│
├── data/                   # Generated data files (gitignored)
├── logs/                   # Pipeline run logs (gitignored)
├── requirements.txt        # Python dependencies
└── README.md               # This file
```

---

## 🔧 Tech Stack

| Component | Tool | Why Chosen |
|-----------|------|------------|
| Language | Python 3.10+ | Industry standard for DE |
| Data Source | Alpha Vantage API | Free, real financial data |
| Data Processing | Pandas + NumPy | Standard for data transformation |
| Database | SQLite (dev) / PostgreSQL (prod) | Easy local dev, prod-ready swap |
| Scheduling | Apache Airflow | Industry standard orchestration |
| HTTP Client | Requests | API calls |
| Logging | Python logging module | Audit trail |
| Version Control | Git | Code management |

---

## 📊 Data Flow — Detailed

### Step 1: EXTRACT (extract.py)
- Connects to Alpha Vantage REST API
- Fetches 100 days of OHLCV data per symbol
- Handles API rate limits (free tier: 5 calls/min)
- Graceful error handling — one symbol failure doesn't stop others
- Returns list of raw JSON dicts

**Key concept demonstrated:** API integration, error handling, logging

---

### Step 2: TRANSFORM (transform.py)
Raw data → Clean, enriched DataFrame

**Data Validation checks:**
- Remove null values in price columns
- Remove zero or negative prices
- Ensure High >= Low (sanity check)
- Remove duplicate date-symbol combinations

**Financial Indicators Added:**
| Indicator | Formula | Finance Use |
|-----------|---------|-------------|
| daily_return_pct | (close - prev_close) / prev_close × 100 | P&L tracking |
| ma_7 | 7-day simple moving average | Short-term trend |
| ma_20 | 20-day simple moving average | Medium-term trend |
| ma_50 | 50-day simple moving average | Long-term trend |
| volatility_20d | 20-day rolling std × √252 | Risk measure |
| daily_range | High - Low | Intraday movement |
| typical_price | (H + L + C) / 3 | VWAP proxy |
| signal | MA7 vs MA20 crossover | BUY/SELL/HOLD |

**Key concept demonstrated:** Pandas, data quality, finance domain

---

### Step 3: LOAD (load.py)
- Creates 3 tables: stock_prices, daily_summary, pipeline_runs
- UPSERT pattern (INSERT OR REPLACE) — idempotent, safe to re-run
- Refreshes daily_summary after each load
- Logs every pipeline run to pipeline_runs for audit

**Key concept demonstrated:** SQL, UPSERT, idempotency, audit trail

---

### Step 4: ORCHESTRATION (airflow_dag.py)
- Runs Mon-Fri at 6 AM (after markets close)
- 4 tasks: extract → transform → load → validate
- Retries 2 times on failure
- Email alert on failure
- XCom for inter-task communication

**Key concept demonstrated:** Airflow, DAG design, scheduling

---

## 🗃️ Database Schema

### Table: stock_prices
```sql
symbol          TEXT    -- e.g. 'AAPL', 'RELIANCE.BSE'
date            DATE    -- trading date
open            REAL    -- opening price
high            REAL    -- day high
low             REAL    -- day low
close           REAL    -- closing price
volume          INTEGER -- shares traded
daily_return_pct REAL   -- % change from previous day
ma_7            REAL    -- 7-day moving average
ma_20           REAL    -- 20-day moving average
ma_50           REAL    -- 50-day moving average
volatility_20d  REAL    -- annualised 20-day volatility
daily_range     REAL    -- high minus low
typical_price   REAL    -- (H+L+C)/3
signal          TEXT    -- BUY / SELL / HOLD
processed_at    TEXT    -- when this row was loaded
data_source     TEXT    -- source of data
UNIQUE(symbol, date)    -- prevents duplicate loading
```

### Table: pipeline_runs (Audit trail)
```sql
run_id          INTEGER -- auto increment
run_at          TEXT    -- when pipeline ran
status          TEXT    -- SUCCESS / FAILED
rows_inserted   INTEGER -- how many rows loaded
rows_failed     INTEGER -- how many rows failed
symbols         TEXT    -- which symbols processed
error_message   TEXT    -- error if failed
duration_secs   REAL    -- how long it took
```

---

## 🚀 How to Run

```bash
# 1. Clone / download the project
cd stock_pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Get free API key
# Go to: https://www.alphavantage.co/support/#api-key
# Takes 30 seconds, completely free

# 4. Set API key
export ALPHA_VANTAGE_KEY="your_key_here"

# 5. Run the pipeline
python src/main_pipeline.py

# 6. Check results
# Data saved to: data/stock_pipeline.db
# Logs saved to: logs/pipeline_YYYYMMDD.log
```

---

## 💡 What I Learned (Say this in interview)

1. **API Rate Limiting** — Free Alpha Vantage allows 5 calls/min.
   I handle this with error detection and logging.

2. **Idempotency** — Running the pipeline twice doesn't duplicate data.
   The UPSERT (INSERT OR REPLACE) pattern ensures this.

3. **Data Quality** — Financial data can have gaps, errors, wrong values.
   Always validate before loading — garbage in = garbage out.

4. **Audit Trail** — Every pipeline run is logged with status, row counts
   and duration. Critical for debugging production issues.

5. **Finance Domain** — Moving averages, volatility, VWAP are standard
   metrics used by portfolio managers and risk teams daily.

---

## 🔄 How to Scale to Production (Tell interviewer this)

| Current (Dev) | Production Upgrade |
|---------------|-------------------|
| SQLite | PostgreSQL / Snowflake |
| Alpha Vantage free | Bloomberg API / Refinitiv |
| Manual run | Apache Airflow on AWS MWAA |
| Local files | AWS S3 for raw data storage |
| 5 symbols | 500+ symbols |
| CSV intermediate | Parquet files (10x faster) |

---

## 📈 Sample Output

After running, query the database:

```
symbol | date       | close  | ma_7   | ma_20  | signal | daily_return_pct
-------|------------|--------|--------|--------|--------|------------------
AAPL   | 2025-01-15 | 228.34 | 225.12 | 221.45 | BUY    | +1.23
MSFT   | 2025-01-15 | 415.20 | 412.30 | 408.90 | BUY    | +0.87
GOOGL  | 2025-01-15 | 192.45 | 190.10 | 188.20 | BUY    | -0.34
TSLA   | 2025-01-15 | 389.20 | 375.40 | 360.10 | BUY    | +3.21
AMZN   | 2025-01-15 | 218.90 | 215.30 | 212.40 | BUY    | +0.92
```

---

## ❓ Expected Interview Questions & Answers

**Q: What is idempotency in a pipeline?**
A: Running the pipeline multiple times produces the same result —
   no duplicate data. I achieve this with INSERT OR REPLACE (UPSERT).

**Q: How would you handle late-arriving data?**
A: Allow reprocessing by date range. The UPSERT pattern handles
   this — if data arrives late, re-running updates existing records.

**Q: How do you ensure data quality?**
A: 4 checks before loading — nulls, negative prices, High>=Low,
   duplicate dates. Failed rows are logged, not silently dropped.

**Q: How would you scale this pipeline?**
A: Replace SQLite with Snowflake, use AWS S3 for raw storage,
   add Airflow on managed AWS MWAA, process in parallel using
   Airflow's TaskGroup for each symbol.

**Q: What is a DAG in Airflow?**
A: Directed Acyclic Graph — defines tasks and their dependencies.
   In my pipeline: extract → transform → load → validate.
   Acyclic means no circular dependencies — tasks flow one direction.
