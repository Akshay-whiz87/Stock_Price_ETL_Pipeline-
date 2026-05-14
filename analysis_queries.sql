-- =============================================================
-- STOCK PIPELINE — SQL ANALYSIS QUERIES
-- =============================================================
-- These are exactly the type of queries IVP will ask you to
-- write in your interview. Study each one carefully.
-- =============================================================


-- ── QUERY 1: Latest price for each stock ──────────────────
-- Concept: MAX with GROUP BY — most basic finance query
SELECT
    symbol,
    MAX(date)   AS latest_date,
    close       AS latest_close
FROM stock_prices
GROUP BY symbol
ORDER BY symbol;


-- ── QUERY 2: Top 3 best performing days per stock ─────────
-- Concept: Window function RANK() — very common interview Q
SELECT *
FROM (
    SELECT
        symbol,
        date,
        close,
        daily_return_pct,
        RANK() OVER (
            PARTITION BY symbol
            ORDER BY daily_return_pct DESC
        ) AS rank_by_return
    FROM stock_prices
    WHERE daily_return_pct IS NOT NULL
) ranked
WHERE rank_by_return <= 3
ORDER BY symbol, rank_by_return;


-- ── QUERY 3: 7-day rolling average close price ────────────
-- Concept: Window function with ROWS BETWEEN — classic ETL
SELECT
    symbol,
    date,
    close,
    AVG(close) OVER (
        PARTITION BY symbol
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_avg
FROM stock_prices
ORDER BY symbol, date DESC
LIMIT 30;


-- ── QUERY 4: Day-over-day price change (LAG function) ─────
-- Concept: LAG — most asked window function in interviews
SELECT
    symbol,
    date,
    close,
    LAG(close) OVER (
        PARTITION BY symbol ORDER BY date
    ) AS prev_close,
    ROUND(
        (close - LAG(close) OVER (PARTITION BY symbol ORDER BY date))
        / LAG(close) OVER (PARTITION BY symbol ORDER BY date) * 100,
        2
    ) AS pct_change_manual
FROM stock_prices
ORDER BY symbol, date DESC
LIMIT 20;


-- ── QUERY 5: Monthly summary (aggregation) ────────────────
-- Concept: STRFTIME for date grouping, multiple aggregations
-- Finance use: monthly P&L reports
SELECT
    symbol,
    STRFTIME('%Y-%m', date)     AS month,
    ROUND(MIN(low), 2)          AS monthly_low,
    ROUND(MAX(high), 2)         AS monthly_high,
    ROUND(AVG(close), 2)        AS avg_close,
    SUM(volume)                 AS total_volume,
    COUNT(*)                    AS trading_days,
    ROUND(
        (MAX(close) - MIN(close)) / MIN(close) * 100, 2
    )                           AS monthly_range_pct
FROM stock_prices
GROUP BY symbol, STRFTIME('%Y-%m', date)
ORDER BY symbol, month DESC;


-- ── QUERY 6: Find stocks above their 20-day MA (BUY signal)
-- Concept: Filtering on computed column — finance logic
SELECT
    symbol,
    date,
    close,
    ma_20,
    ROUND(close - ma_20, 2)     AS above_ma_by,
    signal
FROM stock_prices
WHERE signal = 'BUY'
  AND date = (SELECT MAX(date) FROM stock_prices sp2
              WHERE sp2.symbol = stock_prices.symbol)
ORDER BY above_ma_by DESC;


-- ── QUERY 7: Volatility ranking across stocks ─────────────
-- Concept: CTE + RANK — shows you can write readable SQL
WITH latest_volatility AS (
    SELECT
        symbol,
        volatility_20d,
        date
    FROM stock_prices
    WHERE (symbol, date) IN (
        SELECT symbol, MAX(date)
        FROM stock_prices
        GROUP BY symbol
    )
)
SELECT
    symbol,
    ROUND(volatility_20d, 2)    AS volatility_pct,
    RANK() OVER (ORDER BY volatility_20d DESC) AS volatility_rank,
    CASE
        WHEN volatility_20d > 40 THEN 'HIGH RISK'
        WHEN volatility_20d > 20 THEN 'MEDIUM RISK'
        ELSE 'LOW RISK'
    END AS risk_category
FROM latest_volatility
ORDER BY volatility_rank;


-- ── QUERY 8: Pipeline audit — run history ─────────────────
-- Concept: Operational query for monitoring pipelines
SELECT
    run_id,
    run_at,
    status,
    rows_inserted,
    rows_failed,
    symbols,
    ROUND(duration_secs, 1)     AS duration_secs,
    error_message
FROM pipeline_runs
ORDER BY run_at DESC
LIMIT 10;


-- ── QUERY 9: Detect data gaps (missing trading days) ──────
-- Concept: Finding gaps — critical data quality check
-- Explanation: If difference between consecutive dates > 3 days
-- (accounting for weekends), it may indicate missing data
WITH date_diffs AS (
    SELECT
        symbol,
        date,
        LAG(date) OVER (PARTITION BY symbol ORDER BY date) AS prev_date,
        JULIANDAY(date) -
            JULIANDAY(LAG(date) OVER (PARTITION BY symbol ORDER BY date))
            AS days_gap
    FROM stock_prices
)
SELECT
    symbol,
    prev_date,
    date,
    days_gap,
    'POSSIBLE GAP' AS alert
FROM date_diffs
WHERE days_gap > 5   -- more than 5 days = likely missing data
ORDER BY symbol, date;


-- ── QUERY 10: YTD (Year-to-Date) return per stock ─────────
-- Concept: Joining a table to itself — real finance reporting
WITH year_start AS (
    SELECT symbol, close AS start_price
    FROM stock_prices
    WHERE date = (
        SELECT MIN(date) FROM stock_prices sp2
        WHERE sp2.symbol = stock_prices.symbol
          AND STRFTIME('%Y', date) = STRFTIME('%Y', 'now')
    )
),
year_latest AS (
    SELECT symbol, close AS latest_price, date AS latest_date
    FROM stock_prices
    WHERE (symbol, date) IN (
        SELECT symbol, MAX(date) FROM stock_prices GROUP BY symbol
    )
)
SELECT
    y.symbol,
    ys.start_price,
    y.latest_price,
    y.latest_date,
    ROUND((y.latest_price - ys.start_price) / ys.start_price * 100, 2)
        AS ytd_return_pct,
    CASE
        WHEN y.latest_price > ys.start_price THEN '📈 UP'
        ELSE '📉 DOWN'
    END AS direction
FROM year_latest y
JOIN year_start ys ON y.symbol = ys.symbol
ORDER BY ytd_return_pct DESC;
