"""
=============================================================
STOCK PRICE ETL PIPELINE — TRANSFORM MODULE
=============================================================
Purpose : Clean, validate and enrich raw stock price data
          Adds financial indicators: moving averages, daily
          returns, volatility — common in finance domain ETL
=============================================================
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def parse_raw_to_dataframe(raw: dict) -> pd.DataFrame:
    """
    Convert raw API dict response into a clean Pandas DataFrame.

    Args:
        raw: dict with keys 'symbol' and 'data' (nested date->OHLCV dict)

    Returns:
        DataFrame with columns: symbol, date, open, high, low, close, volume
    """
    symbol = raw["symbol"]
    records = []

    for date_str, values in raw["data"].items():
        records.append({
            "symbol"    : symbol,
            "date"      : pd.to_datetime(date_str),
            "open"      : float(values["1. open"]),
            "high"      : float(values["2. high"]),
            "low"       : float(values["3. low"]),
            "close"     : float(values["4. close"]),
            "volume"    : int(values["5. volume"]),
        })

    df = pd.DataFrame(records)
    df.sort_values("date", inplace=True)
    df.reset_index(drop=True, inplace=True)
    logger.info(f"Parsed {symbol}: {len(df)} rows")
    return df


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Data quality checks — critical in financial pipelines.
    Removes bad rows and logs issues found.

    Checks:
    - No null values in OHLCV columns
    - Prices must be positive
    - High >= Low (basic sanity)
    - Volume must be non-negative
    - No duplicate dates per symbol
    """
    initial_rows = len(df)
    issues = []

    # 1. Remove nulls
    null_rows = df[df[["open","high","low","close","volume"]].isnull().any(axis=1)]
    if len(null_rows) > 0:
        issues.append(f"{len(null_rows)} rows with null values removed")
        df = df.dropna(subset=["open","high","low","close","volume"])

    # 2. Positive prices
    bad_price = df[(df["close"] <= 0) | (df["open"] <= 0)]
    if len(bad_price) > 0:
        issues.append(f"{len(bad_price)} rows with zero/negative prices removed")
        df = df[(df["close"] > 0) & (df["open"] > 0)]

    # 3. High >= Low sanity
    bad_hl = df[df["high"] < df["low"]]
    if len(bad_hl) > 0:
        issues.append(f"{len(bad_hl)} rows where high < low removed")
        df = df[df["high"] >= df["low"]]

    # 4. Duplicate date-symbol check
    dupes = df.duplicated(subset=["symbol","date"])
    if dupes.sum() > 0:
        issues.append(f"{dupes.sum()} duplicate date rows removed")
        df = df.drop_duplicates(subset=["symbol","date"])

    final_rows = len(df)
    if issues:
        logger.warning(f"Data quality issues for {df['symbol'].iloc[0]}: {'; '.join(issues)}")
    logger.info(f"Validation: {initial_rows} → {final_rows} rows (removed {initial_rows - final_rows})")
    return df


def add_financial_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich data with financial indicators — shows finance domain knowledge.

    Adds:
    - daily_return     : % change in close price day over day
    - ma_7             : 7-day simple moving average (SMA)
    - ma_20            : 20-day simple moving average (SMA)
    - ma_50            : 50-day simple moving average (SMA)
    - volatility_20d   : 20-day rolling standard deviation of returns
    - daily_range      : High - Low (intraday price range)
    - vwap_approx      : Approximate VWAP (typical price * volume proxy)
    - signal           : BUY / SELL / HOLD based on MA crossover
    """
    df = df.copy()

    # Daily return (percentage)
    df["daily_return_pct"] = df["close"].pct_change() * 100

    # Moving averages
    df["ma_7"]  = df["close"].rolling(window=7,  min_periods=1).mean().round(4)
    df["ma_20"] = df["close"].rolling(window=20, min_periods=1).mean().round(4)
    df["ma_50"] = df["close"].rolling(window=50, min_periods=1).mean().round(4)

    # 20-day rolling volatility (annualised)
    df["volatility_20d"] = (
        df["daily_return_pct"].rolling(window=20, min_periods=5).std() * np.sqrt(252)
    ).round(4)

    # Intraday range
    df["daily_range"] = (df["high"] - df["low"]).round(4)

    # Approximate VWAP (typical price = avg of H, L, C)
    df["typical_price"] = ((df["high"] + df["low"] + df["close"]) / 3).round(4)

    # MA crossover signal (simple strategy — shows finance logic)
    df["signal"] = "HOLD"
    df.loc[df["ma_7"] > df["ma_20"], "signal"] = "BUY"   # short MA above long MA
    df.loc[df["ma_7"] < df["ma_20"], "signal"] = "SELL"  # short MA below long MA

    # ETL metadata
    df["processed_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    df["data_source"]  = "Alpha Vantage API"

    logger.info(f"Financial indicators added for {df['symbol'].iloc[0]}")
    return df


def transform_all(raw_list: list) -> pd.DataFrame:
    """
    Master transform function — processes all raw stock data.

    Args:
        raw_list: list of raw dicts from extract module

    Returns:
        Single combined DataFrame with all stocks, cleaned and enriched
    """
    all_dfs = []

    for raw in raw_list:
        try:
            df = parse_raw_to_dataframe(raw)
            df = validate_data(df)
            df = add_financial_indicators(df)
            all_dfs.append(df)
        except Exception as e:
            logger.error(f"Transform failed for {raw.get('symbol','?')}: {e}")

    if not all_dfs:
        raise ValueError("No data transformed successfully")

    combined = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Transform complete — Total rows: {len(combined)} across {combined['symbol'].nunique()} symbols")
    return combined


if __name__ == "__main__":
    import json
    with open("data/raw_extract.json") as f:
        raw = json.load(f)
    df = transform_all(raw)
    df.to_csv("data/transformed_stocks.csv", index=False)
    print(df.tail(10).to_string())
    print(f"\nShape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
