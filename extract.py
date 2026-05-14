"""
=============================================================
STOCK PRICE ETL PIPELINE — EXTRACT MODULE
=============================================================
Project  : Financial Data Pipeline for IVP-style Use Case
Author   : [Your Name]
Purpose  : Extract stock price data from Alpha Vantage API
           (Free API — no payment needed)
=============================================================
"""

import requests
import json
import logging
import os
from datetime import datetime

# ── Logging Setup ──────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────
# Get free API key from: https://www.alphavantage.co/support/#api-key
API_KEY = os.getenv("ALPHA_VANTAGE_KEY", "demo")   # set in .env file
BASE_URL = "https://www.alphavantage.co/query"

# Stocks we want to track (NSE/BSE style — but Alpha Vantage uses US symbols for demo)
# For Indian stocks use: BSE:RELIANCE or suffix .BSE
STOCKS = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"]  # swap with Indian tickers


def fetch_daily_prices(symbol: str) -> dict:
    """
    Fetch last 100 days of daily OHLCV data for a given stock symbol.

    Args:
        symbol: Stock ticker symbol e.g. 'AAPL' or 'RELIANCE.BSE'

    Returns:
        dict with raw API response data

    Raises:
        ValueError: If API returns error or empty data
    """
    logger.info(f"Fetching data for {symbol}...")

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",   # last 100 days
        "apikey": API_KEY
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        # API error handling
        if "Error Message" in data:
            raise ValueError(f"API Error for {symbol}: {data['Error Message']}")
        if "Note" in data:
            logger.warning(f"API rate limit hit: {data['Note']}")
            raise ValueError("API rate limit — wait 1 min and retry")
        if "Time Series (Daily)" not in data:
            raise ValueError(f"Unexpected API response for {symbol}: {data}")

        records = data["Time Series (Daily)"]
        logger.info(f"✅ {symbol}: {len(records)} days of data fetched")
        return {"symbol": symbol, "data": records}

    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching {symbol}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error for {symbol}: {e}")
        raise


def extract_all_stocks() -> list:
    """
    Loop through all configured stocks and fetch their data.

    Returns:
        List of raw data dicts for each symbol
    """
    results = []
    failed = []

    for symbol in STOCKS:
        try:
            result = fetch_daily_prices(symbol)
            results.append(result)
        except Exception as e:
            logger.error(f"❌ Failed to fetch {symbol}: {e}")
            failed.append(symbol)

    logger.info(f"Extract complete — Success: {len(results)}, Failed: {len(failed)}")
    if failed:
        logger.warning(f"Failed symbols: {failed}")

    return results


# ── Run standalone ─────────────────────────────────────────
if __name__ == "__main__":
    data = extract_all_stocks()
    # Save raw data for inspection
    with open("data/raw_extract.json", "w") as f:
        json.dump(data, f, indent=2)
    print(f"Raw data saved to data/raw_extract.json")
