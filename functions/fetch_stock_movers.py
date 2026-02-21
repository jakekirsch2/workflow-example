#!/usr/bin/env python3
"""
fetch_stock_movers.py
Downloads last trading day OHLCV data from Yahoo Finance for a broad
universe of S&P 500 / large-cap tickers, computes percentage change
(close vs. previous close), and writes the top movers to an Iceberg table.
"""

import logging
import os
from datetime import datetime, timedelta, date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ── Ticker universe ──────────────────────────────────────────────────────────
TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM", "V",
    "XOM", "MA", "PG", "HD", "COST", "MRK", "CVX", "ABBV", "CRM", "AMD",
    "PEP", "NFLX", "ADBE", "KO", "AVGO", "ACN", "MCD", "WMT", "BAC", "CSCO",
    "LIN", "ORCL", "DHR", "AMGN", "DIS", "TXN", "NKE", "PM", "MS", "GS",
    "HON", "UNP", "IBM", "INTU", "CAT", "BA", "SPGI", "ISRG", "BKNG", "AXP",
    "SBUX", "SYK", "BLK", "GILD", "PFE", "MDT", "C", "ZTS", "BMY", "MDLZ",
    "MMC", "TJX", "REGN", "CI", "SO", "DUK", "WM", "CL", "F", "GM",
    "UBER", "PYPL", "COIN", "PLTR", "SNAP", "SPOT", "RBLX",
]


def _date_range():
    """Return (start_date, end_date) strings covering the last ~7 calendar days."""
    today = date.today()
    start = today - timedelta(days=7)
    return start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


def main(spark, top_n: str = "25"):
    """
    Fetch last-trading-day stock data from Yahoo Finance and write the
    top movers (by absolute % change) to analytics.stock_movers.

    Args:
        spark:  SparkSession (injected by platform)
        top_n:  Number of top movers to keep (default 25)
    """
    import yfinance as yf
    import pandas as pd

    top_n = int(top_n)

    conf = spark.sparkContext.getConf()
    execution_id = conf.get("spark.workflow.executionId", "local")
    environment  = conf.get("spark.workflow.environment", "development")

    logger.info(f"Execution ID : {execution_id}")
    logger.info(f"Environment  : {environment}")
    logger.info(f"Top-N movers : {top_n}")

    start_str, end_str = _date_range()
    logger.info(f"Download window: {start_str} → {end_str}")

    # ── 1. Batch-download all tickers in a single HTTP call ──────────────────
    ticker_str = " ".join(TICKERS)
    logger.info(f"Downloading {len(TICKERS)} tickers via yf.download (single batch) …")

    raw = yf.download(
        tickers=ticker_str,
        start=start_str,
        end=end_str,
        auto_adjust=True,
        progress=False,
        threads=False,   # single-threaded avoids per-ticker connection storms
        group_by="ticker",
    )

    if raw is None or raw.empty:
        raise ValueError("yfinance returned no data – market may be closed or network unavailable.")

    logger.info(f"Raw data shape: {raw.shape}")

    # ── 2. Extract close prices ──────────────────────────────────────────────
    # With group_by="ticker" and multiple tickers the columns are MultiIndex: (ticker, field)
    # Pivot to a (date x ticker) close price DataFrame
    if isinstance(raw.columns, pd.MultiIndex):
        close_px = raw.xs("Close", axis=1, level=1)
    else:
        # Single ticker fallback
        close_px = raw[["Close"]].rename(columns={"Close": TICKERS[0]})

    # Drop dates where ALL tickers are NaN (non-trading days)
    close_px = close_px.dropna(how="all")

    logger.info(f"Trading days retrieved: {len(close_px)}  |  tickers with data: {close_px.notna().any().sum()}")

    if len(close_px) < 2:
        raise ValueError(
            "Need at least 2 trading days to compute % change. "
            f"Dates found: {list(close_px.index)}"
        )

    # ── 3. Compute % change between the last two trading days ────────────────
    prev_row  = close_px.iloc[-2]
    last_row  = close_px.iloc[-1]
    trading_date = close_px.index[-1].date()
    prev_date    = close_px.index[-2].date()
    logger.info(f"Computing change: {prev_date} → {trading_date}")

    pct_series = ((last_row - prev_row) / prev_row * 100).dropna()

    # Volume
    if isinstance(raw.columns, pd.MultiIndex):
        try:
            vol_row = raw.xs("Volume", axis=1, level=1).iloc[-1]
        except KeyError:
            vol_row = pd.Series(dtype=float)
    else:
        vol_row = pd.Series(dtype=float)

    # ── 4. Build results ─────────────────────────────────────────────────────
    records = []
    for ticker in pct_series.index:
        pct = float(pct_series[ticker])
        if pd.isna(pct):
            continue
        vol = None
        if ticker in vol_row.index and not pd.isna(vol_row[ticker]):
            vol = int(vol_row[ticker])
        records.append({
            "ticker":         str(ticker),
            "trading_date":   str(trading_date),
            "prev_date":      str(prev_date),
            "prev_close":     round(float(prev_row[ticker]), 4),
            "last_close":     round(float(last_row[ticker]), 4),
            "pct_change":     round(pct, 4),
            "abs_pct_change": round(abs(pct), 4),
            "direction":      "UP" if pct >= 0 else "DOWN",
            "volume":         vol,
            "ingested_at":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        })

    if not records:
        raise ValueError("No valid percentage-change records produced.")

    pdf = (
        pd.DataFrame(records)
        .sort_values("abs_pct_change", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
    pdf.insert(0, "rank", range(1, len(pdf) + 1))

    logger.info(f"\nTop {top_n} movers on {trading_date}:")
    for _, row in pdf.iterrows():
        sign = "▲" if row["direction"] == "UP" else "▼"
        logger.info(
            f"  #{int(row['rank']):>2}  {row['ticker']:<6}  {sign} {float(row['pct_change']):>+7.2f}%"
            f"   close={float(row['last_close']):.2f}"
        )

    # ── 5. Write to Iceberg ──────────────────────────────────────────────────
    schema_cols = [
        "rank", "ticker", "trading_date", "prev_date", "prev_close", "last_close",
        "pct_change", "abs_pct_change", "direction", "volume", "ingested_at",
    ]
    sdf = spark.createDataFrame(pdf[schema_cols])
    sdf.writeTo("analytics.stock_movers").createOrReplace()

    row_count = sdf.count()
    logger.info(f"✓ Wrote {row_count} rows to analytics.stock_movers")
    return row_count
