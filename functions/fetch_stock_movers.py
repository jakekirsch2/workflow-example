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

# ── Broad ticker universe (S&P 500 large-caps + popular names) ──────────────
TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "GOOG", "META", "TSLA", "BRK-B",
    "UNH", "LLY", "JPM", "V", "XOM", "MA", "PG", "HD", "COST", "MRK", "CVX",
    "ABBV", "CRM", "AMD", "PEP", "NFLX", "ADBE", "KO", "TMO", "AVGO", "ACN",
    "MCD", "WMT", "BAC", "CSCO", "LIN", "ABT", "ORCL", "DHR", "AMGN", "DIS",
    "TXN", "NKE", "PM", "MS", "RTX", "INTC", "QCOM", "GS", "HON", "UNP",
    "IBM", "INTU", "CAT", "BA", "SPGI", "DE", "GE", "ISRG", "BKNG", "AXP",
    "SBUX", "SYK", "BLK", "T", "GILD", "PFE", "MDT", "MO", "C", "CB",
    "ZTS", "BMY", "MDLZ", "MMC", "TJX", "REGN", "CI", "SO", "DUK", "WM",
    "CL", "F", "GM", "UBER", "LYFT", "SNAP", "PINS", "SPOT", "RBLX", "U",
    "PYPL", "SQ", "COIN", "HOOD", "RIVN", "LCID", "NIO", "XPEV", "LI", "PLTR",
]


def _last_two_trading_days():
    """Return (start_date, end_date) strings covering the last 2 trading days."""
    today = date.today()
    # Go back up to 7 calendar days to safely capture the last 2 trading days
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
    environment = conf.get("spark.workflow.environment", "development")

    logger.info(f"Execution ID : {execution_id}")
    logger.info(f"Environment  : {environment}")
    logger.info(f"Top-N movers : {top_n}")

    start_str, end_str = _last_two_trading_days()
    logger.info(f"Download window: {start_str} → {end_str}")

    # ── 1. Download price history ────────────────────────────────────────────
    logger.info(f"Downloading data for {len(TICKERS)} tickers from Yahoo Finance …")
    raw = yf.download(
        tickers=TICKERS,
        start=start_str,
        end=end_str,
        auto_adjust=True,
        progress=False,
        threads=True,
    )

    if raw.empty:
        raise ValueError("yfinance returned no data – market may be closed or dates are invalid.")

    close_px = raw["Close"]
    logger.info(f"Got {len(close_px)} trading day(s) of close prices for up to {close_px.shape[1]} tickers.")

    if len(close_px) < 2:
        raise ValueError(
            "Need at least 2 trading days of data to compute percentage change. "
            f"Only got dates: {list(close_px.index)}"
        )

    # ── 2. Compute % change between the last two trading days ────────────────
    prev_close = close_px.iloc[-2]
    last_close = close_px.iloc[-1]
    trading_date = close_px.index[-1].date()

    pct_change = ((last_close - prev_close) / prev_close * 100).dropna()

    # Also pull volume for context
    volume = raw["Volume"].iloc[-1] if "Volume" in raw.columns else pd.Series(dtype=float)

    # ── 3. Build results DataFrame ───────────────────────────────────────────
    records = []
    for ticker in pct_change.index:
        records.append({
            "ticker":         str(ticker),
            "trading_date":   str(trading_date),
            "prev_close":     round(float(prev_close[ticker]), 4),
            "last_close":     round(float(last_close[ticker]), 4),
            "pct_change":     round(float(pct_change[ticker]), 4),
            "abs_pct_change": round(abs(float(pct_change[ticker])), 4),
            "direction":      "UP" if float(pct_change[ticker]) >= 0 else "DOWN",
            "volume":         int(volume[ticker]) if ticker in volume.index and not pd.isna(volume[ticker]) else None,
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
            f"  #{int(row['rank']):>2}  {row['ticker']:<6}  {sign} {row['pct_change']:>+7.2f}%"
            f"   close={row['last_close']:.2f}"
        )

    # ── 4. Write to Iceberg ──────────────────────────────────────────────────
    schema_cols = [
        "rank", "ticker", "trading_date", "prev_close", "last_close",
        "pct_change", "abs_pct_change", "direction", "volume", "ingested_at",
    ]
    sdf = spark.createDataFrame(pdf[schema_cols])
    sdf.writeTo("analytics.stock_movers").createOrReplace()

    row_count = sdf.count()
    logger.info(f"✓ Wrote {row_count} rows to analytics.stock_movers")
    return row_count
