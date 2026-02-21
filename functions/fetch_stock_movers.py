#!/usr/bin/env python3
"""
fetch_stock_movers.py

Downloads last-trading-day stock data from Yahoo Finance (via yfinance),
computes % change (close vs prior close), and writes the top movers
to analytics.stock_movers.

Uses per-ticker downloads with a short timeout and skips any that fail,
so a single network issue doesn't abort the entire run.
"""

import logging
from datetime import datetime, timedelta, date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ── Ticker universe (S&P 500 large-caps + popular names) ─────────────────────
TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM", "V",
    "XOM", "MA", "PG", "HD", "COST", "MRK", "CVX", "ABBV", "CRM", "AMD",
    "PEP", "NFLX", "ADBE", "KO", "AVGO", "ACN", "MCD", "WMT", "BAC", "CSCO",
    "ORCL", "DHR", "AMGN", "DIS", "TXN", "NKE", "PM", "MS", "GS",
    "HON", "UNP", "IBM", "INTU", "CAT", "BA", "ISRG", "BKNG", "AXP",
    "SBUX", "SYK", "BLK", "GILD", "PFE", "MDT", "C", "ZTS", "BMY",
    "MMC", "TJX", "REGN", "CI", "SO", "DUK", "WM", "CL", "F", "GM",
    "UBER", "PYPL", "COIN", "PLTR", "SNAP", "SPOT",
]


def _fetch_one(ticker: str, start_str: str, end_str: str):
    """Fetch history for a single ticker. Returns DataFrame or None."""
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        df = t.history(start=start_str, end=end_str, auto_adjust=True, timeout=20)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        logger.warning(f"  [{ticker}] failed: {e}")
    return None


def main(spark, top_n: str = "25"):
    """
    Fetch last-trading-day stock data and write top movers to analytics.stock_movers.

    Args:
        spark:  SparkSession (injected by platform)
        top_n:  Number of top movers to surface (default 25)
    """
    import pandas as pd

    top_n = int(top_n)

    conf = spark.sparkContext.getConf()
    execution_id = conf.get("spark.workflow.executionId", "local")
    environment  = conf.get("spark.workflow.environment", "development")

    logger.info(f"Execution ID : {execution_id}")
    logger.info(f"Environment  : {environment}")
    logger.info(f"Top-N movers : {top_n}")

    today = date.today()
    start_str = (today - timedelta(days=10)).strftime("%Y-%m-%d")
    end_str   = today.strftime("%Y-%m-%d")
    logger.info(f"Download window: {start_str} → {end_str}")

    # ── 1. Fetch each ticker individually ────────────────────────────────────
    records = []
    success, fail = 0, 0

    for ticker in TICKERS:
        df = _fetch_one(ticker, start_str, end_str)

        if df is None or len(df) < 2:
            logger.warning(f"  [{ticker}] skipped (insufficient data)")
            fail += 1
            continue

        prev_close = float(df["Close"].iloc[-2])
        last_close = float(df["Close"].iloc[-1])
        trading_date = str(df.index[-1].date())
        prev_date    = str(df.index[-2].date())

        if prev_close == 0:
            fail += 1
            continue

        pct = (last_close - prev_close) / prev_close * 100
        vol_val = df["Volume"].iloc[-1] if "Volume" in df.columns else None
        vol = int(vol_val) if vol_val is not None and not pd.isna(vol_val) else None

        records.append({
            "ticker":         ticker,
            "trading_date":   trading_date,
            "prev_date":      prev_date,
            "prev_close":     round(prev_close, 4),
            "last_close":     round(last_close, 4),
            "pct_change":     round(pct, 4),
            "abs_pct_change": round(abs(pct), 4),
            "direction":      "UP" if pct >= 0 else "DOWN",
            "volume":         vol,
            "ingested_at":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        success += 1
        logger.info(f"  [{ticker}] ✓  pct={pct:+.2f}%  close={last_close:.2f}")

    logger.info(f"Fetch complete: {success} tickers succeeded, {fail} failed/skipped")

    if not records:
        raise ValueError(
            "No stock data could be fetched. "
            "The execution environment may not have outbound internet access to Yahoo Finance."
        )

    # ── 2. Sort and rank ─────────────────────────────────────────────────────
    pdf = (
        pd.DataFrame(records)
        .sort_values("abs_pct_change", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
    pdf.insert(0, "rank", range(1, len(pdf) + 1))

    logger.info(f"\nTop {min(top_n, len(pdf))} movers on {pdf['trading_date'].iloc[0]}:")
    for _, row in pdf.iterrows():
        sign = "▲" if row["direction"] == "UP" else "▼"
        logger.info(
            f"  #{int(row['rank']):>2}  {row['ticker']:<6}  {sign} {float(row['pct_change']):>+7.2f}%"
            f"   close={float(row['last_close']):.2f}"
        )

    # ── 3. Write to Iceberg ──────────────────────────────────────────────────
    schema_cols = [
        "rank", "ticker", "trading_date", "prev_date", "prev_close", "last_close",
        "pct_change", "abs_pct_change", "direction", "volume", "ingested_at",
    ]
    sdf = spark.createDataFrame(pdf[schema_cols])
    sdf.writeTo("analytics.stock_movers").createOrReplace()

    row_count = sdf.count()
    logger.info(f"✓ Wrote {row_count} rows to analytics.stock_movers")
    return row_count
