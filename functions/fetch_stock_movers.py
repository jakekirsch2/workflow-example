#!/usr/bin/env python3
"""
fetch_stock_movers.py

Downloads last-trading-day stock data from Yahoo Finance (via yfinance),
computes % change (close vs prior close), and writes the top movers
to analytics.stock_movers.
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


def main(spark, top_n: str = "25"):
    """
    Fetch last-trading-day stock data and write top movers to analytics.stock_movers.

    Args:
        spark:  SparkSession (injected by platform)
        top_n:  Number of top movers to surface (default 25)
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
    logger.info(f"yfinance version: {yf.__version__}")

    today = date.today()
    start_str = (today - timedelta(days=10)).strftime("%Y-%m-%d")
    end_str   = today.strftime("%Y-%m-%d")
    logger.info(f"Download window: {start_str} -> {end_str}")

    # ── 1. Bulk-fetch all tickers ────────────────────────────────────────────
    logger.info(f"Fetching {len(TICKERS)} tickers from Yahoo Finance ...")
    tickers_str = " ".join(TICKERS)

    raw = yf.download(
        tickers=tickers_str,
        start=start_str,
        end=end_str,
        auto_adjust=True,
        progress=False,
        threads=True,
        timeout=30,
    )

    if raw is None or raw.empty:
        raise ValueError("yfinance returned no data. Network may be unavailable.")

    logger.info(f"Raw data shape: {raw.shape}")
    logger.info(f"Raw columns (first 10): {list(raw.columns)[:10]}")

    # ── 2. Extract Close and Volume columns ──────────────────────────────────
    # yfinance >= 0.2 returns MultiIndex columns: (field, ticker)
    # Check whether we have a MultiIndex
    if isinstance(raw.columns, pd.MultiIndex):
        close_df  = raw["Close"]
        volume_df = raw["Volume"]
    else:
        # Single ticker fallback (shouldn't happen for multiple tickers)
        close_df  = raw[["Close"]].rename(columns={"Close": TICKERS[0]})
        volume_df = raw[["Volume"]].rename(columns={"Volume": TICKERS[0]})

    # Drop fully-NaN rows (non-trading days)
    close_df  = close_df.dropna(how="all")
    volume_df = volume_df.dropna(how="all")

    logger.info(f"Trading days found: {len(close_df)}")

    if len(close_df) < 2:
        raise ValueError(
            f"Not enough trading days in window (got {len(close_df)}). "
            "Try widening the date range."
        )

    last_date = close_df.index[-1]
    prev_date = close_df.index[-2]
    trading_date_str = str(last_date.date())
    prev_date_str    = str(prev_date.date())
    logger.info(f"Using trading dates: prev={prev_date_str}, last={trading_date_str}")

    # ── 3. Compute % change per ticker ───────────────────────────────────────
    records = []
    success, fail = 0, 0

    for ticker in TICKERS:
        if ticker not in close_df.columns:
            logger.warning(f"  [{ticker}] not in response — skipped")
            fail += 1
            continue

        prev_close = close_df.loc[prev_date, ticker]
        last_close = close_df.loc[last_date, ticker]

        if pd.isna(prev_close) or pd.isna(last_close) or float(prev_close) == 0:
            logger.warning(f"  [{ticker}] invalid close prices — skipped")
            fail += 1
            continue

        prev_close = float(prev_close)
        last_close = float(last_close)
        pct = (last_close - prev_close) / prev_close * 100

        vol = None
        if ticker in volume_df.columns:
            v = volume_df.loc[last_date, ticker]
            if not pd.isna(v):
                vol = int(v)

        records.append({
            "ticker":         ticker,
            "trading_date":   trading_date_str,
            "prev_date":      prev_date_str,
            "prev_close":     round(prev_close, 4),
            "last_close":     round(last_close, 4),
            "pct_change":     round(pct, 4),
            "abs_pct_change": round(abs(pct), 4),
            "direction":      "UP" if pct >= 0 else "DOWN",
            "volume":         vol,
            "ingested_at":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        success += 1

    logger.info(f"Fetch complete: {success} tickers succeeded, {fail} failed/skipped")

    if not records:
        raise ValueError(
            "No stock data could be fetched. "
            "The execution environment may not have outbound internet access to Yahoo Finance."
        )

    # ── 4. Sort and rank ─────────────────────────────────────────────────────
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

    # ── 5. Write to Iceberg ──────────────────────────────────────────────────
    schema_cols = [
        "rank", "ticker", "trading_date", "prev_date", "prev_close", "last_close",
        "pct_change", "abs_pct_change", "direction", "volume", "ingested_at",
    ]
    sdf = spark.createDataFrame(pdf[schema_cols])
    sdf.writeTo("analytics.stock_movers").createOrReplace()

    row_count = sdf.count()
    logger.info(f"Wrote {row_count} rows to analytics.stock_movers")
    return row_count
