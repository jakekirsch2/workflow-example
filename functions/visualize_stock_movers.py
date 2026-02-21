#!/usr/bin/env python3
"""
visualize_stock_movers.py
Reads analytics.stock_movers and returns a multi-chart viz spec:
  - Metric cards (top gainer, top loser, total movers)
  - Bar chart of % change for all movers
  - Table with full detail
"""

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main(spark):
    """
    Read analytics.stock_movers and publish a dashboard.
    """
    logger.info("Reading analytics.stock_movers …")
    df = spark.table("analytics.stock_movers")
    rows = df.orderBy("rank").collect()

    if not rows:
        raise ValueError("analytics.stock_movers is empty – run fetch_stock_movers first.")

    trading_date = rows[0]["trading_date"]
    logger.info(f"Building dashboard for trading date: {trading_date}")

    # ── Derived stats ────────────────────────────────────────────────────────
    gainers = [r for r in rows if r["direction"] == "UP"]
    losers  = [r for r in rows if r["direction"] == "DOWN"]

    top_gainer = max(rows, key=lambda r: r["pct_change"])
    top_loser  = min(rows, key=lambda r: r["pct_change"])

    # ── Chart data ───────────────────────────────────────────────────────────
    bar_data = [
        {
            "ticker":     r["ticker"],
            "pct_change": float(r["pct_change"]),
        }
        for r in rows
    ]

    table_data = [
        {
            "rank":        int(r["rank"]),
            "ticker":      r["ticker"],
            "direction":   r["direction"],
            "prev_close":  float(r["prev_close"]),
            "last_close":  float(r["last_close"]),
            "pct_change":  f"{float(r['pct_change']):+.2f}%",
            "volume":      int(r["volume"]) if r["volume"] else 0,
        }
        for r in rows
    ]

    # ── Viz spec ─────────────────────────────────────────────────────────────
    charts = [
        # --- Metric cards ---
        {
            "type":  "metric",
            "title": f"Trading Date",
            "value": trading_date,
        },
        {
            "type":  "metric",
            "title": "Top Gainer",
            "value": f"{top_gainer['ticker']}  {float(top_gainer['pct_change']):+.2f}%",
        },
        {
            "type":  "metric",
            "title": "Top Loser",
            "value": f"{top_loser['ticker']}  {float(top_loser['pct_change']):+.2f}%",
        },
        {
            "type":  "metric",
            "title": "Gainers / Losers",
            "value": f"{len(gainers)} ▲ / {len(losers)} ▼",
        },
        # --- Bar chart ---
        {
            "type":  "bar",
            "title": f"Top Movers by % Change — {trading_date}",
            "data":  bar_data,
            "xKey":  "ticker",
            "series": [
                {
                    "key":   "pct_change",
                    "label": "% Change",
                    "color": "#3b82f6",
                }
            ],
        },
        # --- Detail table ---
        {
            "type":    "table",
            "title":   "Full Movers Breakdown",
            "columns": ["rank", "ticker", "direction", "prev_close", "last_close", "pct_change", "volume"],
            "data":    table_data,
        },
    ]

    logger.info(f"Dashboard built: {len(charts)} charts, {len(rows)} tickers displayed.")
    return charts
