#!/usr/bin/env python3
"""
visualize_world_weather.py
Reads analytics.world_weather and returns a 3-part dashboard:
  1. Metric cards  – hottest city, coldest city, highest humidity
  2. Bar chart     – temperature comparison across all cities
  3. Table         – full weather detail for every city
"""

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main(spark):
    """Read analytics.world_weather and publish a 3-part dashboard."""
    logger.info("Reading analytics.world_weather …")
    df = spark.table("analytics.world_weather")
    rows = df.orderBy("temp_c", ascending=False).collect()

    if not rows:
        raise ValueError("analytics.world_weather is empty – run fetch first.")

    fetched_date = str(rows[0]["fetched_date"])
    logger.info(f"Building dashboard for {len(rows)} cities  (date: {fetched_date})")

    # ── Derived stats ─────────────────────────────────────────────────────────
    hottest  = rows[0]                    # already sorted desc by temp
    coldest  = rows[-1]
    humid    = max(rows, key=lambda r: r["humidity_pct"])
    windiest = max(rows, key=lambda r: r["wind_speed_kmh"])

    avg_temp = sum(float(r["temp_c"]) for r in rows) / len(rows)

    # ── Part 1 – Metric cards ─────────────────────────────────────────────────
    metrics = [
        {
            "type":  "metric",
            "title": "🌡️ Hottest City",
            "value": f"{hottest['city']}  {float(hottest['temp_c']):.1f}°C",
        },
        {
            "type":  "metric",
            "title": "🥶 Coldest City",
            "value": f"{coldest['city']}  {float(coldest['temp_c']):.1f}°C",
        },
        {
            "type":  "metric",
            "title": "💧 Most Humid",
            "value": f"{humid['city']}  {int(humid['humidity_pct'])}%",
        },
        {
            "type":  "metric",
            "title": "💨 Windiest City",
            "value": f"{windiest['city']}  {float(windiest['wind_speed_kmh']):.1f} km/h",
        },
        {
            "type":  "metric",
            "title": "🌍 Avg Temperature",
            "value": f"{avg_temp:.1f}°C",
            "unit":  "across all cities",
        },
    ]

    # ── Part 2 – Bar chart: temperature by city ───────────────────────────────
    bar_data = [
        {
            "city":         r["city"],
            "temp_c":       round(float(r["temp_c"]), 1),
            "feels_like_c": round(float(r["feels_like_c"]), 1),
        }
        for r in sorted(rows, key=lambda r: float(r["temp_c"]), reverse=True)
    ]

    bar_chart = {
        "type":  "bar",
        "title": f"🌡️ Temperature by City — {fetched_date}",
        "data":  bar_data,
        "xKey":  "city",
        "series": [
            {"key": "temp_c",       "label": "Actual Temp (°C)",    "color": "#f97316"},
            {"key": "feels_like_c", "label": "Feels Like (°C)", "color": "#3b82f6"},
        ],
    }

    # ── Part 3 – Detail table ─────────────────────────────────────────────────
    table_data = [
        {
            "city":             r["city"],
            "country":          r["country"],
            "temp_c":           f"{float(r['temp_c']):.1f}°C",
            "feels_like_c":     f"{float(r['feels_like_c']):.1f}°C",
            "humidity":         f"{int(r['humidity_pct'])}%",
            "wind_km_h":        f"{float(r['wind_speed_kmh']):.1f}",
            "precipitation_mm": f"{float(r['precipitation_mm']):.1f}",
            "conditions":       r["weather_desc"],
        }
        for r in sorted(rows, key=lambda r: float(r["temp_c"]), reverse=True)
    ]

    detail_table = {
        "type":    "table",
        "title":   "🗺️ Full City Weather Breakdown",
        "columns": [
            "city", "country", "temp_c", "feels_like_c",
            "humidity", "wind_km_h", "precipitation_mm", "conditions",
        ],
        "data": table_data,
    }

    # ── Assemble dashboard ────────────────────────────────────────────────────
    dashboard = [*metrics, bar_chart, detail_table]

    logger.info(f"Dashboard ready: {len(metrics)} metric cards + bar chart + table")
    return dashboard
