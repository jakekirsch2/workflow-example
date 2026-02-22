#!/usr/bin/env python3
"""
visualize_world_weather.py
Reads analytics.world_weather and returns a multi-part dashboard:
  1. Metric cards  – hottest city, coldest city, most humid, windiest, avg temp
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
    """Read analytics.world_weather and publish a multi-part dashboard."""
    logger.info("Reading analytics.world_weather …")
    df = spark.table("analytics.world_weather")
    rows = df.orderBy("temp_c", ascending=False).collect()

    if not rows:
        raise ValueError("analytics.world_weather is empty – run fetch first.")

    fetched_date = str(rows[0]["fetched_date"])
    logger.info(f"Building dashboard for {len(rows)} cities  (date: {fetched_date})")

    # ── Derived stats ─────────────────────────────────────────────────────────
    valid_temp  = [r for r in rows if r["temp_c"] is not None]
    valid_humid = [r for r in rows if r["humidity_pct"] is not None]
    valid_wind  = [r for r in rows if r["wind_speed_kmh"] is not None]

    hottest  = valid_temp[0]  if valid_temp  else rows[0]
    coldest  = valid_temp[-1] if valid_temp  else rows[-1]
    humid    = max(valid_humid, key=lambda r: r["humidity_pct"]) if valid_humid else rows[0]
    windiest = max(valid_wind,  key=lambda r: r["wind_speed_kmh"]) if valid_wind else rows[0]
    avg_temp = sum(float(r["temp_c"]) for r in valid_temp) / len(valid_temp) if valid_temp else 0.0

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
            "temp_c":       round(float(r["temp_c"]), 1) if r["temp_c"] is not None else 0.0,
            "feels_like_c": round(float(r["feels_like_c"]), 1) if r["feels_like_c"] is not None else 0.0,
        }
        for r in sorted(valid_temp, key=lambda r: float(r["temp_c"]), reverse=True)
    ]

    bar_chart = {
        "type":  "bar",
        "title": f"🌡️ Temperature by City — {fetched_date}",
        "data":  bar_data,
        "xKey":  "city",
        "series": [
            {"key": "temp_c",       "label": "Actual Temp (°C)",  "color": "#f97316"},
            {"key": "feels_like_c", "label": "Feels Like (°C)",   "color": "#3b82f6"},
        ],
    }

    # ── Part 3 – Detail table ─────────────────────────────────────────────────
    table_data = [
        {
            "city":             r["city"],
            "country":          r["country"],
            "temp_c":           f"{float(r['temp_c']):.1f}°C"   if r["temp_c"]          is not None else "N/A",
            "feels_like_c":     f"{float(r['feels_like_c']):.1f}°C" if r["feels_like_c"] is not None else "N/A",
            "humidity":         f"{int(r['humidity_pct'])}%"    if r["humidity_pct"]    is not None else "N/A",
            "wind_km_h":        f"{float(r['wind_speed_kmh']):.1f}" if r["wind_speed_kmh"] is not None else "N/A",
            "precipitation_mm": f"{float(r['precipitation_mm']):.1f}" if r["precipitation_mm"] is not None else "0.0",
            "conditions":       r["weather_desc"] or "N/A",
            "data_date":        str(r["fetched_date"]),
        }
        for r in sorted(rows, key=lambda r: float(r["temp_c"]) if r["temp_c"] is not None else -999, reverse=True)
    ]

    detail_table = {
        "type":    "table",
        "title":   "🗺️ Full City Weather Breakdown",
        "columns": [
            "city", "country", "temp_c", "feels_like_c",
            "humidity", "wind_km_h", "precipitation_mm", "conditions", "data_date",
        ],
        "data": table_data,
    }

    # ── Assemble dashboard ────────────────────────────────────────────────────
    dashboard = [*metrics, bar_chart, detail_table]

    logger.info(f"Dashboard ready: {len(metrics)} metric cards + bar chart + table")
    return dashboard
