#!/usr/bin/env python3
"""
visualize_weather_forecast.py

Reads analytics.weather_forecast and produces a multi-chart dashboard:
  1. Metric cards  — current temperature per city
  2. Line chart    — daily avg temperature trend per city (7 days)
  3. Bar chart     — total daily precipitation per city
  4. Table         — full daily summary
"""

import logging
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Consistent colours per city
CITY_COLORS = {
    "New York":    "#3b82f6",
    "Los Angeles": "#f59e0b",
    "Chicago":     "#10b981",
    "London":      "#8b5cf6",
    "Tokyo":       "#ef4444",
    "Sydney":      "#06b6d4",
}


def main(spark):
    """Read analytics.weather_forecast and return a dashboard spec."""
    logger.info("Reading analytics.weather_forecast ...")
    df = spark.table("analytics.weather_forecast")
    rows = df.collect()
    logger.info(f"Loaded {len(rows)} rows")

    # ── Aggregate to daily per city ──────────────────────────────────────────
    # Structure: daily[city][date] = {temps, precip, wind, humidity}
    daily = defaultdict(lambda: defaultdict(lambda: {
        "temps": [], "precip": [], "wind": [], "humidity": []
    }))

    for r in rows:
        city = r["city"]
        date = r["forecast_date"]
        if r["temperature_f"]   is not None: daily[city][date]["temps"].append(r["temperature_f"])
        if r["precipitation_in"] is not None: daily[city][date]["precip"].append(r["precipitation_in"])
        if r["windspeed_mph"]    is not None: daily[city][date]["wind"].append(r["windspeed_mph"])
        if r["humidity_pct"]     is not None: daily[city][date]["humidity"].append(r["humidity_pct"])

    cities     = sorted(daily.keys())
    all_dates  = sorted({date for city_data in daily.values() for date in city_data})

    def avg(lst):
        return round(sum(lst) / len(lst), 1) if lst else None

    def total(lst):
        return round(sum(lst), 3) if lst else 0.0

    # ── 1. Metric cards — today's average temperature per city ───────────────
    today = all_dates[0] if all_dates else None
    metric_charts = []
    for city in cities:
        temp_today = avg(daily[city].get(today, {}).get("temps", []))
        if temp_today is not None:
            metric_charts.append({
                "type":  "metric",
                "title": f"{city} — Today's Avg Temp",
                "value": temp_today,
                "unit":  "°F",
            })

    # ── 2. Line chart — daily avg temperature trend per city ────────────────
    temp_trend_data = []
    for date in all_dates:
        point = {"date": date}
        for city in cities:
            val = avg(daily[city].get(date, {}).get("temps", []))
            point[city] = val
        temp_trend_data.append(point)

    temp_series = [
        {"key": city, "label": city, "color": CITY_COLORS.get(city, "#6b7280")}
        for city in cities
    ]

    temp_line_chart = {
        "type":   "line",
        "title":  "7-Day Daily Avg Temperature by City (°F)",
        "data":   temp_trend_data,
        "xKey":   "date",
        "series": temp_series,
    }

    # ── 3. Bar chart — total daily precipitation per city ───────────────────
    precip_data = []
    for date in all_dates:
        point = {"date": date}
        for city in cities:
            point[city] = total(daily[city].get(date, {}).get("precip", []))
        precip_data.append(point)

    precip_series = [
        {"key": city, "label": city, "color": CITY_COLORS.get(city, "#6b7280")}
        for city in cities
    ]

    precip_bar_chart = {
        "type":   "bar",
        "title":  "7-Day Daily Total Precipitation by City (inches)",
        "data":   precip_data,
        "xKey":   "date",
        "series": precip_series,
    }

    # ── 4. Summary table — daily avg per city ───────────────────────────────
    table_rows = []
    for city in cities:
        for date in all_dates:
            day = daily[city].get(date, {})
            table_rows.append({
                "city":            city,
                "date":            date,
                "avg_temp_f":      avg(day.get("temps", [])),
                "total_precip_in": total(day.get("precip", [])),
                "avg_wind_mph":    avg(day.get("wind", [])),
                "avg_humidity_pct": avg(day.get("humidity", [])),
            })

    summary_table = {
        "type":    "table",
        "title":   "Daily Weather Summary",
        "columns": ["city", "date", "avg_temp_f", "total_precip_in", "avg_wind_mph", "avg_humidity_pct"],
        "data":    table_rows,
    }

    logger.info("Dashboard spec built successfully")

    return metric_charts + [temp_line_chart, precip_bar_chart, summary_table]
