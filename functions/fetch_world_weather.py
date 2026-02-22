#!/usr/bin/env python3
"""
fetch_world_weather.py
Pulls current weather conditions for a set of major world cities from the
Open-Meteo public API (no API key required) and writes the results to
analytics.world_weather as an Iceberg table.
"""

import logging
import requests
from datetime import date
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, IntegerType, DateType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ── City catalogue ────────────────────────────────────────────────────────────
CITIES = [
    {"city": "New York",     "country": "US",  "lat": 40.71,  "lon": -74.01},
    {"city": "London",       "country": "GB",  "lat": 51.51,  "lon": -0.13},
    {"city": "Tokyo",        "country": "JP",  "lat": 35.69,  "lon": 139.69},
    {"city": "Sydney",       "country": "AU",  "lat": -33.87, "lon": 151.21},
    {"city": "Paris",        "country": "FR",  "lat": 48.85,  "lon": 2.35},
    {"city": "Dubai",        "country": "AE",  "lat": 25.20,  "lon": 55.27},
    {"city": "São Paulo",    "country": "BR",  "lat": -23.55, "lon": -46.63},
    {"city": "Mumbai",       "country": "IN",  "lat": 19.08,  "lon": 72.88},
    {"city": "Cairo",        "country": "EG",  "lat": 30.06,  "lon": 31.25},
    {"city": "Toronto",      "country": "CA",  "lat": 43.65,  "lon": -79.38},
    {"city": "Mexico City",  "country": "MX",  "lat": 19.43,  "lon": -99.13},
    {"city": "Beijing",      "country": "CN",  "lat": 39.91,  "lon": 116.39},
    {"city": "Moscow",       "country": "RU",  "lat": 55.75,  "lon": 37.62},
    {"city": "Lagos",        "country": "NG",  "lat": 6.45,   "lon": 3.40},
    {"city": "Buenos Aires", "country": "AR",  "lat": -34.61, "lon": -58.38},
    {"city": "Seoul",        "country": "KR",  "lat": 37.57,  "lon": 126.98},
    {"city": "Jakarta",      "country": "ID",  "lat": -6.21,  "lon": 106.85},
    {"city": "Cape Town",    "country": "ZA",  "lat": -33.93, "lon": 18.42},
    {"city": "Nairobi",      "country": "KE",  "lat": -1.29,  "lon": 36.82},
    {"city": "Berlin",       "country": "DE",  "lat": 52.52,  "lon": 13.40},
]

# Open-Meteo WMO weather code → human-readable label
WMO_CODES = {
    0: "Clear Sky", 1: "Mainly Clear", 2: "Partly Cloudy", 3: "Overcast",
    45: "Foggy", 48: "Icy Fog",
    51: "Light Drizzle", 53: "Moderate Drizzle", 55: "Dense Drizzle",
    61: "Slight Rain", 63: "Moderate Rain", 65: "Heavy Rain",
    71: "Slight Snow", 73: "Moderate Snow", 75: "Heavy Snow",
    80: "Slight Showers", 81: "Moderate Showers", 82: "Violent Showers",
    95: "Thunderstorm", 96: "Thunderstorm w/ Hail", 99: "Thunderstorm w/ Heavy Hail",
}

BASE_URL = "https://api.open-meteo.com/v1/forecast"

SCHEMA = StructType([
    StructField("city",             StringType(),  False),
    StructField("country",          StringType(),  False),
    StructField("lat",              FloatType(),   False),
    StructField("lon",              FloatType(),   False),
    StructField("temp_c",           FloatType(),   True),
    StructField("feels_like_c",     FloatType(),   True),
    StructField("humidity_pct",     IntegerType(), True),
    StructField("wind_speed_kmh",   FloatType(),   True),
    StructField("wind_direction",   IntegerType(), True),
    StructField("precipitation_mm", FloatType(),   True),
    StructField("weather_code",     IntegerType(), True),
    StructField("weather_desc",     StringType(),  True),
    StructField("fetched_date",     DateType(),    False),
])


def _fetch_city(city_meta: dict) -> dict:
    """Call Open-Meteo for one city and return a flat record."""
    params = {
        "latitude":            city_meta["lat"],
        "longitude":           city_meta["lon"],
        "current":             [
            "temperature_2m",
            "apparent_temperature",
            "relative_humidity_2m",
            "wind_speed_10m",
            "wind_direction_10m",
            "precipitation",
            "weather_code",
        ],
        "wind_speed_unit":     "kmh",
        "timezone":            "UTC",
        "forecast_days":       1,
    }
    resp = requests.get(BASE_URL, params=params, timeout=15)
    resp.raise_for_status()
    cur = resp.json()["current"]

    wcode = int(cur.get("weather_code", 0) or 0)
    return {
        "city":             city_meta["city"],
        "country":          city_meta["country"],
        "lat":              float(city_meta["lat"]),
        "lon":              float(city_meta["lon"]),
        "temp_c":           float(cur.get("temperature_2m") or 0),
        "feels_like_c":     float(cur.get("apparent_temperature") or 0),
        "humidity_pct":     int(cur.get("relative_humidity_2m") or 0),
        "wind_speed_kmh":   float(cur.get("wind_speed_10m") or 0),
        "wind_direction":   int(cur.get("wind_direction_10m") or 0),
        "precipitation_mm": float(cur.get("precipitation") or 0),
        "weather_code":     wcode,
        "weather_desc":     WMO_CODES.get(wcode, f"Code {wcode}"),
        "fetched_date":     date.today(),
    }


def main(spark):
    """Fetch weather for all cities and persist to analytics.world_weather."""
    logger.info(f"Fetching weather for {len(CITIES)} cities via Open-Meteo …")

    records = []
    for city_meta in CITIES:
        try:
            record = _fetch_city(city_meta)
            records.append(record)
            logger.info(
                f"  {city_meta['city']:15s} → {record['temp_c']:.1f}°C  "
                f"{record['weather_desc']}"
            )
        except Exception as exc:
            logger.warning(f"  {city_meta['city']} – skipped ({exc})")

    if not records:
        raise RuntimeError("No weather records fetched – aborting.")

    df = spark.createDataFrame(records, schema=SCHEMA)
    df.writeTo("analytics.world_weather").createOrReplace()

    logger.info(f"Wrote {df.count()} rows to analytics.world_weather")
