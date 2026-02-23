#!/usr/bin/env python3
"""
fetch_weather_forecast.py

Fetches 7-day hourly weather forecasts for a set of cities using the
Open-Meteo API (free, no API key required) and writes the results to
analytics.weather_forecast.

API docs: https://open-meteo.com/en/docs
"""

import logging
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Cities to fetch forecasts for (name, latitude, longitude, timezone)
CITIES = [
    {"name": "New York",    "lat": 40.7128,  "lon": -74.0060, "tz": "America/New_York"},
    {"name": "Los Angeles", "lat": 34.0522,  "lon": -118.2437,"tz": "America/Los_Angeles"},
    {"name": "Chicago",     "lat": 41.8781,  "lon": -87.6298, "tz": "America/Chicago"},
    {"name": "London",      "lat": 51.5074,  "lon": -0.1278,  "tz": "Europe/London"},
    {"name": "Tokyo",       "lat": 35.6762,  "lon": 139.6503, "tz": "Asia/Tokyo"},
    {"name": "Sydney",      "lat": -33.8688, "lon": 151.2093, "tz": "Australia/Sydney"},
]

BASE_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_city(city: dict) -> list[dict]:
    """Fetch hourly forecast for a single city and return a list of row dicts."""
    params = {
        "latitude":            city["lat"],
        "longitude":           city["lon"],
        "hourly":              "temperature_2m,precipitation,windspeed_10m,relativehumidity_2m",
        "temperature_unit":    "fahrenheit",
        "windspeed_unit":      "mph",
        "precipitation_unit":  "inch",
        "timezone":            city["tz"],
        "forecast_days":       7,
    }

    resp = requests.get(BASE_URL, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    hourly     = data["hourly"]
    times      = hourly["time"]
    temps      = hourly["temperature_2m"]
    precip     = hourly["precipitation"]
    wind       = hourly["windspeed_10m"]
    humidity   = hourly["relativehumidity_2m"]
    ingested   = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    rows = []
    for i, ts in enumerate(times):
        rows.append({
            "city":              city["name"],
            "latitude":          city["lat"],
            "longitude":         city["lon"],
            "forecast_time":     ts,                          # "2025-01-01T00:00"
            "forecast_date":     ts[:10],                     # "2025-01-01"
            "temperature_f":     float(temps[i])    if temps[i]    is not None else None,
            "precipitation_in":  float(precip[i])   if precip[i]   is not None else None,
            "windspeed_mph":     float(wind[i])      if wind[i]     is not None else None,
            "humidity_pct":      float(humidity[i])  if humidity[i] is not None else None,
            "ingested_at":       ingested,
        })

    logger.info(f"  [{city['name']}] fetched {len(rows)} hourly records")
    return rows


def main(spark):
    """
    Fetch 7-day hourly forecasts for all cities in parallel and write
    to analytics.weather_forecast.
    """
    conf         = spark.sparkContext.getConf()
    execution_id = conf.get("spark.workflow.executionId", "local")
    environment  = conf.get("spark.workflow.environment", "development")

    logger.info(f"Execution ID : {execution_id}")
    logger.info(f"Environment  : {environment}")
    logger.info(f"Fetching forecasts for {len(CITIES)} cities via Open-Meteo API ...")

    # Fetch all cities concurrently
    all_rows = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(fetch_city, city): city["name"] for city in CITIES}
        for future, city_name in futures.items():
            try:
                rows = future.result()
                all_rows.extend(rows)
            except Exception as e:
                logger.error(f"  [{city_name}] fetch failed: {e}")

    if not all_rows:
        raise ValueError("No forecast data returned from Open-Meteo API.")

    logger.info(f"Total rows fetched: {len(all_rows)}")

    # Write to Iceberg
    sdf = spark.createDataFrame(all_rows)
    sdf.writeTo("analytics.weather_forecast").createOrReplace()

    row_count = sdf.count()
    logger.info(f"Wrote {row_count} rows to analytics.weather_forecast")
    return row_count
