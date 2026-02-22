#!/usr/bin/env python3
"""
fetch_world_weather.py
Pulls recent weather conditions for 20 major world cities from the
NOAA GSOD BigQuery public dataset (no outbound HTTP required) and writes
the results to analytics.world_weather as an Iceberg table.

Station IDs sourced from bigquery-public-data.noaa_gsod.stations.
Data is pulled from the most recent available date across all stations.
"""

import logging
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, IntegerType, DateType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ── City → NOAA GSOD station mapping ─────────────────────────────────────────
# usaf / wban uniquely identify each station in bigquery-public-data.noaa_gsod
CITIES = [
    {"city": "New York",     "country": "US", "usaf": "744860", "wban": "94789"},
    {"city": "London",       "country": "GB", "usaf": "037720", "wban": "99999"},
    {"city": "Tokyo",        "country": "JP", "usaf": "476710", "wban": "99999"},
    {"city": "Sydney",       "country": "AU", "usaf": "947675", "wban": "99999"},
    {"city": "Paris",        "country": "FR", "usaf": "071570", "wban": "99999"},
    {"city": "Dubai",        "country": "AE", "usaf": "411940", "wban": "99999"},
    {"city": "São Paulo",    "country": "BR", "usaf": "837810", "wban": "99999"},
    {"city": "Mumbai",       "country": "IN", "usaf": "430020", "wban": "99999"},
    {"city": "Cairo",        "country": "EG", "usaf": "623660", "wban": "99999"},
    {"city": "Toronto",      "country": "CA", "usaf": "726240", "wban": "99999"},
    {"city": "Mexico City",  "country": "MX", "usaf": "766793", "wban": "99999"},
    {"city": "Beijing",      "country": "CN", "usaf": "545110", "wban": "99999"},
    {"city": "Moscow",       "country": "RU", "usaf": "275243", "wban": "99999"},
    {"city": "Lagos",        "country": "NG", "usaf": "652030", "wban": "99999"},
    {"city": "Buenos Aires", "country": "AR", "usaf": "875850", "wban": "99999"},
    {"city": "Seoul",        "country": "KR", "usaf": "471080", "wban": "99999"},
    {"city": "Jakarta",      "country": "ID", "usaf": "967410", "wban": "99999"},
    {"city": "Cape Town",    "country": "ZA", "usaf": "688160", "wban": "99999"},
    {"city": "Nairobi",      "country": "KE", "usaf": "637400", "wban": "99999"},
    {"city": "Berlin",       "country": "DE", "usaf": "103890", "wban": "99999"},
]

# NOAA GSOD temperature / dew-point are in °F; wind in knots; precip in inches
# 999.9 / 9999.9 are NOAA sentinel "missing" values

OUTPUT_SCHEMA = StructType([
    StructField("city",             StringType(),  False),
    StructField("country",          StringType(),  False),
    StructField("usaf",             StringType(),  False),
    StructField("temp_c",           FloatType(),   True),
    StructField("feels_like_c",     FloatType(),   True),   # derived from dewpoint
    StructField("humidity_pct",     IntegerType(), True),   # derived from temp + dewpoint
    StructField("wind_speed_kmh",   FloatType(),   True),
    StructField("precipitation_mm", FloatType(),   True),
    StructField("weather_desc",     StringType(),  True),
    StructField("fetched_date",     DateType(),    False),
])


def _f_to_c(f):
    """Fahrenheit → Celsius, returns None for NOAA missing sentinel (999.9)."""
    if f is None or f >= 999.9:
        return None
    return round((f - 32.0) * 5.0 / 9.0, 1)


def _knots_to_kmh(k):
    """Knots → km/h, returns None for sentinel (999.9)."""
    if k is None or k >= 999.9:
        return None
    return round(k * 1.852, 1)


def _in_to_mm(inches):
    """Inches → mm; 99.99 is NOAA trace/missing sentinel."""
    if inches is None or inches >= 99.99:
        return 0.0
    return round(inches * 25.4, 1)


def _humidity(temp_c, dewp_c):
    """Approximate relative humidity (%) from temperature and dew-point (°C)."""
    if temp_c is None or dewp_c is None:
        return None
    # Magnus formula approximation
    import math
    rh = 100.0 * math.exp((17.625 * dewp_c) / (243.04 + dewp_c)) / \
         math.exp((17.625 * temp_c) / (243.04 + temp_c))
    return max(0, min(100, round(rh)))


def _weather_desc(rain, snow, thunder, fog):
    """Build a human-readable condition string from NOAA indicator flags."""
    flags = []
    if thunder == "1":
        flags.append("Thunderstorm")
    if snow == "1":
        flags.append("Snow")
    if rain == "1":
        flags.append("Rain")
    if fog == "1":
        flags.append("Fog")
    return ", ".join(flags) if flags else "Clear / Partly Cloudy"


def main(spark):
    """Fetch NOAA GSOD weather for all cities and persist to analytics.world_weather."""

    usaf_list = [c["usaf"] for c in CITIES]
    wban_list  = [c["wban"] for c in CITIES]

    # ── 1. Read from NOAA GSOD 2025 (BigQuery public dataset) ────────────────
    logger.info("Reading NOAA GSOD 2025 from BigQuery public dataset …")
    gsod = (
        spark.read.format("bigquery")
        .option("table", "bigquery-public-data.noaa_gsod.gsod2025")
        .load()
        .filter(F.col("stn").isin(usaf_list))
    )

    # Pick the most recent date that has data for our stations
    max_date_row = gsod.agg(F.max("date").alias("max_date")).collect()[0]
    max_date = max_date_row["max_date"]
    logger.info(f"Most recent GSOD date available: {max_date}")

    # For each station keep only the latest available date (may vary by station)
    window = __import__("pyspark.sql.window", fromlist=["Window"]).Window \
        .partitionBy("stn", "wban") \
        .orderBy(F.col("date").desc())

    latest = (
        gsod
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select("stn", "wban", "date", "temp", "dewp", "wdsp",
                "prcp", "rain_drizzle", "snow_ice_pellets", "thunder", "fog")
        .collect()
    )

    # Index by (usaf, wban) for fast lookup
    gsod_map = {(r["stn"], r["wban"]): r for r in latest}
    logger.info(f"Station rows fetched: {len(gsod_map)}")

    # ── 2. Build output records ───────────────────────────────────────────────
    records = []
    for meta in CITIES:
        key = (meta["usaf"], meta["wban"])
        row = gsod_map.get(key)
        if row is None:
            logger.warning(f"  {meta['city']:15s} – no data found, skipping")
            continue

        temp_c    = _f_to_c(float(row["temp"]))  if row["temp"]  else None
        dewp_c    = _f_to_c(float(row["dewp"]))  if row["dewp"]  else None
        wind_kmh  = _knots_to_kmh(float(row["wdsp"])) if row["wdsp"] else None
        prcp_mm   = _in_to_mm(float(row["prcp"])) if row["prcp"] else 0.0
        humidity  = _humidity(temp_c, dewp_c)
        # Feels-like: simplified — dew-point offset as proxy
        feels_c   = round(temp_c + (dewp_c - temp_c) * 0.15, 1) \
                    if temp_c is not None and dewp_c is not None else temp_c
        desc      = _weather_desc(row["rain_drizzle"], row["snow_ice_pellets"],
                                  row["thunder"], row["fog"])

        from datetime import date as date_cls
        record = {
            "city":             meta["city"],
            "country":          meta["country"],
            "usaf":             meta["usaf"],
            "temp_c":           float(temp_c) if temp_c is not None else None,
            "feels_like_c":     float(feels_c) if feels_c is not None else None,
            "humidity_pct":     int(humidity) if humidity is not None else None,
            "wind_speed_kmh":   float(wind_kmh) if wind_kmh is not None else None,
            "precipitation_mm": float(prcp_mm),
            "weather_desc":     desc,
            "fetched_date":     date_cls.fromisoformat(str(row["date"])),
        }
        records.append(record)
        logger.info(
            f"  {meta['city']:15s} → {record['temp_c']}°C  "
            f"humidity {record['humidity_pct']}%  {desc}  ({row['date']})"
        )

    if not records:
        raise RuntimeError("No weather records fetched – aborting.")

    # ── 3. Write to Iceberg ───────────────────────────────────────────────────
    df = spark.createDataFrame(records, schema=OUTPUT_SCHEMA)
    df.writeTo("analytics.world_weather").createOrReplace()
    logger.info(f"Wrote {df.count()} rows to analytics.world_weather")
