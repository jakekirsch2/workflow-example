import logging
import os
import requests
from datetime import datetime, date

logger = logging.getLogger(__name__)

# ── Test guard: set to True for first run, False for full run ──────────────────
TEST_MODE = True
TEST_PAGE_LIMIT = 1  # Only fetch 1 page (~250 stocks) on test run
# ─────────────────────────────────────────────────────────────────────────────


def fetch_aa_yield(fred_api_key: str) -> tuple[float, str]:
    """Fetch the latest ICE BofA AA US Corporate Index Effective Yield from FRED."""
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": "BAMLC0A2CAAEY",
        "api_key": fred_api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 5,  # get last 5 obs so we can skip any "." missing values
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    observations = resp.json().get("observations", [])

    for obs in observations:
        if obs.get("value") not in (".", "", None):
            yield_pct = float(obs["value"])
            obs_date = obs["date"]
            logger.info(f"AA yield: {yield_pct}% as of {obs_date}")
            return yield_pct, obs_date

    raise ValueError("No valid AA yield observations found in FRED response")


def fetch_screener_page(fmp_api_key: str, page: int, market_cap_min: int = 2_000_000_000) -> list:
    """Fetch one page of NYSE+NASDAQ stocks from FMP screener (250 per page)."""
    url = "https://financialmodelingprep.com/api/v3/stock-screener"
    params = {
        "marketCapMoreThan": market_cap_min,
        "isActivelyTrading": "true",
        "exchange": "NYSE,NASDAQ",
        "limit": 250,
        "apikey": fmp_api_key,
        "page": page,
    }
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and "Error Message" in data:
        raise ValueError(f"FMP API error: {data['Error Message']}")
    return data if isinstance(data, list) else []


def fetch_all_screener_stocks(fmp_api_key: str) -> list:
    """Paginate through FMP stock screener, respecting TEST_MODE limit."""
    all_stocks = []
    page = 0
    while True:
        if TEST_MODE and page >= TEST_PAGE_LIMIT:
            logger.info(f"TEST_MODE: stopping after {TEST_PAGE_LIMIT} page(s)")
            break
        logger.info(f"Fetching screener page {page} ...")
        batch = fetch_screener_page(fmp_api_key, page)
        if not batch:
            logger.info(f"Empty page {page} — done paginating")
            break
        all_stocks.extend(batch)
        logger.info(f"  → {len(batch)} stocks on page {page}, total so far: {len(all_stocks)}")
        page += 1

    return all_stocks


def main(spark):
    fred_api_key = os.environ.get("FRED_API_KEY")
    fmp_api_key = os.environ.get("FMP_API_KEY")

    if not fred_api_key:
        raise EnvironmentError("FRED_API_KEY environment variable is not set")
    if not fmp_api_key:
        raise EnvironmentError("FMP_API_KEY environment variable is not set")

    # ── Step 1: Fetch AA bond yield from FRED ────────────────────────────────
    aa_yield_pct, yield_date = fetch_aa_yield(fred_api_key)
    aa_yield_decimal = aa_yield_pct / 100.0
    max_pe = 1.0 / aa_yield_decimal
    logger.info(f"AA yield: {aa_yield_pct:.4f}% → Graham max P/E: {max_pe:.2f}")

    # ── Step 2: Fetch stocks from FMP screener ───────────────────────────────
    all_stocks = fetch_all_screener_stocks(fmp_api_key)
    logger.info(f"Total raw stocks fetched: {len(all_stocks)}")

    if not all_stocks:
        raise ValueError("FMP screener returned no stocks — check API key or plan limits")

    # ── Step 3: Filter by Graham P/E criterion ───────────────────────────────
    qualified = []
    skipped_no_pe = 0
    skipped_negative_pe = 0
    skipped_high_pe = 0

    for stock in all_stocks:
        pe = stock.get("pe")
        if pe is None:
            skipped_no_pe += 1
            continue
        try:
            pe = float(pe)
        except (ValueError, TypeError):
            skipped_no_pe += 1
            continue
        if pe <= 0:
            skipped_negative_pe += 1
            continue
        if pe >= max_pe:
            skipped_high_pe += 1
            continue
        qualified.append(stock)

    logger.info(
        f"Filtering results: {len(qualified)} qualify | "
        f"{skipped_no_pe} no P/E | {skipped_negative_pe} negative P/E | "
        f"{skipped_high_pe} P/E too high (≥{max_pe:.2f})"
    )

    # ── Step 4: Build Spark DataFrame and save to Iceberg ───────────────────
    run_date = date.today().isoformat()

    rows = []
    for s in qualified:
        rows.append({
            "run_date":        run_date,
            "yield_date":      yield_date,
            "aa_yield_pct":    float(aa_yield_pct),
            "max_pe":          float(round(max_pe, 4)),
            "symbol":          str(s.get("symbol", "")),
            "company_name":    str(s.get("companyName", "")),
            "sector":          str(s.get("sector", "") or ""),
            "industry":        str(s.get("industry", "") or ""),
            "exchange":        str(s.get("exchangeShortName", "") or ""),
            "price":           float(s.get("price") or 0.0),
            "pe_ratio":        float(s.get("pe") or 0.0),
            "market_cap":      float(s.get("marketCap") or 0.0),
            "volume":          float(s.get("volume") or 0.0),
            "beta":            float(s.get("beta") or 0.0),
            "dividend_yield":  float(s.get("lastAnnualDividend") or 0.0),
            "country":         str(s.get("country", "") or ""),
            "is_test_run":     TEST_MODE,
        })

    if not rows:
        logger.warning("No stocks passed the Graham filter — writing empty result set")
        rows = [{
            "run_date": run_date, "yield_date": yield_date,
            "aa_yield_pct": float(aa_yield_pct), "max_pe": float(round(max_pe, 4)),
            "symbol": "", "company_name": "NO_RESULTS", "sector": "",
            "industry": "", "exchange": "", "price": 0.0, "pe_ratio": 0.0,
            "market_cap": 0.0, "volume": 0.0, "beta": 0.0,
            "dividend_yield": 0.0, "country": "", "is_test_run": TEST_MODE,
        }]

    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, BooleanType
    )

    schema = StructType([
        StructField("run_date",       StringType(),  True),
        StructField("yield_date",     StringType(),  True),
        StructField("aa_yield_pct",   DoubleType(),  True),
        StructField("max_pe",         DoubleType(),  True),
        StructField("symbol",         StringType(),  True),
        StructField("company_name",   StringType(),  True),
        StructField("sector",         StringType(),  True),
        StructField("industry",       StringType(),  True),
        StructField("exchange",       StringType(),  True),
        StructField("price",          DoubleType(),  True),
        StructField("pe_ratio",       DoubleType(),  True),
        StructField("market_cap",     DoubleType(),  True),
        StructField("volume",         DoubleType(),  True),
        StructField("beta",           DoubleType(),  True),
        StructField("dividend_yield", DoubleType(),  True),
        StructField("country",        StringType(),  True),
        StructField("is_test_run",    BooleanType(), True),
    ])

    df = spark.createDataFrame(rows, schema=schema)
    df.writeTo("analytics.graham_screener_results").createOrReplace()

    logger.info(f"Wrote {df.count()} qualifying stocks to analytics.graham_screener_results")
    logger.info(
        f"Summary — AA yield: {aa_yield_pct:.4f}% | Max P/E: {max_pe:.2f} | "
        f"Qualifying stocks: {len(qualified)} | Test mode: {TEST_MODE}"
    )
