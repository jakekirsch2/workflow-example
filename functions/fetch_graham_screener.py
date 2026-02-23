import logging
import os
import requests
from datetime import date

logger = logging.getLogger(__name__)

# ── Test guard ─────────────────────────────────────────────────────────────────
# On the first test run we fetch only 1 page (250 stocks) via the screener.
# Set to None to paginate through all pages.
TEST_PAGE_LIMIT = 1  # set to None for full run
# ─────────────────────────────────────────────────────────────────────────────

MIN_MARKET_CAP = 2_000_000_000   # $2B minimum market cap
PAGE_SIZE      = 250             # FMP screener max per page


def fetch_aa_yield(fred_api_key: str) -> tuple:
    """Fetch the latest ICE BofA AA US Corporate Index Effective Yield from FRED."""
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": "BAMLC0A2CAAEY",
        "api_key": fred_api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 5,
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


def fetch_screener_page(fmp_api_key: str, offset: int) -> list:
    """
    Fetch one page of large-cap NYSE+NASDAQ stocks via the free stock-screener endpoint.
    Returns a list of stock dicts, or an empty list if there are no more results.
    """
    url = "https://financialmodelingprep.com/api/v3/stock-screener"
    params = {
        "exchange":         "NYSE,NASDAQ",
        "marketCapMoreThan": int(MIN_MARKET_CAP),
        "country":          "US",
        "limit":            PAGE_SIZE,
        "offset":           offset,
        "apikey":           fmp_api_key,
    }
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and "Error Message" in data:
        raise ValueError(f"FMP API error: {data['Error Message']}")
    if not isinstance(data, list):
        raise ValueError(f"Unexpected screener response type: {type(data)}")
    return data


def fetch_all_screener_stocks(fmp_api_key: str) -> list:
    """
    Paginate through the FMP stock screener to fetch all large-cap US stocks.
    Respects TEST_PAGE_LIMIT if set.
    """
    all_stocks = []
    page = 0
    while True:
        offset = page * PAGE_SIZE
        logger.info(f"Fetching screener page {page} (offset={offset}) ...")
        batch = fetch_screener_page(fmp_api_key, offset)
        if not batch:
            logger.info(f"No more results at page {page}. Done.")
            break
        all_stocks.extend(batch)
        logger.info(f"  → {len(batch)} stocks (total so far: {len(all_stocks)})")
        page += 1
        if TEST_PAGE_LIMIT is not None and page >= TEST_PAGE_LIMIT:
            logger.info(f"TEST_MODE: stopping after {TEST_PAGE_LIMIT} page(s)")
            break
    return all_stocks


def main(spark):
    fred_api_key = os.environ.get("FRED_API_KEY")
    fmp_api_key  = os.environ.get("FMP_API_KEY")

    if not fred_api_key:
        raise EnvironmentError("FRED_API_KEY environment variable is not set")
    if not fmp_api_key:
        raise EnvironmentError("FMP_API_KEY environment variable is not set")

    # ── Step 1: Fetch AA bond yield from FRED ────────────────────────────────
    aa_yield_pct, yield_date = fetch_aa_yield(fred_api_key)
    aa_yield_decimal = aa_yield_pct / 100.0
    max_pe = 1.0 / aa_yield_decimal
    logger.info(f"AA yield: {aa_yield_pct:.4f}% → Graham max P/E: {max_pe:.2f}")

    # ── Step 2: Fetch all large-cap US stocks via the free screener ───────────
    all_stocks = fetch_all_screener_stocks(fmp_api_key)
    logger.info(f"Total candidate stocks fetched: {len(all_stocks)}")

    # ── Step 3: Filter by Graham P/E criterion ────────────────────────────────
    qualified = []
    skipped_no_pe       = 0
    skipped_negative_pe = 0
    skipped_high_pe     = 0

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
        f"Filter results: {len(qualified)} qualify | "
        f"{skipped_no_pe} no P/E | {skipped_negative_pe} negative P/E | "
        f"{skipped_high_pe} P/E ≥ {max_pe:.2f}"
    )

    # ── Step 4: Build rows for Iceberg ───────────────────────────────────────
    # The screener response includes: symbol, companyName, marketCap, price,
    # beta, volume, lastAnnualDividend, exchange, sector, industry, country
    run_date = date.today().isoformat()

    rows = []
    for s in qualified:
        price_val = float(s.get("price") or 0.0)
        last_div  = float(s.get("lastAnnualDividend") or 0.0)
        div_yield_pct = (last_div / price_val * 100.0) if price_val > 0 else 0.0

        rows.append({
            "run_date":       run_date,
            "yield_date":     yield_date,
            "aa_yield_pct":   float(aa_yield_pct),
            "max_pe":         float(round(max_pe, 4)),
            "symbol":         str(s.get("symbol", "") or ""),
            "company_name":   str(s.get("companyName", "") or ""),
            "exchange":       str(s.get("exchangeShortName", "") or s.get("exchange", "") or ""),
            "sector":         str(s.get("sector", "") or ""),
            "industry":       str(s.get("industry", "") or ""),
            "country":        str(s.get("country", "") or ""),
            "price":          price_val,
            "pe_ratio":       float(s.get("pe") or 0.0),
            "market_cap":     float(s.get("marketCap") or 0.0),
            "volume":         float(s.get("volume") or 0.0),
            "beta":           float(s.get("beta") or 0.0),
            "dividend_yield": div_yield_pct,
            "is_test_run":    TEST_PAGE_LIMIT is not None,
        })

    if not rows:
        logger.warning("No stocks passed the Graham filter — writing placeholder row")
        rows = [{
            "run_date": run_date, "yield_date": yield_date,
            "aa_yield_pct": float(aa_yield_pct), "max_pe": float(round(max_pe, 4)),
            "symbol": "", "company_name": "NO_RESULTS", "exchange": "",
            "sector": "", "industry": "", "country": "",
            "price": 0.0, "pe_ratio": 0.0, "market_cap": 0.0,
            "volume": 0.0, "beta": 0.0, "dividend_yield": 0.0, "is_test_run": True,
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
        StructField("exchange",       StringType(),  True),
        StructField("sector",         StringType(),  True),
        StructField("industry",       StringType(),  True),
        StructField("country",        StringType(),  True),
        StructField("price",          DoubleType(),  True),
        StructField("pe_ratio",       DoubleType(),  True),
        StructField("market_cap",     DoubleType(),  True),
        StructField("volume",         DoubleType(),  True),
        StructField("beta",           DoubleType(),  True),
        StructField("dividend_yield", DoubleType(),  True),
        StructField("is_test_run",    BooleanType(), True),
    ])

    df = spark.createDataFrame(rows, schema=schema)
    df.writeTo("analytics.graham_screener_results").createOrReplace()

    count = df.count()
    logger.info(f"Wrote {count} qualifying stocks to analytics.graham_screener_results")
    logger.info(
        f"Summary — AA yield: {aa_yield_pct:.4f}% | Max P/E: {max_pe:.2f} | "
        f"Qualifying stocks: {len(qualified)} | Test pages: {TEST_PAGE_LIMIT}"
    )
