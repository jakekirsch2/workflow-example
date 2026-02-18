#!/usr/bin/env python3
"""
Visualize Sales Function
Reads transformed data from the lakehouse and publishes a revenue chart
to a shareable public URL via the vizId configured in the pipeline YAML.
"""
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main(spark):
    """
    Reads sales_summary from the Iceberg lakehouse and returns a viz spec.
    The platform runner uploads this to a public URL because vizId is set
    in the pipeline YAML for this task.

    Args:
        spark: SparkSession with Iceberg catalog configured
    """
    logger.info("Building sales visualization...")

    # Aggregate revenue and orders by region across all product categories
    region_df = spark.sql("""
        SELECT
            region,
            SUM(total_revenue)  AS revenue,
            SUM(total_orders)   AS orders
        FROM sales.sales_summary
        GROUP BY region
        ORDER BY revenue DESC
    """)

    rows = region_df.collect()
    logger.info(f"Loaded {len(rows)} regions from sales.sales_summary")

    chart_data = [
        {
            "region": r.region,
            "revenue": round(float(r.revenue), 2),
            "orders": int(r.orders),
        }
        for r in rows
    ]

    today = datetime.utcnow().strftime("%Y-%m-%d")

    # Return the viz spec — the platform runner serializes this to GCS
    # and serves it at the public URL defined by vizId in daily_etl.yaml
    return {
        "type": "bar",
        "title": "Daily Sales by Region",
        "subtitle": f"As of {today} UTC",
        "data": chart_data,
        "xKey": "region",
        "series": [
            {"key": "revenue", "color": "#3b82f6", "label": "Revenue ($)"},
            {"key": "orders",  "color": "#10b981", "label": "Orders"},
        ],
    }


if __name__ == "__main__":
    print("Use 'python dev.py run functions/visualize_sales.py' for local testing")
