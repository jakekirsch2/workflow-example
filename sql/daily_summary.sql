-- Daily Summary Aggregation SQL
-- Generates daily summary metrics from transformed sales data
-- Output: Appended to daily_summary table with partitioning

-- Get the processing date (yesterday by default)
DECLARE processing_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

-- Delete existing data for this date (idempotent)
DELETE FROM `${PROJECT_ID}.${DATASET}.daily_summary`
WHERE summary_date = processing_date;

-- Insert daily summary
INSERT INTO `${PROJECT_ID}.${DATASET}.daily_summary` (
    summary_date,
    total_transactions,
    total_revenue,
    total_tax_collected,
    total_revenue_with_tax,
    average_order_value,
    median_order_value,
    unique_customers,
    new_customers,
    returning_customers,
    total_items_sold,
    average_items_per_order,

    -- Category breakdowns
    revenue_by_category,
    transactions_by_category,

    -- Payment method breakdowns
    revenue_by_payment_method,
    transactions_by_payment_method,

    -- Customer segment breakdowns
    revenue_by_segment,
    customers_by_segment,

    -- Time-based metrics
    peak_hour,
    transactions_by_hour,

    -- Store metrics
    revenue_by_store,
    top_store,

    -- Product metrics
    top_products,

    -- Discount tier metrics
    revenue_by_tier,

    -- Quality metrics
    bulk_order_count,
    high_value_order_count,
    bulk_order_revenue,
    high_value_order_revenue,

    -- Metadata
    created_at,
    pipeline_version
)

WITH daily_data AS (
    SELECT *
    FROM `${PROJECT_ID}.${DATASET}.transformed_sales`
    WHERE DATE(transaction_date) = processing_date
),

-- Calculate customer history for new vs returning
customer_history AS (
    SELECT DISTINCT customer_id
    FROM `${PROJECT_ID}.${DATASET}.transformed_sales`
    WHERE DATE(transaction_date) < processing_date
),

-- Category aggregations
category_aggs AS (
    SELECT
        product_category,
        SUM(total_with_tax) AS revenue,
        COUNT(*) AS transactions
    FROM daily_data
    GROUP BY product_category
),

-- Payment method aggregations
payment_aggs AS (
    SELECT
        payment_method,
        SUM(total_with_tax) AS revenue,
        COUNT(*) AS transactions
    FROM daily_data
    GROUP BY payment_method
),

-- Customer segment aggregations
segment_aggs AS (
    SELECT
        customer_segment,
        SUM(total_with_tax) AS revenue,
        COUNT(DISTINCT customer_id) AS customers
    FROM daily_data
    GROUP BY customer_segment
),

-- Hourly aggregations
hourly_aggs AS (
    SELECT
        transaction_hour,
        COUNT(*) AS transactions
    FROM daily_data
    GROUP BY transaction_hour
),

-- Store aggregations
store_aggs AS (
    SELECT
        store_id,
        SUM(total_with_tax) AS revenue
    FROM daily_data
    GROUP BY store_id
),

-- Top products
top_products AS (
    SELECT
        product_id,
        product_category,
        SUM(quantity) AS total_quantity,
        SUM(total_with_tax) AS total_revenue
    FROM daily_data
    GROUP BY product_id, product_category
    ORDER BY total_revenue DESC
    LIMIT 10
),

-- Discount tier aggregations
tier_aggs AS (
    SELECT
        discount_tier,
        SUM(total_with_tax) AS revenue
    FROM daily_data
    GROUP BY discount_tier
),

-- Calculate median
percentiles AS (
    SELECT
        APPROX_QUANTILES(total_with_tax, 100)[OFFSET(50)] AS median_order
    FROM daily_data
)

SELECT
    processing_date AS summary_date,

    -- Transaction metrics
    COUNT(*) AS total_transactions,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(SUM(tax_amount), 2) AS total_tax_collected,
    ROUND(SUM(total_with_tax), 2) AS total_revenue_with_tax,
    ROUND(AVG(total_with_tax), 2) AS average_order_value,
    ROUND((SELECT median_order FROM percentiles), 2) AS median_order_value,

    -- Customer metrics
    COUNT(DISTINCT d.customer_id) AS unique_customers,
    COUNT(DISTINCT CASE WHEN h.customer_id IS NULL THEN d.customer_id END) AS new_customers,
    COUNT(DISTINCT CASE WHEN h.customer_id IS NOT NULL THEN d.customer_id END) AS returning_customers,

    -- Item metrics
    SUM(quantity) AS total_items_sold,
    ROUND(AVG(quantity), 2) AS average_items_per_order,

    -- Category breakdowns (as JSON)
    TO_JSON((SELECT AS STRUCT * FROM (SELECT * FROM category_aggs))) AS revenue_by_category,
    TO_JSON((SELECT AS STRUCT * FROM (SELECT product_category, transactions FROM category_aggs))) AS transactions_by_category,

    -- Payment breakdowns
    TO_JSON((SELECT AS STRUCT * FROM (SELECT * FROM payment_aggs))) AS revenue_by_payment_method,
    TO_JSON((SELECT AS STRUCT * FROM (SELECT payment_method, transactions FROM payment_aggs))) AS transactions_by_payment_method,

    -- Segment breakdowns
    TO_JSON((SELECT AS STRUCT * FROM (SELECT * FROM segment_aggs))) AS revenue_by_segment,
    TO_JSON((SELECT AS STRUCT * FROM (SELECT customer_segment, customers FROM segment_aggs))) AS customers_by_segment,

    -- Peak hour
    (SELECT transaction_hour FROM hourly_aggs ORDER BY transactions DESC LIMIT 1) AS peak_hour,
    TO_JSON((SELECT AS STRUCT * FROM (SELECT * FROM hourly_aggs ORDER BY transaction_hour))) AS transactions_by_hour,

    -- Store metrics
    TO_JSON((SELECT AS STRUCT * FROM (SELECT * FROM store_aggs ORDER BY revenue DESC))) AS revenue_by_store,
    (SELECT store_id FROM store_aggs ORDER BY revenue DESC LIMIT 1) AS top_store,

    -- Top products
    TO_JSON((SELECT AS STRUCT * FROM (SELECT * FROM top_products))) AS top_products,

    -- Tier metrics
    TO_JSON((SELECT AS STRUCT * FROM (SELECT * FROM tier_aggs))) AS revenue_by_tier,

    -- Quality metrics
    COUNTIF(is_bulk_order) AS bulk_order_count,
    COUNTIF(is_high_value) AS high_value_order_count,
    ROUND(SUM(CASE WHEN is_bulk_order THEN total_with_tax ELSE 0 END), 2) AS bulk_order_revenue,
    ROUND(SUM(CASE WHEN is_high_value THEN total_with_tax ELSE 0 END), 2) AS high_value_order_revenue,

    -- Metadata
    CURRENT_TIMESTAMP() AS created_at,
    '1.0.0' AS pipeline_version

FROM daily_data d
LEFT JOIN customer_history h ON d.customer_id = h.customer_id;

-- Output summary for logging
SELECT
    summary_date,
    total_transactions,
    total_revenue_with_tax,
    unique_customers,
    new_customers,
    average_order_value
FROM `${PROJECT_ID}.${DATASET}.daily_summary`
WHERE summary_date = processing_date;
