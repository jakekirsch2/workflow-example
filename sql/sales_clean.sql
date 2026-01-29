-- Sales Data Cleaning SQL
-- Cleans and validates raw transaction data from staging table
-- Output: Cleaned transactions ready for transformation

-- Configuration (replaced by orchestrator)
DECLARE dataset_name STRING DEFAULT '${DATASET}';
DECLARE project_id STRING DEFAULT '${PROJECT_ID}';

-- Create cleaned transactions table
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.cleaned_transactions` AS

WITH raw_data AS (
    -- Select from staging table populated by extract step
    SELECT
        transaction_id,
        customer_id,
        product_id,
        quantity,
        unit_price,
        total_amount,
        transaction_date,
        store_id,
        payment_method,
        extracted_at,
        source_system
    FROM `${PROJECT_ID}.${DATASET}.staging_raw_transactions`
),

-- Remove duplicates based on transaction_id
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY extracted_at DESC
        ) AS row_num
    FROM raw_data
),

-- Data quality checks and cleaning
cleaned AS (
    SELECT
        -- Trim and clean string fields
        TRIM(transaction_id) AS transaction_id,
        TRIM(customer_id) AS customer_id,
        TRIM(product_id) AS product_id,

        -- Validate and clean numeric fields
        CASE
            WHEN quantity <= 0 THEN 1
            WHEN quantity > 10000 THEN 10000  -- Cap unreasonable quantities
            ELSE quantity
        END AS quantity,

        CASE
            WHEN unit_price < 0 THEN 0
            WHEN unit_price > 100000 THEN 100000  -- Cap unreasonable prices
            ELSE ROUND(unit_price, 2)
        END AS unit_price,

        CASE
            WHEN total_amount < 0 THEN 0
            ELSE ROUND(total_amount, 2)
        END AS total_amount,

        -- Standardize dates
        CASE
            WHEN transaction_date IS NULL THEN CURRENT_TIMESTAMP()
            WHEN transaction_date > CURRENT_TIMESTAMP() THEN CURRENT_TIMESTAMP()
            ELSE transaction_date
        END AS transaction_date,

        -- Standardize categorical fields
        UPPER(TRIM(COALESCE(store_id, 'UNKNOWN'))) AS store_id,

        CASE UPPER(TRIM(payment_method))
            WHEN 'CC' THEN 'CREDIT_CARD'
            WHEN 'CREDIT' THEN 'CREDIT_CARD'
            WHEN 'CREDIT_CARD' THEN 'CREDIT_CARD'
            WHEN 'DC' THEN 'DEBIT_CARD'
            WHEN 'DEBIT' THEN 'DEBIT_CARD'
            WHEN 'DEBIT_CARD' THEN 'DEBIT_CARD'
            WHEN 'CASH' THEN 'CASH'
            WHEN 'CHECK' THEN 'CHECK'
            WHEN 'WIRE' THEN 'WIRE_TRANSFER'
            WHEN 'WIRE_TRANSFER' THEN 'WIRE_TRANSFER'
            WHEN 'PAYPAL' THEN 'DIGITAL_WALLET'
            WHEN 'VENMO' THEN 'DIGITAL_WALLET'
            WHEN 'APPLE_PAY' THEN 'DIGITAL_WALLET'
            WHEN 'GOOGLE_PAY' THEN 'DIGITAL_WALLET'
            ELSE 'OTHER'
        END AS payment_method,

        extracted_at,
        source_system,

        -- Add data quality flags
        CASE
            WHEN quantity <= 0 THEN TRUE
            WHEN unit_price < 0 THEN TRUE
            WHEN total_amount < 0 THEN TRUE
            WHEN transaction_date IS NULL THEN TRUE
            ELSE FALSE
        END AS had_data_quality_issues,

        -- Recalculate total if needed
        CASE
            WHEN ABS(total_amount - (quantity * unit_price)) > 0.01
            THEN quantity * unit_price
            ELSE total_amount
        END AS calculated_total

    FROM deduplicated
    WHERE row_num = 1
),

-- Final validation - remove invalid records
validated AS (
    SELECT
        transaction_id,
        customer_id,
        product_id,
        quantity,
        unit_price,
        -- Use calculated total if original was wrong
        CASE
            WHEN ABS(total_amount - calculated_total) > total_amount * 0.1
            THEN calculated_total
            ELSE total_amount
        END AS total_amount,
        transaction_date,
        store_id,
        payment_method,
        extracted_at,
        source_system,
        had_data_quality_issues,
        CURRENT_TIMESTAMP() AS cleaned_at
    FROM cleaned
    WHERE
        -- Remove records that can't be salvaged
        transaction_id IS NOT NULL
        AND LENGTH(TRIM(transaction_id)) > 0
        AND customer_id IS NOT NULL
        AND LENGTH(TRIM(customer_id)) > 0
        AND quantity > 0
        AND unit_price >= 0
)

SELECT * FROM validated;

-- Log cleaning statistics
SELECT
    COUNT(*) AS total_records,
    COUNTIF(had_data_quality_issues) AS records_with_issues,
    ROUND(COUNTIF(had_data_quality_issues) / COUNT(*) * 100, 2) AS issue_percentage,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT product_id) AS unique_products,
    MIN(transaction_date) AS earliest_transaction,
    MAX(transaction_date) AS latest_transaction,
    SUM(total_amount) AS total_revenue
FROM `${PROJECT_ID}.${DATASET}.cleaned_transactions`;
