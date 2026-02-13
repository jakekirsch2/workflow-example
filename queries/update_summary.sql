CREATE OR REPLACE TABLE sales_summary AS
SELECT
  DATE '2025-01-15' AS date, 'us-east' AS region, 'electronics' AS product_category, 142 AS total_orders, 28450.00 AS total_revenue, 200.35 AS avg_order_value UNION ALL
SELECT DATE '2025-01-15', 'us-west', 'electronics', 98, 19600.00, 200.00 UNION ALL
SELECT DATE '2025-01-15', 'us-east', 'clothing', 215, 10750.00, 50.00 UNION ALL
SELECT DATE '2025-01-15', 'eu-west', 'electronics', 76, 15200.00, 200.00 UNION ALL
SELECT DATE '2025-01-15', 'eu-west', 'clothing', 134, 6700.00, 50.00 UNION ALL
SELECT DATE '2025-01-16', 'us-east', 'electronics', 156, 31200.00, 200.00 UNION ALL
SELECT DATE '2025-01-16', 'us-west', 'clothing', 189, 9450.00, 50.00 UNION ALL
SELECT DATE '2025-01-16', 'eu-west', 'electronics', 82, 16400.00, 200.00;
