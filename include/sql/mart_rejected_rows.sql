CREATE OR REPLACE TABLE mart_rejected_rows AS
SELECT file, reason, count(*) AS rows_rejected
FROM (
    SELECT 'orders.csv' AS file, unnest(string_split(failure_reason, '; ')) AS reason FROM stg_orders WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT 'payments.csv', unnest(string_split(failure_reason, '; ')) FROM stg_payments WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT 'customers.csv', unnest(string_split(failure_reason, '; ')) FROM stg_customers WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT 'products.csv', unnest(string_split(failure_reason, '; ')) FROM stg_products WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT 'stores.csv', unnest(string_split(failure_reason, '; ')) FROM stg_stores WHERE dq_status = 'FAIL'
)
GROUP BY ALL
ORDER BY file, rows_rejected DESC;