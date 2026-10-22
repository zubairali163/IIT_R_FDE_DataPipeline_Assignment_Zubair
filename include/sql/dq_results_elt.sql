INSERT INTO dq_results
WITH catalog(file, check_name, dimension, severity) AS (
    VALUES
    ('orders','order_id_unique','Uniqueness','Critical'),
    ('orders','customer_id_not_null','Completeness','Critical'),
    ('orders','delivered_has_minutes','Completeness','Critical'),
    ('orders','status_allowed','Validity','Critical'),
    ('orders','payment_mode_allowed','Validity','Critical'),
    ('orders','order_ts_parses','Validity','Critical'),
    ('orders','discount_0_to_100','Validity','Critical'),
    ('orders','quantity_positive','Accuracy','Critical'),
    ('orders','amount_matches_formula','Accuracy','Critical'),
    ('orders','delivery_under_120','Accuracy','Critical'),
    ('orders','customer_exists','Consistency','Critical'),
    ('orders','product_exists','Consistency','Critical'),
    ('orders','store_exists','Consistency','Critical'),
    ('orders','order_ts_not_future','Timeliness','Critical'),
    ('payments','payment_id_unique','Uniqueness','Critical'),
    ('payments','payment_mode_allowed','Validity','Critical'),
    ('payments','amount_not_negative','Accuracy','Critical'),
    ('payments','order_exists','Consistency','Warning'),
    ('payments','gateway_ref_unless_cod','Completeness','Warning'),
    ('customers','customer_id_unique','Uniqueness','Critical'),
    ('customers','signup_not_future','Timeliness','Critical'),
    ('customers','email_format','Validity','Warning'),
    ('customers','phone_10_digits','Validity','Warning'),
    ('customers','pincode_6_digits','Validity','Warning'),
    ('customers','city_not_null','Completeness','Warning'),
    ('customers','tier_allowed','Consistency','Warning'),
    ('products','product_id_unique','Uniqueness','Critical'),
    ('products','category_not_null','Completeness','Critical'),
    ('products','mrp_positive','Accuracy','Critical'),
    ('products','cost_le_mrp','Accuracy','Critical'),
    ('stores','store_id_unique','Uniqueness','Critical'),
    ('stores','zone_not_null','Completeness','Critical'),
    ('stores','capacity_positive','Accuracy','Warning')
),
totals AS (
    SELECT 'orders' AS file, count(*) AS n FROM stg_orders
    UNION ALL SELECT 'payments', count(*) FROM stg_payments
    UNION ALL SELECT 'customers', count(*) FROM stg_customers
    UNION ALL SELECT 'products', count(*) FROM stg_products
    UNION ALL SELECT 'stores', count(*) FROM stg_stores
),
reasons AS (
    SELECT 'orders' AS file, unnest(string_split(concat_ws('; ', failure_reason, warning_reason), '; ')) AS check_name FROM stg_orders
    UNION ALL SELECT 'payments', unnest(string_split(concat_ws('; ', failure_reason, warning_reason), '; ')) FROM stg_payments
    UNION ALL SELECT 'customers', unnest(string_split(concat_ws('; ', failure_reason, warning_reason), '; ')) FROM stg_customers
    UNION ALL SELECT 'products', unnest(string_split(concat_ws('; ', failure_reason, warning_reason), '; ')) FROM stg_products
    UNION ALL SELECT 'stores', unnest(string_split(concat_ws('; ', failure_reason, warning_reason), '; ')) FROM stg_stores
),
counts AS (
    SELECT file, check_name, count(*) AS n FROM reasons WHERE check_name <> '' GROUP BY 1, 2
),
uniq AS (
    -- staging collapses duplicates, so uniqueness is measured on the latest raw load of each base file
    SELECT 'orders' AS file, 'order_id_unique' AS check_name,
           count(*) - count(DISTINCT trim(order_id)) AS n, count(*) AS rows_checked
    FROM raw_orders WHERE _source_file = 'orders.csv'
      AND _run_id = (SELECT arg_max(_run_id, _loaded_at) FROM raw_orders WHERE _source_file = 'orders.csv')
    UNION ALL
    SELECT 'payments', 'payment_id_unique', count(*) - count(DISTINCT trim(payment_id)), count(*)
    FROM raw_payments WHERE _run_id = (SELECT arg_max(_run_id, _loaded_at) FROM raw_payments)
    UNION ALL
    SELECT 'customers', 'customer_id_unique', count(*) - count(DISTINCT trim(customer_id)), count(*)
    FROM raw_customers WHERE _run_id = (SELECT arg_max(_run_id, _loaded_at) FROM raw_customers)
    UNION ALL
    SELECT 'products', 'product_id_unique', count(*) - count(DISTINCT trim(product_id)), count(*)
    FROM raw_products WHERE _run_id = (SELECT arg_max(_run_id, _loaded_at) FROM raw_products)
    UNION ALL
    SELECT 'stores', 'store_id_unique', count(*) - count(DISTINCT trim(store_id)), count(*)
    FROM raw_stores WHERE _run_id = (SELECT arg_max(_run_id, _loaded_at) FROM raw_stores)
)
SELECT '__RUN_ID__' AS run_id, '__DAG_ID__' AS dag_id, c.check_name, c.dimension, c.severity,
       coalesce(u.rows_checked, t.n) AS rows_checked,
       coalesce(u.n, k.n, 0) AS rows_failed,
       CASE WHEN coalesce(u.n, k.n, 0) = 0 THEN 'PASS'
            WHEN c.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END AS status,
       CAST(now() AS VARCHAR) AS checked_at
FROM catalog c
JOIN totals t ON t.file = c.file
LEFT JOIN counts k ON k.file = c.file AND k.check_name = c.check_name
LEFT JOIN uniq u ON u.file = c.file AND u.check_name = c.check_name;