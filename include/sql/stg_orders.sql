CREATE OR REPLACE TABLE stg_orders AS
WITH a AS (
    SELECT *, rowid AS _rid FROM raw_orders
),
latest AS (
    SELECT * FROM a
    --QUALIFY row_number() OVER (PARTITION BY trim(order_id) ORDER BY _loaded_at DESC, _rid) = 1
        QUALIFY row_number() OVER (
        PARTITION BY trim(order_id)
        ORDER BY CASE WHEN _source_file = 'orders.csv' THEN 0 ELSE 1 END DESC, _loaded_at DESC, _rid
    ) = 1
),
clean AS (
    SELECT trim(order_id) AS order_id,
           try_cast(trim(order_ts) AS TIMESTAMP) AS order_ts,
           coalesce(trim(customer_id), '') AS customer_id,
           trim(store_id) AS store_id,
           trim(product_id) AS product_id,
           try_cast(trim(quantity) AS INTEGER) AS quantity,
           try_cast(trim(unit_price) AS DOUBLE) AS unit_price,
           try_cast(trim(discount_pct) AS DOUBLE) AS discount_pct,
           try_cast(trim(order_amount) AS DOUBLE) AS order_amount,
           CASE lower(trim(payment_mode))
                WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card'
                WHEN 'cod' THEN 'Cash on Delivery' WHEN 'cash on delivery' THEN 'Cash on Delivery'
                WHEN 'wallet' THEN 'Wallet' ELSE trim(payment_mode) END AS payment_mode,
           upper(trim(order_status)) AS order_status,
           trim(channel) AS channel,
           try_cast(trim(delivery_minutes) AS DOUBLE) AS delivery_minutes,
           _run_id, _source_file
    FROM latest
),
flagged AS (
    SELECT *,
        concat_ws('; ',
            CASE WHEN customer_id = '' THEN 'customer_id_not_null' END,
            CASE WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL THEN 'delivered_has_minutes' END,
            CASE WHEN coalesce(order_status, '') NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'status_allowed' END,
            CASE WHEN coalesce(payment_mode, '') NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'payment_mode_allowed' END,
            CASE WHEN order_ts IS NULL THEN 'order_ts_parses' END,
            CASE WHEN NOT coalesce(discount_pct BETWEEN 0 AND 100, false) THEN 'discount_0_to_100' END,
            CASE WHEN NOT coalesce(quantity > 0, false) THEN 'quantity_positive' END,
            CASE WHEN NOT coalesce(abs(round(quantity * unit_price * (1 - discount_pct / 100), 2) - order_amount) <= 0.01, false)
                 THEN 'amount_matches_formula' END,
            CASE WHEN coalesce(delivery_minutes >= 120, false) THEN 'delivery_under_120' END,
            CASE WHEN customer_id <> '' AND NOT coalesce(customer_id IN (SELECT customer_id FROM stg_customers WHERE dq_status <> 'FAIL'), false)
                 THEN 'customer_exists' END,
            CASE WHEN NOT coalesce(product_id IN (SELECT product_id FROM stg_products WHERE dq_status <> 'FAIL'), false)
                 THEN 'product_exists' END,
            CASE WHEN NOT coalesce(store_id IN (SELECT store_id FROM stg_stores WHERE dq_status <> 'FAIL'), false)
                 THEN 'store_exists' END,
            CASE WHEN order_ts > CAST(current_timestamp AS TIMESTAMP) THEN 'order_ts_not_future' END
        ) AS failure_reason,
        '' AS warning_reason
    FROM clean
)
SELECT *, CASE WHEN failure_reason <> '' THEN 'FAIL'
               WHEN warning_reason <> '' THEN 'WARN' ELSE 'PASS' END AS dq_status
FROM flagged;