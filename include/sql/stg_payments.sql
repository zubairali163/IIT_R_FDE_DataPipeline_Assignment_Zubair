CREATE OR REPLACE TABLE stg_payments AS
WITH a AS (
    SELECT *, rowid AS _rid FROM raw_payments
),
latest AS (
    SELECT * FROM a
    QUALIFY row_number() OVER (PARTITION BY trim(payment_id) ORDER BY _loaded_at DESC, _rid) = 1
),
clean AS (
    SELECT trim(payment_id) AS payment_id,
           trim(order_id) AS order_id,
           try_cast(trim(payment_ts) AS TIMESTAMP) AS payment_ts,
           CASE lower(trim(payment_mode))
                WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card'
                WHEN 'cod' THEN 'Cash on Delivery' WHEN 'cash on delivery' THEN 'Cash on Delivery'
                WHEN 'wallet' THEN 'Wallet' ELSE trim(payment_mode) END AS payment_mode,
           try_cast(trim(amount_paid) AS DOUBLE) AS amount_paid,
           upper(trim(payment_status)) AS payment_status,
           coalesce(trim(gateway_ref), '') AS gateway_ref,
           _run_id, _source_file
    FROM latest
),
flagged AS (
    SELECT *,
        concat_ws('; ',
            CASE WHEN coalesce(payment_mode, '') NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'payment_mode_allowed' END,
            CASE WHEN NOT coalesce(amount_paid >= 0, false) THEN 'amount_not_negative' END
        ) AS failure_reason,
        concat_ws('; ',
            CASE WHEN NOT coalesce(order_id IN (SELECT trim(order_id) FROM raw_orders WHERE order_id IS NOT NULL), false) THEN 'order_exists' END,
            CASE WHEN payment_mode <> 'Cash on Delivery' AND gateway_ref = '' THEN 'gateway_ref_unless_cod' END
        ) AS warning_reason
    FROM clean
)
SELECT *, CASE WHEN failure_reason <> '' THEN 'FAIL'
               WHEN warning_reason <> '' THEN 'WARN' ELSE 'PASS' END AS dq_status
FROM flagged;