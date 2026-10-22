CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH p AS (
    SELECT * FROM stg_payments WHERE dq_status <> 'FAIL'
),
bad_pay AS (
    SELECT DISTINCT order_id FROM stg_payments WHERE dq_status = 'FAIL' AND order_id IS NOT NULL
),
j AS (
    SELECT o.order_id, o.order_status, o.order_amount, o.payment_mode AS order_mode,
           p.payment_id, p.payment_status, p.amount_paid, p.payment_mode AS pay_mode
    FROM stg_orders o
    LEFT JOIN p ON o.order_id = p.order_id
    --WHERE o.dq_status = 'PASS'
    WHERE o.dq_status = 'PASS'
      AND o.order_id IN (SELECT order_id FROM raw_orders WHERE _source_file = 'orders.csv')
),
issues AS (
    SELECT order_id, payment_id, order_amount, amount_paid, 'payment_quarantined' AS reason
    FROM j WHERE order_status = 'DELIVERED' AND payment_id IS NULL AND order_id IN (SELECT order_id FROM bad_pay)
    UNION ALL
    SELECT order_id, payment_id, order_amount, amount_paid, 'missing_payment'
    FROM j WHERE order_status = 'DELIVERED' AND payment_id IS NULL AND order_id NOT IN (SELECT order_id FROM bad_pay)
    UNION ALL
    SELECT order_id, payment_id, order_amount, amount_paid, 'amount_mismatch'
    FROM j WHERE order_status = 'DELIVERED' AND payment_status = 'SUCCESS' AND abs(amount_paid - order_amount) > 0.01
    UNION ALL
    SELECT order_id, payment_id, order_amount, amount_paid, 'payment_failed_but_delivered'
    FROM j WHERE order_status = 'DELIVERED' AND payment_status = 'FAILED'
    UNION ALL
    SELECT order_id, payment_id, order_amount, amount_paid, 'refunded_but_delivered'
    FROM j WHERE order_status = 'DELIVERED' AND payment_status = 'REFUNDED'
    UNION ALL
    SELECT order_id, payment_id, order_amount, amount_paid, 'returned_not_refunded'
    FROM j WHERE order_status = 'RETURNED' AND payment_status = 'SUCCESS'
    UNION ALL
    SELECT order_id, payment_id, order_amount, amount_paid, 'mode_mismatch'
    FROM j WHERE payment_id IS NOT NULL AND order_mode <> pay_mode
    UNION ALL
    SELECT p.order_id, p.payment_id, NULL, p.amount_paid,
           CASE WHEN p.order_id IN (SELECT order_id FROM stg_orders) THEN 'order_quarantined'
                ELSE 'payment_for_unknown_order' END
    FROM p WHERE p.order_id NOT IN (SELECT order_id FROM stg_orders WHERE dq_status = 'PASS')
)
SELECT *,
       CASE WHEN reason IN ('payment_failed_but_delivered', 'refunded_but_delivered') THEN -order_amount
            ELSE coalesce(amount_paid, 0) - coalesce(order_amount, 0) END AS difference
FROM issues;