CREATE OR REPLACE TABLE mart_top_customers AS
SELECT c.customer_id, c.full_name, c.loyalty_tier, c.email_masked, c.phone_masked,
       count(*) AS delivered_orders,
       round(sum(o.order_amount), 2) AS total_spend
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.customer_id
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY total_spend DESC
LIMIT 10;