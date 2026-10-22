CREATE OR REPLACE TABLE mart_category_margin AS
SELECT p.category,
       round(sum(o.order_amount), 2) AS net_revenue,
       round(sum(o.order_amount - o.quantity * p.cost_price), 2) AS gross_margin,
       round(100 * sum(o.order_amount - o.quantity * p.cost_price) / sum(o.order_amount), 1) AS margin_pct
FROM stg_orders o
JOIN stg_products p ON o.product_id = p.product_id
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY net_revenue DESC;