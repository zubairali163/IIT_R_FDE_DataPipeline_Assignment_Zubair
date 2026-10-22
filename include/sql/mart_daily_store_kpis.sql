CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT CAST(o.order_ts AS DATE) AS order_date, s.zone, o.store_id, s.store_name,
       count(*) AS delivered_orders,
       round(sum(o.order_amount), 2) AS net_revenue
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY ALL;