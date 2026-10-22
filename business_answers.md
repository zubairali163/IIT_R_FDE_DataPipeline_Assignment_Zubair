# Business questions: SQL and results, both pipelines

## Q1 Daily net revenue by zone and store (delivered only)

Result: ETL and ELT **match**.

**ETL SQL**
```sql
SELECT order_date, zone, store_id, store_name, delivered_orders, round(net_revenue, 2) AS net_revenue FROM etl_daily_zone_revenue ORDER BY order_date, zone, store_id
```

| order_date | zone | store_id | store_name | delivered_orders | net_revenue |
|---|---|---|---|---|---|
| 2026-09-01 00:00:00 | East | S08 | NammaMart Whitefield | 4 | 2984.7 |
| 2026-09-01 00:00:00 | East | S09 | NammaMart Indiranagar | 4 | 2089.6 |
| 2026-09-01 00:00:00 | East | S10 | NammaMart Marathahalli | 3 | 2141.6 |
| 2026-09-01 00:00:00 | East | S11 | NammaMart HSR Layout | 6 | 2121.15 |
| 2026-09-01 00:00:00 | North | S01 | NammaMart Hebbal | 3 | 1410.8 |
| 2026-09-01 00:00:00 | North | S02 | NammaMart Yelahanka | 6 | 2385.55 |
| 2026-09-01 00:00:00 | North | S03 | NammaMart RT Nagar | 6 | 4583.8 |
| 2026-09-01 00:00:00 | South | S05 | NammaMart JP Nagar | 2 | 803.0 |
| 2026-09-01 00:00:00 | South | S06 | NammaMart Banashankari | 6 | 2812.0 |
| 2026-09-01 00:00:00 | West | S12 | NammaMart Rajajinagar | 1 | 300.0 |
| 2026-09-01 00:00:00 | West | S13 | NammaMart Malleshwaram | 3 | 1469.1 |
| 2026-09-01 00:00:00 | West | S14 | NammaMart Vijayanagar | 7 | 3095.85 |

_429 rows in total, first 12 shown; full result in the CSV._

**ELT SQL**
```sql
SELECT order_date, zone, store_id, store_name, delivered_orders, round(net_revenue, 2) AS net_revenue FROM mart_daily_store_kpis ORDER BY order_date, zone, store_id
```

| order_date | zone | store_id | store_name | delivered_orders | net_revenue |
|---|---|---|---|---|---|
| 2026-09-01 00:00:00 | East | S08 | NammaMart Whitefield | 4 | 2984.7 |
| 2026-09-01 00:00:00 | East | S09 | NammaMart Indiranagar | 4 | 2089.6 |
| 2026-09-01 00:00:00 | East | S10 | NammaMart Marathahalli | 3 | 2141.6 |
| 2026-09-01 00:00:00 | East | S11 | NammaMart HSR Layout | 6 | 2121.15 |
| 2026-09-01 00:00:00 | North | S01 | NammaMart Hebbal | 3 | 1410.8 |
| 2026-09-01 00:00:00 | North | S02 | NammaMart Yelahanka | 6 | 2385.55 |
| 2026-09-01 00:00:00 | North | S03 | NammaMart RT Nagar | 6 | 4583.8 |
| 2026-09-01 00:00:00 | South | S05 | NammaMart JP Nagar | 2 | 803.0 |
| 2026-09-01 00:00:00 | South | S06 | NammaMart Banashankari | 6 | 2812.0 |
| 2026-09-01 00:00:00 | West | S12 | NammaMart Rajajinagar | 1 | 300.0 |
| 2026-09-01 00:00:00 | West | S13 | NammaMart Malleshwaram | 3 | 1469.1 |
| 2026-09-01 00:00:00 | West | S14 | NammaMart Vijayanagar | 7 | 3095.85 |

_429 rows in total, first 12 shown; full result in the CSV._

## Q2 Revenue and gross margin by category

Result: ETL and ELT **match**.

**ETL SQL**
```sql
SELECT category, round(net_revenue, 2) AS net_revenue, round(gross_margin, 2) AS gross_margin, round(margin_pct, 2) AS margin_pct FROM etl_category_margin ORDER BY category
```

| category | net_revenue | gross_margin | margin_pct |
|---|---|---|---|
| Dairy & Bakery | 169005.0 | 44761.99 | 26.5 |
| Fruits & Vegetables | 86740.95 | 20538.74 | 23.7 |
| Household | 194112.9 | 49547.43 | 25.5 |
| Personal Care | 195893.45 | 43799.67 | 22.4 |
| Snacks & Beverages | 166393.7 | 39115.74 | 23.5 |
| Staples | 240776.85 | 61194.48 | 25.4 |

**ELT SQL**
```sql
SELECT category, round(net_revenue, 2) AS net_revenue, round(gross_margin, 2) AS gross_margin, round(margin_pct, 2) AS margin_pct FROM mart_category_margin ORDER BY category
```

| category | net_revenue | gross_margin | margin_pct |
|---|---|---|---|
| Dairy & Bakery | 169005.0 | 44761.99 | 26.5 |
| Fruits & Vegetables | 86740.95 | 20538.74 | 23.7 |
| Household | 194112.9 | 49547.43 | 25.5 |
| Personal Care | 195893.45 | 43799.67 | 22.4 |
| Snacks & Beverages | 166393.7 | 39115.74 | 23.5 |
| Staples | 240776.85 | 61194.48 | 25.4 |

## Q3 Average delivery time per store and % within 15 minutes

Result: ETL and ELT **match**.

**ETL SQL**
```sql
SELECT s.store_id, s.store_name, s.zone, count(*) AS delivered_orders, round(avg(f.delivery_minutes), 1) AS avg_delivery_minutes, round(100.0 * avg(CASE WHEN f.within_15_min THEN 1 ELSE 0 END), 1) AS pct_within_15_min FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id) WHERE f.order_status = 'DELIVERED' GROUP BY 1, 2, 3 ORDER BY 1
```

| store_id | store_name | zone | delivered_orders | avg_delivery_minutes | pct_within_15_min |
|---|---|---|---|---|---|
| S01 | NammaMart Hebbal | North | 141 | 20.6 | 31.2 |
| S02 | NammaMart Yelahanka | North | 139 | 21.0 | 30.9 |
| S03 | NammaMart RT Nagar | North | 138 | 20.7 | 29.0 |
| S04 | NammaMart Jayanagar | South | 130 | 21.6 | 26.9 |
| S05 | NammaMart JP Nagar | South | 134 | 21.2 | 29.1 |
| S06 | NammaMart Banashankari | South | 145 | 22.3 | 22.1 |
| S08 | NammaMart Whitefield | East | 155 | 20.6 | 35.5 |
| S09 | NammaMart Indiranagar | East | 127 | 21.1 | 27.6 |
| S10 | NammaMart Marathahalli | East | 141 | 21.9 | 27.0 |
| S11 | NammaMart HSR Layout | East | 144 | 21.0 | 27.8 |
| S12 | NammaMart Rajajinagar | West | 113 | 22.0 | 23.9 |
| S13 | NammaMart Malleshwaram | West | 137 | 21.8 | 27.0 |

_14 rows in total, first 12 shown; full result in the CSV._

**ELT SQL**
```sql
SELECT store_id, store_name, zone, delivered_orders, round(avg_delivery_minutes, 2) AS avg_delivery_minutes, round(pct_within_15_min, 2) AS pct_within_15_min FROM mart_delivery_sla ORDER BY store_id
```

| store_id | store_name | zone | delivered_orders | avg_delivery_minutes | pct_within_15_min |
|---|---|---|---|---|---|
| S01 | NammaMart Hebbal | North | 141 | 20.6 | 31.2 |
| S02 | NammaMart Yelahanka | North | 139 | 21.0 | 30.9 |
| S03 | NammaMart RT Nagar | North | 138 | 20.7 | 29.0 |
| S04 | NammaMart Jayanagar | South | 130 | 21.6 | 26.9 |
| S05 | NammaMart JP Nagar | South | 134 | 21.2 | 29.1 |
| S06 | NammaMart Banashankari | South | 145 | 22.3 | 22.1 |
| S08 | NammaMart Whitefield | East | 155 | 20.6 | 35.5 |
| S09 | NammaMart Indiranagar | East | 127 | 21.1 | 27.6 |
| S10 | NammaMart Marathahalli | East | 141 | 21.9 | 27.0 |
| S11 | NammaMart HSR Layout | East | 144 | 21.0 | 27.8 |
| S12 | NammaMart Rajajinagar | West | 113 | 22.0 | 23.9 |
| S13 | NammaMart Malleshwaram | West | 137 | 21.8 | 27.0 |

_14 rows in total, first 12 shown; full result in the CSV._

## Q4a Revenue by payment mode

Result: ETL and ELT **match**.

**ETL SQL**
```sql
SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 1
```

| payment_mode | delivered_orders | net_revenue |
|---|---|---|
| Card | 400 | 216908.85 |
| Cash on Delivery | 272 | 153611.85 |
| UPI | 1057 | 559699.15 |
| Wallet | 220 | 122703.0 |

**ELT SQL**
```sql
SELECT payment_mode, count(*) AS delivered_orders, round(sum(order_amount), 2) AS net_revenue FROM stg_orders WHERE dq_status = 'PASS' AND order_status = 'DELIVERED' GROUP BY 1 ORDER BY 1
```

| payment_mode | delivered_orders | net_revenue |
|---|---|---|
| Card | 400 | 216908.85 |
| Cash on Delivery | 272 | 153611.85 |
| UPI | 1057 | 559699.15 |
| Wallet | 220 | 122703.0 |

## Q4b Amount that does not reconcile with the payment gateway

Result: ETL and ELT **match**.

**ETL SQL**
```sql
SELECT reason, count(*) AS orders, round(sum(abs(difference)), 2) AS amount FROM etl_payment_reconciliation WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_Zubair' ORDER BY start_time DESC LIMIT 1) AND reason <> 'order_quarantined' GROUP BY 1 ORDER BY 1
```

| reason | orders | amount |
|---|---|---|
| amount_mismatch | 11 | 560.0 |
| missing_payment | 26 | 16381.35 |
| payment_failed_but_delivered | 40 | 24009.65 |
| payment_for_unknown_order | 8 | 4580.57 |
| payment_quarantined | 1 | 56.95 |
| returned_not_refunded | 9 | 0.0 |

**ELT SQL**
```sql
SELECT reason, count(*) AS orders, round(sum(abs(difference)), 2) AS amount FROM mart_payment_reconciliation WHERE reason <> 'order_quarantined' GROUP BY 1 ORDER BY 1
```

| reason | orders | amount |
|---|---|---|
| amount_mismatch | 11 | 560.0 |
| missing_payment | 26 | 16381.35 |
| payment_failed_but_delivered | 40 | 24009.65 |
| payment_for_unknown_order | 8 | 4580.57 |
| payment_quarantined | 1 | 56.95 |
| returned_not_refunded | 9 | 0.0 |

## Q5 Top 10 customers by spend (no email or phone)

Result: ETL and ELT **match**.

**ETL SQL**
```sql
SELECT c.customer_id, c.loyalty_tier, count(*) AS delivered_orders, round(sum(f.net_revenue), 2) AS total_spend FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id) WHERE f.order_status = 'DELIVERED' GROUP BY 1, 2 ORDER BY total_spend DESC, customer_id LIMIT 10
```

| customer_id | loyalty_tier | delivered_orders | total_spend |
|---|---|---|---|
| C00136 | Bronze | 8 | 8149.1 |
| C00115 | Bronze | 7 | 7337.7 |
| C00310 | Bronze | 8 | 7258.95 |
| C00236 | Platinum | 10 | 7151.6 |
| C00265 | Gold | 7 | 6940.3 |
| C00338 | Silver | 8 | 6696.55 |
| C00072 | Silver | 11 | 6433.95 |
| C00283 | Bronze | 6 | 6241.4 |
| C00129 | Bronze | 10 | 6215.1 |
| C00202 | Silver | 7 | 6096.75 |

**ELT SQL**
```sql
SELECT customer_id, loyalty_tier, delivered_orders, round(total_spend, 2) AS total_spend FROM mart_top_customers ORDER BY total_spend DESC, customer_id LIMIT 10
```

| customer_id | loyalty_tier | delivered_orders | total_spend |
|---|---|---|---|
| C00136 | Bronze | 8 | 8149.1 |
| C00115 | Bronze | 7 | 7337.7 |
| C00310 | Bronze | 8 | 7258.95 |
| C00236 | Platinum | 10 | 7151.6 |
| C00265 | Gold | 7 | 6940.3 |
| C00338 | Silver | 8 | 6696.55 |
| C00072 | Silver | 11 | 6433.95 |
| C00283 | Bronze | 6 | 6241.4 |
| C00129 | Bronze | 10 | 6215.1 |
| C00202 | Silver | 7 | 6096.75 |

## Q6 Rows rejected by file and reason

Result: ETL and ELT **different**.

**ETL SQL**
```sql
SELECT file || '.csv' AS file, reason, count(*) AS rows_rejected FROM (SELECT file, unnest(string_split(failure_reason, '; ')) AS reason FROM etl_rejected_rows WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_Zubair' ORDER BY start_time DESC LIMIT 1)) GROUP BY 1, 2 ORDER BY 1, 3 DESC, 2
```

| file | reason | rows_rejected |
|---|---|---|
| customers.csv | customer_id_unique | 5 |
| customers.csv | signup_not_future | 3 |
| orders.csv | store_exists | 181 |
| orders.csv | product_exists | 135 |
| orders.csv | amount_matches_formula | 18 |
| orders.csv | customer_exists | 18 |
| orders.csv | customer_id_not_null | 18 |
| orders.csv | order_id_unique | 12 |
| orders.csv | quantity_positive | 10 |
| orders.csv | delivered_has_minutes | 8 |
| orders.csv | delivery_under_120 | 6 |
| orders.csv | order_ts_parses | 6 |

_24 rows in total, first 12 shown; full result in the CSV._

**ELT SQL**
```sql
SELECT file, reason, rows_rejected FROM mart_rejected_rows ORDER BY file, rows_rejected DESC, reason
```

| file | reason | rows_rejected |
|---|---|---|
| customers.csv | signup_not_future | 3 |
| orders.csv | store_exists | 186 |
| orders.csv | product_exists | 145 |
| orders.csv | customer_exists | 19 |
| orders.csv | amount_matches_formula | 18 |
| orders.csv | customer_id_not_null | 18 |
| orders.csv | quantity_positive | 10 |
| orders.csv | delivered_has_minutes | 8 |
| orders.csv | delivery_under_120 | 6 |
| orders.csv | order_ts_parses | 6 |
| orders.csv | order_ts_not_future | 5 |
| orders.csv | discount_0_to_100 | 3 |

_20 rows in total, first 12 shown; full result in the CSV._
