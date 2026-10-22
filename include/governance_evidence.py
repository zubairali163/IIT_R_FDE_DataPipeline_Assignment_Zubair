import duckdb

ETL, ELT = "include/warehouse_Zubair.duckdb", "include/warehouse_Zubair_elt.duckdb"
etl = duckdb.connect(ETL, read_only=True)
elt = duckdb.connect(ELT, read_only=True)
show = lambda c, sql, p=None: c.execute(sql, p or []).fetchdf().to_string(index=False)

print("=== Q3 LINEAGE: the biggest number on the daily store mart ===")
d, s, n, rev = elt.execute("SELECT order_date, store_id, delivered_orders, net_revenue FROM mart_daily_store_kpis "
                           "ORDER BY net_revenue DESC, order_date, store_id LIMIT 1").fetchone()
print(f"ELT mart row: store {s}, date {d}, {n} orders, revenue {rev}")
print(show(elt, "SELECT order_id, order_amount, _source_file, _run_id FROM stg_orders WHERE dq_status = 'PASS' "
                "AND order_status = 'DELIVERED' AND CAST(order_ts AS DATE) = ? AND store_id = ? ORDER BY 1 LIMIT 5", [d, s]))
print("ELT sum of those rows:", elt.execute("SELECT round(sum(order_amount),2), count(*) FROM stg_orders WHERE dq_status='PASS' "
      "AND order_status='DELIVERED' AND CAST(order_ts AS DATE)=? AND store_id=?", [d, s]).fetchone())
print("ETL sum of the same rows:", etl.execute("SELECT round(sum(net_revenue),2), count(*) FROM etl_fact_orders WHERE order_status='DELIVERED' "
      "AND order_date=? AND store_id=?", [d, s]).fetchone())
print(show(etl, "SELECT order_id, net_revenue, source_file FROM etl_fact_orders WHERE order_status='DELIVERED' "
                "AND order_date=? AND store_id=? ORDER BY 1 LIMIT 5", [d, s]))

print("\n=== Q8 AUDIT: one rejected order ===")
oid = elt.execute("SELECT order_id FROM stg_orders WHERE dq_status='FAIL' AND order_status='DELIVERED' "
                  "AND failure_reason LIKE '%amount_matches_formula%' ORDER BY order_id LIMIT 1").fetchone()[0]
print("order:", oid)
print("ELT as received (raw):\n" + show(elt, "SELECT order_id, quantity, unit_price, discount_pct, order_amount, order_status, _source_file, _run_id "
      "FROM raw_orders WHERE trim(order_id) = ?", [oid]))
print("ELT rule broken:", elt.execute("SELECT failure_reason, dq_status FROM stg_orders WHERE order_id = ?", [oid]).fetchone())
print("ELT: rows for this order that count as revenue:", elt.execute(
      "SELECT count(*) FROM stg_orders WHERE order_id = ? AND dq_status = 'PASS'", [oid]).fetchone()[0])
print("ETL rejected-rows entry:\n" + show(etl, "SELECT run_id, file, row_key, failure_reason FROM etl_rejected_rows WHERE row_key = ? "
      "ORDER BY run_id DESC LIMIT 1", [oid]))
print("ETL: rows for this order in the fact table:", etl.execute("SELECT count(*) FROM etl_fact_orders WHERE order_id = ?", [oid]).fetchone()[0])
print("ETL source line as received:")
for line in open("include/data/nammamart/orders.csv", encoding="utf-8"):
    if line.startswith(oid + ","):
        print(" ", line.strip())

print("\n=== Q9 CHANGE HISTORY: the 9 orders DELIVERED in September, RETURNED in the increment ===")
ids = [r[0] for r in etl.execute("SELECT order_id FROM etl_fact_orders WHERE source_file LIKE '%increment%' "
                                 "AND order_status = 'RETURNED' ORDER BY 1").fetchall()]
print("orders:", ids)
lst = ",".join(f"'{i}'" for i in ids)
print("ETL September revenue now:", etl.execute("SELECT round(sum(net_revenue),2) FROM etl_fact_orders WHERE order_status='DELIVERED' "
      "AND order_date BETWEEN '2026-09-01' AND '2026-09-30'").fetchone()[0])
print("ELT September revenue now:", elt.execute("SELECT round(sum(order_amount),2) FROM stg_orders WHERE dq_status='PASS' AND order_status='DELIVERED' "
      "AND CAST(order_ts AS DATE) BETWEEN '2026-09-01' AND '2026-09-30'").fetchone()[0])
print("ELT 'as it was' (original rows still in raw):", elt.execute(
      f"SELECT round(sum(try_cast(order_amount AS DOUBLE)),2), count(*) FROM raw_orders WHERE _source_file='orders.csv' "
      f"AND trim(order_id) IN ({lst}) AND upper(trim(order_status))='DELIVERED'").fetchone())
print("ELT both versions of these orders kept in raw:", elt.execute(
      f"SELECT _source_file, upper(trim(order_status)) AS status, count(*) FROM raw_orders WHERE trim(order_id) IN ({lst}) GROUP BY 1, 2 ORDER BY 1").fetchall())