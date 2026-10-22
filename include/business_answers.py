import duckdb, pandas as pd
from pathlib import Path

ETL_DB, ELT_DB = "include/warehouse_Zubair.duckdb", "include/warehouse_Zubair_elt.duckdb"
LATEST = "(SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_Zubair' ORDER BY start_time DESC LIMIT 1)"

Q = {
 "Q1 Daily net revenue by zone and store (delivered only)": (
  "SELECT order_date, zone, store_id, store_name, delivered_orders, round(net_revenue, 2) AS net_revenue "
  "FROM etl_daily_zone_revenue ORDER BY order_date, zone, store_id",
  "SELECT order_date, zone, store_id, store_name, delivered_orders, round(net_revenue, 2) AS net_revenue "
  "FROM mart_daily_store_kpis ORDER BY order_date, zone, store_id"),
 "Q2 Revenue and gross margin by category": (
  "SELECT category, round(net_revenue, 2) AS net_revenue, round(gross_margin, 2) AS gross_margin, "
  "round(margin_pct, 2) AS margin_pct FROM etl_category_margin ORDER BY category",
  "SELECT category, round(net_revenue, 2) AS net_revenue, round(gross_margin, 2) AS gross_margin, "
  "round(margin_pct, 2) AS margin_pct FROM mart_category_margin ORDER BY category"),
 "Q3 Average delivery time per store and % within 15 minutes": (
  "SELECT s.store_id, s.store_name, s.zone, count(*) AS delivered_orders, "
    "round(avg(f.delivery_minutes), 1) AS avg_delivery_minutes, "
  "round(100.0 * avg(CASE WHEN f.within_15_min THEN 1 ELSE 0 END), 1) AS pct_within_15_min "
  "FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id) "
  "WHERE f.order_status = 'DELIVERED' GROUP BY 1, 2, 3 ORDER BY 1",
  "SELECT store_id, store_name, zone, delivered_orders, round(avg_delivery_minutes, 2) AS avg_delivery_minutes, "
  "round(pct_within_15_min, 2) AS pct_within_15_min FROM mart_delivery_sla ORDER BY store_id"),
 "Q4a Revenue by payment mode": (
  "SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue "
  "FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 1",
  "SELECT payment_mode, count(*) AS delivered_orders, round(sum(order_amount), 2) AS net_revenue "
  "FROM stg_orders WHERE dq_status = 'PASS' AND order_status = 'DELIVERED' GROUP BY 1 ORDER BY 1"),
 "Q4b Amount that does not reconcile with the payment gateway": (
  "SELECT reason, count(*) AS orders, round(sum(abs(difference)), 2) AS amount "
  f"FROM etl_payment_reconciliation WHERE run_id = {LATEST} AND reason <> 'order_quarantined' GROUP BY 1 ORDER BY 1",
  "SELECT reason, count(*) AS orders, round(sum(abs(difference)), 2) AS amount "
  "FROM mart_payment_reconciliation WHERE reason <> 'order_quarantined' GROUP BY 1 ORDER BY 1"),
 "Q5 Top 10 customers by spend (no email or phone)": (
  "SELECT c.customer_id, c.loyalty_tier, count(*) AS delivered_orders, round(sum(f.net_revenue), 2) AS total_spend "
  "FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id) WHERE f.order_status = 'DELIVERED' "
  "GROUP BY 1, 2 ORDER BY total_spend DESC, customer_id LIMIT 10",
  "SELECT customer_id, loyalty_tier, delivered_orders, round(total_spend, 2) AS total_spend "
  "FROM mart_top_customers ORDER BY total_spend DESC, customer_id LIMIT 10"),
 "Q6 Rows rejected by file and reason": (
  "SELECT file || '.csv' AS file, reason, count(*) AS rows_rejected FROM (SELECT file, unnest(string_split(failure_reason, '; ')) AS reason "
  f"FROM etl_rejected_rows WHERE run_id = {LATEST}) GROUP BY 1, 2 ORDER BY 1, 3 DESC, 2",
  "SELECT file, reason, rows_rejected FROM mart_rejected_rows ORDER BY file, rows_rejected DESC, reason"),
}

def run(db, sql):
    con = duckdb.connect(db, read_only=True)
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()

def md(df, n=12):
    if df.empty:
        return "_no rows_"
    cols = list(df.columns)
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in df.head(n).itertuples(index=False):
        lines.append("| " + " | ".join(str(v) for v in r) + " |")
    if len(df) > n:
        lines.append(f"\n_{len(df)} rows in total, first {n} shown; full result in the CSV._")
    return "\n".join(lines)

out = Path("business_answers"); out.mkdir(exist_ok=True)
doc = ["# Business questions: SQL and results, both pipelines\n"]
for title, (etl_sql, elt_sql) in Q.items():
    key = title.split()[0].lower()
    a, b = run(ETL_DB, etl_sql), run(ELT_DB, elt_sql)
    a.to_csv(out / f"{key}_etl.csv", index=False); b.to_csv(out / f"{key}_elt.csv", index=False)
    try:
        pd.testing.assert_frame_equal(a.reset_index(drop=True), b.reset_index(drop=True),
                                      check_exact=False, atol=0.01, check_dtype=False)
        verdict = "MATCH"
    except AssertionError:
        verdict = "DIFFERENT"
    print(f"{title}: ETL {len(a)} rows, ELT {len(b)} rows -> {verdict}")
    doc += [f"## {title}\n", f"Result: ETL and ELT **{verdict.lower()}**.\n",
            "**ETL SQL**\n```sql\n" + etl_sql + "\n```\n", md(a) + "\n",
            "**ELT SQL**\n```sql\n" + elt_sql + "\n```\n", md(b) + "\n"]
Path("business_answers.md").write_text("\n".join(doc), encoding="utf-8")

for label, db, sql in [("ETL", ETL_DB, "SELECT round(sum(net_revenue),2) FROM etl_fact_orders WHERE order_status='DELIVERED'"),
                       ("ELT", ELT_DB, "SELECT round(sum(order_amount),2) FROM stg_orders WHERE dq_status='PASS' AND order_status='DELIVERED'")]:
    print(label, "total delivered revenue:", run(db, sql).iloc[0, 0])