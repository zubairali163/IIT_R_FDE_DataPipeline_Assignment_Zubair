import sys, duckdb

ETL, ELT = "include/warehouse_Zubair.duckdb", "include/warehouse_Zubair_elt.duckdb"
DAGS = {"ETL": (ETL, "nammamart_etl_Zubair"), "ELT": (ELT, "nammamart_elt_Zubair")}
CHECKS = "('customer_id_not_null','quantity_positive','store_exists','status_allowed','order_ts_not_future','amount_matches_formula')"


def latest(con, dag):
    return con.execute("SELECT run_id FROM pipeline_runs WHERE dag_id = ? ORDER BY start_time DESC LIMIT 1", [dag]).fetchone()[0]


def s01():
    for label, (db, dag) in DAGS.items():
        con = duckdb.connect(db, read_only=True)
        tbl = "etl_daily_zone_revenue" if label == "ETL" else "mart_daily_store_kpis"
        print(label, "store S01 (revenue, delivered orders):",
              con.execute(f"SELECT round(sum(net_revenue),2), sum(delivered_orders) FROM {tbl} WHERE store_id = 'S01'").fetchone())
        print(label, "all stores revenue:", con.execute(f"SELECT round(sum(net_revenue),2) FROM {tbl}").fetchone()[0])
        rid = latest(con, dag)
        print(label, "quality results of the run:", con.execute("SELECT status, count(*) FROM dq_results WHERE run_id = ? GROUP BY 1 ORDER BY 1", [rid]).fetchall())


def five():
    for label, (db, dag) in DAGS.items():
        con = duckdb.connect(db, read_only=True)
        rid = latest(con, dag)
        print(f"\n== {label}: the five new rows ==")
        if label == "ETL":
            print(con.execute("SELECT row_key, failure_reason FROM etl_rejected_rows WHERE run_id = ? AND row_key LIKE 'ORDX%' ORDER BY 1", [rid]).fetchdf().to_string(index=False))
        else:
            print(con.execute("SELECT order_id, dq_status, failure_reason FROM stg_orders WHERE order_id LIKE 'ORDX%' ORDER BY 1").fetchdf().to_string(index=False))
        print(f"{label} dq_results for the relevant checks (orders only):")
        print(con.execute(f"SELECT check_name, rows_checked, rows_failed, status FROM dq_results WHERE run_id = ? AND check_name IN {CHECKS} ORDER BY 1", [rid]).fetchdf().to_string(index=False))


{"s01": s01, "5": five}.get((sys.argv + ["?"])[1], lambda: sys.exit("usage: s01 | 5"))()