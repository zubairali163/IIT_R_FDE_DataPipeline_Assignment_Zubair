import duckdb

CFG = [("ETL", "include/warehouse_Zubair.duckdb", "nammamart_etl_Zubair"),
       ("ELT", "include/warehouse_Zubair_elt.duckdb", "nammamart_elt_Zubair")]
show = lambda con, sql, p=None: con.execute(sql, p or []).fetchdf().to_string(index=False)

for label, db, dag in CFG:
    con = duckdb.connect(db, read_only=True)
    rid = con.execute("SELECT run_id FROM pipeline_runs WHERE dag_id = ? ORDER BY start_time DESC LIMIT 1", [dag]).fetchone()[0]
    print(f"\n########## {label}: pipeline_runs (latest 4) ##########")
    print(show(con, "SELECT run_id, start_time, rows_extracted, rows_loaded, rows_quarantined, status FROM pipeline_runs "
                    "WHERE dag_id = ? ORDER BY start_time DESC LIMIT 4", [dag]))
    print(f"\n########## {label}: dq_results of run {rid} ##########")
    print(show(con, "SELECT check_name, dimension, severity, rows_checked, rows_failed, status FROM dq_results "
                    "WHERE run_id = ? ORDER BY dimension, check_name", [rid]))
    print(f"\n########## {label}: payment reconciliation by reason ##########")
    if label == "ETL":
        sql = ("SELECT reason, count(*) AS orders, round(sum(abs(difference)),2) AS amount FROM etl_payment_reconciliation "
               "WHERE run_id = ? GROUP BY 1 ORDER BY 1")
        print(show(con, sql, [rid]))
    else:
        print(show(con, "SELECT reason, count(*) AS orders, round(sum(abs(difference)),2) AS amount "
                        "FROM mart_payment_reconciliation GROUP BY 1 ORDER BY 1"))