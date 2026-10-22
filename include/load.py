import re
from datetime import datetime, timedelta
import duckdb
import pandas as pd
from pathlib import Path


def load(run_id, base, who):
    safe = re.sub(r"[^\w\-]", "_", run_id)
    work = Path(base) / "include" / "work" / safe
    db = Path(base) / "include" / f"warehouse_{who}.duckdb"
    src = lambda name: f"read_csv_auto('{work / (name + '.csv')}', sample_size=-1)"

    con = duckdb.connect(str(db))
    try:
        con.execute("BEGIN")

        # dimensions: full snapshot, replace
        for t in ["dim_customer", "dim_product", "dim_store"]:
            con.execute(f"CREATE OR REPLACE TABLE etl_{t} AS SELECT * FROM {src(t)}")

        # fact: delete incoming order_ids, then insert (idempotent upsert)
        con.execute(f"CREATE TABLE IF NOT EXISTS etl_fact_orders AS SELECT * FROM {src('fact_orders')} WHERE 1=0")
        #con.execute(f"DELETE FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM {src('fact_orders')})")
        #con.execute(f"INSERT INTO etl_fact_orders BY NAME SELECT * FROM {src('fact_orders')}")

        # dq scorecard: history, but a retry of the same run_id must not double it
        dq = pd.concat([pd.read_csv(p) for p in work.glob("dq_results_*.csv")])
        con.register("dq_df", dq)
        con.execute("CREATE TABLE IF NOT EXISTS dq_results AS SELECT * FROM dq_df WHERE 1=0")
        con.execute("DELETE FROM dq_results WHERE run_id = ?", [run_id])
        con.execute("INSERT INTO dq_results BY NAME SELECT * FROM dq_df")
        con.execute(f"DELETE FROM etl_fact_orders WHERE source_file = 'orders.csv' AND order_id IN (SELECT order_id FROM {src('fact_orders')})")
        con.execute(f"INSERT INTO etl_fact_orders BY NAME SELECT * FROM {src('fact_orders')} WHERE order_id NOT IN (SELECT order_id FROM etl_fact_orders)")
        # extra Warning checks: volume drop and stale data
        dag_id = str(dq["dag_id"].iloc[0])
        cur = int(dq.loc[dq["check_name"] == "order_id_unique", "rows_checked"].iloc[0])
        prev = con.execute(
            "SELECT rows_checked FROM dq_results WHERE check_name = 'order_id_unique' "
            "AND dag_id = ? AND run_id <> ? ORDER BY checked_at DESC LIMIT 1", [dag_id, run_id]).fetchone()
        dropped = prev is not None and cur < 0.1 * prev[0]
        newest = con.execute("SELECT max(order_ts) FROM etl_fact_orders").fetchone()[0]
        stale = newest is None or newest < datetime.now() - timedelta(days=1)
        now = datetime.now().isoformat()
        extra = pd.DataFrame([
            dict(run_id=run_id, dag_id=dag_id, check_name="volume_drop_over_90pct", dimension="Completeness",
                 severity="Warning", rows_checked=cur, rows_failed=int(prev[0] - cur) if dropped else 0,
                 status="WARN" if dropped else "PASS", checked_at=now),
            dict(run_id=run_id, dag_id=dag_id, check_name="latest_order_older_than_1_day", dimension="Timeliness",
                 severity="Warning", rows_checked=1, rows_failed=1 if stale else 0,
                 status="WARN" if stale else "PASS", checked_at=now),
        ])
        con.register("extra_df", extra)
        con.execute("INSERT INTO dq_results BY NAME SELECT * FROM extra_df")
        con.execute("COMMIT")
        # rejected rows, one line per row and reason, so "what was rejected today?" is one query
        parts = []
        for name in ["orders", "payments", "customers", "products", "stores"]:
            p = work / f"{name}_failed.csv"
            if p.exists() and p.stat().st_size > 0:
                f = pd.read_csv(p, dtype=str, keep_default_na=False)
                if len(f):
                    parts.append(pd.DataFrame({"run_id": run_id, "file": name,
                                               "row_key": f.iloc[:, 0],
                                               "failure_reason": f["failure_reason"].str.strip().str.rstrip(";").str.strip()}))
        rej = pd.concat(parts) if parts else pd.DataFrame(columns=["run_id", "file", "row_key", "failure_reason"])
        con.register("rej_df", rej)
        con.execute("CREATE TABLE IF NOT EXISTS etl_rejected_rows AS SELECT * FROM rej_df WHERE 1=0")
        con.execute("DELETE FROM etl_rejected_rows WHERE run_id = ?", [run_id])
        con.execute("INSERT INTO etl_rejected_rows BY NAME SELECT * FROM rej_df")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()

    con = duckdb.connect(str(db), read_only=True)
    rows, rev = con.execute("SELECT count(*), round(sum(net_revenue), 2) FROM etl_fact_orders").fetchone()
    con.close()
    print(f"warehouse: etl_fact_orders has {rows} rows, net revenue {rev}")
    return {"rows_loaded": int(rows), "net_revenue": float(rev)}