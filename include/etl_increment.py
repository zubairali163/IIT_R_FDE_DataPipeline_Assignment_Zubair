import re
import duckdb
import pandas as pd
from pathlib import Path
from include.quality import standardise, order_checks, run_checks

num = lambda s: pd.to_numeric(s, errors="coerce")


def etl_increment(run_id, base, who, source_file, dag_id):
    safe = re.sub(r"[^\w\-]", "_", run_id)
    base = Path(base)
    raw = pd.read_csv(base / "include" / "data" / "nammamart" / source_file,
                      dtype=str, keep_default_na=False)
    df = standardise(raw, "orders")

    con = duckdb.connect(str(base / "include" / f"warehouse_{who}.duckdb"))
    ids = lambda t, c: set(con.execute(f"SELECT {c} FROM {t}").df()[c])
    checks = order_checks(ids("etl_dim_customer", "customer_id"),
                          ids("etl_dim_product", "product_id"),
                          ids("etl_dim_store", "store_id"))
    passed, failed, results = run_checks(df, checks, dag_id, run_id)

    quar = base / "include" / "quarantine" / who
    quar.mkdir(parents=True, exist_ok=True)
    failed.to_csv(quar / f"orders_increment_failed_{safe}.csv", index=False)

    # same derivations as the main transform
    cost = con.execute("SELECT product_id, cost_price FROM etl_dim_product").df()
    f = passed.copy()
    for c in ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"]:
        f[c] = num(f[c])
    f["order_ts"] = pd.to_datetime(f["order_ts"])
    f["order_date"] = f["order_ts"].dt.date.astype(str)
    f = f.merge(cost, on="product_id", how="left")
    assert f["cost_price"].notna().all(), "an order has no matching product"
    delivered = f["order_status"] == "DELIVERED"
    f["net_revenue"] = f["order_amount"].where(delivered, 0.0)
    f["gross_margin"] = (f["net_revenue"] - f["quantity"] * f["cost_price"]).where(delivered, 0.0)
    f["within_15_min"] = delivered & (f["delivery_minutes"] <= 15)
    f = f.drop(columns=["cost_price"])
    f["source_file"] = source_file
    assert len(f) == len(passed), "rows were lost in the transform"

    # how many are updates vs inserts, and how many really changed status
    old = con.execute("SELECT order_id, order_status FROM etl_fact_orders").df()
    m = f.merge(old, on="order_id", suffixes=("_new", "_old"))
    updated, changed = len(m), int((m["order_status_new"] != m["order_status_old"]).sum())

    con.register("inc", f)
    con.register("dq_df", pd.DataFrame(results))
    con.execute("BEGIN")
    try:
        con.execute("DELETE FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM inc)")
        con.execute("INSERT INTO etl_fact_orders BY NAME SELECT * FROM inc")
        con.execute("DELETE FROM dq_results WHERE run_id = ?", [run_id])
        con.execute("INSERT INTO dq_results BY NAME SELECT * FROM dq_df")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    rows = con.execute("SELECT count(*), count(DISTINCT order_id) FROM etl_fact_orders").fetchone()
    con.close()

    print(f"increment rows: {len(raw)} | passed: {len(passed)} | quarantined: {len(failed)}")
    print(f"updated existing: {updated} (status changed: {changed}) | inserted new: {len(f) - updated}")
    print(f"etl_fact_orders now: {rows[0]} rows, {rows[1]} distinct order_ids")
    return {"rows_in": len(raw), "passed": len(passed), "quarantined": len(failed),
            "updated": updated, "inserted": len(f) - updated}