"""Experiment helper for Part F.  Run from the project root:
   python include\\exp.py backup | break <4|5|6|7|8|9> | restore | snap
Never edits the originals without a backup first."""
import sys, shutil
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

DATA = Path("include/data/nammamart")
BAK = Path("include/data/_original")
CSVS = ["orders", "payments", "customers", "products", "stores"]
TS = "%Y-%m-%d %H:%M:%S"


def backup():
    BAK.mkdir(parents=True, exist_ok=True)
    for n in CSVS:
        if not (BAK / f"{n}.csv").exists():
            shutil.copyfile(DATA / f"{n}.csv", BAK / f"{n}.csv")
    print("originals are safe in", BAK)


def restore():
    for n in CSVS:
        shutil.copyfile(BAK / f"{n}.csv", DATA / f"{n}.csv")
    for h in DATA.glob("*.hidden"):
        h.unlink()
    print("restored all five files from", BAK)


def rd(n):
    return pd.read_csv(BAK / f"{n}.csv", dtype=str, keep_default_na=False)


def wr(df, n):
    df.to_csv(DATA / f"{n}.csv", index=False)


def brk(x):
    if not BAK.exists() or not all((BAK / f"{n}.csv").exists() for n in CSVS):
        sys.exit("run 'backup' first")
    restore()                                   # always start from clean originals
    o = rd("orders")
    if x == "4":                                # missing source file
        (DATA / "products.csv").rename(DATA / "products.csv.hidden")
        print("products.csv hidden")
    elif x == "5":                              # five new bad rows
        base = o[(o["order_status"] == "DELIVERED") & (o["customer_id"] != "")].iloc[0].copy()
        rows = []
        for i, (col, val) in enumerate([("customer_id", ""), ("quantity", "-1"), ("store_id", "S99"),
                                         ("order_status", "SHIPPED"), ("order_ts", "2027-01-01 10:00:00")]):
            r = base.copy()
            r["order_id"] = f"ORDX{i + 1:05d}"
            r[col] = val
            rows.append(r)
        wr(pd.concat([o, pd.DataFrame(rows)], ignore_index=True), "orders")
        print("added ORDX00001..ORDX00005 (null customer, negative quantity, unknown store, SHIPPED, 2027)")
    elif x == "6":                              # schema change
        wr(o.rename(columns={"order_amount": "total"}), "orders")
        print("order_amount renamed to total")
    elif x == "7":                              # volume drop
        wr(o.head(50), "orders")
        print("orders.csv cut to 50 rows")
    elif x == "8":                              # distribution drift, consistent so no rule fires
        m = o["store_id"].str.strip() == "S01"
        for c in ["unit_price", "order_amount"]:
            v = pd.to_numeric(o.loc[m, c], errors="coerce") * 100
            o.loc[m, c] = v.round(2).astype(str).where(v.notna(), o.loc[m, c])
        wr(o, "orders")
        print(f"unit_price and order_amount x100 for store S01 ({int(m.sum())} rows)")
    elif x == "9":                              # stale data
        t = pd.to_datetime(o["order_ts"], errors="coerce")
        delta = (datetime.now() - timedelta(days=30)) - t.max()
        o["order_ts"] = [(a + delta).strftime(TS) if pd.notna(a) else b for a, b in zip(t, o["order_ts"])]
        wr(o, "orders")
        print("order_ts shifted so the newest order is 30 days old")
    else:
        sys.exit("break takes 4, 5, 6, 7, 8 or 9")


def snap():
    import duckdb
    for label, db, dag in [("ETL", "include/warehouse_Zubair.duckdb", "nammamart_etl_Zubair"),
                           ("ELT", "include/warehouse_Zubair_elt.duckdb", "nammamart_elt_Zubair")]:
        con = duckdb.connect(db, read_only=True)
        print(f"\n===== {label} =====")
        print(con.execute("SELECT run_id, rows_extracted, rows_loaded, rows_quarantined, status FROM pipeline_runs "
                          "WHERE dag_id = ? ORDER BY start_time DESC LIMIT 2", [dag]).fetchdf().to_string(index=False))
        rid = con.execute("SELECT run_id FROM pipeline_runs WHERE dag_id = ? ORDER BY start_time DESC LIMIT 1", [dag]).fetchone()
        rid = rid[0] if rid else ""
        print("dq_results of that run:", con.execute("SELECT status, count(*) FROM dq_results WHERE run_id = ? GROUP BY 1 ORDER BY 1", [rid]).fetchall())
        print(con.execute("SELECT check_name, rows_checked, rows_failed, status FROM dq_results WHERE run_id = ? AND "
                          "(status <> 'PASS' AND check_name IN ('volume_drop_over_90pct','latest_order_older_than_1_day'))",
                          [rid]).fetchdf().to_string(index=False))
        if label == "ETL":
            print("etl_fact_orders:", con.execute("SELECT count(*), round(sum(net_revenue),2) FROM etl_fact_orders").fetchone())
        else:
            print("raw_orders:", con.execute("SELECT count(*) FROM raw_orders").fetchone()[0],
                  "| stg_orders:", con.execute("SELECT count(*), sum((dq_status='PASS')::INT) FROM stg_orders").fetchone())
        con.close()


if __name__ == "__main__":
    a = sys.argv[1:] or ["?"]
    {"backup": backup, "restore": restore, "snap": snap}.get(a[0], lambda: brk(a[1]) if a[0] == "break" and len(a) > 1 else sys.exit(__doc__))()