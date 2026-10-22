import duckdb
from pathlib import Path
from include.elt_raw import load_raw

MERGE_FILE = "merge_stg_orders.sql"


def run_merge(run_id, base, who, source_file):
    sqld = Path(base) / "include" / "sql"
    # 1. raw: append the increment rows exactly as received
    raw = load_raw("orders", run_id, base, who, source_file=source_file)

    con = duckdb.connect(str(Path(base) / "include" / f"warehouse_{who}_elt.duckdb"))
    # 2. staging-shaped table built only from the increment rows
    tmpl = (sqld / "stg_orders.sql").read_text()
    head = "CREATE OR REPLACE TABLE stg_orders AS"
    reader = "SELECT *, rowid AS _rid FROM raw_orders"
    assert head in tmpl and reader in tmpl, "stg_orders.sql changed shape - update run_merge"
    safe_src = source_file.replace("'", "''")
    con.execute(tmpl.replace(head, "CREATE OR REPLACE TABLE stg_orders_delta AS")
                    .replace(reader, f"{reader} WHERE _source_file = '{safe_src}'"))

    total = con.execute("SELECT count(*) FROM stg_orders_delta").fetchone()[0]
    updates = con.execute(
        "SELECT count(*) FROM stg_orders_delta WHERE order_id IN (SELECT order_id FROM stg_orders)").fetchone()[0]
    status_changed = con.execute(
        "SELECT count(*) FROM stg_orders_delta d JOIN stg_orders t USING (order_id) "
        "WHERE d.order_status <> t.order_status").fetchone()[0]

    # 3. upsert into staging
    con.execute((sqld / MERGE_FILE).read_text())

    rows = con.execute("SELECT count(*), count(DISTINCT order_id) FROM stg_orders").fetchone()
    con.close()
    print(f"increment rows: {total} | updated existing: {updates} (status changed: {status_changed}) "
          f"| inserted new: {total - updates}")
    print(f"stg_orders now: {rows[0]} rows, {rows[1]} distinct order_ids")
    return {"raw_rows": raw["rows"], "updated": int(updates), "inserted": int(total - updates),
            "stg_rows": int(rows[0])}