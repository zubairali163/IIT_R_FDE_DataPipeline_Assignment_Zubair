import re
import duckdb
import pandas as pd
from pathlib import Path


def reconcile(run_id, base, who):
    safe = re.sub(r"[^\w\-]", "_", run_id)
    work = Path(base) / "include" / "work" / safe
    data = Path(base) / "include" / "data" / "nammamart"
    db = Path(base) / "include" / f"warehouse_{who}.duckdb"

    pay = pd.read_csv(work / "payments_passed.csv", dtype=str, keep_default_na=False)
    raw_pay = pd.read_csv(data / "payments.csv", dtype=str, keep_default_na=False)
    raw_order_ids = set(raw_pay["order_id"].str.strip())
    # orders covered by the payments feed = orders in the base export
    base_orders = set(pd.read_csv(data / "orders.csv", dtype=str, keep_default_na=False,
                                  usecols=["order_id"])["order_id"].str.strip())
    pay["amount_paid"] = pd.to_numeric(pay["amount_paid"], errors="coerce")
    pay = pay.drop_duplicates("order_id")
    pay = pay.rename(columns={"payment_mode": "pay_mode"})

    con = duckdb.connect(str(db))
    fact_all = con.execute(
        "SELECT order_id, order_status, order_amount, payment_mode FROM etl_fact_orders").df()
    fact = fact_all[fact_all["order_id"].isin(base_orders)]
    print(f"orders outside the payment feed (not reconciled yet): {len(fact_all) - len(fact)}")

    m = fact.merge(pay[["order_id", "payment_id", "payment_status", "amount_paid", "pay_mode"]],
                   on="order_id", how="left")
    delivered = m["order_status"] == "DELIVERED"
    no_pay = m["payment_id"].isna()
    diff = (m["amount_paid"] - m["order_amount"]).abs() > 0.01

    rules = [
        ("payment_quarantined",          delivered & no_pay & m["order_id"].isin(raw_order_ids)),
        ("missing_payment",              delivered & no_pay & ~m["order_id"].isin(raw_order_ids)),
        ("amount_mismatch",              delivered & (m["payment_status"] == "SUCCESS") & diff),
        ("payment_failed_but_delivered", delivered & (m["payment_status"] == "FAILED")),
        ("refunded_but_delivered",       delivered & (m["payment_status"] == "REFUNDED")),
        ("returned_not_refunded",        (m["order_status"] == "RETURNED") & (m["payment_status"] == "SUCCESS")),
        ("mode_mismatch",                ~no_pay & (m["payment_mode"] != m["pay_mode"])),
    ]
    parts = []
    for reason, mask in rules:
        p = m[mask][["order_id", "payment_id", "order_amount", "amount_paid"]].copy()
        p["reason"] = reason
        parts.append(p)

    orphans = pay[~pay["order_id"].isin(fact_all["order_id"])][["order_id", "payment_id", "amount_paid"]].copy()
    orphans["order_amount"] = None
    orphans["reason"] = orphans["order_id"].apply(
        lambda x: "order_quarantined" if x in base_orders else "payment_for_unknown_order")
    parts.append(orphans)

    out = pd.concat(parts, ignore_index=True)
    out["difference"] = out["amount_paid"].fillna(0) - out["order_amount"].astype(float).fillna(0)
    failed = out["reason"].isin(["payment_failed_but_delivered", "refunded_but_delivered"])
    out.loc[failed, "difference"] = -out.loc[failed, "order_amount"].astype(float)
    out["run_id"] = run_id

    con.register("rec", out)
    con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM rec")
    summary = out.groupby("reason").agg(rows=("order_id", "count"), amount=("difference", "sum")).round(2)
    print(summary.to_string())
    total = round(float(out[out["reason"] != "order_quarantined"]["difference"].abs().sum()), 2)
    print("total amount that does not reconcile:", total)
    con.close()
    return {"mismatches": int(len(out)), "unreconciled_amount": total}