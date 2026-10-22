import pandas as pd

ALLOWED_STATUS = ["DELIVERED", "CANCELLED", "RETURNED"]
ALLOWED_MODES = ["UPI", "Card", "Cash on Delivery", "Wallet"]
MODE_MAP = {"upi": "UPI", "card": "Card", "cod": "Cash on Delivery",
            "cash on delivery": "Cash on Delivery", "wallet": "Wallet"}
# "Cashh" is deliberately NOT mapped: we don't guess. It will be quarantined
# until the client confirms what it means.

num = lambda s: pd.to_numeric(s, errors="coerce")


def standardise_orders(df):
    """FIX step: trim spaces, fix casing. Never changes the meaning of a value."""
    df = df.copy()
    for c in df.columns:
        df[c] = df[c].str.strip()
    df["order_status"] = df["order_status"].str.upper()
    df["payment_mode"] = df["payment_mode"].str.lower().map(MODE_MAP).fillna(df["payment_mode"])
    return df


def order_checks(customer_ids, product_ids, store_ids):
    """Each check = (name, dimension, severity, function returning True for BAD rows)."""
    calc = lambda d: (num(d.quantity) * num(d.unit_price) * (1 - num(d.discount_pct) / 100)).round(2)
    return [
        ("order_id_unique",       "Uniqueness",   "Critical", lambda d: d.duplicated("order_id", keep="first")),
        ("customer_id_not_null",  "Completeness", "Critical", lambda d: d.customer_id == ""),
        ("delivered_has_minutes", "Completeness", "Critical", lambda d: (d.order_status == "DELIVERED") & (d.delivery_minutes == "")),
        ("status_allowed",        "Validity",     "Critical", lambda d: ~d.order_status.isin(ALLOWED_STATUS)),
        ("payment_mode_allowed",  "Validity",     "Critical", lambda d: ~d.payment_mode.isin(ALLOWED_MODES)),
        ("order_ts_parses",       "Validity",     "Critical", lambda d: pd.to_datetime(d.order_ts, errors="coerce").isna()),
        ("discount_0_to_100",     "Validity",     "Critical", lambda d: ~num(d.discount_pct).between(0, 100)),
        ("quantity_positive",     "Accuracy",     "Critical", lambda d: ~(num(d.quantity) > 0)),
        ("amount_matches_formula","Accuracy",     "Critical", lambda d: ~((calc(d) - num(d.order_amount)).abs() <= 0.01)),
        ("delivery_under_120",    "Accuracy",     "Critical", lambda d: num(d.delivery_minutes) >= 120),
        ("customer_exists",       "Consistency",  "Critical", lambda d: (d.customer_id != "") & ~d.customer_id.isin(customer_ids)),
        ("product_exists",        "Consistency",  "Critical", lambda d: ~d.product_id.isin(product_ids)),
        ("store_exists",          "Consistency",  "Critical", lambda d: ~d.store_id.isin(store_ids)),
        ("order_ts_not_future",   "Timeliness",   "Critical", lambda d: pd.to_datetime(d.order_ts, errors="coerce") > pd.Timestamp.now()),
    ]


def run_checks(df, checks, dag_id, run_id):
    """Returns (passed_rows, failed_rows_with_reason, list_of_dq_result_dicts)."""
    reason = pd.Series("", index=df.index)
    results = []
    for name, dim, sev, is_bad in checks:
        bad = is_bad(df).fillna(True)
        n_bad = int(bad.sum())
        results.append(dict(
            run_id=run_id, dag_id=dag_id, check_name=name, dimension=dim, severity=sev,
            rows_checked=len(df), rows_failed=n_bad,
            status="PASS" if n_bad == 0 else ("FAIL" if sev == "Critical" else "WARN"),
            checked_at=pd.Timestamp.now().isoformat()))
        if sev == "Critical":                      # only Critical rows get quarantined
            reason[bad] = reason[bad] + name + "; "
    failed_mask = reason != ""
    failed = df[failed_mask].assign(failure_reason=reason[failed_mask])
    return df[~failed_mask], failed, results

import re
from pathlib import Path

REGEX_EMAIL = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
TIERS = ["Bronze", "Silver", "Gold", "Platinum"]


def standardise(df, name):
    """FIX step for any file: trim spaces, fix casing, map known variants."""
    df = df.copy()
    for c in df.columns:
        df[c] = df[c].str.strip()
    if name == "orders":
        df["order_status"] = df["order_status"].str.upper()
    if name == "payments":
        df["payment_status"] = df["payment_status"].str.upper()
    if name in ("orders", "payments"):
        df["payment_mode"] = df["payment_mode"].str.lower().map(MODE_MAP).fillna(df["payment_mode"])
    if name == "customers":
        df["loyalty_tier"] = df["loyalty_tier"].str.title()
    return df


def customer_checks(r):
    return [
        ("customer_id_unique", "Uniqueness",   "Critical", lambda d: d.duplicated("customer_id", keep="first")),
        ("signup_not_future",  "Timeliness",   "Critical", lambda d: pd.to_datetime(d.signup_date, errors="coerce") > pd.Timestamp.now()),
        ("email_format",       "Validity",     "Warning",  lambda d: ~d.email.str.match(REGEX_EMAIL)),
        ("phone_10_digits",    "Validity",     "Warning",  lambda d: ~d.phone.str.fullmatch(r"\d{10}")),
        ("pincode_6_digits",   "Validity",     "Warning",  lambda d: ~d.pincode.str.fullmatch(r"\d{6}")),
        ("city_not_null",      "Completeness", "Warning",  lambda d: d.city == ""),
        ("tier_allowed",       "Consistency",  "Warning",  lambda d: ~d.loyalty_tier.isin(TIERS)),
    ]


def product_checks(r):
    return [
        ("product_id_unique", "Uniqueness",   "Critical", lambda d: d.duplicated("product_id", keep="first")),
        ("category_not_null", "Completeness", "Critical", lambda d: d.category == ""),
        ("mrp_positive",      "Accuracy",     "Critical", lambda d: ~(num(d.mrp) > 0)),
        ("cost_le_mrp",       "Accuracy",     "Critical", lambda d: ~(num(d.cost_price) <= num(d.mrp))),
    ]


def store_checks(r):
    return [
        ("store_id_unique",  "Uniqueness",   "Critical", lambda d: d.duplicated("store_id", keep="first")),
        ("zone_not_null",    "Completeness", "Critical", lambda d: d.zone == ""),
        ("capacity_positive","Accuracy",     "Warning",  lambda d: ~(num(d.capacity_orders_per_day) > 0)),
    ]


def payment_checks(r):
    return [
        ("payment_id_unique",    "Uniqueness",   "Critical", lambda d: d.duplicated("payment_id", keep="first")),
        ("payment_mode_allowed", "Validity",     "Critical", lambda d: ~d.payment_mode.isin(ALLOWED_MODES)),
        ("amount_not_negative",  "Accuracy",     "Critical", lambda d: ~(num(d.amount_paid) >= 0)),
        ("order_exists",         "Consistency",  "Warning",  lambda d: ~d.order_id.isin(r["order_ids"])),
        ("gateway_ref_unless_cod","Completeness","Warning",  lambda d: (d.payment_mode != "Cash on Delivery") & (d.gateway_ref == "")),
    ]


CHECKS = {
    "orders":    lambda r: order_checks(r["customer_ids"], r["product_ids"], r["store_ids"]),
    "payments":  payment_checks,
    "customers": customer_checks,
    "products":  product_checks,
    "stores":    store_checks,
}


def validate_file(name, dag_id, run_id, base, who):
    safe = re.sub(r"[^\w\-]", "_", run_id)
    #data = Path(base) / "include" / "data" / "nammamart"
    data = Path(base) / "include" / "work" / safe / "input"   # the frozen copy made by extract
    rd = lambda n: pd.read_csv(data / f"{n}.csv", dtype=str, keep_default_na=False)
    #ids = lambda n, col: set(rd(n)[col].str.strip())
    #ref = {"customer_ids": ids("customers", "customer_id"), "product_ids": ids("products", "product_id"),
        #   "store_ids": ids("stores", "store_id"), "order_ids": ids("orders", "order_id")}

    work = Path(base) / "include" / "work" / safe
    ids = lambda n, col: set(rd(n)[col].str.strip())

    def master_ids(n, col):
        if name == "orders":   # orders are checked against masters that PASSED
            # no fallback on purpose: if the file is missing we want a loud failure
            return set(pd.read_csv(work / f"{n}_passed.csv", dtype=str,
                                   keep_default_na=False)[col].str.strip())
        return ids(n, col)

    ref = {"customer_ids": master_ids("customers", "customer_id"),
           "product_ids": master_ids("products", "product_id"),
           "store_ids": master_ids("stores", "store_id"),
           "order_ids": ids("orders", "order_id")}

    df = standardise(rd(name), name)
    passed, failed, results = run_checks(df, CHECKS[name](ref), dag_id, run_id)

    #work = Path(base) / "include" / "work" / safe
    #quar = Path(base) / "include" / "quarantine" / who
    work.mkdir(parents=True, exist_ok=True)
    #quar.mkdir(parents=True, exist_ok=True)
    passed.to_csv(work / f"{name}_passed.csv", index=False)
    #failed.to_csv(quar / f"{name}_failed_{safe}.csv", index=False)
    failed.to_csv(work / f"{name}_failed.csv", index=False)
    pd.DataFrame(results).to_csv(work / f"dq_results_{name}.csv", index=False)
    print(f"{name}: in={len(df)} passed={len(passed)} quarantined={len(failed)}")
    return {"file": name, "rows_in": len(df), "rows_passed": len(passed), "rows_quarantined": len(failed)}