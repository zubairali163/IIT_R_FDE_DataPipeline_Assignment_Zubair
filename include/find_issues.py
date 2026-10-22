"""Part 0, step 2: list the EXACT bad rows and save a draft Data Issues Log.
Run from the project root:   python find_issues.py
Output: issues_log_draft.csv  (open it in VS Code with Rainbow CSV)
Read-only on the data. You still need to review it and add your own findings.
"""
import pandas as pd
from pathlib import Path

D = Path(__file__).resolve().parent / "data" / "nammamart"
rd = lambda n: pd.read_csv(D / f"{n}.csv", dtype=str, keep_default_na=False)
orders, pay, cust, prod, stores = (rd(n) for n in ["orders", "payments", "customers", "products", "stores"])

issues = []  # each item = one problem on one row


def add(file, key, col, problem, dim, rule, sev):
    issues.append(dict(file=file, row_key=key, column=col, problem=problem,
                       quality_dimension=dim, proposed_rule=rule, severity=sev))


def flag(df, mask, file, keycol, col, problem, dim, rule, sev):
    for k in df.loc[mask, keycol]:
        add(file, k, col, problem, dim, rule, sev)


blank = lambda s: s.str.strip() == ""
num = lambda s: pd.to_numeric(s, errors="coerce")

# ---------- ORDERS ----------
o = orders
flag(o, o["order_id"].duplicated(keep=False), "orders.csv", "order_id", "order_id",
     "order_id appears more than once", "Uniqueness", "keep one row per order_id", "Critical")
flag(o, blank(o["customer_id"]), "orders.csv", "order_id", "customer_id",
     "customer_id is missing", "Completeness", "customer_id must not be null", "Critical")
ts = pd.to_datetime(o["order_ts"], errors="coerce")
flag(o, ts.isna(), "orders.csv", "order_id", "order_ts",
     "order_ts missing or not a valid timestamp", "Validity", "order_ts must parse", "Critical")
flag(o, ts > pd.Timestamp.now(), "orders.csv", "order_id", "order_ts",
     "order_ts is in the future", "Timeliness", "order_ts <= now", "Critical")
flag(o, num(o["quantity"]) <= 0, "orders.csv", "order_id", "quantity",
     "quantity zero or negative", "Accuracy", "quantity > 0", "Critical")
dp = num(o["discount_pct"])
flag(o, (dp < 0) | (dp > 100), "orders.csv", "order_id", "discount_pct",
     "discount outside 0-100", "Validity", "0 <= discount_pct <= 100", "Critical")
calc = (num(o["quantity"]) * num(o["unit_price"]) * (1 - dp / 100)).round(2)
flag(o, (calc - num(o["order_amount"])).abs() > 0.01, "orders.csv", "order_id", "order_amount",
     "order_amount does not match the formula", "Accuracy", "recompute and compare", "Critical")
st = o["order_status"].str.strip()
flag(o, ~st.isin(["DELIVERED", "CANCELLED", "RETURNED"]) & ~st.str.upper().isin(["DELIVERED", "CANCELLED", "RETURNED"]),
     "orders.csv", "order_id", "order_status", "status not in allowed list",
     "Validity", "status in DELIVERED/CANCELLED/RETURNED", "Critical")
flag(o, st.isin(["delivered"]), "orders.csv", "order_id", "order_status",
     "status has wrong casing", "Validity", "standardise to upper case", "Warning")
ok_modes = ["UPI", "Card", "Cash on Delivery", "Wallet"]
flag(o, ~o["payment_mode"].isin(ok_modes), "orders.csv", "order_id", "payment_mode",
     "payment_mode not exactly an allowed value (casing/spacing/typo)", "Validity",
     "map known variants; reject unknown", "Warning")
dm = num(o["delivery_minutes"])
flag(o, (st.str.upper() == "DELIVERED") & dm.isna(), "orders.csv", "order_id", "delivery_minutes",
     "DELIVERED order has no delivery_minutes", "Completeness", "required when DELIVERED", "Critical")
flag(o, dm > 120, "orders.csv", "order_id", "delivery_minutes",
     "delivery_minutes impossibly large", "Accuracy", "delivery_minutes < 120", "Critical")
flag(o, ~o["customer_id"].isin(cust["customer_id"]) & ~blank(o["customer_id"]), "orders.csv", "order_id",
     "customer_id", "customer_id not found in customers", "Consistency", "FK must exist", "Critical")
flag(o, ~o["product_id"].isin(prod["product_id"]), "orders.csv", "order_id", "product_id",
     "product_id not found in products", "Consistency", "FK must exist", "Critical")
flag(o, ~o["store_id"].isin(stores["store_id"]), "orders.csv", "order_id", "store_id",
     "store_id not found in stores", "Consistency", "FK must exist", "Critical")

# ---------- PAYMENTS ----------
p = pay
flag(p, p["payment_id"].duplicated(keep=False), "payments.csv", "payment_id", "payment_id",
     "payment_id appears more than once", "Uniqueness", "one row per payment_id", "Critical")
flag(p, ~p["order_id"].isin(orders["order_id"]), "payments.csv", "payment_id", "order_id",
     "payment refers to an order that does not exist", "Consistency", "FK must exist", "Critical")
flag(p, ~p["payment_mode"].isin(ok_modes), "payments.csv", "payment_id", "payment_mode",
     "payment_mode not exactly an allowed value", "Validity", "map known variants", "Warning")
m = p.merge(orders.drop_duplicates("order_id")[["order_id", "order_amount"]], on="order_id", how="left")
bad = (m["payment_status"] == "SUCCESS") & ((num(m["amount_paid"]) - num(m["order_amount"])).abs() > 0.01)
for k in m.loc[bad, "payment_id"]:
    add("payments.csv", k, "amount_paid", "SUCCESS payment differs from order_amount",
        "Consistency", "SUCCESS amount == order amount", "Critical")
flag(p, num(p["amount_paid"]) < 0, "payments.csv", "payment_id", "amount_paid",
     "negative amount_paid", "Accuracy", "amount_paid >= 0", "Critical")

# ---------- CUSTOMERS ----------
c = cust
flag(c, c["customer_id"].duplicated(keep=False), "customers.csv", "customer_id", "customer_id",
     "customer_id appears more than once", "Uniqueness", "one row per customer_id", "Critical")
flag(c, ~c["email"].str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), "customers.csv", "customer_id", "email",
     "email missing or badly formatted", "Validity", "valid email format", "Warning")
flag(c, ~c["phone"].str.fullmatch(r"\d{10}"), "customers.csv", "customer_id", "phone",
     "phone is not 10 digits", "Validity", "10 digits", "Warning")
flag(c, ~c["pincode"].str.fullmatch(r"\d{6}"), "customers.csv", "customer_id", "pincode",
     "pincode is not 6 digits", "Validity", "6 digits", "Warning")
flag(c, blank(c["city"]), "customers.csv", "customer_id", "city",
     "city is missing", "Completeness", "city should be present", "Warning")
flag(c, ~c["loyalty_tier"].isin(["Bronze", "Silver", "Gold", "Platinum"]), "customers.csv", "customer_id",
     "loyalty_tier", "loyalty_tier has inconsistent casing", "Consistency", "standardise to Title case", "Warning")
flag(c, pd.to_datetime(c["signup_date"], errors="coerce") > pd.Timestamp.now(), "customers.csv", "customer_id",
     "signup_date", "signup_date is in the future", "Timeliness", "signup_date <= today", "Critical")

# ---------- PRODUCTS ----------
r = prod
flag(r, r["product_id"].duplicated(keep=False), "products.csv", "product_id", "product_id",
     "product_id appears more than once", "Uniqueness", "one row per product_id", "Critical")
flag(r, blank(r["category"]), "products.csv", "product_id", "category",
     "category is missing", "Completeness", "category not null", "Critical")
flag(r, num(r["mrp"]) <= 0, "products.csv", "product_id", "mrp",
     "mrp is zero or negative", "Accuracy", "mrp > 0", "Critical")
flag(r, num(r["cost_price"]) > num(r["mrp"]), "products.csv", "product_id", "cost_price",
     "cost_price is higher than mrp", "Accuracy", "cost_price <= mrp", "Critical")

# ---------- STORES ----------
s = stores
flag(s, blank(s["zone"]), "stores.csv", "store_id", "zone",
     "zone is missing", "Completeness", "zone not null", "Critical")
flag(s, num(s["capacity_orders_per_day"]) <= 0, "stores.csv", "store_id", "capacity_orders_per_day",
     "capacity is zero", "Accuracy", "capacity > 0", "Warning")

log = pd.DataFrame(issues)
log.to_csv("issues_log_draft.csv", index=False)
print(f"Saved {len(log)} issue rows to issues_log_draft.csv\n")
print(log.groupby(["file", "column", "problem"]).size().to_string())
