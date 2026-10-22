"""Part 0 - Data profiling for NammaMart. Run from the project root:
    python profile_data.py
Reads CSVs from include/data/ and prints findings. Read-only: never edits the data.
"""
import pandas as pd
from pathlib import Path

D = Path("include/data/")
f = {n: pd.read_csv(D / f"{n}.csv", dtype=str, keep_default_na=False)
     for n in ["orders", "payments", "customers", "products", "stores"]}
inc = pd.read_csv(D / "orders_2026-10-01_increment.csv", dtype=str, keep_default_na=False)
f["orders_increment"] = inc

def blank(s):  # treat "", "NULL", "nan" as missing
    return s.str.strip().str.lower().isin(["", "null", "none", "nan", "n/a"])

print("=" * 70, "\n1. BASIC PROFILE\n", "=" * 70)
for name, df in f.items():
    print(f"\n--- {name}: {len(df)} rows, {len(df.columns)} cols")
    print("Missing per column:", {c: int(blank(df[c]).sum()) for c in df.columns if blank(df[c]).sum()})
    for c in df.columns:
        num = pd.to_numeric(df[c], errors="coerce")
        if num.notna().sum() > 0.8 * len(df):
            print(f"  {c}: min={num.min()} max={num.max()}  non-numeric={int((num.isna() & ~blank(df[c])).sum())}")

keys = {"orders": "order_id", "payments": "payment_id", "customers": "customer_id",
        "products": "product_id", "stores": "store_id"}
print("\n", "=" * 70, "\n2. DUPLICATE KEYS\n", "=" * 70)
for n, k in keys.items():
    print(f"{n}.{k}: {int(f[n][k].duplicated().sum())} duplicate rows")
print("payments.order_id duplicates:", int(f["payments"]["order_id"].duplicated().sum()))

print("\n", "=" * 70, "\n3. DISTINCT VALUES (look for casing/spacing variants)\n", "=" * 70)
for n, c in [("orders", "order_status"), ("orders", "payment_mode"), ("orders", "channel"),
             ("payments", "payment_mode"), ("payments", "payment_status"),
             ("customers", "loyalty_tier"), ("customers", "is_active"),
             ("stores", "zone"), ("products", "category")]:
    print(f"\n{n}.{c}:", f[n][c].value_counts(dropna=False).to_dict())

print("\n", "=" * 70, "\n4. REFERENTIAL INTEGRITY\n", "=" * 70)
o = f["orders"]
for col, ref, k in [("customer_id", "customers", "customer_id"),
                    ("product_id", "products", "product_id"),
                    ("store_id", "stores", "store_id")]:
    bad = ~o[col].str.strip().isin(f[ref][k].str.strip())
    print(f"orders.{col} not in {ref}: {int(bad.sum())}")
p_bad = ~f["payments"]["order_id"].isin(o["order_id"])
print("payments.order_id not in orders:", int(p_bad.sum()))

print("\n", "=" * 70, "\n5. BUSINESS RULES\n", "=" * 70)
q = pd.to_numeric(o["quantity"], errors="coerce")
up = pd.to_numeric(o["unit_price"], errors="coerce")
dp = pd.to_numeric(o["discount_pct"], errors="coerce")
amt = pd.to_numeric(o["order_amount"], errors="coerce")
calc = (q * up * (1 - dp / 100)).round(2)
print("quantity <= 0:", int((q <= 0).sum()))
print("discount outside 0-100:", int(((dp < 0) | (dp > 100)).sum()))
print("order_amount != formula:", int(((calc - amt).abs() > 0.01).sum()))
st = o["order_status"].str.strip().str.upper()
dm = pd.to_numeric(o["delivery_minutes"], errors="coerce")
print("DELIVERED without delivery_minutes:", int(((st == "DELIVERED") & dm.isna()).sum()))
print("delivery_minutes > 120:", int((dm > 120).sum()), "| outside 8-35:", int(((dm < 8) | (dm > 35)).sum()))
ts = pd.to_datetime(o["order_ts"], errors="coerce")
print("unparseable order_ts:", int(ts.isna().sum()), "| future order_ts:", int((ts > pd.Timestamp.now()).sum()))
print("order_ts range:", ts.min(), "->", ts.max())

pm = f["payments"].merge(o[["order_id", "order_amount"]], on="order_id", how="left")
succ = pm[pm["payment_status"].str.strip().str.upper() == "SUCCESS"]
diff = (pd.to_numeric(succ["amount_paid"], errors="coerce") - pd.to_numeric(succ["order_amount"], errors="coerce")).abs() > 0.01
print("SUCCESS payments with amount != order_amount:", int(diff.sum()))

pr = f["products"]
mrp = pd.to_numeric(pr["mrp"], errors="coerce"); cp = pd.to_numeric(pr["cost_price"], errors="coerce")
print("products mrp <= 0:", int((mrp <= 0).sum()), "| cost_price > mrp:", int((cp > mrp).sum()))

c = f["customers"]
print("bad email format:", int((~c["email"].str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")).sum()))
print("phone not 10 digits:", int((~c["phone"].str.fullmatch(r"\d{10}")).sum()))
print("pincode not 6 digits:", int((~c["pincode"].str.fullmatch(r"\d{6}")).sum()))
print("future signup_date:", int((pd.to_datetime(c["signup_date"], errors="coerce") > pd.Timestamp.now()).sum()))

print("\n", "=" * 70, "\n6. INCREMENT FILE vs ORDERS\n", "=" * 70)
overlap = inc["order_id"].isin(o["order_id"])
print("increment rows whose order_id already exists (corrections):", int(overlap.sum()))
print("brand-new orders:", int((~overlap).sum()))
print("\nCorrections - status change:")
m = inc[overlap].merge(o[["order_id", "order_status"]], on="order_id", suffixes=("_new", "_old"))
print(m[["order_id", "order_status_old", "order_status_new"]].to_string(index=False))
