import re
import pandas as pd
from pathlib import Path
from include.pii import mask_email, mask_phone

num = lambda s: pd.to_numeric(s, errors="coerce")


def transform(run_id, base):
    safe = re.sub(r"[^\w\-]", "_", run_id)
    work = Path(base) / "include" / "work" / safe
    rd = lambda n: pd.read_csv(work / f"{n}_passed.csv", dtype=str, keep_default_na=False)
    orders, customers, products, stores = rd("orders"), rd("customers"), rd("products"), rd("stores")

    # ---------- dimensions ----------
    dim_customer = customers.copy()
    dim_customer["email_masked"] = dim_customer["email"].map(mask_email)
    dim_customer["phone_masked"] = dim_customer["phone"].map(mask_phone)
    dim_customer = dim_customer.drop(columns=["email", "phone"])   # raw PII never leaves this function

    dim_product = products.copy()
    for c in ["mrp", "cost_price"]:
        dim_product[c] = num(dim_product[c])

    dim_store = stores.copy()
    dim_store["capacity_orders_per_day"] = num(dim_store["capacity_orders_per_day"])

    # ---------- fact ----------
    f = orders.copy()
    for c in ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"]:
        f[c] = num(f[c])
    f["order_ts"] = pd.to_datetime(f["order_ts"])
    f["order_date"] = f["order_ts"].dt.date.astype(str)

    f = f.merge(dim_product[["product_id", "cost_price"]], on="product_id", how="left")
    assert f["cost_price"].notna().all(), "an order has no matching product - validation should have caught it"
    assert f["customer_id"].isin(dim_customer["customer_id"]).all(), "order with unknown customer"
    assert f["store_id"].isin(dim_store["store_id"]).all(), "order with unknown store"

    delivered = f["order_status"] == "DELIVERED"
    f["net_revenue"] = f["order_amount"].where(delivered, 0.0)
    f["gross_margin"] = (f["net_revenue"] - f["quantity"] * f["cost_price"]).where(delivered, 0.0)
    f["within_15_min"] = delivered & (f["delivery_minutes"] <= 15)
    f = f.drop(columns=["cost_price"])

    f["source_file"] = "orders.csv"

    assert len(f) == len(orders), "rows were lost in the transform"

    for name, df in [("fact_orders", f), ("dim_customer", dim_customer),
                     ("dim_product", dim_product), ("dim_store", dim_store)]:
        df.to_csv(work / f"{name}.csv", index=False)
        print(f"{name}: {len(df)} rows")
    print("net revenue (delivered):", round(f["net_revenue"].sum(), 2))
    return {"fact_rows": len(f), "net_revenue": round(float(f["net_revenue"].sum()), 2)}