import duckdb
from pathlib import Path

PII_NO, PII_YES, PII_MASK = "No", "Yes", "Masked"
ST = "DELIVERED, CANCELLED, RETURNED"
PM = "UPI, Card, Cash on Delivery, Wallet"

# column: (meaning, source, allowed values, personal data)
D = {
 "etl_fact_orders": {
  "order_id": ("Unique order identifier", "orders.order_id", "ORDnnnnnn, unique", PII_NO),
  "order_ts": ("When the order was placed", "orders.order_ts", "valid timestamp, not in the future", PII_NO),
  "customer_id": ("Customer who ordered (key to etl_dim_customer)", "orders.customer_id", "must exist in customers", PII_NO),
  "store_id": ("Store that fulfilled it (key to etl_dim_store)", "orders.store_id", "must exist in stores", PII_NO),
  "product_id": ("Product ordered (key to etl_dim_product)", "orders.product_id", "must exist in products", PII_NO),
  "quantity": ("Units ordered", "orders.quantity", "greater than 0", PII_NO),
  "unit_price": ("Price per unit before discount", "orders.unit_price", "number", PII_NO),
  "discount_pct": ("Discount percentage", "orders.discount_pct", "0 to 100", PII_NO),
  "order_amount": ("Amount after discount as supplied", "orders.order_amount", "equals quantity x unit_price x (1 - discount/100)", PII_NO),
  "payment_mode": ("How the customer paid", "orders.payment_mode", PM, PII_NO),
  "order_status": ("Order outcome", "orders.order_status", ST + "; only DELIVERED counts as revenue", PII_NO),
  "channel": ("Where the order came from", "orders.channel", "App, Web", PII_NO),
  "delivery_minutes": ("Minutes from order to delivery", "orders.delivery_minutes", "required when DELIVERED; below 120", PII_NO),
  "order_date": ("Date part of order_ts", "derived", "date", PII_NO),
  "net_revenue": ("Revenue after discount; reports count only DELIVERED", "derived from quantity, unit_price, discount_pct", "number", PII_NO),
  "gross_margin": ("Net revenue minus quantity x cost_price", "derived", "number", PII_NO),
  "within_15_min": ("True if delivered within 15 minutes", "derived from delivery_minutes", "true / false", PII_NO),
  "source_file": ("Which file this row came from (orders.csv or an increment)", "added by pipeline", "file name", PII_NO)},
 "etl_dim_customer": {
  "customer_id": ("Unique customer identifier", "customers.customer_id", "Cnnnnn, unique", "Pseudonymous id"),
  "full_name": ("Customer name", "customers.full_name", "text", PII_YES),
  "city": ("City", "customers.city", "text", PII_YES + " (location)"),
  "area": ("Neighbourhood", "customers.area", "text", PII_YES + " (location)"),
  "pincode": ("Postal code", "customers.pincode", "6 digits", PII_YES + " (location)"),
  "signup_date": ("When the customer joined", "customers.signup_date", "date, not in the future", PII_NO),
  "loyalty_tier": ("Loyalty programme level", "customers.loyalty_tier", "Bronze, Silver, Gold, Platinum", PII_NO),
  "is_active": ("Whether the account is active", "customers.is_active", "Y, N", PII_NO),
  "email_masked": ("Email with the middle hidden (pr***@example.com)", "customers.email, masked", "masked text or INVALID", PII_MASK),
  "phone_masked": ("Phone with the middle hidden (98******10)", "customers.phone, masked", "masked text or INVALID", PII_MASK)},
 "etl_dim_product": {
  "product_id": ("Unique product identifier", "products.product_id", "Pnnnn, unique", PII_NO),
  "product_name": ("Product name", "products.product_name", "text", PII_NO),
  "category": ("Product category", "products.category", "required", PII_NO),
  "sub_category": ("Product sub-category", "products.sub_category", "text", PII_NO),
  "brand": ("Brand", "products.brand", "text", PII_NO),
  "mrp": ("Maximum retail price", "products.mrp", "greater than 0", PII_NO),
  "cost_price": ("What NammaMart pays", "products.cost_price", "not above mrp", PII_NO),
  "unit": ("Pack size or unit", "products.unit", "text", PII_NO),
  "is_perishable": ("Whether it spoils", "products.is_perishable", "true / false", PII_NO),
  "launch_date": ("When the product was launched", "products.launch_date", "date", PII_NO)},
 "etl_dim_store": {
  "store_id": ("Unique store identifier", "stores.store_id", "Snn, unique", PII_NO),
  "store_name": ("Store name", "stores.store_name", "text", PII_NO),
  "zone": ("Delivery zone", "stores.zone", "East, North, South, West (required)", PII_NO),
  "area": ("Neighbourhood", "stores.area", "text", PII_NO),
  "pincode": ("Postal code", "stores.pincode", "6 digits", PII_NO),
  "open_date": ("When the store opened", "stores.open_date", "date", PII_NO),
  "manager_name": ("Store manager", "stores.manager_name", "text", PII_YES + " (employee)"),
  "capacity_orders_per_day": ("Orders the store can handle per day", "stores.capacity_orders_per_day", "greater than 0", PII_NO)},
}

con = duckdb.connect("include/warehouse_Zubair.duckdb", read_only=True)
out = ["# Data dictionary: ETL star schema\n"]
for table, cols in D.items():
    types = dict(con.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?", [table]).fetchall())
    out += [f"## {table}\n", "| Column | Type | Meaning | Source | Allowed values | Personal data |", "|---|---|---|---|---|---|"]
    for c, (meaning, src, allowed, pii) in cols.items():
        out.append(f"| {c} | {types.get(c, '??')} | {meaning} | {src} | {allowed} | {pii} |")
    missing = set(types) - set(cols)
    if missing:
        print("WARNING, columns not described in", table, ":", sorted(missing))
    out.append("")
Path("data_dictionary.md").write_text("\n".join(out), encoding="utf-8")
print("written; '??' in the file means a column name I guessed wrong:", sum(l.count("| ?? |") for l in out))