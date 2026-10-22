import requests

PLAIN = {
    "order_id_unique": "duplicate order (extra copy)",
    "customer_id_not_null": "order has no customer",
    "delivered_has_minutes": "delivered order with no delivery time",
    "status_allowed": "unknown order status",
    "payment_mode_allowed": "unknown payment mode",
    "order_ts_parses": "unreadable order date",
    "discount_0_to_100": "discount outside 0-100%",
    "quantity_positive": "zero or negative quantity",
    "amount_matches_formula": "order amount does not match the formula",
    "delivery_under_120": "impossible delivery time",
    "customer_exists": "customer missing or rejected (orders held back)",
    "product_exists": "product missing or rejected (orders held back)",
    "store_exists": "store missing or rejected (orders held back)",
    "zone_not_null": "store has no zone",
    "store_id_unique": "duplicate store (extra copy)",
    "capacity_positive": "store capacity is zero",
    "category_not_null": "product has no category",
    "product_id_unique": "duplicate product (extra copy)",
    "mrp_positive": "product price (MRP) is zero or negative",
    "cost_le_mrp": "product cost price is higher than its price",
    "customer_id_unique": "duplicate customer (extra copy)",
    "signup_not_future": "customer signup date is in the future",
    "payment_id_unique": "duplicate payment (extra copy)",
    "amount_not_negative": "negative payment amount",
    "order_ts_not_future": "order dated in the future",
}

def get_var(name):
    try:
        try:
            from airflow.sdk import Variable
        except ImportError:
            from airflow.models import Variable
        return Variable.get(name)
    except Exception:
        return None


FILE_ICON = {"orders": "🛒", "payments": "💳", "customers": "👤", "products": "📦", "stores": "🏬"}


def severity(total):
    if total == 0:
        return "✅"
    return "🚨" if total > 500 else "⚠️"


def explain_quarantine(summary, run_id):
    #total = sum(v["rows"] for v in summary.values())
    #lines = [f"NammaMart ETL run {run_id}: {total} rows were quarantined."]
    #for f, v in summary.items():
    #    if v["rows"]:
     #       top = sorted(v["reasons"].items(), key=lambda x: -x[1])[:3]
      #      why = "; ".join(f"{PLAIN.get(r, r)} ({n})" for r, n in top)
       #     lines.append(f"- {f}: {v['rows']} rows. Main causes: {why}")
    #text = "\n".join(lines)
    total = sum(v["rows"] for v in summary.values())
    lines = [f"{severity(total)} *NammaMart ETL run finished*",
             f"🔖 Run: `{run_id}`",
             f"🧹 *{total}* rows quarantined"]
    for f, v in summary.items():
        if v["rows"]:
            top = sorted(v["reasons"].items(), key=lambda x: -x[1])[:3]
            why = "; ".join(f"{PLAIN.get(r, r)} ({n})" for r, n in top)
            lines.append(f"{FILE_ICON.get(f, '📄')} *{f}*: {v['rows']} rows, main causes: {why}")
    text = "\n".join(lines)

    key = get_var("ANTHROPIC_API_KEY")          # optional, paid; free version is the default
    if key:
        try:
            r = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": key, "anthropic-version": "2023-06-01"},
                json={"model": "claude-haiku-5-5", "max_tokens": 300,
                      "messages": [{"role": "user", "content":
                          "Summarise this data-quality report for an operations manager "
                          "in 3 plain sentences:\n" + text}]},
                timeout=30)
            r.raise_for_status()
            text = r.json()["content"][0]["text"]
        except Exception as e:
            print("LLM explainer failed, using rule-based text:", e)
    print(text)
    return text


def send_slack(message):
    url = get_var("SLACK_WEBHOOK_URL")
    if not url:
        print("SLACK_WEBHOOK_URL not set; would have sent:\n", message)
        return {"sent": False}
    requests.post(url, json={"text": message}, timeout=10).raise_for_status()
    return {"sent": True}