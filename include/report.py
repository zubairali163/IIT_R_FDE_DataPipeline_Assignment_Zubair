import duckdb
from pathlib import Path


def report(base, who):
    con = duckdb.connect(str(Path(base) / "include" / f"warehouse_{who}.duckdb"))

    con.execute("""
        CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
        SELECT f.order_date, s.zone, f.store_id, s.store_name,
               count(*) AS delivered_orders,
               round(sum(f.net_revenue), 2) AS net_revenue
        FROM etl_fact_orders f
        JOIN etl_dim_store s ON f.store_id = s.store_id
        WHERE f.order_status = 'DELIVERED'
        GROUP BY ALL
    """)

    con.execute("""
        CREATE OR REPLACE TABLE etl_category_margin AS
        SELECT p.category,
               round(sum(f.net_revenue), 2) AS net_revenue,
               round(sum(f.gross_margin), 2) AS gross_margin,
               round(100 * sum(f.gross_margin) / sum(f.net_revenue), 1) AS margin_pct
        FROM etl_fact_orders f
        JOIN etl_dim_product p ON f.product_id = p.product_id
        WHERE f.order_status = 'DELIVERED'
        GROUP BY ALL
        ORDER BY net_revenue DESC
    """)

    # safety check: the report must add up to the fact table
    fact_total = con.execute("SELECT sum(net_revenue) FROM etl_fact_orders").fetchone()[0]
    zone_total = con.execute("SELECT sum(net_revenue) FROM etl_daily_zone_revenue").fetchone()[0]
    cat_total = con.execute("SELECT sum(net_revenue) FROM etl_category_margin").fetchone()[0]
    assert abs(fact_total - zone_total) < 0.01, f"zone report {zone_total} != fact {fact_total}"
    assert abs(fact_total - cat_total) < 0.01, f"category report {cat_total} != fact {fact_total}"

    print("Revenue by zone (September total):")
    print(con.sql("SELECT zone, sum(delivered_orders) AS orders, round(sum(net_revenue),2) AS revenue "
                  "FROM etl_daily_zone_revenue GROUP BY zone ORDER BY revenue DESC").df().to_string(index=False))
    print("\nCategory margin:")
    print(con.sql("SELECT * FROM etl_category_margin").df().to_string(index=False))
    con.close()
    return {"net_revenue": float(fact_total)}