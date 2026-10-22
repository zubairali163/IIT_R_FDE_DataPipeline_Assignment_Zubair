from pathlib import Path
import pendulum

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

from include.elt_incremental import run_merge
from include.elt_sql import run_sql

BASE = Path(__file__).parent.parent
INCREMENT = "orders_2026-10-01_increment.csv"
MARTS = ["mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
         "mart_payment_reconciliation", "mart_top_customers", "mart_rejected_rows"]


@dag(
    dag_id="nammamart_elt_incremental_Zubair",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
    tags=["nammamart"],
)
def nammamart_elt_incremental():

    @task
    def merge_orders(run_id=None):
        return run_merge(run_id, BASE, "Zubair", INCREMENT)

    @task
    def sql_step(name, run_id=None):
        return run_sql(name, run_id, BASE, "Zubair")

    previous = merge_orders()
    for m in MARTS:
        current = sql_step.override(task_id=m)(m)
        previous >> current
        previous = current


nammamart_elt_incremental()