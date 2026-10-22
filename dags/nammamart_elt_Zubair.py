from pathlib import Path
import pendulum

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

from include.elt_raw import load_raw, FILES
from include.elt_sql import run_sql
from datetime import timedelta
from include.elt_dq import run_elt_dq
from include.elt_audit import write_elt_audit
from include.elt_incremental import run_merge

BASE = Path(__file__).parent.parent


@dag(
    dag_id="nammamart_elt_Zubair",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    schedule="0 9 * * 1-6",
    catchup=False,
    params={"increment_date": ""},
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["nammamart"],
)
def nammamart_elt():

    @task
    def raw_load(name, run_id=None):
        return load_raw(name, run_id, BASE, "Zubair")

    @task
    def sql_step(name, run_id=None):
        return run_sql(name, run_id, BASE, "Zubair")

    previous = None
    for n in FILES:
        current = raw_load.override(task_id=f"raw_{n}")(n)
        if previous is not None:
            previous >> current
        previous = current
    @task
    def merge_increment(dag_run=None, params=None, run_id=None):
        day = (dag_run.conf or {}).get("increment_date") or (params or {}).get("increment_date") or pendulum.instance(
            dag_run.logical_date or dag_run.run_after).in_timezone("Asia/Kolkata").format("YYYY-MM-DD")
        name = f"orders_{day}_increment.csv"
        if not (BASE / "include" / "data" / "nammamart" / name).exists():
            print(f"no increment file for {day} ({name}) - skipping")
            return {"skipped": True, "file": name}
        return run_merge(run_id, BASE, "Zubair", name)

    for s in ["stg_customers", "stg_products", "stg_stores", "stg_payments", "stg_orders",
              "merge_increment",
              "mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
              "mart_payment_reconciliation", "mart_top_customers", "mart_rejected_rows"]:
        current = merge_increment() if s == "merge_increment" else sql_step.override(task_id=s)(s)
        previous >> current
        previous = current

    @task
    def dq_results_elt(run_id=None):
        return run_elt_dq(run_id, BASE, "Zubair", "nammamart_elt_Zubair")

    dq = dq_results_elt()
    previous >> dq

    @task(trigger_rule="all_done")
    def run_audit_elt(dag=None, dag_run=None, ti=None, run_id=None):
        ids = [t for t in dag.task_ids if t != "run_audit_elt"]
        states = ti.get_task_states(dag_id=dag.dag_id, task_ids=ids, run_ids=[run_id]).get(run_id, {})
        failed = [t for t, s in states.items() if s in ("failed", "upstream_failed")]
        return write_elt_audit(dag_run, run_id, "nammamart_elt_Zubair", BASE, "Zubair", failed)

    dq >> run_audit_elt()


nammamart_elt()