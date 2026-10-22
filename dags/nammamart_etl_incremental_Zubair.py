from pathlib import Path
import pendulum

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

from include.etl_increment import etl_increment
from include.report import report

BASE = Path(__file__).parent.parent
INCREMENT = "orders_2026-10-01_increment.csv"
DAG_ID = "nammamart_etl_incremental_Zubair"


@dag(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
    tags=["nammamart"],
)
def nammamart_etl_incremental():

    @task
    def load_increment(run_id=None):
        return etl_increment(run_id, BASE, "Zubair", INCREMENT, DAG_ID)

    @task
    def build_reports(prev):
        return report(BASE, "Zubair")

    build_reports(load_increment())


nammamart_etl_incremental()