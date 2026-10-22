from pathlib import Path
import pandas as pd
import pendulum
import re, json
import shutil, hashlib
from datetime import timedelta
##from include.quality import standardise_orders, order_checks, run_checks
from include.quality import validate_file
from include.transform import transform
from include.load import load
from include.reconcile import reconcile
from include.report import report
from include.etl_increment import etl_increment

try:
    from airflow.sdk import dag, task            # newer Airflow (3.x)
except ImportError:
    from airflow.decorators import dag, task     # older Airflow (2.x)

from include.audit import write_audit
try:
    from airflow.sdk import get_current_context
except ImportError:
    from airflow.operators.python import get_current_context

from include.quarantine import quarantine
from include.notify import explain_quarantine, send_slack


BASE = Path(__file__).parent.parent
DATA_DIR = Path(__file__).parent.parent / "include" / "data" / "nammamart"
FILES = ["orders", "payments", "customers", "products", "stores"]


@dag(
    dag_id="nammamart_etl_Zubair",
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
def nammamart_etl():

    @task
    def extract(name: str, run_id=None) -> dict:
        safe = re.sub(r"[^\w\-]", "_", run_id)
        dest_dir = BASE / "include" / "work" / safe / "input"
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / f"{name}.csv"
        shutil.copyfile(DATA_DIR / f"{name}.csv", dest)          # frozen copy for this run
        sha = hashlib.sha256(dest.read_bytes()).hexdigest()[:12]
        df = pd.read_csv(dest, dtype=str, keep_default_na=False)
        print(f"{name}: read {len(df)} rows, {len(df.columns)} columns, sha256 {sha}")
        return {"file": name, "path": str(dest), "rows": len(df), "sha256": sha}

    ##[extract.override(task_id=f"extract_{n}")(n) for n in FILES]
    extracts = [extract.override(task_id=f"extract_{n}")(n) for n in FILES]

    @task
    def validate(name, extract_info, run_id=None):
        return validate_file(name, "nammamart_etl_Zubair", run_id, BASE, "Zubair")
    validations = [validate.override(task_id=f"validate_{n}")(n, extracts) for n in FILES]
    by_name = dict(zip(FILES, validations))
    [by_name["customers"], by_name["products"], by_name["stores"]] >> by_name["orders"]

    @task
    def transform_all(validation_results, run_id=None):
        return transform(run_id, BASE)

    transformed = transform_all(validations)

    @task
    def quarantine_failed(validation_results, run_id=None):
        return quarantine(run_id, BASE, "Zubair")

    @task
    def ai_explain(summary, run_id=None):
        return {"message": explain_quarantine(summary, run_id)}

    @task
    def slack_alert(explained):
        return send_slack(explained["message"])

    quarantined = quarantine_failed(validations)
    explained = ai_explain(quarantined)
    alerted = slack_alert(explained)

    @task
    def load_warehouse(transform_result, run_id=None):
        return load(run_id, BASE, "Zubair")

    loaded = load_warehouse(transformed)

    @task
    def apply_increment(load_result, dag_run=None, params=None, run_id=None):
        day = (dag_run.conf or {}).get("increment_date") or (params or {}).get("increment_date") or pendulum.instance(
            dag_run.logical_date or dag_run.run_after).in_timezone("Asia/Kolkata").format("YYYY-MM-DD")
        name = f"orders_{day}_increment.csv"
        if not (DATA_DIR / name).exists():
            print(f"no increment file for {day} ({name}) - skipping")
            return {"skipped": True, "file": name}
        return etl_increment(f"{run_id}__increment", BASE, "Zubair", name, "nammamart_etl_Zubair__increment")

    incremented = apply_increment(loaded)

    @task
    def reconcile_payments(load_result, run_id=None):
        return reconcile(run_id, BASE, "Zubair")

    reconciled = reconcile_payments(incremented)

    @task
    def build_reports(reconcile_result):
        return report(BASE, "Zubair")

    reported = build_reports(reconciled)




    @task(trigger_rule="all_done")
    def run_audit():
        ctx = get_current_context()
        return write_audit(ctx["ti"], ctx["dag_run"], ctx["run_id"],
                           "nammamart_etl_Zubair", BASE, "Zubair")

    audit = run_audit()
    #[*extracts, *validations, transformed, loaded, reconciled, reported] >> audit
    #[*extracts, *validations, quarantined, explained, alerted, transformed, loaded, reconciled, reported] >> audit
    [*extracts, *validations, quarantined, explained, alerted, transformed, loaded, incremented, reconciled, reported] >> audit
    

nammamart_etl()