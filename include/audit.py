import duckdb
import pandas as pd
from pathlib import Path

FILES = ["orders", "payments", "customers", "products", "stores"]


def write_audit(ti, dag_run, run_id, dag_id, base, who):
    def pull(task_id):
        try:
            return ti.xcom_pull(task_ids=task_id, key="return_value")
        except Exception:
            return None

    extract = {n: pull(f"extract_{n}") for n in FILES}
    validate = {n: pull(f"validate_{n}") for n in FILES}
    steps = {**{f"extract_{n}": v for n, v in extract.items()},
             **{f"validate_{n}": v for n, v in validate.items()},
             "transform_all": pull("transform_all"),
             "load_warehouse": pull("load_warehouse"),
             "reconcile_payments": pull("reconcile_payments"),
             "build_reports": pull("build_reports")}
    failed = [k for k, v in steps.items() if v is None]

    row = dict(
        run_id=run_id, dag_id=dag_id,
        start_time=str(dag_run.start_date),
        end_time=str(pd.Timestamp.now(tz="UTC")),
        rows_extracted=sum(v["rows"] for v in extract.values() if v),
        rows_loaded=(steps["load_warehouse"] or {}).get("rows_loaded", 0),
        rows_quarantined=sum(v["rows_quarantined"] for v in validate.values() if v),
        status="SUCCESS" if not failed else "FAILED",
    )
    df = pd.DataFrame([row])

    con = duckdb.connect(str(Path(base) / "include" / f"warehouse_{who}.duckdb"))
    con.register("audit_df", df)
    con.execute("CREATE TABLE IF NOT EXISTS pipeline_runs AS SELECT * FROM audit_df WHERE 1=0")
    con.execute("DELETE FROM pipeline_runs WHERE run_id = ?", [run_id])   # idempotent
    con.execute("INSERT INTO pipeline_runs BY NAME SELECT * FROM audit_df")
    con.close()
    print(row)
    if failed:
        print("tasks with no result:", failed)
    return row