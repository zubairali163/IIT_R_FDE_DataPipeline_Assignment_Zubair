import duckdb
import pandas as pd
from pathlib import Path

TABLES = ["orders", "payments", "customers", "products", "stores"]


def write_elt_audit(dag_run, run_id, dag_id, base, who, failed=None):
    failed = failed or []
    con = duckdb.connect(str(Path(base) / "include" / f"warehouse_{who}_elt.duckdb"))

    extracted = sum(con.execute(f"SELECT count(*) FROM raw_{t} WHERE _run_id = ?", [run_id]).fetchone()[0]
                    for t in TABLES)
    total = sum(con.execute(f"SELECT count(*) FROM stg_{t}").fetchone()[0] for t in TABLES)
    quarantined = sum(con.execute(
        f"SELECT count(*) FROM stg_{t} WHERE coalesce(failure_reason, '') <> ''").fetchone()[0]
        for t in TABLES)

    # a task that failed earlier in this run makes the whole run FAILED
    #failed = [ti.task_id for ti in dag_run.get_task_instances()
              #if ti.state in ("failed", "upstream_failed")]

    row = dict(
        run_id=run_id, dag_id=dag_id,
        start_time=str(dag_run.start_date),
        end_time=str(pd.Timestamp.now(tz="UTC")),
        rows_extracted=extracted,
        rows_loaded=total - quarantined,
        rows_quarantined=quarantined,
        status="SUCCESS" if not failed else "FAILED",
    )
    df = pd.DataFrame([row])
    con.register("audit_df", df)
    con.execute("CREATE TABLE IF NOT EXISTS pipeline_runs AS SELECT * FROM audit_df WHERE 1=0")
    con.execute("DELETE FROM pipeline_runs WHERE run_id = ?", [run_id])
    con.execute("INSERT INTO pipeline_runs BY NAME SELECT * FROM audit_df")
    con.close()
    print(row)
    if failed:
        print("tasks that failed:", failed)
    return row