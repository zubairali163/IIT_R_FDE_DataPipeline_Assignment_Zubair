import duckdb
from pathlib import Path

SQL_FILES = ["dq_results_elt.sql", "dq_volume_freshness_elt.sql"]


def run_elt_dq(run_id, base, who, dag_id):
    con = duckdb.connect(str(Path(base) / "include" / f"warehouse_{who}_elt.duckdb"))
    con.execute("""CREATE TABLE IF NOT EXISTS dq_results (
        run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR, severity VARCHAR,
        rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at VARCHAR)""")
    con.execute("DELETE FROM dq_results WHERE run_id = ?", [run_id])   # a retry must not double
    for name in SQL_FILES:
        sql = (Path(base) / "include" / "sql" / name).read_text()
        sql = sql.replace("__RUN_ID__", run_id.replace("'", "''")).replace("__DAG_ID__", dag_id)
        con.execute(sql)
    n, bad = con.execute(
        "SELECT count(*), sum(CASE WHEN rows_failed > 0 THEN 1 ELSE 0 END) FROM dq_results WHERE run_id = ?",
        [run_id]).fetchone()
    con.close()
    print(f"dq_results (ELT): {n} checks written, {int(bad)} with failures")
    return {"checks": int(n), "with_failures": int(bad)}