import duckdb
from pathlib import Path


def run_sql(name, run_id, base, who):
    sql = (Path(base) / "include" / "sql" / f"{name}.sql").read_text()
    con = duckdb.connect(str(Path(base) / "include" / f"warehouse_{who}_elt.duckdb"))
    con.execute(sql)
    if name.startswith("stg_"):
        rows = con.execute(f"SELECT dq_status, count(*) FROM {name} GROUP BY 1 ORDER BY 1").fetchall()
        total = sum(n for _, n in rows)
        con.close()
        print(f"{name}: {total} rows  {dict(rows)}")
        return {"table": name, "rows": int(total), "by_status": {k: int(v) for k, v in rows}}
    n = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
    con.close()
    print(f"{name}: {n} rows")
    return {"table": name, "rows": int(n)}