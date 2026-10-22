import duckdb
from pathlib import Path

FILES = ["orders", "payments", "customers", "products", "stores"]


def load_raw(name, run_id, base, who, source_file=None):
    src = Path(base) / "include" / "data" / "nammamart" / (source_file or f"{name}.csv")
    db = Path(base) / "include" / f"warehouse_{who}_elt.duckdb"
    rid = run_id.replace("'", "''")
    con = duckdb.connect(str(db))
    query = f"""
        SELECT *, now() AS _loaded_at, '{src.name}' AS _source_file, '{rid}' AS _run_id
        FROM read_csv('{src}', all_varchar=true, header=true)
    """
    con.execute(f"CREATE TABLE IF NOT EXISTS raw_{name} AS {query} LIMIT 0")
    con.execute(f"INSERT INTO raw_{name} {query}")
    n = con.execute(f"SELECT count(*) FROM raw_{name} WHERE _run_id = '{rid}'").fetchone()[0]
    con.close()
    print(f"raw_{name}: loaded {n} rows from {src.name}")
    return {"table": f"raw_{name}", "rows": int(n)}