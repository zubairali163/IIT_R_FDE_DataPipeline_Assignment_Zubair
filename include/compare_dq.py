import duckdb

con = duckdb.connect()
con.execute("ATTACH 'include/warehouse_Zubair.duckdb' AS etl (READ_ONLY)")
con.execute("ATTACH 'include/warehouse_Zubair_elt.duckdb' AS elt (READ_ONLY)")

q = """
WITH e AS (
  SELECT check_name, rows_checked, rows_failed,
         row_number() OVER (PARTITION BY check_name ORDER BY rows_checked) AS k
  FROM etl.dq_results
  WHERE run_id = (SELECT run_id FROM etl.dq_results
                  WHERE dag_id = 'nammamart_etl_Zubair'
                  ORDER BY checked_at DESC LIMIT 1)
), l AS (
  SELECT check_name, rows_checked, rows_failed,
         row_number() OVER (PARTITION BY check_name ORDER BY rows_checked) AS k
  FROM elt.dq_results
  WHERE run_id = (SELECT run_id FROM elt.dq_results ORDER BY checked_at DESC LIMIT 1)
)
SELECT e.check_name,
       e.rows_checked AS etl_checked, l.rows_checked AS elt_checked,
       e.rows_failed  AS etl_failed,  l.rows_failed  AS elt_failed
FROM e JOIN l USING (check_name, k)
WHERE e.rows_failed <> l.rows_failed OR e.rows_checked <> l.rows_checked
ORDER BY 1
"""
print(con.sql(q))