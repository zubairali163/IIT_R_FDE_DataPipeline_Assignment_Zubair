CREATE OR REPLACE TABLE stg_stores AS
WITH a AS (
    SELECT *, rowid AS _rid FROM raw_stores
),
latest AS (
    SELECT * FROM a
    QUALIFY row_number() OVER (PARTITION BY trim(store_id) ORDER BY _loaded_at DESC, _rid) = 1
),
clean AS (
    SELECT trim(store_id) AS store_id, trim(store_name) AS store_name, trim(zone) AS zone,
           trim(area) AS area, trim(pincode) AS pincode,
           try_cast(trim(open_date) AS DATE) AS open_date,
           trim(manager_name) AS manager_name,
           try_cast(trim(capacity_orders_per_day) AS BIGINT) AS capacity_orders_per_day,
           _run_id, _source_file
    FROM latest
),
flagged AS (
    SELECT *,
        concat_ws('; ', CASE WHEN zone IS NULL OR zone = '' THEN 'zone_not_null' END) AS failure_reason,
        concat_ws('; ', CASE WHEN NOT coalesce(capacity_orders_per_day > 0, false) THEN 'capacity_positive' END) AS warning_reason
    FROM clean
)
SELECT *, CASE WHEN failure_reason <> '' THEN 'FAIL'
               WHEN warning_reason <> '' THEN 'WARN' ELSE 'PASS' END AS dq_status
FROM flagged;