CREATE OR REPLACE TABLE stg_products AS
WITH a AS (
    SELECT *, rowid AS _rid FROM raw_products
),
latest AS (
    SELECT * FROM a
    QUALIFY row_number() OVER (PARTITION BY trim(product_id) ORDER BY _loaded_at DESC, _rid) = 1
),
clean AS (
    SELECT trim(product_id) AS product_id, trim(product_name) AS product_name,
           trim(category) AS category, trim(sub_category) AS sub_category, trim(brand) AS brand,
           try_cast(trim(mrp) AS DOUBLE) AS mrp,
           try_cast(trim(cost_price) AS DOUBLE) AS cost_price,
           trim(unit) AS unit, trim(is_perishable) AS is_perishable,
           try_cast(trim(launch_date) AS DATE) AS launch_date,
           _run_id, _source_file
    FROM latest
),
flagged AS (
    SELECT *,
        concat_ws('; ',
            CASE WHEN category IS NULL OR category = '' THEN 'category_not_null' END,
            CASE WHEN NOT coalesce(mrp > 0, false) THEN 'mrp_positive' END,
            CASE WHEN NOT coalesce(cost_price <= mrp, false) THEN 'cost_le_mrp' END
        ) AS failure_reason,
        '' AS warning_reason
    FROM clean
)
SELECT *, CASE WHEN failure_reason <> '' THEN 'FAIL'
               WHEN warning_reason <> '' THEN 'WARN' ELSE 'PASS' END AS dq_status
FROM flagged;