INSERT INTO dq_results
WITH cur AS (
    SELECT count(*) AS n FROM raw_orders
    WHERE _source_file = 'orders.csv' AND _run_id = '__RUN_ID__'
),
prev AS (
    SELECT n FROM (
        SELECT _run_id, count(*) AS n, max(_loaded_at) AS t FROM raw_orders
        WHERE _source_file = 'orders.csv' AND _run_id <> '__RUN_ID__'
        GROUP BY 1
    ) ORDER BY t DESC LIMIT 1
),
fresh AS (
    --SELECT max(try_cast(order_ts AS TIMESTAMP)) AS last_ts FROM stg_orders
    SELECT max(try_cast(order_ts AS TIMESTAMP)) AS last_ts FROM stg_orders
    WHERE coalesce(failure_reason, '') = ''
)
SELECT '__RUN_ID__', '__DAG_ID__', 'volume_drop_over_90pct', 'Completeness', 'Warning',
       CAST((SELECT n FROM cur) AS BIGINT),
       CAST(CASE WHEN (SELECT n FROM prev) IS NOT NULL AND (SELECT n FROM cur) < 0.1 * (SELECT n FROM prev)
                 THEN (SELECT n FROM prev) - (SELECT n FROM cur) ELSE 0 END AS BIGINT),
       CASE WHEN (SELECT n FROM prev) IS NOT NULL AND (SELECT n FROM cur) < 0.1 * (SELECT n FROM prev)
            THEN 'WARN' ELSE 'PASS' END,
       CAST(now() AS VARCHAR)
UNION ALL
SELECT '__RUN_ID__', '__DAG_ID__', 'latest_order_older_than_1_day', 'Timeliness', 'Warning',
       CAST(1 AS BIGINT),
       CAST(CASE WHEN (SELECT last_ts FROM fresh) < CAST(now() AS TIMESTAMP) - INTERVAL 1 DAY THEN 1 ELSE 0 END AS BIGINT),
       CASE WHEN (SELECT last_ts FROM fresh) < CAST(now() AS TIMESTAMP) - INTERVAL 1 DAY THEN 'WARN' ELSE 'PASS' END,
       CAST(now() AS VARCHAR);