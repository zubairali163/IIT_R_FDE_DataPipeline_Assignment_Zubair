MERGE INTO stg_orders AS t
USING stg_orders_delta AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;