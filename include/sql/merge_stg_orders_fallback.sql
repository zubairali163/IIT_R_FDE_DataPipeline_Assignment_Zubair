BEGIN;
DELETE FROM stg_orders WHERE order_id IN (SELECT order_id FROM stg_orders_delta);
INSERT INTO stg_orders SELECT * FROM stg_orders_delta;
COMMIT;