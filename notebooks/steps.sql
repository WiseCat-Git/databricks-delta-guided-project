-- notebooks/steps.sql
-- Guided Project: core Delta ops you ran

-- 1) Peek & history
SELECT * FROM workspace.demo.public_e_commerce_transactions_dataset LIMIT 5;
DESCRIBE HISTORY workspace.demo.public_e_commerce_transactions_dataset;

-- 2) ACID ops
UPDATE workspace.demo.public_e_commerce_transactions_dataset
SET status = 'review' WHERE status = 'pending';

DELETE FROM workspace.demo.public_e_commerce_transactions_dataset
WHERE is_gift = true;

-- 3) Time travel & recovery (adjust version if needed)
SELECT * FROM workspace.demo.public_e_commerce_transactions_dataset VERSION AS OF 0 LIMIT 5;

INSERT INTO workspace.demo.public_e_commerce_transactions_dataset (customer, is_gift, items, order_date, order_id, status)
SELECT customer, is_gift, items, order_date, order_id, status
FROM workspace.demo.public_e_commerce_transactions_dataset VERSION AS OF 0
WHERE order_id = 1002;

-- 4) MERGE upsert demo (requires the temp view in the PySpark script below or define it inline)
-- CREATE OR REPLACE TEMP VIEW ecommerce_updates AS
-- SELECT 91002 AS order_id, 'delivered' AS status, false AS is_gift, 0.00 AS discount
-- UNION ALL
-- SELECT 91003, 'pending', false, 0.05 AS discount;
-- MERGE INTO workspace.demo.public_e_commerce_transactions_dataset t
-- USING ecommerce_updates s
-- ON t.order_id = s.order_id
-- WHEN MATCHED THEN UPDATE SET t.status=s.status, t.is_gift=s.is_gift, t.discount=s.discount
-- WHEN NOT MATCHED THEN INSERT (order_id, status, is_gift, discount)
-- VALUES (s.order_id, s.status, s.is_gift, s.discount);
