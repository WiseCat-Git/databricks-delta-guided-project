# notebooks/schema_evolution_and_merge.py
# PySpark: schema evolution + MERGE

from pyspark.sql import Row

# 1) Schema evolution (adds 'discount' column)
df_new = spark.createDataFrame([
    Row(order_id=91001, status="shipped", is_gift=False, discount=0.10),
    Row(order_id=91002, status="pending", is_gift=True,  discount=0.00),
])

(df_new.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("workspace.demo.public_e_commerce_transactions_dataset"))

# 2) MERGE upsert (idempotent)
spark.sql("""
CREATE OR REPLACE TEMP VIEW ecommerce_updates AS
SELECT 91002 AS order_id, 'delivered' AS status, false AS is_gift, 0.00 AS discount
UNION ALL
SELECT 91003, 'pending', false, 0.05 AS discount
""")

spark.sql("""
MERGE INTO workspace.demo.public_e_commerce_transactions_dataset AS t
USING ecommerce_updates AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
  t.status   = s.status,
  t.is_gift  = s.is_gift,
  t.discount = s.discount
WHEN NOT MATCHED THEN INSERT (order_id, status, is_gift, discount)
VALUES (s.order_id, s.status, s.is_gift, s.discount)
""")

print(" Schema evolution + MERGE completed.")
