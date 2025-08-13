# Data Management with Databricks: Big Data with Delta Lake (Guided Project)

**Environment**: Databricks (Unity Catalog: workspace, Schema: demo)  
**Tables**: 
- workspace.demo.public_e_commerce_transactions_dataset (Delta)
- workspace.demo.nyc_taxi_rides_sample (Delta)
- workspace.demo.movie_ratings_dataset (Delta)

---

## What this repo shows
- 1 ACID transactions (UPDATE, DELETE, MERGE)
- 2 Time Travel (VERSION AS OF)
- 3 Point-in-time recovery
- 4 Schema enforcement & evolution (mergeSchema)
- 5 Basic performance ops (OPTIMIZE, VACUUM DRY RUN)
- 6 Mini analytics (daily revenue, trip stats, movie ratings)

---

## Repro steps (SQL/PySpark)

1. **Peek & history**
sql SELECT * FROM workspace.demo.public_e_commerce_transactions_dataset LIMIT 5; DESCRIBE HISTORY workspace.demo.public_e_commerce_transactions_dataset;
2. **ACID ops**
sql UPDATE workspace.demo.public_e_commerce_transactions_dataset SET status='review' WHERE status='pending';
sql UPDATE workspace.demo.public_e_commerce_transactions_dataset SET status='review' WHERE status='pending';
3. **Time travel & recovery**
sql SELECT * FROM workspace.demo.public_e_commerce_transactions_dataset VERSION AS OF 0 LIMIT 5;
INSERT INTO workspace.demo.public_e_commerce_transactions_dataset (customer, is_gift, items, order_date, order_id, status)
SELECT customer, is_gift, items, order_date, order_id, status
FROM workspace.demo.public_e_commerce_transactions_dataset VERSION AS OF 0
WHERE order_id = 1002;
4. **Schema evolution (PySpark)**
df_new = spark.createDataFrame([
    Row(order_id=91001, status="shipped", is_gift=False, discount=0.10),
    Row(order_id=91002, status="pending", is_gift=True,  discount=0.00),
])

(df_new.write
  .format("delta")
  .mode("append")
  .option("mergeSchema", "true")  # schema evolution
  .saveAsTable("workspace.demo.public_e_commerce_transactions_dataset"))
  5. **Merge for upsert**
  sql CREATE OR REPLACE TEMP VIEW ecommerce_updates AS SELECT 91002 AS order_id, 'delivered' AS status, false AS is_gift, 0.00 AS discount UNION ALL SELECT 91003, 'pending', false, 0.05 AS discount;
  MERGE INTO workspace.demo.public_e_commerce_transactions_dataset AS t
USING ecommerce_updates AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
  t.status   = s.status,
  t.is_gift  = s.is_gift,
  t.discount = s.discount
WHEN NOT MATCHED THEN INSERT (order_id, status, is_gift, discount)
VALUES (s.order_id, s.status, s.is_gift, s.discount);
---

## Example output after merge

 order_id | status     | is_gift | discount |
----------|-----------|---------|----------|
 91001    | shipped   | false   | 0.10     |
 91002    | delivered | false   | 0.00     |
 91003    | pending   | false   | 0.05     |

---

## Skills Demonstrated
- Delta Lake ACID compliance
- Point-in-time queries & recovery
- Schema enforcement and evolution
- Merge-based upserts
- Performance optimization commands
- SQL + PySpark workflow integration

---

## 🚀 Upgrade Notes

This upgrade adds a Bronze→Silver pipeline and feature engineering:

**DataFrames produced**
- `source_df` (raw nested JSON with `customer`, `items`)
- `bronze_df` (normalized fields + `ingestion_timestamp`, `data_source`, `item_count`, `has_null_items`)
- `silver_enhanced` (features: `order_value_tier`, `order_complexity`, `email_type`, temporal features, status flags, `discount_percentage`, `final_order_value`, `avg_item_value`, `price_per_unit`, `data_completeness`)
- `silver_clean` (quality-checked slice)
- `verification_df` (side-by-side checks for transparency)
- `silver_df` / `customer_features` (for analytics/ML)

**Temporal features**
`days_since_order`, `order_day_of_week`, `order_hour`, `order_month`, `order_year`, `order_day_type`, `order_time_of_day`

**Status flags**
`is_cancelled`, `is_completed`, `is_pending`, `is_shipped`

## Upgrade Notes

This upgrade adds a Bronze→Silver pipeline and feature engineering:

**DataFrames produced**
- `source_df` (raw nested JSON with `customer`, `items`)
- `bronze_df` (normalized fields + `ingestion_timestamp`, `data_source`, `item_count`, `has_null_items`)
- `silver_enhanced` (features: `order_value_tier`, `order_complexity`, `email_type`, temporal features, status flags, `discount_percentage`, `final_order_value`, `avg_item_value`, `price_per_unit`, `data_completeness`)
- `silver_clean` (quality-checked slice)
- `verification_df` (side-by-side checks for transparency)
- `silver_df` / `customer_features` (for analytics/ML)

**Temporal features**
`days_since_order`, `order_day_of_week`, `order_hour`, `order_month`, `order_year`, `order_day_type`, `order_time_of_day`

**Status flags**
`is_cancelled`, `is_completed`, `is_pending`, `is_shipped`
