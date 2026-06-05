# Databricks notebook source
# Cell 1 — Read from Silver layer
df_silver = spark.table("kali_demo.silver.sales_orders")

print("Silver row count:", df_silver.count())
print("Columns:", df_silver.columns)

# Quick preview
df_silver.select(
    "order_id", "customer_name", "product_name",
    "region", "status", "unit_price", "quantity"
).show(5, truncate=False)

# COMMAND ----------

# Cell 2 — Revenue by region
from pyspark.sql.functions import sum, round, desc, count, col

df_revenue_by_region = df_silver \
    .withColumn("order_value", 
                round(col("unit_price") * col("quantity"), 2)) \
    .groupBy("region") \
    .agg(
        round(sum("order_value"), 2).alias("total_revenue"),
        count("order_id").alias("total_orders"),
        round(sum("order_value") / count("order_id"), 2).alias("avg_order_value")
    ) \
    .orderBy(desc("total_revenue"))

print("Revenue by region:")
df_revenue_by_region.show(truncate=False)

# COMMAND ----------

# Cell 3 — Revenue by product
from pyspark.sql.functions import sum, round, desc, count, col, avg

df_revenue_by_product = df_silver \
    .withColumn("order_value",
                round(col("unit_price") * col("quantity"), 2)) \
    .groupBy("product_name") \
    .agg(
        round(sum("order_value"), 2).alias("total_revenue"),
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_units_sold"),
        round(avg("unit_price"), 2).alias("avg_unit_price")
    ) \
    .orderBy(desc("total_revenue"))

print("Revenue by product:")
df_revenue_by_product.show(truncate=False)

# COMMAND ----------

# Cell 4 — Order status breakdown
from pyspark.sql.functions import sum, round, desc, count, col

df_status_breakdown = df_silver \
    .withColumn("order_value",
                round(col("unit_price") * col("quantity"), 2)) \
    .groupBy("status") \
    .agg(
        count("order_id").alias("total_orders"),
        round(sum("order_value"), 2).alias("total_revenue"),
        round(sum("order_value") / count("order_id"), 2).alias("avg_order_value")
    ) \
    .orderBy(desc("total_orders"))

print("Order status breakdown:")
df_status_breakdown.show(truncate=False)

# Revenue at risk — pending and confirmed orders not yet completed
from pyspark.sql.functions import col
df_at_risk = df_silver \
    .filter(col("status").isin(["PENDING", "CONFIRMED"])) \
    .withColumn("order_value",
                round(col("unit_price") * col("quantity"), 2))

total_at_risk = df_at_risk.agg(
    round(sum("order_value"), 2).alias("revenue_at_risk"),
    count("order_id").alias("orders_at_risk")
).collect()[0]

print(f"\nRevenue at risk (PENDING + CONFIRMED orders):")
print(f"  Orders  : {total_at_risk['orders_at_risk']}")
print(f"  Revenue : ₹{total_at_risk['revenue_at_risk']:,.2f}")

# COMMAND ----------

# Cell 5 — Monthly sales trend
from pyspark.sql.functions import (
    sum, round, desc, count, col,
    year, month, date_format, asc
)

df_monthly_trend = df_silver \
    .withColumn("order_value",
                round(col("unit_price") * col("quantity"), 2)) \
    .withColumn("year_month",
                date_format(col("order_date"), "yyyy-MM")) \
    .groupBy("year_month") \
    .agg(
        round(sum("order_value"), 2).alias("monthly_revenue"),
        count("order_id").alias("total_orders"),
        round(sum("order_value") / count("order_id"), 2).alias("avg_order_value")
    ) \
    .orderBy(asc("year_month"))

print("Monthly sales trend:")
df_monthly_trend.show(25, truncate=False)

# Find best and worst months
best_month  = df_monthly_trend.orderBy(desc("monthly_revenue")).first()
worst_month = df_monthly_trend.orderBy(asc("monthly_revenue")).first()

print(f"\nBest month  : {best_month['year_month']} — ₹{best_month['monthly_revenue']:,.2f} ({best_month['total_orders']} orders)")
print(f"Worst month : {worst_month['year_month']} — ₹{worst_month['monthly_revenue']:,.2f} ({worst_month['total_orders']} orders)")

# COMMAND ----------

# Cell 6 — Write Gold Delta tables
from pyspark.sql.functions import current_timestamp, lit

def write_gold_table(df, table_name):
    """Helper function to write a Gold Delta table with metadata"""
    df.withColumn("gold_created_at", current_timestamp()) \
      .write \
      .format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable(f"kali_demo.gold.{table_name}")
    
    count = spark.table(f"kali_demo.gold.{table_name}").count()
    print(f"✓ kali_demo.gold.{table_name} — {count} rows written")

# Write all four Gold tables
write_gold_table(df_revenue_by_region,  "revenue_by_region")
write_gold_table(df_revenue_by_product, "revenue_by_product")
write_gold_table(df_status_breakdown,   "order_status_breakdown")
write_gold_table(df_monthly_trend,      "monthly_sales_trend")

print("\n✓ All Gold tables written successfully")

# COMMAND ----------

# Cell 7 — Add Delta constraints (safe version)
from delta.tables import DeltaTable

def add_constraint_safe(table, constraint_name, condition):
    """Drop constraint if exists, then re-add cleanly"""
    try:
        spark.sql(f"""
            ALTER TABLE {table}
            DROP CONSTRAINT {constraint_name}
        """)
        print(f"  Dropped existing constraint: {constraint_name}")
    except:
        pass  # Constraint didn't exist, that's fine
    
    spark.sql(f"""
        ALTER TABLE {table}
        ADD CONSTRAINT {constraint_name}
        CHECK ({condition})
    """)
    print(f"  ✓ Added constraint: {constraint_name} — CHECK ({condition})")

print("Adding constraints to revenue_by_region...")
add_constraint_safe("kali_demo.gold.revenue_by_region", 
                    "revenue_positive", "total_revenue > 0")
add_constraint_safe("kali_demo.gold.revenue_by_region", 
                    "orders_positive", "total_orders > 0")

print("\nTesting constraint enforcement...")
try:
    spark.sql("""
        INSERT INTO kali_demo.gold.revenue_by_region
        VALUES ('TEST_REGION', -999.99, 0, 0.0, current_timestamp())
    """)
    print("✗ Constraint did not fire — unexpected")
except Exception as e:
    print(f"✓ Constraint correctly rejected bad data:")
    print(f"  {str(e)[:150]}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cell 8 — Full Medallion Architecture verification
# MAGIC
# MAGIC -- Bronze layer
# MAGIC SELECT 'BRONZE' AS layer, 'sales_orders_csv' AS table_name, COUNT(*) AS row_count 
# MAGIC FROM kali_demo.bronze.sales_orders_csv
# MAGIC UNION ALL
# MAGIC SELECT 'BRONZE', 'sales_orders_postgres', COUNT(*) 
# MAGIC FROM kali_demo.bronze.sales_orders_postgres
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Silver layer
# MAGIC SELECT 'SILVER', 'sales_orders', COUNT(*) 
# MAGIC FROM kali_demo.silver.sales_orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Gold layer
# MAGIC SELECT 'GOLD', 'revenue_by_region', COUNT(*) 
# MAGIC FROM kali_demo.gold.revenue_by_region
# MAGIC UNION ALL
# MAGIC SELECT 'GOLD', 'revenue_by_product', COUNT(*) 
# MAGIC FROM kali_demo.gold.revenue_by_product
# MAGIC UNION ALL
# MAGIC SELECT 'GOLD', 'order_status_breakdown', COUNT(*) 
# MAGIC FROM kali_demo.gold.order_status_breakdown
# MAGIC UNION ALL
# MAGIC SELECT 'GOLD', 'monthly_sales_trend', COUNT(*) 
# MAGIC FROM kali_demo.gold.monthly_sales_trend
# MAGIC
# MAGIC ORDER BY layer, table_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta transaction history for Gold revenue_by_region
# MAGIC DESCRIBE HISTORY kali_demo.gold.revenue_by_region