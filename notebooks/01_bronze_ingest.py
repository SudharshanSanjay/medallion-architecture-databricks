# Databricks notebook source
# Cell 1 — Setup
from pyspark.sql.functions import current_timestamp, lit

print("Spark version:", spark.version)
print("Starting Bronze layer ingestion...")

# COMMAND ----------

# Cell 2 — Ingest CSV into Bronze Delta table
csv_path = "/Volumes/kali_demo/bronze/raw_files/sales_orders_csv.csv"

df_csv = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(csv_path)

# Add metadata columns
df_csv = df_csv \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("source", lit("csv"))

# Preview
print("CSV row count:", df_csv.count())
print("Schema:")
df_csv.printSchema()
df_csv.show(5, truncate=False)

# COMMAND ----------

# Cell 3 — Write to Bronze Delta table (CSV source)
df_csv.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("kali_demo.bronze.sales_orders_csv")

print("✓ CSV data written to kali_demo.bronze.sales_orders_csv")

# Verify by reading back
verify_df = spark.table("kali_demo.bronze.sales_orders_csv")
print("Row count in Delta table:", verify_df.count())

# COMMAND ----------

# Cell 4 — Read from Postgres via JDBC
jdbc_url = "jdbc:postgresql://5.tcp.eu.ngrok.io:16097/kali_source"

df_postgres = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "sales_orders") \
    .option("user", "kali_user") \
    .option("password", "kali_pass") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Add metadata columns — same pattern as CSV
from pyspark.sql.functions import current_timestamp, lit
df_postgres = df_postgres \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("source", lit("postgres"))

print("Postgres row count:", df_postgres.count())
print("Schema:")
df_postgres.printSchema()
df_postgres.show(5, truncate=False)

# COMMAND ----------

# Cell 5 — Write Postgres data to Bronze Delta table
df_postgres.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("kali_demo.bronze.sales_orders_postgres")

print("✓ Postgres data written to kali_demo.bronze.sales_orders_postgres")

# Verify by reading back
verify_pg = spark.table("kali_demo.bronze.sales_orders_postgres")
print("Row count in Delta table:", verify_pg.count())

# COMMAND ----------

# Cell 6 — Bronze layer audit
print("=" * 50)
print("BRONZE LAYER AUDIT")
print("=" * 50)

# CSV table summary
df_csv_check = spark.table("kali_demo.bronze.sales_orders_csv")
print(f"\n📄 sales_orders_csv")
print(f"   Row count     : {df_csv_check.count()}")
print(f"   Source label  : {df_csv_check.select('source').distinct().collect()}")
print(f"   Date range    : {df_csv_check.selectExpr('min(order_date)', 'max(order_date)').collect()}")
print(f"   Null order_ids: {df_csv_check.filter('order_id IS NULL').count()}")

# Postgres table summary
df_pg_check = spark.table("kali_demo.bronze.sales_orders_postgres")
print(f"\n🐘 sales_orders_postgres")
print(f"   Row count     : {df_pg_check.count()}")
print(f"   Source label  : {df_pg_check.select('source').distinct().collect()}")
print(f"   Date range    : {df_pg_check.selectExpr('min(order_date)', 'max(order_date)').collect()}")
print(f"   Null order_ids: {df_pg_check.filter('order_id IS NULL').count()}")

print("\n" + "=" * 50)
print("✓ Bronze layer ingestion complete")
print("=" * 50)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify both Bronze tables are SQL queryable
# MAGIC SELECT 
# MAGIC   'sales_orders_csv'      AS table_name,
# MAGIC   COUNT(*)                AS row_count,
# MAGIC   MIN(order_date)         AS earliest_order,
# MAGIC   MAX(order_date)         AS latest_order,
# MAGIC   COUNT(DISTINCT region)  AS unique_regions
# MAGIC FROM kali_demo.bronze.sales_orders_csv
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'sales_orders_postgres' AS table_name,
# MAGIC   COUNT(*)                AS row_count,
# MAGIC   MIN(order_date)         AS earliest_order,
# MAGIC   MAX(order_date)         AS latest_order,
# MAGIC   COUNT(DISTINCT region)  AS unique_regions
# MAGIC FROM kali_demo.bronze.sales_orders_postgres