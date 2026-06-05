# Databricks notebook source
# Cell 1 — Read from Bronze layer
df_bronze_csv = spark.table("kali_demo.bronze.sales_orders_csv")
df_bronze_pg  = spark.table("kali_demo.bronze.sales_orders_postgres")

print("Bronze CSV rows     :", df_bronze_csv.count())
print("Bronze Postgres rows:", df_bronze_pg.count())
print("\nCSV Schema:")
df_bronze_csv.printSchema()

# COMMAND ----------

# Diagnose column mismatch
print("CSV columns:", df_bronze_csv.columns)
print("\nPostgres columns:", df_bronze_pg.columns)

# Find the difference
csv_cols = set(df_bronze_csv.columns)
pg_cols  = set(df_bronze_pg.columns)

print("\nColumns in Postgres but NOT in CSV:", pg_cols - csv_cols)
print("Columns in CSV but NOT in Postgres:", csv_cols - pg_cols)

# COMMAND ----------

# Cell 2 — Combine both Bronze sources (schema aligned)
from pyspark.sql.functions import col

# Drop created_at from Postgres — it doesn't exist in CSV source
# ingested_at already captures when each row was loaded
df_bronze_pg_aligned = df_bronze_pg.drop("created_at")

print("Postgres columns after alignment:", df_bronze_pg_aligned.columns)
print("CSV columns                      :", df_bronze_csv.columns)

# Now union safely by name
df_combined = df_bronze_csv.unionByName(df_bronze_pg_aligned)

print("\nCombined row count:", df_combined.count())
print("Expected          : 800 (300 CSV + 500 Postgres)")

# Check overlap — order_ids that exist in BOTH sources
csv_ids      = df_bronze_csv.select("order_id")
postgres_ids = df_bronze_pg_aligned.select("order_id")
overlap      = csv_ids.intersect(postgres_ids)

print(f"\nOverlapping order_ids between sources: {overlap.count()}")
print("These are candidates for deduplication in Cell 3")

# Preview combined data showing both sources
print("\nSample rows from combined DataFrame:")
df_combined.select("order_id", "customer_name", "product_name", "source") \
    .orderBy("order_id") \
    .show(10, truncate=False)

# COMMAND ----------

# Cell 3 — Deduplicate using Window function
from pyspark.sql.functions import row_number, when
from pyspark.sql.window import Window

# Assign priority: postgres = 1 (master), csv = 2 (secondary)
# When same order_id exists in both, Postgres wins
df_combined = df_combined.withColumn(
    "source_priority",
    when(col("source") == "postgres", 1).otherwise(2)
)

# Define a window partitioned by order_id, ordered by priority
# This ranks rows within each order_id group
window_spec = Window.partitionBy("order_id").orderBy("source_priority")

# Add row number — rank 1 = the row we want to keep
df_ranked = df_combined.withColumn("row_num", row_number().over(window_spec))

# Keep only rank 1 rows — one row per order_id
df_deduped = df_ranked.filter(col("row_num") == 1) \
                      .drop("row_num", "source_priority")

print("Rows before dedup:", df_combined.count())
print("Rows after dedup :", df_deduped.count())
print("Duplicates removed:", df_combined.count() - df_deduped.count())

# Verify — check order_id 1 specifically to confirm Postgres won
print("\norder_id = 1 after dedup (should be postgres source):")
df_deduped.filter(col("order_id") == 1) \
          .select("order_id", "customer_name", "product_name", "source") \
          .show(truncate=False)

# COMMAND ----------

# Cell 4 — Clean and standardise
from pyspark.sql.functions import trim, initcap, upper, when, col

df_cleaned = df_deduped \
    .withColumn("customer_name", initcap(trim(col("customer_name")))) \
    .withColumn("product_name",  initcap(trim(col("product_name")))) \
    .withColumn("product_sku",   upper(trim(col("product_sku")))) \
    .withColumn("region",        upper(trim(col("region")))) \
    .withColumn("status",        upper(trim(col("status")))) \
    .withColumn("customer_name", 
                when(trim(col("customer_name")) == "", None)
                .otherwise(col("customer_name"))) \
    .withColumn("status",
                when(trim(col("status")) == "", None)
                .otherwise(col("status")))

# Verify cleaning worked
print("Distinct status values after cleaning:")
df_cleaned.select("status").distinct().orderBy("status").show()

print("Distinct region values after cleaning:")
df_cleaned.select("region").distinct().orderBy("region").show()

print("Null check after cleaning:")
print("  Null customer_name:", df_cleaned.filter(col("customer_name").isNull()).count())
print("  Null status        :", df_cleaned.filter(col("status").isNull()).count())
print("  Null region        :", df_cleaned.filter(col("region").isNull()).count())

print("\nSample cleaned rows:")
df_cleaned.select("customer_name", "product_name", "product_sku", "region", "status") \
          .show(5, truncate=False)

# COMMAND ----------

# Cell 5 — Enforce strict schema types
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, DateType, StringType, TimestampType

df_schema_enforced = df_cleaned \
    .withColumn("order_id",     col("order_id").cast(IntegerType())) \
    .withColumn("customer_id",  col("customer_id").cast(IntegerType())) \
    .withColumn("quantity",     col("quantity").cast(IntegerType())) \
    .withColumn("unit_price",   col("unit_price").cast(DoubleType())) \
    .withColumn("order_date",   col("order_date").cast(DateType())) \
    .withColumn("customer_name",col("customer_name").cast(StringType())) \
    .withColumn("product_name", col("product_name").cast(StringType())) \
    .withColumn("product_sku",  col("product_sku").cast(StringType())) \
    .withColumn("region",       col("region").cast(StringType())) \
    .withColumn("status",       col("status").cast(StringType())) \
    .withColumn("ingested_at",  col("ingested_at").cast(TimestampType()))

# Add a silver_processed_at timestamp to track when Silver processing ran
from pyspark.sql.functions import current_timestamp
df_schema_enforced = df_schema_enforced \
    .withColumn("silver_processed_at", current_timestamp())

print("Final Silver schema:")
df_schema_enforced.printSchema()
print("Row count:", df_schema_enforced.count())

# COMMAND ----------

# Cell 6 — MERGE INTO Silver table (upsert)

# Step 1 — Create Silver table if it doesn't exist yet
# We create it from the first run of df_schema_enforced
# On subsequent runs MERGE will handle updates and inserts
spark.sql("""
    CREATE TABLE IF NOT EXISTS kali_demo.silver.sales_orders (
        order_id            INT,
        customer_id         INT,
        customer_name       STRING,
        product_sku         STRING,
        product_name        STRING,
        quantity            INT,
        unit_price          DOUBLE,
        order_date          DATE,
        region              STRING,
        status              STRING,
        ingested_at         TIMESTAMP,
        source              STRING,
        silver_processed_at TIMESTAMP
    )
    USING DELTA
""")

print("✓ Silver table created or already exists")

# Step 2 — Register df_schema_enforced as a temp view
# MERGE INTO is a SQL operation so we need a SQL-accessible view
df_schema_enforced.createOrReplaceTempView("silver_updates")

# Step 3 — MERGE INTO
# Match on order_id — if found update, if not found insert
spark.sql("""
    MERGE INTO kali_demo.silver.sales_orders AS target
    USING silver_updates AS source
    ON target.order_id = source.order_id
    WHEN MATCHED THEN
        UPDATE SET
            target.customer_id         = source.customer_id,
            target.customer_name       = source.customer_name,
            target.product_sku         = source.product_sku,
            target.product_name        = source.product_name,
            target.quantity            = source.quantity,
            target.unit_price          = source.unit_price,
            target.order_date          = source.order_date,
            target.region              = source.region,
            target.status              = source.status,
            target.ingested_at         = source.ingested_at,
            target.source              = source.source,
            target.silver_processed_at = source.silver_processed_at
    WHEN NOT MATCHED THEN
        INSERT *
""")

print("✓ MERGE INTO complete")

# Step 4 — Verify
silver_df = spark.table("kali_demo.silver.sales_orders")
print("Silver table row count:", silver_df.count())

# COMMAND ----------

# Cell 7 — Silver layer verification and Delta history

silver_df = spark.table("kali_demo.silver.sales_orders")

print("=" * 55)
print("SILVER LAYER VERIFICATION")
print("=" * 55)

# Row and column count
print(f"\nRow count  : {silver_df.count()}")
print(f"Column count: {len(silver_df.columns)}")

# Data quality check
print("\nNull checks across key columns:")
from pyspark.sql.functions import col
for c in ["order_id", "customer_name", "product_name", 
          "region", "status", "unit_price"]:
    null_count = silver_df.filter(col(c).isNull()).count()
    status_icon = "✓" if null_count == 0 else "✗"
    print(f"  {status_icon} {c}: {null_count} nulls")

# Distinct value checks
print("\nDistinct status values:")
silver_df.select("status").distinct().orderBy("status").show()

print("Distinct regions:")
silver_df.select("region").distinct().orderBy("region").show()

# Sample rows
print("Sample cleaned Silver rows:")
silver_df.select(
    "order_id", "customer_name", "product_name",
    "region", "status", "unit_price", "source"
).show(5, truncate=False)

print("=" * 55)
print("✓ Silver layer verification complete")
print("=" * 55)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cell 8 — Delta transaction history
# MAGIC -- Shows every operation ever performed on the Silver table
# MAGIC DESCRIBE HISTORY kali_demo.silver.sales_orders

# COMMAND ----------

