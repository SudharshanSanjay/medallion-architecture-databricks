from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, row_number, when, trim, initcap, upper, current_timestamp
)
from pyspark.sql.window import Window


def build_silver(df_csv: DataFrame, df_pg: DataFrame) -> DataFrame:
    """
    Combines Bronze CSV and Postgres DataFrames into a clean Silver DataFrame.

    Steps:
    1. Align schemas — drop created_at from Postgres if present
    2. Union both sources
    3. Deduplicate using Window function — Postgres wins on conflict
    4. Clean string columns — trim, initcap, upper
    5. Enforce types and add silver_processed_at audit column

    Returns a clean, deduplicated DataFrame ready for MERGE INTO Silver.
    """
    # Step 1 — align schemas
    if "created_at" in df_pg.columns:
        df_pg = df_pg.drop("created_at")

    # Step 2 — union by name (safe even if column order differs)
    df_combined = df_csv.unionByName(df_pg)

    # Step 3 — deduplicate: Postgres = master source
    df_combined = df_combined.withColumn(
        "source_priority",
        when(col("source") == "postgres", 1).otherwise(2)
    )
    window_spec = Window.partitionBy("order_id").orderBy("source_priority")
    df_deduped  = df_combined \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num", "source_priority")

    # Step 4 — clean string columns
    df_cleaned = df_deduped \
        .withColumn("customer_name",
                    initcap(trim(col("customer_name")))) \
        .withColumn("product_name",
                    initcap(trim(col("product_name")))) \
        .withColumn("product_sku",
                    upper(trim(col("product_sku")))) \
        .withColumn("region",
                    upper(trim(col("region")))) \
        .withColumn("status",
                    upper(trim(col("status")))) \
        .withColumn("customer_name",
                    when(trim(col("customer_name")) == "", None)
                    .otherwise(col("customer_name"))) \
        .withColumn("status",
                    when(trim(col("status")) == "", None)
                    .otherwise(col("status")))

    # Step 5 — add silver audit timestamp
    return df_cleaned.withColumn("silver_processed_at", current_timestamp())