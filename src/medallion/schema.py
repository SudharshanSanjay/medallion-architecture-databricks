from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType, DateType, TimestampType
)

# Canonical schema for raw sales_orders data
# Used by Bronze ingestion and Silver enforcement
SALES_ORDERS_SCHEMA = StructType([
    StructField("order_id",     IntegerType(), nullable=True),
    StructField("customer_id",  IntegerType(), nullable=True),
    StructField("customer_name",StringType(),  nullable=True),
    StructField("product_sku",  StringType(),  nullable=True),
    StructField("product_name", StringType(),  nullable=True),
    StructField("quantity",     IntegerType(), nullable=True),
    StructField("unit_price",   DoubleType(),  nullable=True),
    StructField("order_date",   DateType(),    nullable=True),
    StructField("region",       StringType(),  nullable=True),
    StructField("status",       StringType(),  nullable=True),
])

# Silver schema adds two audit columns
SILVER_SCHEMA = StructType(
    SALES_ORDERS_SCHEMA.fields + [
        StructField("ingested_at",         TimestampType(), nullable=True),
        StructField("source",              StringType(),    nullable=True),
        StructField("silver_processed_at", TimestampType(), nullable=False),
    ]
)