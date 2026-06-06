from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, round, sum, count, avg, desc, asc,
    date_format, current_timestamp
)


def revenue_by_region(df_silver: DataFrame) -> DataFrame:
    """Total revenue, order count and avg order value per region."""
    return df_silver \
        .withColumn("order_value",
                    round(col("unit_price") * col("quantity"), 2)) \
        .groupBy("region") \
        .agg(
            round(sum("order_value"), 2).alias("total_revenue"),
            count("order_id").alias("total_orders"),
            round(sum("order_value") / count("order_id"), 2)
                .alias("avg_order_value")
        ) \
        .orderBy(desc("total_revenue")) \
        .withColumn("gold_created_at", current_timestamp())


def revenue_by_product(df_silver: DataFrame) -> DataFrame:
    """Total revenue, order count, units sold and avg price per product."""
    return df_silver \
        .withColumn("order_value",
                    round(col("unit_price") * col("quantity"), 2)) \
        .groupBy("product_name") \
        .agg(
            round(sum("order_value"), 2).alias("total_revenue"),
            count("order_id").alias("total_orders"),
            sum("quantity").alias("total_units_sold"),
            round(avg("unit_price"), 2).alias("avg_unit_price")
        ) \
        .orderBy(desc("total_revenue")) \
        .withColumn("gold_created_at", current_timestamp())


def order_status_breakdown(df_silver: DataFrame) -> DataFrame:
    """Order count, revenue and avg order value per status."""
    return df_silver \
        .withColumn("order_value",
                    round(col("unit_price") * col("quantity"), 2)) \
        .groupBy("status") \
        .agg(
            count("order_id").alias("total_orders"),
            round(sum("order_value"), 2).alias("total_revenue"),
            round(sum("order_value") / count("order_id"), 2)
                .alias("avg_order_value")
        ) \
        .orderBy(desc("total_orders")) \
        .withColumn("gold_created_at", current_timestamp())


def monthly_sales_trend(df_silver: DataFrame) -> DataFrame:
    """Monthly revenue, order count and avg order value over time."""
    return df_silver \
        .withColumn("order_value",
                    round(col("unit_price") * col("quantity"), 2)) \
        .withColumn("year_month",
                    date_format(col("order_date"), "yyyy-MM")) \
        .groupBy("year_month") \
        .agg(
            round(sum("order_value"), 2).alias("monthly_revenue"),
            count("order_id").alias("total_orders"),
            round(sum("order_value") / count("order_id"), 2)
                .alias("avg_order_value")
        ) \
        .orderBy(asc("year_month")) \
        .withColumn("gold_created_at", current_timestamp())