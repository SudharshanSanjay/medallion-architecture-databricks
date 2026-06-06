"""

run_local.py — one-command local Bronze → Silver → Gold pipeline runner.

Usage:
    python run_local.py

Writes Delta tables to ./lakehouse/
No cloud account needed.
"""

import sys
import os
import logging

# Add src/ to path so medallion package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from pyspark.sql import SparkSession
import medallion.config as cfg
from medallion.bronze import ingest_csv, write_bronze
from medallion.silver import build_silver
from medallion import gold as g
from medallion.forecast import run_forecast

# ── Suppress noisy Spark/Hadoop INFO logs ─────────────────────────
logging.getLogger("py4j").setLevel(logging.ERROR)


def build_spark() -> SparkSession:
    """Build a local SparkSession with Delta Lake enabled."""
    return (
        SparkSession.builder
        .appName("MedallionLocal")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.postgresql:postgresql:42.7.3"
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def run_bronze(spark: SparkSession) -> tuple:
    """Ingest CSV into Bronze Delta table."""
    print("\n── Bronze layer ──────────────────────────────────")
    print("  Reading CSV...")
    df_csv = ingest_csv(spark)
    count  = write_bronze(df_csv, cfg.LOCAL_BRONZE_CSV, env="local")
    print(f"  ✓ Bronze CSV written → {cfg.LOCAL_BRONZE_CSV}")
    print(f"    Rows: {count}")
    return df_csv, count


def run_silver(spark: SparkSession, df_csv) -> tuple:
    """Build Silver from Bronze — dedup, clean, enforce schema."""
    print("\n── Silver layer ──────────────────────────────────")
    print("  Reading Bronze CSV Delta table...")
    df_bronze_csv = spark.read.format("delta").load(cfg.LOCAL_BRONZE_CSV)

    print("  Building Silver (dedup + clean)...")
    df_silver = build_silver(df_bronze_csv, df_bronze_csv)

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(cfg.LOCAL_SILVER)

    count = spark.read.format("delta").load(cfg.LOCAL_SILVER).count()
    print(f"  ✓ Silver written → {cfg.LOCAL_SILVER}")
    print(f"    Rows: {count}")
    return df_silver, count


def run_gold(spark: SparkSession, df_silver) -> dict:
    """Build all 4 Gold aggregations from Silver."""
    print("\n── Gold layer ────────────────────────────────────")
    counts = {}

    aggregations = {
        "revenue_by_region":  g.revenue_by_region(df_silver),
        "revenue_by_product": g.revenue_by_product(df_silver),
        "order_status":       g.order_status_breakdown(df_silver),
        "monthly_trend":      g.monthly_sales_trend(df_silver),
    }

    for name, df_gold in aggregations.items():
        path = f"{cfg.LOCAL_GOLD}/{name}"
        df_gold.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(path)
        count = spark.read.format("delta").load(path).count()
        counts[name] = count
        print(f"  ✓ {name} → {path} ({count} rows)")

    return counts

def run_forecast_layer(spark: SparkSession) -> dict:
    """Run Prophet forecasting on Gold monthly trend."""
    metrics = run_forecast(
        spark=spark,
        monthly_trend_path=f"{cfg.LOCAL_GOLD}/monthly_trend",
        forecast_output_path=f"{cfg.LOCAL_GOLD}/revenue_forecast",
        chart_output_path="docs/images/revenue_forecast.png",
        horizon_months=6,
        holdout_months=2,
    )
    return metrics

def print_audit(bronze_count, silver_count, gold_counts, forecast_metrics=None):
    """Print final medallion audit summary."""
    print("\n" + "=" * 52)
    print("  MEDALLION PIPELINE — LOCAL RUN COMPLETE")
    print("=" * 52)
    print(f"  Bronze CSV rows   : {bronze_count}")
    print(f"  Silver rows       : {silver_count}")
    print(f"  Dedup removed     : {bronze_count - silver_count}")
    print()
    for name, count in gold_counts.items():
        print(f"  Gold {name:<22}: {count} rows")
    if forecast_metrics:
        print()
        print(f"  Forecast MAE      : ₹{forecast_metrics['mae']:,.2f}")
        print(f"  Forecast MAPE     : {forecast_metrics['mape_pct']:.1f}%")
    print()
    print(f"  Delta tables written to: ./lakehouse/")
    print(f"  MLflow runs at        : ./mlruns/")
    print(f"  Forecast chart at     : docs/images/revenue_forecast.png")
    print("=" * 52)


def main():
    print("Starting Medallion local pipeline...")
    print(f"Environment : {cfg.ENV}")
    print(f"CSV source  : {cfg.CSV_PATH}")

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        df_csv,    bronze_count = run_bronze(spark)
        df_silver, silver_count = run_silver(spark, df_csv)
        gold_counts             = run_gold(spark, df_silver)
        forecast_metrics = run_forecast_layer(spark)
        print_audit(bronze_count, silver_count, gold_counts,forecast_metrics)
    finally:
        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    main()