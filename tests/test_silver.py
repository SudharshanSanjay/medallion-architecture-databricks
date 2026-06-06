"""
test_silver.py — unit tests for medallion.silver.build_silver()

Tests deduplication and cleaning logic using tiny hand-built
DataFrames with known values and predictable outcomes.
"""

import pytest
from datetime import date
from pyspark.sql import Row
from pyspark.sql.functions import col
from medallion.silver import build_silver
from datetime import date, datetime


def make_csv_df(spark):
    """
    Small CSV source DataFrame — 3 orders.
    order_id 1 and 2 overlap with Postgres.
    order_id 3 is CSV-only.
    """
    return spark.createDataFrame([
        Row(order_id=1, customer_id=10,
            customer_name="priya nair",
            product_sku="sku-0001",
            product_name="conveyor belt",
            quantity=2, unit_price=25000.0,
            order_date=date(2025, 10, 1),
            region="karnataka", status="pending",
            ingested_at=datetime(2025, 10, 1, 0, 0, 0), source="csv"),
        Row(order_id=2, customer_id=20,
            customer_name="ARJUN MEHTA",
            product_sku="SKU-0002",
            product_name="BUCKET ELEVATOR",
            quantity=3, unit_price=35000.0,
            order_date=date(2025, 11, 1),
            region="GUJARAT", status="CONFIRMED",
            ingested_at=datetime(2025, 10, 1, 0, 0, 0), source="csv"),
        Row(order_id=3, customer_id=30,
            customer_name="Divya Rao",
            product_sku="SKU-0003",
            product_name="Screw Conveyor",
            quantity=1, unit_price=28000.0,
            order_date=date(2025, 12, 1),
            region="Tamil Nadu", status="shipped",
            ingested_at=datetime(2025, 10, 1, 0, 0, 0), source="csv"),
    ])


def make_pg_df(spark):
    """
    Small Postgres source DataFrame — 2 orders.
    order_id 1 and 2 overlap with CSV — Postgres should win.
    Different customer names to verify which source won.
    """
    return spark.createDataFrame([
        Row(order_id=1, customer_id=10,
            customer_name="sanjay rajan",      # different from CSV
            product_sku="sku-0001",
            product_name="belt feeder",         # different from CSV
            quantity=5, unit_price=22000.0,
            order_date=date(2025, 10, 1),
            region="telangana", status="delivered",
            ingested_at=datetime(2025, 10, 1, 0, 0, 0), source="postgres"),
        Row(order_id=2, customer_id=20,
            customer_name="meena pillai",       # different from CSV
            product_sku="SKU-0002",
            product_name="rotary valve",        # different from CSV
            quantity=4, unit_price=18000.0,
            order_date=date(2025, 11, 1),
            region="maharashtra", status="cancelled",
            ingested_at=datetime(2025, 10, 1, 0, 0, 0), source="postgres"),
    ])


class TestDeduplication:
    """Tests for the Window-function dedup logic."""

    def test_row_count_after_dedup(self, spark):
        """
        CSV has 3 rows, Postgres has 2 rows.
        order_id 1 and 2 overlap → should deduplicate to 3 unique rows.
        """
        df_csv = make_csv_df(spark)
        df_pg  = make_pg_df(spark)
        df_silver = build_silver(df_csv, df_pg)
        assert df_silver.count() == 3, \
            "Expected 3 rows after dedup (2 Postgres + 1 CSV-only)"

    def test_postgres_wins_on_conflict(self, spark):
        """
        For order_id=1, Postgres has customer_name='Sanjay Rajan'
        and CSV has 'Priya Nair'. Postgres should win.
        """
        df_csv = make_csv_df(spark)
        df_pg  = make_pg_df(spark)
        df_silver = build_silver(df_csv, df_pg)

        row = df_silver.filter(col("order_id") == 1) \
                       .select("customer_name") \
                       .collect()[0]

        assert row["customer_name"] == "Sanjay Rajan", \
            f"Expected 'Sanjay Rajan' (Postgres), got '{row['customer_name']}'"

    def test_csv_only_row_preserved(self, spark):
        """
        order_id=3 exists only in CSV — should appear in Silver.
        """
        df_csv = make_csv_df(spark)
        df_pg  = make_pg_df(spark)
        df_silver = build_silver(df_csv, df_pg)

        count = df_silver.filter(col("order_id") == 3).count()
        assert count == 1, "CSV-only order_id=3 should be preserved in Silver"


class TestCleaning:
    """Tests for string standardisation applied in build_silver."""

    def test_region_uppercased(self, spark):
        """Regions should all be uppercase after cleaning."""
        df_csv = make_csv_df(spark)
        df_pg  = make_pg_df(spark)
        df_silver = build_silver(df_csv, df_pg)

        regions = [r["region"] for r in
                   df_silver.select("region").collect()]
        for region in regions:
            assert region == region.upper(), \
                f"Region '{region}' is not uppercase"

    def test_status_uppercased(self, spark):
        """Status values should all be uppercase after cleaning."""
        df_csv = make_csv_df(spark)
        df_pg  = make_pg_df(spark)
        df_silver = build_silver(df_csv, df_pg)

        statuses = [r["status"] for r in
                    df_silver.select("status").collect()]
        for status in statuses:
            assert status == status.upper(), \
                f"Status '{status}' is not uppercase"

    def test_customer_name_title_cased(self, spark):
        """Customer names should be title cased (initcap) after cleaning."""
        df_csv = make_csv_df(spark)
        df_pg  = make_pg_df(spark)
        df_silver = build_silver(df_csv, df_pg)

        names = [r["customer_name"] for r in
                 df_silver.select("customer_name").collect()]
        for name in names:
            # Each word should start with uppercase
            for word in name.split():
                assert word[0].isupper(), \
                    f"Name '{name}' is not properly title cased"

    def test_silver_processed_at_not_null(self, spark):
        """Every Silver row must have a silver_processed_at timestamp."""
        df_csv = make_csv_df(spark)
        df_pg  = make_pg_df(spark)
        df_silver = build_silver(df_csv, df_pg)

        null_count = df_silver.filter(
            col("silver_processed_at").isNull()
        ).count()
        assert null_count == 0, \
            "silver_processed_at should never be null"