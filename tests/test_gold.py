"""
test_gold.py — unit tests for medallion.gold aggregation functions.

Uses a tiny 4-row fixture with known values so expected
totals can be verified exactly.

Row 1: Tamil Nadu, Conveyor Belt,   qty=2, price=10000 → value=20000
Row 2: Tamil Nadu, Bucket Elevator, qty=1, price=30000 → value=30000
Row 3: Karnataka, Conveyor Belt,    qty=3, price=10000 → value=30000
Row 4: Karnataka, Screw Conveyor,   qty=2, price=20000 → value=40000

Tamil Nadu total : 20000 + 30000 = 50000
Karnataka total  : 30000 + 40000 = 70000
Conveyor Belt    : 20000 + 30000 = 50000, 5 units
"""

import pytest
from datetime import date, datetime
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType,
    DoubleType, DateType, TimestampType
)
from medallion.gold import (
    revenue_by_region,
    revenue_by_product,
    order_status_breakdown,
    monthly_sales_trend,
)

TS = datetime(2025, 10, 1, 0, 0, 0)

SILVER_SCHEMA = StructType([
    StructField("order_id",            IntegerType(),   True),
    StructField("customer_id",         IntegerType(),   True),
    StructField("customer_name",       StringType(),    True),
    StructField("product_sku",         StringType(),    True),
    StructField("product_name",        StringType(),    True),
    StructField("quantity",            IntegerType(),   True),
    StructField("unit_price",          DoubleType(),    True),
    StructField("order_date",          DateType(),      True),
    StructField("region",              StringType(),    True),
    StructField("status",              StringType(),    True),
    StructField("ingested_at",         TimestampType(), True),
    StructField("source",              StringType(),    True),
    StructField("silver_processed_at", TimestampType(), True),
])


@pytest.fixture
def df_silver(spark):
    data = [
        (1, 1, "Arjun Mehta",  "SKU-0001", "Conveyor Belt",
         2, 10000.0, date(2025, 10, 1),  "TAMIL NADU", "DELIVERED", TS, "csv",      TS),
        (2, 2, "Priya Nair",   "SKU-0002", "Bucket Elevator",
         1, 30000.0, date(2025, 10, 15), "TAMIL NADU", "SHIPPED",   TS, "csv",      TS),
        (3, 3, "Vikram Singh", "SKU-0001", "Conveyor Belt",
         3, 10000.0, date(2025, 11, 1),  "KARNATAKA",  "DELIVERED", TS, "postgres", TS),
        (4, 4, "Divya Rao",    "SKU-0003", "Screw Conveyor",
         2, 20000.0, date(2025, 11, 15), "KARNATAKA",  "PENDING",   TS, "postgres", TS),
    ]
    return spark.createDataFrame(data, schema=SILVER_SCHEMA)


class TestRevenueByRegion:

    def test_row_count(self, spark, df_silver):
        """Should have exactly 2 regions."""
        assert revenue_by_region(df_silver).count() == 2

    def test_karnataka_total_revenue(self, spark, df_silver):
        """Karnataka: (3×10000) + (2×20000) = 70000."""
        row = revenue_by_region(df_silver) \
            .filter(col("region") == "KARNATAKA") \
            .collect()[0]
        assert row["total_revenue"] == 70000.0, \
            f"Expected 70000, got {row['total_revenue']}"

    def test_tamil_nadu_total_revenue(self, spark, df_silver):
        """Tamil Nadu: (2×10000) + (1×30000) = 50000."""
        row = revenue_by_region(df_silver) \
            .filter(col("region") == "TAMIL NADU") \
            .collect()[0]
        assert row["total_revenue"] == 50000.0, \
            f"Expected 50000, got {row['total_revenue']}"

    def test_ordered_by_revenue_desc(self, spark, df_silver):
        """Karnataka (70000) should appear before Tamil Nadu (50000)."""
        rows = revenue_by_region(df_silver).select("region").collect()
        assert rows[0]["region"] == "KARNATAKA", \
            "Karnataka should be first (highest revenue)"


class TestRevenueByProduct:

    def test_row_count(self, spark, df_silver):
        """Should have exactly 3 products."""
        assert revenue_by_product(df_silver).count() == 3

    def test_conveyor_belt_total_revenue(self, spark, df_silver):
        """Conveyor Belt: (2×10000) + (3×10000) = 50000."""
        row = revenue_by_product(df_silver) \
            .filter(col("product_name") == "Conveyor Belt") \
            .collect()[0]
        assert row["total_revenue"] == 50000.0, \
            f"Expected 50000, got {row['total_revenue']}"

    def test_total_units_sold(self, spark, df_silver):
        """Conveyor Belt: 2 + 3 = 5 units sold."""
        row = revenue_by_product(df_silver) \
            .filter(col("product_name") == "Conveyor Belt") \
            .collect()[0]
        assert row["total_units_sold"] == 5, \
            f"Expected 5 units, got {row['total_units_sold']}"


class TestOrderStatusBreakdown:

    def test_row_count(self, spark, df_silver):
        """Should have 3 distinct statuses: DELIVERED, SHIPPED, PENDING."""
        assert order_status_breakdown(df_silver).count() == 3

    def test_delivered_order_count(self, spark, df_silver):
        """DELIVERED appears in rows 1 and 3 — count should be 2."""
        row = order_status_breakdown(df_silver) \
            .filter(col("status") == "DELIVERED") \
            .collect()[0]
        assert row["total_orders"] == 2, \
            f"Expected 2 DELIVERED orders, got {row['total_orders']}"


class TestMonthlySalesTrend:

    def test_row_count(self, spark, df_silver):
        """Rows 1&2 are Oct 2025, rows 3&4 are Nov 2025 — 2 months."""
        assert monthly_sales_trend(df_silver).count() == 2

    def test_october_revenue(self, spark, df_silver):
        """Oct 2025: (2×10000) + (1×30000) = 50000."""
        row = monthly_sales_trend(df_silver) \
            .filter(col("year_month") == "2025-10") \
            .collect()[0]
        assert row["monthly_revenue"] == 50000.0, \
            f"Expected 50000, got {row['monthly_revenue']}"

    def test_ordered_chronologically(self, spark, df_silver):
        """Oct 2025 should appear before Nov 2025."""
        months = [r["year_month"] for r in
                  monthly_sales_trend(df_silver)
                  .select("year_month").collect()]
        assert months == sorted(months), \
            "Monthly trend should be ordered chronologically"