"""
conftest.py — shared pytest fixtures for the medallion test suite.
Creates a single local SparkSession with Delta enabled,
reused across all tests for speed.
"""

import sys
import os
import pytest

# Add src/ to path so medallion package is importable in tests
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture(scope="session")
def spark():
    """
    Session-scoped SparkSession with Delta Lake enabled.
    Created once, shared across all tests, stopped at the end.
    scope="session" means it starts once and lives for the whole
    pytest run — much faster than creating per test.
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("MedallionTests")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0"
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    yield spark  # hand the session to tests

    spark.stop()  # clean up after all tests finish