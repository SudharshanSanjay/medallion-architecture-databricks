from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit
from medallion.schema import SALES_ORDERS_SCHEMA
from medallion.config import CSV_PATH, is_databricks


def ingest_csv(spark: SparkSession) -> DataFrame:
    """
    Read the sales orders CSV from the configured path.
    Uses explicit schema — no inferSchema guessing.
    Returns DataFrame with ingested_at and source columns added.
    """
    df = spark.read \
        .schema(SALES_ORDERS_SCHEMA) \
        .option("header", "true") \
        .csv(CSV_PATH)

    return df \
        .withColumn("ingested_at", current_timestamp()) \
        .withColumn("source", lit("csv"))


def ingest_postgres(
    spark: SparkSession,
    host: str,
    port: int,
    database: str = "kali_source",
    user: str     = "kali_user",
    password: str = "kali_pass",
    table: str    = "sales_orders",
) -> DataFrame:
    """
    Read the sales orders table from PostgreSQL via JDBC.
    Host and port come from the ngrok tunnel when running locally,
    or from a real DB connection string in production.
    Returns DataFrame with ingested_at and source columns added.
    """
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    return df \
        .withColumn("ingested_at", current_timestamp()) \
        .withColumn("source", lit("postgres"))


def write_bronze(df: DataFrame, path_or_table: str, env: str = "local") -> int:
    """
    Write a Bronze DataFrame as a Delta table.
    In local mode: writes to a local Delta path.
    In databricks mode: saves as a Unity Catalog table.
    Returns row count written.
    """
    writer = df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true")

    if env == "databricks":
        writer.saveAsTable(path_or_table)
    else:
        writer.save(path_or_table)

    return df.count()