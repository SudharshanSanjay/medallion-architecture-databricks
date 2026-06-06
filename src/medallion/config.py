import os

# Environment switch — set ENV=databricks when running in Databricks
# Defaults to local for clone-and-run reproducibility
ENV = os.getenv("MEDALLION_ENV", "local")

# ── Local paths (used when ENV=local) ──────────────────────────────
LOCAL_BASE       = "./lakehouse"
LOCAL_BRONZE_CSV = f"{LOCAL_BASE}/bronze/sales_orders_csv"
LOCAL_BRONZE_PG  = f"{LOCAL_BASE}/bronze/sales_orders_postgres"
LOCAL_SILVER     = f"{LOCAL_BASE}/silver/sales_orders"
LOCAL_GOLD       = f"{LOCAL_BASE}/gold"

# ── Databricks Unity Catalog names (used when ENV=databricks) ──────
CATALOG          = "kali_demo"
BRONZE_SCHEMA    = "bronze"
SILVER_SCHEMA    = "silver"
GOLD_SCHEMA      = "gold"

BRONZE_CSV_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.sales_orders_csv"
BRONZE_PG_TABLE  = f"{CATALOG}.{BRONZE_SCHEMA}.sales_orders_postgres"
SILVER_TABLE     = f"{CATALOG}.{SILVER_SCHEMA}.sales_orders"

GOLD_TABLES = {
    "revenue_by_region":    f"{CATALOG}.{GOLD_SCHEMA}.revenue_by_region",
    "revenue_by_product":   f"{CATALOG}.{GOLD_SCHEMA}.revenue_by_product",
    "order_status":         f"{CATALOG}.{GOLD_SCHEMA}.order_status_breakdown",
    "monthly_trend":        f"{CATALOG}.{GOLD_SCHEMA}.monthly_sales_trend",
}

# ── CSV source path ────────────────────────────────────────────────
CSV_SOURCE_LOCAL      = "data/raw/sales_orders_csv.csv"
CSV_SOURCE_DATABRICKS = "/Volumes/kali_demo/bronze/raw_files/sales_orders_csv.csv"

CSV_PATH = (
    CSV_SOURCE_DATABRICKS if ENV == "databricks"
    else CSV_SOURCE_LOCAL
)


def is_databricks() -> bool:
    return ENV == "databricks"


def is_local() -> bool:
    return ENV == "local"