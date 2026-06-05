# Medallion Architecture with Databricks

A end-to-end data engineering project implementing the Bronze → Silver → Gold Medallion Architecture using Databricks, Delta Lake, and Unity Catalog.

## Architecture
## Tech Stack

- **Databricks** — Serverless compute, notebooks
- **Delta Lake** — ACID transactions, time travel, versioning
- **Unity Catalog** — Data governance and lineage
- **PySpark** — ETL transformations
- **PostgreSQL** — Source database (Dockerized)
- **ngrok** — Secure tunnel for hybrid cloud/on-premise connectivity
- **Docker** — Local infrastructure
- **Python 3.12** — Data generation scripts

## Project Structure
## Medallion Layers

### Bronze — Raw Ingestion
- Ingests CSV from Databricks Volume
- Ingests PostgreSQL table via JDBC over ngrok tunnel
- Lands raw data as Delta tables in `kali_demo.bronze`

### Silver — Cleaned and Trusted
- Combines both Bronze sources using `unionByName`
- Deduplicates using Window functions (Postgres = master source)
- Standardises casing, trims whitespace, enforces schema types
- Uses `MERGE INTO` for idempotent upserts

### Gold — Business Aggregations
- Revenue by region
- Revenue by product
- Order status breakdown with revenue at risk
- Monthly sales trend
- Delta table constraints for data quality enforcement

## Key Concepts Demonstrated

- Medallion Architecture (Bronze/Silver/Gold)
- Delta Lake ACID transactions and time travel
- Unity Catalog data governance
- Window functions for deduplication
- MERGE INTO idempotent upserts
- Hybrid cloud/on-premise connectivity via ngrok + JDBC
- Delta table constraints for data quality
