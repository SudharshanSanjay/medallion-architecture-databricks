# Improvement Plan: From POC to Portfolio-Grade Project

## Context

This repository is currently a clean but small **proof-of-concept**: a Databricks
Bronze → Silver → Gold medallion pipeline over a B2B industrial-equipment sales dataset
(~800 rows), built from three notebooks, a Dockerized PostgreSQL source exposed over an ngrok
tunnel, and a CSV generator. It demonstrates good patterns (MERGE upserts, window-function
dedup, Delta constraints) but is **not showcase-ready**:

- **Not runnable by a reviewer** — requires a live Databricks workspace, Unity Catalog, and a
  hardcoded, expiring ngrok URL (`5.tcp.eu.ngrok.io:16097`).
- **No ML/analytics payoff** — the Gold layer ends at static aggregations; there's no
  predictive or business-insight capstone.
- **Thin docs, no tests, no diagrams, no data-quality gate, hardcoded credentials.**

**Goal:** a *targeted polish* upgrade that lands two flagship features —
**(1) a local PySpark + Delta runnable path** so anyone can clone-and-run, and **(2) a sales-
forecasting ML layer** (Prophet/scikit-learn + MLflow) on top of Gold — plus the
data-engineering-depth and documentation glue that makes it read as a complete, professional
project. The existing Databricks notebooks stay intact as the "cloud / production" path; the new
local path shares the transformation logic rather than duplicating it.

---

## Guiding principles

- **Don't break the Databricks story.** The notebooks stay as the "production / cloud" path.
  Everything new is additive.
- **Share logic, not copies.** Extract the core transformation logic (silver dedup/clean, gold
  aggregations) into a small `src/medallion/` Python package that *both* the Databricks notebooks
  and the local runner import — this is the data-engineering-depth signal and avoids drift.
- **Local-first reproducibility.** A reviewer should `pip install -r requirements.txt` and run
  one command to materialize Bronze→Silver→Gold→ML locally with no cloud account.
- **Quote the value.** Docs + a diagram + an ML capstone are what make a recruiter stop scrolling.

---

## Target structure (additions in **bold**)

```
medallion-architecture-databricks/
├── notebooks/                      # unchanged — the Databricks/cloud path
│   ├── 00_setup.py
│   ├── 01_bronze_ingest.py
│   ├── 02_silver_transform.py
│   ├── 03_gold_aggregate.py
│   └── 04_gold_forecast.py         # ML capstone notebook (Databricks variant)
├── src/medallion/                  # shared, importable transformation package
│   ├── __init__.py
│   ├── config.py                   # paths, catalog/schema names, env switch (local vs databricks)
│   ├── schema.py                   # explicit StructType for sales_orders (reused everywhere)
│   ├── bronze.py                   # ingest CSV + Postgres → bronze
│   ├── silver.py                   # union + window-dedup + clean (extracted from notebook 02)
│   ├── gold.py                     # the 4 aggregations (extracted from notebook 03)
│   └── forecast.py                 # sales-forecasting model + MLflow logging
├── run_local.py                    # one-command local Bronze→Silver→Gold→ML driver (local Spark+Delta)
├── src/data_generator.py           # extended (see below)
├── sql/seed_postgres.sql           # unchanged
├── docker-compose.yml              # unchanged (kept for the cloud/hybrid demo)
├── requirements.txt                # pyspark, delta-spark, mlflow, prophet/scikit-learn, pytest, etc.
├── tests/                          # pytest suite over the shared logic
│   ├── test_silver.py              # dedup + cleaning correctness on tiny fixtures
│   ├── test_gold.py                # aggregation math correctness
│   └── conftest.py                 # local SparkSession fixture with Delta enabled
├── .github/workflows/ci.yml        # lint + pytest on push (DataOps depth signal)
├── docs/
│   ├── IMPROVEMENT_PLAN.md          # this document
│   ├── architecture.md             # narrative + embedded diagram
│   └── images/architecture.png     # (or mermaid in README)
├── .env.example                    # documents Postgres creds / ngrok URL instead of hardcoding
└── README.md                       # rewritten — see below
```

---

## Workstreams

### 1. Shared transformation package — `src/medallion/` (data-engineering depth)
Extract the logic currently inline in the notebooks into pure, testable functions that take and
return Spark DataFrames. This is the backbone everything else reuses.

- `schema.py` — one explicit `StructType` for the sales-orders schema (replaces the notebooks'
  `inferSchema`/ad-hoc casts; referenced by bronze, silver, tests).
- `silver.py` — `build_silver(df_csv, df_pg)`: `unionByName` → window-function dedup
  (Postgres as master, exactly as in `02_silver_transform.py`) → trim/initcap/type-enforce.
- `gold.py` — `revenue_by_region(df)`, `revenue_by_product(df)`, `order_status_breakdown(df)`,
  `monthly_sales_trend(df)` — lifted verbatim from `03_gold_aggregate.py` cells 2–5.
- `config.py` — central place for catalog/schema names (`kali_demo.bronze` …), local Delta
  output paths, and an `ENV` switch (`local` | `databricks`) so the same functions write to a
  local `./lakehouse/` dir or to Unity Catalog tables.
- The existing notebooks are then **refactored to import these functions** (thin cells that call
  `medallion.silver.build_silver(...)` etc.), proving the cloud and local paths run identical code.

### 2. Local runnable path — `run_local.py` + `requirements.txt` (reproducibility)
- `requirements.txt`: `pyspark==3.5.*`, `delta-spark` (matching), `mlflow`, `prophet`
  (or `scikit-learn` fallback), `pandas`, `matplotlib`, `pytest`, `psycopg2-binary` (optional).
- `run_local.py`: builds a local `SparkSession` configured with the Delta extension
  (`io.delta.sql.DeltaSparkSessionExtension` + catalog), reads the generated CSV (and optionally
  the Dockerized Postgres if running) → calls `bronze` → `silver` → `gold` → `forecast`,
  writing Delta tables under `./lakehouse/`. Prints the same medallion audit summary the
  notebooks do. One command: `python run_local.py`.
- Document the Spark/Delta version-pinning gotcha in the README (delta-spark must match the
  PySpark minor version).

### 3. Sales-forecasting ML capstone — `src/medallion/forecast.py` + `notebooks/04_gold_forecast.py`
- Consume `monthly_sales_trend` (and optionally per-region series) from Gold.
- Fit a time-series model (Prophet preferred; scikit-learn linear/seasonal fallback if Prophet
  install is heavy) to forecast the next N months of revenue, overall and per region.
- **Track with MLflow**: log params (horizon, model type), metrics (MAE/MAPE on a holdout),
  and the forecast plot artifact. Locally this writes to `./mlruns/`; on Databricks it uses the
  managed MLflow tracking server.
- Write a `gold.revenue_forecast` Delta table (date, region, yhat, yhat_lower, yhat_upper) so the
  forecast is a first-class Gold asset.
- Produce a forecast vs. actuals chart saved to `docs/images/` for the README.

> **Data caveat:** the current generator produces *uniformly random* dates/prices, so there's no
> real trend/seasonality to forecast. **Workstream 5 fixes this** by upgrading the generator to
> emit a realistic trend + seasonality, making the forecast meaningful rather than noise. This
> dependency is intentional — do Workstream 5 before finalizing the model.

### 4. Tests + CI (DataOps depth, low cost)
- `tests/conftest.py`: session-scoped local Spark + Delta fixture.
- `test_silver.py`: feed a tiny hand-built DataFrame with known duplicate order_ids across
  sources → assert Postgres record wins and row count is correct; assert casing/trim applied.
- `test_gold.py`: feed a 3–4 row fixture → assert `revenue_by_region` totals and
  `revenue_at_risk` math are exact.
- `.github/workflows/ci.yml`: `pip install -r requirements.txt`, run `ruff`/`flake8` (optional)
  and `pytest`. This is the green-checkmark "I do DataOps" signal on the repo.

### 5. Realistic data generator upgrade — `src/data_generator.py`
- Inject a **growth trend + seasonal pattern** into `order_date`/volume (e.g. monthly order
  counts ramp up over the year with a seasonal bump) so the forecast model has real signal.
- Make row count and output path CLI args; create `data/raw/` if missing.
- Optionally bump default volume (e.g. 2k–5k rows) so aggregations and the forecast look
  substantial — still tiny enough to run locally in seconds.

### 6. Documentation overhaul — `README.md` + `docs/`
The current README has empty "Architecture" and "Project Structure" sections. Rewrite to a
portfolio-grade README:
- One-paragraph **what & why**, a **Mermaid architecture diagram** (sources → Bronze → Silver →
  Gold → ML/forecast → dashboard-ready), and the rendered forecast chart.
- **Two run paths** clearly labeled: *Local (any laptop)* — `pip install` + `python run_local.py`;
  and *Databricks (cloud)* — import notebooks, set Unity Catalog, configure Postgres/ngrok.
- **Results section** with sample Gold metrics and the forecast plot (the recruiter payoff).
- Move credentials/ngrok URL to `.env.example` + note; stop hardcoding `kali_pass` and the tunnel.
- `docs/architecture.md`: deeper narrative on medallion layers, dedup strategy, idempotency,
  and the ML approach.

---

## Suggested execution order
1. **Workstream 5** (better generator) — unblocks meaningful forecasting and bigger aggregations.
2. **Workstream 1** (shared `src/medallion/` package) — backbone; refactor notebooks to import it.
3. **Workstream 2** (`run_local.py` + `requirements.txt`) — makes it runnable end-to-end locally.
4. **Workstream 3** (forecasting + MLflow) — the ML flagship.
5. **Workstream 4** (tests + CI) — lock in correctness, add the green checkmark.
6. **Workstream 6** (README + docs + diagram) — last, so it describes the finished system.

---

## Verification (end-to-end, local — no cloud account needed)
1. **Generate data:** `python src/data_generator.py --rows 3000` → confirm `data/raw/sales_orders_csv.csv` exists with a visible trend.
2. **Run the pipeline:** `python run_local.py` → confirm `./lakehouse/` has bronze/silver/gold Delta tables and the printed medallion audit shows expected row counts (silver deduped < bronze sum).
3. **Forecast:** confirm `gold.revenue_forecast` Delta table is written, `./mlruns/` has an MLflow run with MAE/MAPE logged, and a forecast chart is saved under `docs/images/`.
4. **Tests:** `pytest` → all green (silver dedup + gold math assertions pass).
5. **CI:** push a branch → GitHub Actions `ci.yml` runs `pytest` and goes green.
6. **Databricks path (optional, if workspace available):** notebooks `01`→`04` still run top-to-bottom against Unity Catalog using the shared `medallion` package, proving cloud/local parity.
7. **Docs:** README renders the Mermaid diagram + forecast image; both run paths documented; no hardcoded credentials remain in tracked files.

---

## Explicitly out of scope (targeted-polish boundary)
- Databricks Asset Bundles (DABs) / full DLT-Lakeflow rewrite.
- Streaming / true CDC ingestion.
- A hosted BI dashboard (Gold + forecast tables are left dashboard-ready instead).
- GenAI natural-language querying layer.
