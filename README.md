# Stock Market Pipeline

A local data pipeline that fetches daily stock prices for **AAL**, **TSLA**, and **GOOGL** from Yahoo Finance, loads them into PostgreSQL, and transforms them with dbt — all orchestrated by Apache Airflow running in Docker.

---

## Architecture

```
Yahoo Finance (yfinance)
        ↓
Airflow fetch_data task       → writes daily Parquet files
        ↓
Airflow load_data task        → upserts into PostgreSQL staging table
        ↓
Airflow dbt_build task        → builds analytics models and runs tests
        ↓
PostgreSQL analytics schema   → fct_stock_prices, daily_returns
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) with at least **4 GB RAM** and **2 CPUs** allocated
- Git

---

## Local setup

### 1. Clone the repository

```bash
git clone https://github.com/tm-icezack/stock_market_pipeline.git
cd stock_market_pipeline
```

### 2. Create the environment file

Create `airflow/.env` with the following content:

```dotenv
AIRFLOW_UID=50000
FERNET_KEY=<generate-with-step-below>

DB_HOST=postgres
DB_PORT=5432
DB_NAME=stock_market
DB_USER=airflow
DB_PASSWORD=airflow
```

Generate a Fernet key:

```bash
docker run --rm apache/airflow:3.0.2 python -c \
  "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Paste the output as the `FERNET_KEY` value.

### 3. Build the images and initialise Airflow

```bash
cd airflow
docker compose build
docker compose up airflow-init
```

Wait for `airflow-init` to exit successfully, then start the full stack:

```bash
docker compose up -d
```

### 4. Build the dbt image

```bash
docker compose --profile dbt build dbt
```

---

## Accessing services

| Service | URL / connection | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8081 | `airflow` / `airflow` |
| PostgreSQL (pgAdmin/DBeaver) | `127.0.0.1:5433` | `stock_market_admin` / `StockMarketLocal2026` |

> **pgAdmin settings:** Host `127.0.0.1`, Port `5433`, Database `stock_market`

---

## Running the pipeline

### Daily pipeline (automatic)

The `stock_market` DAG runs daily and executes three tasks in sequence:

```
fetch_data → load_data → dbt_build
```

To trigger it manually:

1. Open the Airflow UI at http://localhost:8081
2. Find `stock_market` and unpause it
3. Click **Trigger DAG**

### Load historical data (one-time backfill)

Trigger the `stock_market_backfill` DAG with the desired date range:

1. Open the Airflow UI
2. Click `stock_market_backfill` → **Trigger DAG w/ config**
3. Pass:

```json
{
    "start_date": "2024-01-01",
    "end_date": "2026-09-12"
}
```

---

## Verifying a successful run

**Check raw Parquet files were written:**

```bash
ls airflow/raw_data/
```

**Check records loaded into PostgreSQL:**

```bash
cd airflow
docker compose exec postgres psql -U airflow -d stock_market -c \
  "SELECT ticker, COUNT(*) AS records, MIN(date), MAX(date)
   FROM staging.stg_stock_prices
   GROUP BY ticker ORDER BY ticker;"
```

**Check dbt analytics models were built:**

```bash
docker compose exec postgres psql -U airflow -d stock_market -c \
  "SELECT ticker, COUNT(*) FROM analytics.fct_stock_prices GROUP BY ticker;"
```

---

## Project structure

```
stock_market_pipeline/
├─ README.md
├─ .gitignore
└─ airflow/
   ├─ docker-compose.yaml       Airflow, PostgreSQL, Redis, dbt services
   ├─ Dockerfile                Custom Airflow image
   ├─ requirements.txt          Pinned Python dependencies
   ├─ .env                      Local secrets — not committed to Git
   ├─ dags/
   │  ├─ stock_market.py        Daily pipeline DAG
   │  └─ stock_market_backfill.py  One-shot historical load DAG
   ├─ src/
   │  ├─ fetchers.py            Yahoo Finance → Parquet
   │  └─ loader.py              Parquet → PostgreSQL (upsert)
   ├─ dbt/
   │  ├─ Dockerfile             dbt image build
   │  ├─ dbt_project.yml        dbt project configuration
   │  ├─ profiles.yml           Database connection — not committed to Git
   │  └─ models/
   │     ├─ sources.yml         Declares staging.stg_stock_prices
   │     ├─ staging/
   │     │  └─ stg_stock_prices.sql
   │     └─ marts/
   │        ├─ fct_stock_prices.sql
   │        ├─ daily_returns.sql
   │        └─ schema.yml       dbt data quality tests
   ├─ sql/
   │  └─ init.sql               Auto-creates stock_market DB on first start
   └─ raw_data/                 Generated Parquet files — not committed to Git
```

---

## Database objects

| Layer | Schema | Object | Description |
|---|---|---|---|
| Raw | `staging` | `stg_stock_prices` | Daily OHLCV prices loaded by Airflow |
| Staging | `staging` | `stg_stock_prices` (view) | Cleaned source — built by dbt |
| Mart | `analytics` | `fct_stock_prices` | Analytics-ready fact table |
| Mart | `analytics` | `daily_returns` | Daily percentage return per ticker |

---

## Adding a new ticker

1. Open `airflow/src/fetchers.py`
2. Add the ticker symbol to the `TICKERS` list:

```python
TICKERS = ["AAL", "TSLA", "GOOGL", "AAPL"]
```

3. Update the accepted values test in `airflow/dbt/models/marts/schema.yml`:

```yaml
- accepted_values:
    values: ["AAL", "GOOGL", "TSLA", "AAPL"]
```

4. Trigger the backfill DAG to load historical data for the new ticker.

---

## Stopping the stack

```bash
cd airflow
docker compose down
```

To also remove the PostgreSQL data volume (irreversible — deletes all loaded data):

```bash
docker compose down -v
```

---

## Git hygiene

The following are excluded from version control:

```
airflow/.env
airflow/logs/
airflow/config/airflow.cfg
airflow/raw_data/
airflow/dbt/target/
airflow/dbt/logs/
airflow/dbt/.user.yml
airflow/dbt/profiles.yml
```
