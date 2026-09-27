"""
stock_market DAG — daily pipeline: fetch → load → dbt build

Tasks
-----
fetch_data : Download ~780 days of AAL/TSLA/GOOGL from Yahoo Finance
             and write one Parquet file per trading date to /opt/airflow/raw_data.
load_data  : Upsert Parquet files into staging.stg_stock_prices (idempotent).
dbt_build  : Run dbt build inside the stock_market_dbt container:
             - stg_stock_prices (view)
             - fct_stock_prices (table)
             - daily_returns    (table)
             - dbt data-quality tests
             If any test fails the task fails and the DAG is marked failed.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# The Compose network that connects Airflow workers to the dbt container.
# Default: airflow_default  (project folder name + "_default")
DOCKER_NETWORK = os.getenv("AIRFLOW_DOCKER_NETWORK", "airflow_default")

# Path to the dbt project on the host, mounted into the dbt container.
DBT_PROJECT_HOST_PATH = os.getenv(
    "DBT_PROJECT_HOST_PATH",
    "/opt/airflow/dbt",   # container-side path via volume mount
)

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="stock_market",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["stock_market", "ingestion", "dbt"],
) as dag:

    # ------------------------------------------------------------------
    # 1. Extract: Yahoo Finance → Parquet files
    # ------------------------------------------------------------------
    fetch_data = BashOperator(
        task_id="fetch_data",
        bash_command="python3 /opt/airflow/src/fetchers.py",
    )

    # ------------------------------------------------------------------
    # 2. Load: Parquet files → staging.stg_stock_prices (upsert)
    # ------------------------------------------------------------------
    load_data = BashOperator(
        task_id="load_data",
        bash_command="python3 /opt/airflow/src/loader.py",
    )

    # ------------------------------------------------------------------
    # 3. Transform & test: dbt build
    #    Runs inside the stock_market_dbt image on the same Docker network
    #    so it can reach the postgres service by hostname.
    # ------------------------------------------------------------------
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "docker run --rm "
            f"--network {DOCKER_NETWORK} "
            "-v $(pwd)/dbt:/usr/app/dbt "
            "-w /usr/app/dbt "
            "-e DBT_PROFILES_DIR=/usr/app/dbt "
            "-e DB_HOST=postgres "
            "-e DB_PORT=5432 "
            "-e DB_NAME=stock_market "
            "-e DB_USER=airflow "
            "-e DB_PASSWORD=airflow "
            "stock_market_dbt:latest build --fail-fast"
        ),
    )

    # ------------------------------------------------------------------
    # Pipeline order
    # ------------------------------------------------------------------
    fetch_data >> load_data >> dbt_build
