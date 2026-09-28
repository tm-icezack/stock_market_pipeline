"""
stock_market_backfill DAG — one-shot historical data load.

Trigger this manually to populate the raw_data folder and database
with the full historical range. It does NOT run on a schedule.

After the backfill completes the daily stock_market DAG takes over,
fetching only the scheduled execution date each day.

To trigger:
    Airflow UI → stock_market_backfill → Trigger DAG w/ config:
    {
        "start_date": "2024-01-01",
        "end_date":   "2026-09-12"
    }

    Or accept the defaults below.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from common import backfill_default_args

BACKFILL_START = "2024-01-01"
BACKFILL_END   = "2026-09-12"

with DAG(
    dag_id="stock_market_backfill",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=backfill_default_args,
    tags=["stock_market", "backfill"],
    params={
        "start_date": BACKFILL_START,
        "end_date":   BACKFILL_END,
    },
) as dag:

    fetch_history = BashOperator(
        task_id="fetch_history",
        bash_command=(
            "python3 /opt/airflow/src/fetchers.py "
            "--start {{ params.start_date }} "
            "--end   {{ params.end_date }}"
        ),
    )

    load_history = BashOperator(
        task_id="load_history",
        bash_command="python3 /opt/airflow/src/loader.py",
    )

    fetch_history >> load_history
