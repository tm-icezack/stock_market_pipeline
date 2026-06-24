from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="stock_market",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",   
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    fetch_data = BashOperator(
        task_id="fetch_data",
        bash_command="python3 /opt/airflow/src/fetchers.py",
    )

    load_data = BashOperator(
        task_id="load_data",
        bash_command="python3 /opt/airflow/src/loader.py",
    )

    fetch_data >> load_data