FROM apache/airflow:3.0.2

USER root

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER airflow