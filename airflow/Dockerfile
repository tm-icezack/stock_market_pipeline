FROM apache/airflow:3.0.2

COPY requirements.txt /requirements.txt

# The official Airflow image intentionally blocks package installation as root.
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt