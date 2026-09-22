FROM python:3.12-slim

RUN pip install --no-cache-dir \
    dbt-postgres==1.9.0 \
    dbt-core==1.9.0

WORKDIR /usr/app/dbt

ENTRYPOINT ["dbt"]
CMD ["--help"]
