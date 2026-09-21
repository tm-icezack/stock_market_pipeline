-- Runs only when PostgreSQL creates a new, empty data volume.
-- The initial database is "airflow"; create a separate database for pipeline data.
CREATE DATABASE stock_market;

\connect stock_market

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.stg_stock_prices (
    date DATE NOT NULL,
    ticker TEXT NOT NULL,
    adj_close NUMERIC,
    close NUMERIC,
    high NUMERIC,
    low NUMERIC,
    open NUMERIC,
    volume BIGINT,
    ingestion_timestamp TIMESTAMPTZ,
    data_source TEXT,
    PRIMARY KEY (date, ticker)
);
