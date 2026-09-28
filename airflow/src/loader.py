"""
loader.py — Load daily Parquet files into PostgreSQL using upserts.

Each run reads all Parquet files from the shared raw-data directory,
then inserts or updates rows in staging.stg_stock_prices using the
(date, ticker) primary key. Rows that already exist are updated in place;
new rows are inserted. This makes every run safe to re-run.

Per-ticker watermark
--------------------
Before upserting, the loader queries the last loaded date for EACH ticker
independently. Only rows newer than that ticker's watermark are sent to
the database. This means:
  - If TSLA loads successfully but AAL fails, AAL's watermark stays behind
    and will be retried correctly on the next run.
  - Adding a new ticker does not cause it to be skipped because another
    ticker has a later watermark.
"""

import logging
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

UPSERT_SQL = """
INSERT INTO staging.{table} (
    date, ticker, adj_close, close, high, low, open,
    volume, ingestion_timestamp, data_source
)
VALUES (
    :date, :ticker, :adj_close, :close, :high, :low, :open,
    :volume, :ingestion_timestamp, :data_source
)
ON CONFLICT (date, ticker)
DO UPDATE SET
    adj_close           = EXCLUDED.adj_close,
    close               = EXCLUDED.close,
    high                = EXCLUDED.high,
    low                 = EXCLUDED.low,
    open                = EXCLUDED.open,
    volume              = EXCLUDED.volume,
    ingestion_timestamp = EXCLUDED.ingestion_timestamp,
    data_source         = EXCLUDED.data_source;
"""

WATERMARK_SQL = """
SELECT ticker, MAX(date) AS last_date
FROM staging.{table}
GROUP BY ticker;
"""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_engine(db_url: str):
    """Create and verify a SQLAlchemy engine."""
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("Database connection verified.")
        return engine
    except OperationalError as exc:
        log.error("Could not connect to the database: %s", exc)
        raise


def _read_parquet_files(parquet_dir: str) -> pd.DataFrame:
    """Read and combine all Parquet files from parquet_dir."""
    files = sorted(Path(parquet_dir).glob("*.parquet"))
    if not files:
        log.warning("No Parquet files found in %s.", parquet_dir)
        return pd.DataFrame()

    log.info("Reading %d Parquet file(s) from %s.", len(files), parquet_dir)
    df = pd.concat(
        [pd.read_parquet(f) for f in files],
        ignore_index=True,
    )
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def _validate(df: pd.DataFrame) -> None:
    """Raise ValueError if the DataFrame fails basic quality checks."""
    required_columns = {"date", "ticker", "close", "open", "high", "low", "volume"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if df["date"].isna().sum():
        raise ValueError(f"{df['date'].isna().sum()} rows have null date.")
    if df["ticker"].isna().sum():
        raise ValueError(f"{df['ticker'].isna().sum()} rows have null ticker.")
    if df["close"].isna().sum():
        raise ValueError(f"{df['close'].isna().sum()} rows have null close price.")
    if (df["close"] < 0).sum():
        raise ValueError(f"{(df['close'] < 0).sum()} rows have negative close price.")

    log.info("Validation passed: %d rows across %d ticker(s).",
             len(df), df["ticker"].nunique())


def _get_watermarks(engine, table_name: str) -> dict:
    """
    Return a dict of {ticker: last_loaded_date} from the database.
    Returns an empty dict if the table is empty or does not exist yet.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(WATERMARK_SQL.format(table=table_name))
            ).fetchall()
        watermarks = {row.ticker: row.last_date for row in rows}
        if watermarks:
            for ticker, last_date in watermarks.items():
                log.info("Watermark — %s: last loaded date = %s.", ticker, last_date)
        else:
            log.info("No existing data found; loading all rows.")
        return watermarks
    except ProgrammingError:
        # Table exists in schema but has no data or watermark query failed.
        log.info("Could not read watermarks; loading all rows.")
        return {}


def _apply_watermarks(df: pd.DataFrame, watermarks: dict) -> pd.DataFrame:
    """
    Filter df to keep only rows newer than each ticker's watermark.
    Tickers with no watermark (new tickers) are loaded in full.
    """
    if not watermarks:
        return df

    mask = pd.Series(True, index=df.index)
    for ticker, last_date in watermarks.items():
        ticker_mask = (df["ticker"] == ticker) & (df["date"] <= last_date)
        mask = mask & ~ticker_mask

    filtered = df[mask]
    skipped  = len(df) - len(filtered)
    if skipped:
        log.info("Skipped %d already-loaded row(s) based on per-ticker watermarks.", skipped)
    return filtered


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def load_data_to_db(parquet_dir: str, db_url: str, table_name: str) -> None:
    """
    Read Parquet files and upsert new rows into staging.{table_name}.

    Uses a per-ticker watermark so each ticker's progress is tracked
    independently. Re-running is fully safe and idempotent.
    """
    engine    = _build_engine(db_url)
    df        = _read_parquet_files(parquet_dir)

    if df.empty:
        log.info("Nothing to load.")
        return

    _validate(df)

    watermarks = _get_watermarks(engine, table_name)
    df         = _apply_watermarks(df, watermarks)

    if df.empty:
        log.info("All tickers are up to date. Nothing new to load.")
        return

    upsert_sql = text(UPSERT_SQL.format(table=table_name))
    records    = df.to_dict(orient="records")

    try:
        with engine.begin() as conn:
            conn.execute(upsert_sql, records)
    except ProgrammingError as exc:
        log.error(
            "SQL error during upsert — check that staging.%s exists: %s",
            table_name, exc,
        )
        raise
    except OperationalError as exc:
        log.error("Database connection lost during upsert: %s", exc)
        raise

    log.info("Upserted %d new row(s) into staging.%s.", len(records), table_name)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parquet_dir = "/opt/airflow/raw_data"

    db_host     = os.getenv("DB_HOST")
    db_name     = os.getenv("DB_NAME")
    db_user     = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_port     = os.getenv("DB_PORT", "5432")

    missing_env = [k for k, v in {
        "DB_HOST": db_host, "DB_NAME": db_name,
        "DB_USER": db_user, "DB_PASSWORD": db_password,
    }.items() if not v]

    if missing_env:
        log.error("Missing required environment variables: %s", missing_env)
        sys.exit(1)

    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    load_data_to_db(parquet_dir, db_url, table_name="stg_stock_prices")

import logging
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Upsert helper
# ---------------------------------------------------------------------------

UPSERT_SQL = """
INSERT INTO staging.{table} (
    date, ticker, adj_close, close, high, low, open,
    volume, ingestion_timestamp, data_source
)
VALUES (
    :date, :ticker, :adj_close, :close, :high, :low, :open,
    :volume, :ingestion_timestamp, :data_source
)
ON CONFLICT (date, ticker)
DO UPDATE SET
    adj_close           = EXCLUDED.adj_close,
    close               = EXCLUDED.close,
    high                = EXCLUDED.high,
    low                 = EXCLUDED.low,
    open                = EXCLUDED.open,
    volume              = EXCLUDED.volume,
    ingestion_timestamp = EXCLUDED.ingestion_timestamp,
    data_source         = EXCLUDED.data_source;
"""


def _build_engine(db_url: str):
    """Create and verify a SQLAlchemy engine."""
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("Database connection verified.")
        return engine
    except OperationalError as exc:
        log.error("Could not connect to the database: %s", exc)
        raise


def _read_parquet_files(parquet_dir: str) -> pd.DataFrame:
    """Read and combine all Parquet files from parquet_dir."""
    files = sorted(Path(parquet_dir).glob("*.parquet"))
    if not files:
        log.warning("No Parquet files found in %s.", parquet_dir)
        return pd.DataFrame()

    log.info("Reading %d Parquet file(s) from %s.", len(files), parquet_dir)
    df = pd.concat(
        [pd.read_parquet(f) for f in files],
        ignore_index=True,
    )
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def _validate(df: pd.DataFrame) -> None:
    """Raise ValueError if the DataFrame fails basic quality checks."""
    required_columns = {"date", "ticker", "close", "open", "high", "low", "volume"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    null_date = df["date"].isna().sum()
    null_ticker = df["ticker"].isna().sum()
    null_close = df["close"].isna().sum()

    if null_date:
        raise ValueError(f"{null_date} rows have null date.")
    if null_ticker:
        raise ValueError(f"{null_ticker} rows have null ticker.")
    if null_close:
        raise ValueError(f"{null_close} rows have null close price.")

    negative_prices = (df["close"] < 0).sum()
    if negative_prices:
        raise ValueError(f"{negative_prices} rows have negative close price.")

    log.info("Validation passed: %d rows, %d tickers.",
             len(df), df["ticker"].nunique())


def load_data_to_db(parquet_dir: str, db_url: str, table_name: str) -> None:
    """
    Read Parquet files and upsert rows into staging.{table_name}.

    Every (date, ticker) pair is inserted or updated. Re-running the
    loader is safe and idempotent.
    """
    engine = _build_engine(db_url)
    df = _read_parquet_files(parquet_dir)

    if df.empty:
        log.info("Nothing to load.")
        return

    _validate(df)

    upsert_sql = text(UPSERT_SQL.format(table=table_name))
    records = df.to_dict(orient="records")

    try:
        with engine.begin() as conn:          # auto-commits on success, rolls back on error
            conn.execute(upsert_sql, records)
    except ProgrammingError as exc:
        log.error(
            "SQL error during upsert — check that staging.%s exists: %s",
            table_name, exc,
        )
        raise
    except OperationalError as exc:
        log.error("Database connection lost during upsert: %s", exc)
        raise

    log.info("Upserted %d rows into staging.%s.", len(records), table_name)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parquet_dir = "/opt/airflow/raw_data"

    db_host     = os.getenv("DB_HOST")
    db_name     = os.getenv("DB_NAME")
    db_user     = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_port     = os.getenv("DB_PORT", "5432")

    missing_env = [k for k, v in {
        "DB_HOST": db_host, "DB_NAME": db_name,
        "DB_USER": db_user, "DB_PASSWORD": db_password,
    }.items() if not v]

    if missing_env:
        log.error("Missing required environment variables: %s", missing_env)
        sys.exit(1)

    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    load_data_to_db(parquet_dir, db_url, table_name="stg_stock_prices")
