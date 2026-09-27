"""
fetchers.py — Download stock-price data from Yahoo Finance and save as Parquet.

Usage
-----
Daily (Airflow passes the logical execution date):
    python3 fetchers.py --date 2026-09-12

Backfill (downloads a full date range):
    python3 fetchers.py --start 2024-01-01 --end 2026-09-12

When running without arguments the script falls back to fetching the previous
trading day, which is useful for local testing.
"""

import argparse
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

TICKERS = ["AAL", "TSLA", "GOOGL"]
OUTPUT_DIR = "/opt/airflow/raw_data"

# Number of extra days fetched before the target date to account for
# weekends and public holidays when markets are closed.
LOOKBACK_DAYS = 3


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def _download(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """Download data from Yahoo Finance and return a flat DataFrame."""
    log.info("Downloading %s from %s to %s.", tickers, start, end)

    raw = yf.download(
        tickers,
        start=start,
        end=end,
        auto_adjust=False,
        progress=False,
    )

    if raw.empty:
        raise ValueError(
            f"Yahoo Finance returned no data for {tickers} between {start} and {end}."
        )

    # Flatten MultiIndex columns (price type × ticker) into rows
    df = raw.stack(level="Ticker").reset_index()

    df["ingestion_timestamp"] = pd.Timestamp.now(tz="UTC")
    df["data_source"] = "yfinance"

    df.columns = (
        df.columns.str.strip().str.lower().str.replace(" ", "_")
    )

    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def _save_parquet(df: pd.DataFrame, output_dir: str) -> int:
    """
    Write one Parquet file per trading date.
    Overwrites an existing file so re-runs are idempotent.
    Returns the number of files written.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    saved = 0
    for trade_date, daily_df in df.groupby("date"):
        file_path = out / f"{trade_date}.parquet"
        daily_df.to_parquet(file_path, index=False)
        log.info("Saved %s (%d rows).", file_path.name, len(daily_df))
        saved += 1

    return saved


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def fetch_for_date(target_date: date, output_dir: str = OUTPUT_DIR) -> None:
    """
    Fetch data for a single trading date (used by the daily Airflow task).

    Downloads target_date plus LOOKBACK_DAYS before it so that weekends
    and holidays do not produce an empty result. Only the Parquet file for
    target_date is written; earlier dates already exist from previous runs.
    """
    start = target_date - timedelta(days=LOOKBACK_DAYS)
    end   = target_date + timedelta(days=1)   # yfinance end is exclusive

    df = _download(TICKERS, start.isoformat(), end.isoformat())

    # Keep only the target date; earlier days are already on disk.
    df = df[df["date"] == target_date]

    if df.empty:
        log.warning(
            "%s is not a trading day (weekend or holiday). Nothing saved.",
            target_date,
        )
        return

    saved = _save_parquet(df, output_dir)
    log.info("fetch_for_date complete: %d file(s) written.", saved)


def fetch_range(start_date: date, end_date: date, output_dir: str = OUTPUT_DIR) -> None:
    """
    Fetch a full date range (used by the backfill DAG or local testing).
    Overwrites existing Parquet files so the backfill is idempotent.
    """
    df = _download(TICKERS, start_date.isoformat(), end_date.isoformat())
    saved = _save_parquet(df, output_dir)
    log.info("fetch_range complete: %d file(s) written.", saved)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Fetch data for a single trading date (daily mode).",
    )
    group.add_argument(
        "--start",
        metavar="YYYY-MM-DD",
        help="Start date for a range fetch (backfill mode).",
    )
    parser.add_argument(
        "--end",
        metavar="YYYY-MM-DD",
        help="End date for a range fetch (required with --start).",
    )
    return parser.parse_args()


def main() -> None:
    t0   = time.time()
    args = _parse_args()

    if args.date:
        # Daily mode: Airflow passes {{ ds }}
        target = datetime.strptime(args.date, "%Y-%m-%d").date()
        fetch_for_date(target)

    elif args.start:
        # Backfill mode
        if not args.end:
            log.error("--end is required when using --start.")
            sys.exit(1)
        start = datetime.strptime(args.start, "%Y-%m-%d").date()
        end   = datetime.strptime(args.end,   "%Y-%m-%d").date()
        fetch_range(start, end)

    else:
        # Fallback for local testing: fetch yesterday
        yesterday = date.today() - timedelta(days=1)
        log.info("No date argument supplied; defaulting to yesterday (%s).", yesterday)
        fetch_for_date(yesterday)

    log.info("Total execution time: %.2fs.", time.time() - t0)


if __name__ == "__main__":
    main()
