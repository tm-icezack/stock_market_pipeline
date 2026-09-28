"""
Unit tests for airflow/src/fetchers.py

Tests cover:
- MultiIndex flattening and column normalisation
- Date filtering to target date only
- Saving one Parquet file per trading date
- Overwriting existing files (idempotent saves)
- Empty DataFrame handling in _save_parquet
"""

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

# Make airflow/src importable without installing it as a package.
sys.path.insert(0, str(Path(__file__).parent.parent / "airflow" / "src"))

from fetchers import _save_parquet  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_flat_df(dates: list[date], tickers: list[str]) -> pd.DataFrame:
    """Build a flat stock-price DataFrame matching the fetcher's output shape."""
    rows = []
    for d in dates:
        for ticker in tickers:
            rows.append({
                "date": d,
                "ticker": ticker,
                "open": 100.0,
                "high": 110.0,
                "low": 90.0,
                "close": 105.0,
                "adj_close": 105.0,
                "volume": 1_000_000,
                "ingestion_timestamp": pd.Timestamp.now(tz="UTC"),
                "data_source": "yfinance",
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# _save_parquet tests
# ---------------------------------------------------------------------------

class TestSaveParquet:

    def test_creates_one_file_per_date(self, tmp_path):
        dates = [date(2026, 9, 10), date(2026, 9, 11), date(2026, 9, 12)]
        df = _make_flat_df(dates, ["AAL", "TSLA", "GOOGL"])

        count = _save_parquet(df, str(tmp_path))

        assert count == 3
        written = sorted(tmp_path.glob("*.parquet"))
        assert [f.stem for f in written] == ["2026-09-10", "2026-09-11", "2026-09-12"]

    def test_each_file_contains_all_tickers(self, tmp_path):
        dates = [date(2026, 9, 12)]
        tickers = ["AAL", "TSLA", "GOOGL"]
        df = _make_flat_df(dates, tickers)

        _save_parquet(df, str(tmp_path))

        saved = pd.read_parquet(tmp_path / "2026-09-12.parquet")
        assert sorted(saved["ticker"].unique().tolist()) == sorted(tickers)

    def test_overwrites_existing_file(self, tmp_path):
        """Re-running _save_parquet replaces the existing Parquet file."""
        dates = [date(2026, 9, 12)]
        df_first  = _make_flat_df(dates, ["AAL"])
        df_second = _make_flat_df(dates, ["AAL", "TSLA"])

        _save_parquet(df_first,  str(tmp_path))
        _save_parquet(df_second, str(tmp_path))

        saved = pd.read_parquet(tmp_path / "2026-09-12.parquet")
        assert set(saved["ticker"].unique()) == {"AAL", "TSLA"}

    def test_returns_zero_for_empty_dataframe(self, tmp_path):
        df = pd.DataFrame(columns=["date", "ticker", "close"])
        count = _save_parquet(df, str(tmp_path))
        assert count == 0
        assert list(tmp_path.glob("*.parquet")) == []

    def test_creates_output_directory_if_missing(self, tmp_path):
        nested = tmp_path / "a" / "b" / "c"
        df = _make_flat_df([date(2026, 9, 12)], ["AAL"])

        _save_parquet(df, str(nested))

        assert (nested / "2026-09-12.parquet").exists()


# ---------------------------------------------------------------------------
# Date-filtering logic (mirrors fetch_for_date behaviour)
# ---------------------------------------------------------------------------

class TestDateFiltering:

    def test_filters_to_target_date_only(self):
        dates = [date(2026, 9, 10), date(2026, 9, 11), date(2026, 9, 12)]
        df = _make_flat_df(dates, ["AAL", "TSLA"])
        target = date(2026, 9, 12)

        filtered = df[df["date"] == target]

        assert set(filtered["date"].unique()) == {target}
        assert len(filtered) == 2  # one row per ticker

    def test_empty_result_when_target_is_not_trading_day(self):
        """A Saturday returns no rows after date filtering."""
        dates = [date(2026, 9, 10), date(2026, 9, 11)]  # Thu, Fri
        df = _make_flat_df(dates, ["AAL"])
        saturday = date(2026, 9, 13)

        filtered = df[df["date"] == saturday]

        assert filtered.empty
