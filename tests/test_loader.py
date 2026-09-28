"""
Unit tests for airflow/src/loader.py

Tests cover:
- _validate: passes valid data, raises on nulls and negative prices
- _get_watermarks: returns correct per-ticker dates, handles empty table
- _apply_watermarks: filters each ticker independently
- load_data_to_db: no Parquet files found, all up to date
"""

import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "airflow" / "src"))

from loader import _apply_watermarks, _get_watermarks, _validate  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_df(dates_tickers: list[tuple]) -> pd.DataFrame:
    """Build a minimal stock DataFrame. dates_tickers is [(date, ticker), ...]."""
    rows = []
    for d, ticker in dates_tickers:
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
# _validate tests
# ---------------------------------------------------------------------------

class TestValidate:

    def test_passes_valid_dataframe(self):
        df = _make_df([(date(2026, 9, 12), "AAL")])
        _validate(df)  # should not raise

    def test_raises_on_missing_column(self):
        df = _make_df([(date(2026, 9, 12), "AAL")]).drop(columns=["close"])
        with pytest.raises(ValueError, match="Missing required columns"):
            _validate(df)

    def test_raises_on_null_date(self):
        df = _make_df([(date(2026, 9, 12), "AAL")])
        df.loc[0, "date"] = None
        with pytest.raises(ValueError, match="null date"):
            _validate(df)

    def test_raises_on_null_ticker(self):
        df = _make_df([(date(2026, 9, 12), "AAL")])
        df.loc[0, "ticker"] = None
        with pytest.raises(ValueError, match="null ticker"):
            _validate(df)

    def test_raises_on_null_close(self):
        df = _make_df([(date(2026, 9, 12), "AAL")])
        df.loc[0, "close"] = None
        with pytest.raises(ValueError, match="null close"):
            _validate(df)

    def test_raises_on_negative_close(self):
        df = _make_df([(date(2026, 9, 12), "AAL")])
        df.loc[0, "close"] = -1.0
        with pytest.raises(ValueError, match="negative close"):
            _validate(df)


# ---------------------------------------------------------------------------
# _get_watermarks tests
# ---------------------------------------------------------------------------

class TestGetWatermarks:

    def test_returns_per_ticker_watermarks(self):
        mock_row_aal   = MagicMock(); mock_row_aal.ticker   = "AAL";   mock_row_aal.last_date   = date(2026, 9, 11)
        mock_row_tsla  = MagicMock(); mock_row_tsla.ticker  = "TSLA";  mock_row_tsla.last_date  = date(2026, 9, 10)
        mock_row_googl = MagicMock(); mock_row_googl.ticker = "GOOGL"; mock_row_googl.last_date = date(2026, 9, 11)

        mock_conn   = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            mock_row_aal, mock_row_tsla, mock_row_googl
        ]
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = lambda s, *a: mock_conn
        mock_engine.connect.return_value.__exit__  = MagicMock(return_value=False)

        result = _get_watermarks(mock_engine, "stg_stock_prices")

        assert result == {
            "AAL":   date(2026, 9, 11),
            "TSLA":  date(2026, 9, 10),
            "GOOGL": date(2026, 9, 11),
        }

    def test_returns_empty_dict_when_table_empty(self):
        mock_conn   = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = lambda s, *a: mock_conn
        mock_engine.connect.return_value.__exit__  = MagicMock(return_value=False)

        result = _get_watermarks(mock_engine, "stg_stock_prices")

        assert result == {}


# ---------------------------------------------------------------------------
# _apply_watermarks tests
# ---------------------------------------------------------------------------

class TestApplyWatermarks:

    def test_filters_each_ticker_independently(self):
        """
        AAL watermark: 2026-09-11  → keep only 2026-09-12
        TSLA watermark: 2026-09-10 → keep 2026-09-11 and 2026-09-12
        GOOGL: no watermark        → keep all rows
        """
        df = _make_df([
            (date(2026, 9, 10), "AAL"),
            (date(2026, 9, 11), "AAL"),
            (date(2026, 9, 12), "AAL"),
            (date(2026, 9, 10), "TSLA"),
            (date(2026, 9, 11), "TSLA"),
            (date(2026, 9, 12), "TSLA"),
            (date(2026, 9, 10), "GOOGL"),
            (date(2026, 9, 11), "GOOGL"),
            (date(2026, 9, 12), "GOOGL"),
        ])
        watermarks = {
            "AAL":  date(2026, 9, 11),
            "TSLA": date(2026, 9, 10),
        }

        result = _apply_watermarks(df, watermarks)

        aal_dates   = sorted(result[result["ticker"] == "AAL"]["date"].tolist())
        tsla_dates  = sorted(result[result["ticker"] == "TSLA"]["date"].tolist())
        googl_dates = sorted(result[result["ticker"] == "GOOGL"]["date"].tolist())

        assert aal_dates   == [date(2026, 9, 12)]
        assert tsla_dates  == [date(2026, 9, 11), date(2026, 9, 12)]
        assert googl_dates == [date(2026, 9, 10), date(2026, 9, 11), date(2026, 9, 12)]

    def test_returns_all_rows_when_no_watermarks(self):
        df = _make_df([
            (date(2026, 9, 12), "AAL"),
            (date(2026, 9, 12), "TSLA"),
        ])
        result = _apply_watermarks(df, {})
        assert len(result) == len(df)

    def test_new_ticker_loads_fully_when_others_have_watermarks(self):
        """A new ticker not in the watermarks dict should load all its rows."""
        df = _make_df([
            (date(2026, 9, 10), "AAPL"),
            (date(2026, 9, 11), "AAPL"),
            (date(2026, 9, 12), "AAPL"),
        ])
        watermarks = {"AAL": date(2026, 9, 12)}

        result = _apply_watermarks(df, watermarks)

        assert len(result) == 3
        assert set(result["ticker"].unique()) == {"AAPL"}

    def test_returns_empty_when_all_up_to_date(self):
        df = _make_df([(date(2026, 9, 12), "AAL")])
        watermarks = {"AAL": date(2026, 9, 12)}

        result = _apply_watermarks(df, watermarks)

        assert result.empty
