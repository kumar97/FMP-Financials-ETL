"""Offline checks: no network, no database.

    python -m pytest tests/test_offline.py -q
    python tests/test_offline.py            # also runs standalone
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

import pandas as pd

# The package is imported as ``data_pullv2.*``, so the repository root -- the
# package's parent -- is what has to be importable. tests/ sits at the repo
# root, so that is exactly two levels up from this file.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_pullv2.core.ratelimit import AsyncRateLimiter
from data_pullv2.storage.schema import TABLES
from data_pullv2.storage.writer import PG_MAX_BIND_PARAMS, TableWriter, rows_per_statement
from data_pullv2.transform.fields import (
    ALL_TABLES,
    LIQUIDITY_RATIOS,
    SRC_BALANCE_SHEET,
    STOCKS_DAILY,
    api_fields_needed,
)
from data_pullv2.transform.processor import Processor


# --- chunking ---------------------------------------------------------------

def test_rows_per_statement_respects_bind_limit():
    for columns in (2, 6, 16, 40):
        rows = rows_per_statement(columns, hard_cap=100_000)
        assert rows * columns <= PG_MAX_BIND_PARAMS, columns
        assert rows >= 1


def test_chunking_prevents_the_original_overflow():
    """The old code sent one statement for every row.

    stocks_daily has 15 columns; 3,762 rows (3 tickers of history) is 56,430
    bind parameters, and 250 tickers is ~4.7M -- both unsendable as one
    statement.
    """
    columns = len(STOCKS_DAILY.all_columns())
    rows_250_tickers = 1254 * 250
    assert rows_250_tickers * columns > PG_MAX_BIND_PARAMS

    per_stmt = rows_per_statement(columns)
    statements = -(-rows_250_tickers // per_stmt)
    assert per_stmt * columns <= PG_MAX_BIND_PARAMS
    assert statements > 1


# --- writer preparation -----------------------------------------------------

def _writer() -> TableWriter:
    return TableWriter(engine=None, max_rows_per_statement=2000)


def test_prepare_drops_unknown_columns():
    df = pd.DataFrame([
        {"symbol": "AAPL", "date": pd.Timestamp("2026-08-07"),
         "adj_close": 1.0, "not_a_column": "boom"},
    ])
    out, _ = _writer()._prepare(TABLES["stocks_daily"], df)
    assert "not_a_column" not in out.columns
    assert "adj_close" in out.columns
    assert "created_at" in out.columns


def test_prepare_collapses_duplicate_conflict_keys():
    """PostgreSQL rejects ON CONFLICT DO UPDATE when one statement contains
    the same conflict key twice."""
    df = pd.DataFrame([
        {"symbol": "AAPL", "date": pd.Timestamp("2026-08-07"), "adj_close": 1.0},
        {"symbol": "AAPL", "date": pd.Timestamp("2026-08-07"), "adj_close": 2.0},
        {"symbol": "MSFT", "date": pd.Timestamp("2026-08-07"), "adj_close": 3.0},
    ])
    out, dropped = _writer()._prepare(TABLES["stocks_daily"], df)
    assert dropped == 1
    assert len(out) == 2
    # keep='last' matches upsert semantics
    aapl = out[out["symbol"] == "AAPL"]["adj_close"].iloc[0]
    assert aapl == 2.0


def test_prepare_converts_nan_to_none():
    df = pd.DataFrame([
        {"symbol": "AAPL", "date": pd.Timestamp("2026-08-07"), "vwap": float("nan")},
    ])
    out, _ = _writer()._prepare(TABLES["stocks_daily"], df)
    assert out["vwap"].iloc[0] is None


def test_prepare_drops_rows_missing_conflict_key():
    df = pd.DataFrame([
        {"symbol": "AAPL", "date": pd.NaT, "adj_close": 1.0},
        {"symbol": None, "date": pd.Timestamp("2026-08-07"), "adj_close": 2.0},
        {"symbol": "MSFT", "date": pd.Timestamp("2026-08-07"), "adj_close": 3.0},
    ])
    out, _ = _writer()._prepare(TABLES["stocks_daily"], df)
    assert len(out) == 1


# --- schema parity ----------------------------------------------------------

def test_schema_column_names_are_pinned():
    """The generated DDL is pinned, so a field-map edit cannot silently
    reshape a table.

    This started as a parity check against the legacy schema. It now differs
    from it by exactly two deliberate omissions -- ``price_earnings_ratio``
    and ``change_pct``, dead duplicates of ``price_to_earnings_ratio`` and
    ``change_percent`` that could never be populated. They were carried while
    the original tables were live; those have since been dropped and rebuilt.
    """
    expected = {
        "liquidity_ratios": ["symbol", "date", "current_ratio", "cash_ratio",
                             "days_of_sales_outstanding", "operating_cash_flow_ratio",
                             "debt_to_assets_ratio", "debt_to_equity_ratio",
                             "interest_coverage", "debt_service_coverage_ratio",
                             "created_at"],
        "earnings_ratios": ["symbol", "date",
                            "price_to_earnings_ratio", "price_to_sales_ratio",
                            "enterprise_value_multiple", "ev_to_sales", "created_at"],
        "financial_growth": ["symbol", "date", "revenue_growth", "ebit_growth",
                             "ebitda_growth", "operating_income_growth",
                             "net_income_growth", "eps_growth",
                             "operating_cash_flow_growth", "free_cash_flow_growth",
                             "created_at"],
        "profit_margins": ["symbol", "date", "operating_profit_margin",
                           "net_profit_margin", "created_at"],
        "stocks_daily": ["symbol", "adj_open", "adj_close", "high", "low",
                         "change_percent", "date", "exchange", "sector", "industry",
                         "volume", "vwap", "market_cap",
                         "shares_outstanding", "created_at"],
    }
    for spec in ALL_TABLES:
        assert spec.all_columns() == expected[spec.name], spec.name


def test_no_column_is_declared_without_a_source():
    """Every column must be populated by something.

    ``price_earnings_ratio`` and ``change_pct`` were exactly this: declared in
    the DDL with nothing behind them, so they read as data that was missing
    rather than data that was never collected.
    """
    for spec in ALL_TABLES:
        for f in spec.fields:
            if f.db_column in ("symbol", "date", "created_at"):
                continue  # keys and the ingest stamp are set by the writer
            assert f.api_fields, f"{spec.name}.{f.db_column} has no source"


# --- rate limiter -----------------------------------------------------------

def test_rate_limiter_enforces_throughput():
    """60/min = 1/s. After draining the burst, 3 more must take ~3s.

    The previous limiter reset its clock incorrectly after sleeping and
    handed out bursts above the configured rate.
    """
    async def scenario():
        limiter = AsyncRateLimiter(60, burst=1)
        await limiter.acquire()          # consume the burst
        start = time.monotonic()
        for _ in range(3):
            await limiter.acquire()
        return time.monotonic() - start

    elapsed = asyncio.run(scenario())
    assert 2.5 <= elapsed <= 4.5, elapsed


def test_rate_limiter_is_concurrency_safe():
    """Parallel acquirers must not exceed the rate either."""
    async def scenario():
        limiter = AsyncRateLimiter(120, burst=2)  # 2/s
        await asyncio.gather(*(limiter.acquire() for _ in range(2)))
        start = time.monotonic()
        await asyncio.gather(*(limiter.acquire() for _ in range(4)))
        return time.monotonic() - start

    elapsed = asyncio.run(scenario())
    assert elapsed >= 1.5, elapsed


# --- processor --------------------------------------------------------------

def test_processor_tolerates_empty_and_malformed_payloads():
    p = Processor()
    for payload in (None, [], {}, "garbage", 42):
        out = p.build_stocks_daily({"historical-price-eod": payload})
        assert out.empty

    for spec in ALL_TABLES[:4]:
        assert p.build_fundamentals(spec, {}).empty


def test_processor_broadcasts_symbol_level_reference_data():
    p = Processor()
    payload = [
        {"symbol": "AAPL", "date": "2026-08-07", "open": 1.0, "close": 2.0,
         "high": 3.0, "low": 0.5, "volume": 10, "vwap": 1.5, "changePercent": 0.1},
        {"symbol": "AAPL", "date": "2026-08-06", "open": 1.0, "close": 2.0,
         "high": 3.0, "low": 0.5, "volume": 10, "vwap": 1.5, "changePercent": 0.1},
    ]
    out = p.build_stocks_daily(
        {"historical-price-eod": payload},
        profile={"sector": "Technology", "industry": "Consumer Electronics",
                 "exchange": "NASDAQ"},
    )
    assert len(out) == 2
    assert set(out["sector"]) == {"Technology"}
    assert set(out["exchange"]) == {"NASDAQ"}


# --- derived fields ---------------------------------------------------------

def _balance_sheet(**overrides):
    row = {
        "symbol": "AAPL",
        "date": "2025-09-27",
        # AAPL FY2025, as returned by the live endpoint.
        "cashAndCashEquivalents": 35_934_000_000,
        "totalCurrentLiabilities": 165_631_000_000,
    }
    row.update(overrides)
    return [row]


def test_api_fields_needed_reports_derived_inputs():
    """A derived column must declare its inputs, not a single api_field."""
    assert api_fields_needed(SRC_BALANCE_SHEET) == [
        "cashAndCashEquivalents", "totalCurrentLiabilities",
    ]


def test_cash_ratio_is_computed_and_merged_onto_the_ratio_rows():
    out = Processor().build_fundamentals(LIQUIDITY_RATIOS, {
        "key-metrics": [{"symbol": "AAPL", "date": "2025-09-27", "currentRatio": 0.87}],
        SRC_BALANCE_SHEET: _balance_sheet(),
    })
    assert len(out) == 1
    expected = 35_934_000_000 / 165_631_000_000
    assert abs(out["cash_ratio"].iloc[0] - expected) < 1e-12
    # Same date as the other liquidity sources, so it merges rather than
    # doubling the row count.
    assert out["current_ratio"].iloc[0] == 0.87


def test_cash_ratio_is_null_when_the_denominator_is_zero():
    """Division must yield NULL, not inf, or the column is unusable."""
    out = Processor().build_fundamentals(LIQUIDITY_RATIOS, {
        SRC_BALANCE_SHEET: _balance_sheet(totalCurrentLiabilities=0),
    })
    assert pd.isna(out["cash_ratio"].iloc[0])


def test_cash_ratio_survives_a_payload_missing_its_inputs():
    out = Processor().build_fundamentals(LIQUIDITY_RATIOS, {
        SRC_BALANCE_SHEET: [{"symbol": "AAPL", "date": "2025-09-27"}],
    })
    assert pd.isna(out["cash_ratio"].iloc[0])


def _run_all():
    fns = [(n, f) for n, f in sorted(globals().items())
           if n.startswith("test_") and callable(f)]
    failures = 0
    for name, fn in fns:
        try:
            fn()
            print(f"  PASS  {name}")
        except AssertionError as exc:
            failures += 1
            print(f"  FAIL  {name}: {exc}")
        except Exception as exc:
            failures += 1
            print(f"  ERROR {name}: {type(exc).__name__}: {exc}")
    print(f"\n{len(fns) - failures}/{len(fns)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(_run_all())
