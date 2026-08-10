"""Single source of truth: API field -> DB column.

Previously this mapping was spread across three places that drifted silently:

  1. ``DatabaseManager.TABLE_SCHEMAS``      -- the DB columns
  2. hard-coded column lists in ``processor.py`` -- the API fields
  3. ``_normalize_column_names``            -- a camelCase regex bridging them

Nothing raised when a name stopped matching on either side: ``filter_columns``
quietly dropped unknown API fields and the column simply stayed NULL. That is
how seven columns ended up 100% NULL in the live database.

Here the mapping is explicit and one-directional, so a wrong API field name is
visible in a diff instead of invisible in the data.

--------------------------------------------------------------------------
KNOWN-WRONG MAPPINGS (left disabled deliberately)
--------------------------------------------------------------------------
The columns below are NULL for every row in the live DB because the old code
requested API fields that do not exist on this plan. The correct names are
recorded here and verified against the live API, but they stay disabled so
this refactor does not change what lands in your tables.

  interest_coverage      <- ratios.interestCoverageRatio     (was 'interestCoverage')
  ebit_growth            <- financial-growth.ebitgrowth      (was 'ebitGrowth')
  eps_growth             <- financial-growth.epsgrowth       (was 'epsGrowth')
  shares_outstanding     <- shares-float-all.outstandingShares (fetched, never merged)

Flip ``ENABLE_CORRECTED_FIELDS`` to True (or set CORRECTED_FIELDS=1 in .env)
to populate the four that are recoverable. No DDL change is required for any
of them -- the columns already exist.

Three columns have since left that list:

  cash_ratio            -- no cashRatio field exists on this plan, but its
                           inputs are on ``balance-sheet-statement``, so it is
                           now a DERIVED field: one column computed from
                           several fields of one endpoint (``FieldSpec.compute``)
                           rather than copied from one.
  price_earnings_ratio  -- DROPPED. A dead duplicate of price_to_earnings_ratio,
                           named after FMP's v3 field ``priceEarningsRatio``,
                           which the stable /ratios endpoint does not return.
  change_pct            -- DROPPED. A dead duplicate of change_percent.

The two drops are the only place where this schema deliberately diverges from
the legacy one. They were kept while the original tables were live, so that
``create_all`` could not alter them; those tables have since been dropped and
rebuilt from this file, so the compatibility constraint is gone.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd
from sqlalchemy import BigInteger, DateTime, Float, String
from sqlalchemy.types import TypeEngine

ENABLE_CORRECTED_FIELDS = os.getenv("CORRECTED_FIELDS", "").strip().lower() in {
    "1", "true", "yes", "on",
}

# Logical source names used by the processors.
SRC_KEY_METRICS = "key-metrics"
SRC_RATIOS = "ratios"
SRC_FIN_GROWTH = "financial-growth"
SRC_EOD = "historical-price-eod"
SRC_MARKET_CAP = "market-cap"
SRC_PROFILE = "profile"
SRC_SHARES = "shares-float"
SRC_BALANCE_SHEET = "balance-sheet-statement"


@dataclass(frozen=True)
class FieldSpec:
    db_column: str
    dtype: TypeEngine
    source: Optional[str] = None
    api_field: Optional[str] = None
    scale: float = 1.0
    #  False -> column exists in the DDL but is never written.
    enabled: bool = True
    #  True  -> only enabled when ENABLE_CORRECTED_FIELDS is set.
    corrected: bool = False
    note: str = ""
    # Derived column: ``compute`` receives one numeric Series per name in
    # ``inputs``, in order, and returns the column. Set instead of api_field.
    inputs: Tuple[str, ...] = ()
    compute: Optional[Callable[..., pd.Series]] = None

    @property
    def api_fields(self) -> Tuple[str, ...]:
        """Every field this column needs from its endpoint."""
        if self.compute is not None:
            return self.inputs
        return (self.api_field,) if self.api_field else ()

    @property
    def active(self) -> bool:
        if self.corrected:
            return ENABLE_CORRECTED_FIELDS
        return self.enabled


@dataclass(frozen=True)
class TableSpec:
    name: str
    fields: Tuple[FieldSpec, ...]
    key_columns: Tuple[str, ...] = ("symbol", "date")

    def all_columns(self) -> List[str]:
        """Every column in the DDL, including deliberately unpopulated ones."""
        return [f.db_column for f in self.fields]

    def active_fields(self) -> List[FieldSpec]:
        return [f for f in self.fields if f.active and f.api_fields]

    def fields_for(self, source: str) -> List[FieldSpec]:
        return [f for f in self.active_fields() if f.source == source]

    def sources(self) -> List[str]:
        seen: List[str] = []
        for f in self.active_fields():
            if f.source and f.source not in seen:
                seen.append(f.source)
        return seen


def cash_ratio(
    cash_and_cash_equivalents: pd.Series,
    total_current_liabilities: pd.Series,
) -> pd.Series:
    """cashAndCashEquivalents / totalCurrentLiabilities.

    A zero denominator yields NaN rather than inf, so the column lands as NULL.
    """
    return cash_and_cash_equivalents / total_current_liabilities.where(
        total_current_liabilities != 0
    )


_SYMBOL = FieldSpec("symbol", String(10), None, None, note="join key")
_DATE = FieldSpec("date", DateTime, None, None, note="join key")
_CREATED = FieldSpec("created_at", DateTime, None, None, note="ingest timestamp")


LIQUIDITY_RATIOS = TableSpec(
    "liquidity_ratios",
    (
        _SYMBOL,
        _DATE,
        FieldSpec("current_ratio", Float, SRC_KEY_METRICS, "currentRatio"),
        FieldSpec("cash_ratio", Float, SRC_BALANCE_SHEET, None,
                  inputs=("cashAndCashEquivalents", "totalCurrentLiabilities"),
                  compute=cash_ratio,
                  note="derived: no cashRatio field on this plan"),
        FieldSpec("days_of_sales_outstanding", Float, SRC_KEY_METRICS, "daysOfSalesOutstanding"),
        FieldSpec("operating_cash_flow_ratio", Float, SRC_RATIOS, "operatingCashFlowRatio"),
        FieldSpec("debt_to_assets_ratio", Float, SRC_RATIOS, "debtToAssetsRatio"),
        FieldSpec("debt_to_equity_ratio", Float, SRC_RATIOS, "debtToEquityRatio"),
        FieldSpec("interest_coverage", Float, SRC_RATIOS, "interestCoverageRatio",
                  corrected=True, note="old code asked for 'interestCoverage'"),
        FieldSpec("debt_service_coverage_ratio", Float, SRC_RATIOS, "debtServiceCoverageRatio"),
        _CREATED,
    ),
)

EARNINGS_RATIOS = TableSpec(
    "earnings_ratios",
    (
        _SYMBOL,
        _DATE,
        FieldSpec("price_to_earnings_ratio", Float, SRC_RATIOS, "priceToEarningsRatio"),
        FieldSpec("price_to_sales_ratio", Float, SRC_RATIOS, "priceToSalesRatio"),
        FieldSpec("enterprise_value_multiple", Float, SRC_RATIOS, "enterpriseValueMultiple"),
        FieldSpec("ev_to_sales", Float, SRC_KEY_METRICS, "evToSales"),
        _CREATED,
    ),
)

# Growth values are stored as percentages, matching the existing rows.
_PCT = 100.0

FINANCIAL_GROWTH = TableSpec(
    "financial_growth",
    (
        _SYMBOL,
        _DATE,
        FieldSpec("revenue_growth", Float, SRC_FIN_GROWTH, "revenueGrowth", scale=_PCT),
        FieldSpec("ebit_growth", Float, SRC_FIN_GROWTH, "ebitgrowth", scale=_PCT,
                  corrected=True, note="API spells it 'ebitgrowth' (lowercase g)"),
        # Was sourced from income-statement-growth.growthEBITDA. That endpoint is
        # no longer fetched: financial-growth.ebitdaGrowth is bit-identical.
        FieldSpec("ebitda_growth", Float, SRC_FIN_GROWTH, "ebitdaGrowth", scale=_PCT),
        FieldSpec("operating_income_growth", Float, SRC_FIN_GROWTH, "operatingIncomeGrowth", scale=_PCT),
        FieldSpec("net_income_growth", Float, SRC_FIN_GROWTH, "netIncomeGrowth", scale=_PCT),
        FieldSpec("eps_growth", Float, SRC_FIN_GROWTH, "epsgrowth", scale=_PCT,
                  corrected=True, note="API spells it 'epsgrowth' (lowercase g)"),
        FieldSpec("operating_cash_flow_growth", Float, SRC_FIN_GROWTH, "operatingCashFlowGrowth", scale=_PCT),
        FieldSpec("free_cash_flow_growth", Float, SRC_FIN_GROWTH, "freeCashFlowGrowth", scale=_PCT),
        _CREATED,
    ),
)

PROFIT_MARGINS = TableSpec(
    "profit_margins",
    (
        _SYMBOL,
        _DATE,
        FieldSpec("operating_profit_margin", Float, SRC_RATIOS, "operatingProfitMargin"),
        FieldSpec("net_profit_margin", Float, SRC_RATIOS, "netProfitMargin"),
        _CREATED,
    ),
)

STOCKS_DAILY = TableSpec(
    "stocks_daily",
    (
        _SYMBOL,
        FieldSpec("adj_open", Float, SRC_EOD, "open"),
        FieldSpec("adj_close", Float, SRC_EOD, "close"),
        FieldSpec("high", Float, SRC_EOD, "high"),
        FieldSpec("low", Float, SRC_EOD, "low"),
        FieldSpec("change_percent", Float, SRC_EOD, "changePercent"),
        _DATE,
        FieldSpec("exchange", String(10), SRC_PROFILE, "exchange"),
        FieldSpec("sector", String(50), SRC_PROFILE, "sector"),
        FieldSpec("industry", String(100), SRC_PROFILE, "industry"),
        # BigInteger, not Integer: daily volume for high-float tickers and any
        # share count above 2.1e9 overflows PostgreSQL int4. Widening is
        # forward-compatible -- existing int4 columns accept these values.
        FieldSpec("volume", BigInteger, SRC_EOD, "volume"),
        FieldSpec("vwap", Float, SRC_EOD, "vwap"),
        FieldSpec("market_cap", Float, SRC_MARKET_CAP, "marketCap"),
        FieldSpec("shares_outstanding", BigInteger, SRC_SHARES, "outstandingShares",
                  corrected=True, note="was fetched per-ticker then discarded"),
        _CREATED,
    ),
)

ALL_TABLES: Tuple[TableSpec, ...] = (
    LIQUIDITY_RATIOS,
    EARNINGS_RATIOS,
    FINANCIAL_GROWTH,
    PROFIT_MARGINS,
    STOCKS_DAILY,
)

TABLES_BY_NAME: Dict[str, TableSpec] = {t.name: t for t in ALL_TABLES}

FUNDAMENTAL_TABLES = ("liquidity_ratios", "earnings_ratios", "financial_growth", "profit_margins")
PRICE_TABLES = ("stocks_daily",)


def api_fields_needed(source: str) -> List[str]:
    """Every API field this pipeline consumes from one endpoint."""
    needed: List[str] = []
    for table in ALL_TABLES:
        for spec in table.fields_for(source):
            for api_field in spec.api_fields:
                if api_field not in needed:
                    needed.append(api_field)
    return needed
