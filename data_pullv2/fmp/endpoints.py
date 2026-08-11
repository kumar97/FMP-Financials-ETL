"""Declarative endpoint registry.

Adding or retargeting an endpoint is a data change here, not a code change in
the client. The previous client hard-coded a special case
(``if endpoint == 'historical-price-eod'``) inside its URL builder.

Cardinality is the important axis, because it determines call cost:

    PER_SYMBOL  -> 1 request per symbol         (irreducible)
    BATCH       -> 1 request per N symbols      (N ~ 500)
    UNIVERSE    -> 1 request for everything     (paginated at most)

Verified against the live API on this subscription (2026-08-09):
  * bulk endpoints (batch-eod, profile-bulk, *-ttm-bulk) return HTTP 402
  * per-symbol endpoints ignore comma-separated symbols and return []
    -> so PER_SYMBOL really is 1 call per symbol, not a batching oversight.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional


class Cardinality(str, Enum):
    PER_SYMBOL = "per_symbol"
    BATCH = "batch"
    UNIVERSE = "universe"


@dataclass(frozen=True)
class EndpointSpec:
    name: str
    path: str
    cardinality: Cardinality
    supports_date_range: bool = False
    # Reference data that is cacheable across runs.
    cache_key: Optional[str] = None
    notes: str = ""


# --- Per-symbol fundamentals -------------------------------------------------
# NOTE: 'period=quarter' returns HTTP 402 for ratios/key-metrics on this plan,
# so these default to annual (FY) periods. financial-growth does accept it.
KEY_METRICS = EndpointSpec("key-metrics", "key-metrics", Cardinality.PER_SYMBOL)
RATIOS = EndpointSpec("ratios", "ratios", Cardinality.PER_SYMBOL)
FINANCIAL_GROWTH = EndpointSpec("financial-growth", "financial-growth", Cardinality.PER_SYMBOL)
BALANCE_SHEET = EndpointSpec(
    "balance-sheet-statement",
    "balance-sheet-statement",
    Cardinality.PER_SYMBOL,
    notes="Only for the derived liquidity_ratios.cash_ratio. Returns the same "
          "5 annual periods, on the same dates, as ratios and key-metrics.",
)

# income-statement-growth is deliberately absent. Its only consumed field,
# growthEBITDA, is bit-identical to financial-growth.ebitdaGrowth (verified
# across AAPL/MSFT/JPM/XOM). Fetching it cost 1 extra call per symbol for
# zero additional information.

# --- Per-symbol price / market data -----------------------------------------
EOD_FULL = EndpointSpec(
    "historical-price-eod",
    "historical-price-eod/full",
    Cardinality.PER_SYMBOL,
    supports_date_range=True,
)
HISTORICAL_MARKET_CAP = EndpointSpec(
    "historical-market-capitalization",
    "historical-market-capitalization",
    Cardinality.PER_SYMBOL,
    supports_date_range=True,
    notes="Only needed for backfill; the daily job uses MARKET_CAP_BATCH.",
)

# --- Batch / universe replacements ------------------------------------------
MARKET_CAP_BATCH = EndpointSpec(
    "market-capitalization-batch",
    "market-capitalization-batch",
    Cardinality.BATCH,
    notes="~1000 symbols per call; returns latest marketCap plus its date.",
)
SCREENER = EndpointSpec(
    "company-screener",
    "company-screener",
    Cardinality.UNIVERSE,
    cache_key="screener",
    notes="Replaces per-symbol 'profile': sector, industry, exchange, marketCap.",
)
SHARES_FLOAT_ALL = EndpointSpec(
    "shares-float-all",
    "shares-float-all",
    Cardinality.UNIVERSE,
    cache_key="shares_float",
    notes="Replaces per-symbol 'shares-float'; paginated at 5000/page.",
)

# Endpoints fetched once per symbol during a full backfill.
BACKFILL_PER_SYMBOL = (
    KEY_METRICS,
    RATIOS,
    FINANCIAL_GROWTH,
    BALANCE_SHEET,
    EOD_FULL,
    HISTORICAL_MARKET_CAP,
)

# Endpoints fetched once per symbol during a daily EOD run.
EOD_PER_SYMBOL = (EOD_FULL,)

REGISTRY: Dict[str, EndpointSpec] = {
    spec.name: spec
    for spec in (
        KEY_METRICS,
        RATIOS,
        FINANCIAL_GROWTH,
        BALANCE_SHEET,
        EOD_FULL,
        HISTORICAL_MARKET_CAP,
        MARKET_CAP_BATCH,
        SCREENER,
        SHARES_FLOAT_ALL,
    )
}
