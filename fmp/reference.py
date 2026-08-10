"""Slow-moving reference data fetched in bulk rather than per symbol.

Two replacements, both verified working on this subscription:

  per-symbol ``shares-float``      -> ``shares-float-all``            (paginated)
  per-symbol market capitalisation -> ``market-capitalization-batch`` (~500/call)

The shares-float data was previously fetched once per ticker and then thrown
away entirely -- the merge in ``processor.process_stocks_daily`` is commented
out, so 1 request in every 8 produced nothing.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

from core.cache import JsonCache
from fmp import endpoints
from fmp.client import FMPClient

log = logging.getLogger(__name__)


class ReferenceDataProvider:
    def __init__(self, client: FMPClient, cache: JsonCache):
        self.client = client
        self.cache = cache

    async def get_shares_float(
        self, symbols: Optional[Sequence[str]] = None
    ) -> Dict[str, int]:
        """``{symbol: outstandingShares}`` for the whole market.

        Cached, because float changes on the order of weeks.
        """
        cached = self.cache.get("shares_float")
        if cached is None:
            rows = await self.client.fetch_paginated(endpoints.SHARES_FLOAT_ALL)
            cached = [
                {"symbol": r.get("symbol"), "outstandingShares": r.get("outstandingShares")}
                for r in rows
                if r.get("symbol")
            ]
            if cached:
                self.cache.set("shares_float", cached)
            log.info("shares-float-all: %d symbols cached", len(cached))

        wanted = set(symbols) if symbols else None
        out: Dict[str, int] = {}
        for row in cached:
            symbol = row.get("symbol")
            shares = row.get("outstandingShares")
            if not symbol or shares in (None, 0):
                continue
            if wanted is not None and symbol not in wanted:
                continue
            out[symbol] = shares
        return out

    async def get_latest_market_caps(
        self, symbols: Sequence[str]
    ) -> Dict[str, Dict[str, Any]]:
        """``{symbol: {date, marketCap}}`` using the batch endpoint.

        For a 250-symbol daily run this is 1 request instead of 250.
        Not cached: market cap moves every day.
        """
        if not symbols:
            return {}
        rows = await self.client.fetch_batched_symbols(
            endpoints.MARKET_CAP_BATCH, list(symbols)
        )
        out: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            symbol = row.get("symbol")
            if not symbol:
                continue
            out[symbol] = {"date": row.get("date"), "marketCap": row.get("marketCap")}
        missing = len(symbols) - len(out)
        if missing > 0:
            log.debug("market-cap batch returned no row for %d symbol(s)", missing)
        return out
