"""Symbol universe resolution.

Previously the universe came from ``stock-list`` (38,677 entries, including
mutual funds such as AEGFX) sliced to ``[:250]``. ``company-screener`` returns
a filtered, actively-traded equity universe -- 5,939 symbols on this plan -- in
a single request, and carries sector / industry / exchange / marketCap with it,
which is what the per-symbol ``profile`` endpoint was being used for.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

from core.cache import JsonCache
from fmp import endpoints
from fmp.client import FMPClient
from settings import RunSettings

log = logging.getLogger(__name__)


class UniverseProvider:
    """Resolves the working set of symbols and their static attributes."""

    def __init__(self, client: FMPClient, run: RunSettings, cache: JsonCache):
        self.client = client
        self.run = run
        self.cache = cache

    async def _screener_rows(self) -> List[Dict[str, Any]]:
        cached = self.cache.get("screener")
        if cached is not None:
            return cached

        params: Dict[str, Any] = {
            "limit": 10000,
            "isActivelyTrading": "true",
        }
        if not self.run.include_funds:
            params["isFund"] = "false"
        if not self.run.include_etfs:
            params["isEtf"] = "false"

        result = await self.client.fetch(endpoints.SCREENER, params, key="universe:screener")
        if not result.success or not isinstance(result.data, list):
            log.error("screener request failed: %s", result.error)
            return []

        rows = result.data
        self.cache.set("screener", rows)
        log.info("screener returned %d symbols in 1 request", len(rows))
        return rows

    async def get_universe(
        self,
        limit: Optional[int] = None,
        symbols: Optional[Sequence[str]] = None,
    ) -> List[str]:
        """Return the ordered list of symbols this run should process."""
        if symbols:
            log.info("using %d explicitly supplied symbols", len(symbols))
            return list(symbols)

        rows = await self._screener_rows()
        if not rows:
            log.error("empty universe; aborting")
            return []

        allowed = set(self.run.exchanges) if self.run.exchanges else None
        picked: List[str] = []
        for row in rows:
            symbol = row.get("symbol")
            if not symbol:
                continue
            if allowed and row.get("exchangeShortName") not in allowed:
                continue
            picked.append(symbol)

        # Largest companies first: a truncated run then covers the most
        # meaningful names rather than an arbitrary alphabetical slice.
        by_cap = {r.get("symbol"): (r.get("marketCap") or 0) for r in rows}
        picked.sort(key=lambda s: by_cap.get(s, 0), reverse=True)

        cap = limit if limit is not None else self.run.max_symbols
        if cap and cap > 0:
            picked = picked[:cap]

        log.info(
            "universe: %d symbols (exchanges=%s, etfs=%s, funds=%s)",
            len(picked), sorted(allowed) if allowed else "all",
            self.run.include_etfs, self.run.include_funds,
        )
        return picked

    async def get_profiles(self) -> Dict[str, Dict[str, Any]]:
        """``{symbol: {sector, industry, exchange}}`` for the whole universe.

        Costs 0-1 requests total. The previous pipeline spent one ``profile``
        request per ticker for the same three fields.
        """
        rows = await self._screener_rows()
        return {
            row["symbol"]: {
                "sector": row.get("sector"),
                "industry": row.get("industry"),
                "exchange": row.get("exchangeShortName"),
            }
            for row in rows
            if row.get("symbol")
        }
