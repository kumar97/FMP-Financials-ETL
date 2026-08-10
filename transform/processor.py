"""Raw API payloads -> DataFrames shaped exactly like the target tables.

Every column produced here is declared in ``transform.fields``. There is no
camelCase-to-snake_case regex and no ad-hoc rename map, so a column can only
appear in the output if something explicitly asked for it.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence

import pandas as pd

from transform.fields import (
    SRC_EOD,
    SRC_MARKET_CAP,
    SRC_PROFILE,
    SRC_SHARES,
    TableSpec,
)

log = logging.getLogger(__name__)


def _frame(payload: Optional[Any]) -> pd.DataFrame:
    """Coerce an API payload to a DataFrame, tolerating None/dict/list."""
    if payload is None:
        return pd.DataFrame()
    if isinstance(payload, Mapping):
        payload = [payload]
    if not isinstance(payload, (list, tuple)) or len(payload) == 0:
        return pd.DataFrame()
    try:
        return pd.DataFrame(list(payload))
    except Exception as exc:  # malformed payload must not kill the run
        log.warning("could not build DataFrame from payload: %s", exc)
        return pd.DataFrame()


def _extract(
    payload: Optional[Any],
    table: TableSpec,
    source: str,
    include_date: bool = True,
) -> pd.DataFrame:
    """Pull the fields ``table`` wants from one endpoint's payload."""
    specs = table.fields_for(source)
    if not specs:
        return pd.DataFrame()

    df = _frame(payload)
    if df.empty:
        return pd.DataFrame()

    out = pd.DataFrame()
    if "symbol" in df.columns:
        out["symbol"] = df["symbol"]
    if include_date and "date" in df.columns:
        out["date"] = pd.to_datetime(df["date"], errors="coerce", format="mixed")

    for spec in specs:
        if spec.api_field in df.columns:
            series = pd.to_numeric(df[spec.api_field], errors="coerce") \
                if spec.scale != 1.0 else df[spec.api_field]
            out[spec.db_column] = series * spec.scale if spec.scale != 1.0 else series
        else:
            # Declared but absent from this response: emit the column as NULL
            # so every chunk has a stable shape.
            out[spec.db_column] = pd.NA
    return out


def _merge_sources(frames: List[pd.DataFrame], on: Sequence[str]) -> pd.DataFrame:
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()
    merged = frames[0]
    for right in frames[1:]:
        join_keys = [k for k in on if k in merged.columns and k in right.columns]
        if not join_keys:
            continue
        merged = merged.merge(right, on=join_keys, how="outer")
    return merged


class Processor:
    """Builds one table's DataFrame from a symbol's fetched payloads."""

    def build_fundamentals(
        self,
        table: TableSpec,
        payloads: Mapping[str, Any],
    ) -> pd.DataFrame:
        """Fundamental tables: all sources are date-indexed per symbol."""
        frames = [
            _extract(payloads.get(source), table, source)
            for source in table.sources()
        ]
        merged = _merge_sources(frames, on=("symbol", "date"))
        if merged.empty:
            return merged
        return merged.dropna(subset=["symbol", "date"])

    def build_stocks_daily(
        self,
        payloads: Mapping[str, Any],
        profile: Optional[Mapping[str, Any]] = None,
        market_cap: Optional[Mapping[str, Any]] = None,
        shares_outstanding: Optional[int] = None,
    ) -> pd.DataFrame:
        """Price rows enriched with symbol-level reference data.

        ``profile``/``market_cap``/``shares_outstanding`` come from the batch
        and universe endpoints rather than per-symbol requests.
        """
        from transform.fields import STOCKS_DAILY as table

        eod = _extract(payloads.get(SRC_EOD), table, SRC_EOD)
        if eod.empty or "date" not in eod.columns:
            return pd.DataFrame()

        eod = eod.dropna(subset=["symbol", "date"])
        if eod.empty:
            return eod

        # Symbol-level attributes broadcast across every date.
        for spec in table.fields_for(SRC_PROFILE):
            value = (profile or {}).get(spec.api_field)
            eod[spec.db_column] = value

        for spec in table.fields_for(SRC_SHARES):
            eod[spec.db_column] = shares_outstanding

        # Market cap: prefer a date-aligned historical series when present,
        # otherwise broadcast the latest batch value.
        mcap_specs = table.fields_for(SRC_MARKET_CAP)
        if mcap_specs:
            hist = payloads.get(SRC_MARKET_CAP)
            hist_df = _extract(hist, table, SRC_MARKET_CAP)
            if not hist_df.empty and "date" in hist_df.columns:
                hist_df = hist_df.dropna(subset=["symbol", "date"])
                eod = eod.merge(hist_df, on=["symbol", "date"], how="left",
                                suffixes=("", "_hist"))
                for spec in mcap_specs:
                    dup = f"{spec.db_column}_hist"
                    if dup in eod.columns:
                        eod[spec.db_column] = eod[spec.db_column].fillna(eod[dup])
                        eod = eod.drop(columns=[dup])
            else:
                # No historical series for this run (the daily job does not
                # fetch one): broadcast the latest batch value across the
                # window. Correct for a 1-5 day incremental update.
                for spec in mcap_specs:
                    eod[spec.db_column] = (market_cap or {}).get(spec.api_field)

        return eod
