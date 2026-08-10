"""Full history backfill: fundamentals + price series.

Call cost per symbol, before and after:

    before   key-metrics, ratios, financial-growth, income-statement-growth,
             historical-price-eod, historical-market-capitalization,
             profile, shares-float                              = 8 requests
    after    key-metrics, ratios, financial-growth,
             historical-price-eod, historical-market-capitalization = 5 requests
             + O(1) shared: 1 screener call, ~8 shares-float pages

income-statement-growth is dropped (its only used field duplicates
financial-growth.ebitdaGrowth exactly); profile and shares-float move to the
universe/reference providers and are cached across runs.

Symbols are processed in chunks and each chunk is committed independently, so
peak memory is bounded by the chunk size and a crash costs at most one chunk
instead of the entire run.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional, Sequence

import pandas as pd

from fmp import endpoints
from fmp.client import FMPClient
from fmp.reference import ReferenceDataProvider
from fmp.universe import UniverseProvider
from jobs.base import JobContext, RunReport, chunked
from settings import Settings
from transform.fields import (
    ALL_TABLES,
    FUNDAMENTAL_TABLES,
    SRC_EOD,
    SRC_FIN_GROWTH,
    SRC_KEY_METRICS,
    SRC_MARKET_CAP,
    SRC_RATIOS,
    STOCKS_DAILY,
    TABLES_BY_NAME,
)

log = logging.getLogger(__name__)

# Maps the endpoint registry names onto the logical source names the field map
# and processors use.
_SOURCE_FOR_ENDPOINT = {
    endpoints.KEY_METRICS.name: SRC_KEY_METRICS,
    endpoints.RATIOS.name: SRC_RATIOS,
    endpoints.FINANCIAL_GROWTH.name: SRC_FIN_GROWTH,
    endpoints.EOD_FULL.name: SRC_EOD,
    endpoints.HISTORICAL_MARKET_CAP.name: SRC_MARKET_CAP,
}


async def run_backfill(
    settings: Settings,
    symbols: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
    export_dir: Optional[str] = None,
) -> RunReport:
    ctx = JobContext(settings, dry_run=dry_run)
    report = RunReport(dry_run=dry_run)

    try:
        async with FMPClient(settings.fmp) as client:
            universe = UniverseProvider(client, settings.run, ctx.cache)
            reference = ReferenceDataProvider(client, ctx.cache)

            working_set = await universe.get_universe(limit=limit, symbols=symbols)
            report.symbols_requested = len(working_set)
            if not working_set:
                log.error("no symbols to process")
                return report

            # O(1) shared reference data, fetched once for the whole run.
            profiles = await universe.get_profiles()
            shares = await reference.get_shares_float(working_set)

            if not dry_run:
                ctx.repo.ensure_schema()

            exported: Dict[str, List[pd.DataFrame]] = {t.name: [] for t in ALL_TABLES}
            chunk_size = settings.run.symbol_chunk
            total_chunks = (len(working_set) + chunk_size - 1) // chunk_size

            for index, chunk in enumerate(chunked(working_set, chunk_size), start=1):
                log.info(
                    "chunk %d/%d - fetching %d symbols x %d endpoints",
                    index, total_chunks, len(chunk), len(endpoints.BACKFILL_PER_SYMBOL),
                )

                raw = await client.fetch_symbol_endpoints(
                    chunk, endpoints.BACKFILL_PER_SYMBOL
                )
                tables = _build_tables(ctx, raw, profiles, shares, report)

                if dry_run or export_dir:
                    for name, df in tables.items():
                        if not df.empty:
                            exported[name].append(df)
                if not dry_run:
                    results = ctx.writer.upsert_many(tables)
                    report.add_writes(results)
                else:
                    for name, df in tables.items():
                        report.rows_written[name] = report.rows_written.get(name, 0) + len(df)

            stats = client.stats.summary()
            report.api_requests = stats["total_requests"]
            report.api_failures = stats["failed"]
            if client.stats.failed:
                log.warning("failure reasons: %s", client.stats.failures_by_reason())

        if export_dir:
            _export(exported, export_dir)

    finally:
        ctx.close()

    report.log_summary("Backfill")
    return report


def _build_tables(
    ctx: JobContext,
    raw: Dict[str, Dict[str, object]],
    profiles: Dict[str, Dict[str, object]],
    shares: Dict[str, int],
    report: RunReport,
) -> Dict[str, pd.DataFrame]:
    """Transform one chunk of fetched payloads into per-table DataFrames."""
    collected: Dict[str, List[pd.DataFrame]] = {t.name: [] for t in ALL_TABLES}

    for symbol, by_endpoint in raw.items():
        payloads = {
            _SOURCE_FOR_ENDPOINT[name]: data
            for name, data in by_endpoint.items()
            if name in _SOURCE_FOR_ENDPOINT
        }
        if all(v is None for v in payloads.values()):
            report.symbols_failed += 1
            continue
        report.symbols_with_data += 1

        for table_name in FUNDAMENTAL_TABLES:
            spec = TABLES_BY_NAME[table_name]
            df = ctx.processor.build_fundamentals(spec, payloads)
            if not df.empty:
                collected[table_name].append(df)

        daily = ctx.processor.build_stocks_daily(
            payloads,
            profile=profiles.get(symbol),
            shares_outstanding=shares.get(symbol),
        )
        if not daily.empty:
            collected[STOCKS_DAILY.name].append(daily)

    return {
        name: (pd.concat(frames, ignore_index=True) if frames else pd.DataFrame())
        for name, frames in collected.items()
    }


def _export(tables: Dict[str, List[pd.DataFrame]], directory: str) -> None:
    import os

    os.makedirs(directory, exist_ok=True)
    for name, frames in tables.items():
        if not frames:
            continue
        df = pd.concat(frames, ignore_index=True)
        path = os.path.join(directory, f"{name}.csv")
        df.to_csv(path, index=False)
        log.info("exported %s (%d rows) -> %s", name, len(df), path)


def main(**kwargs) -> RunReport:
    from data_pullv2.core.logging_setup import configure
    from settings import load_settings

    settings = load_settings()
    configure(settings.run.log_level)
    return asyncio.run(run_backfill(settings, **kwargs))


if __name__ == "__main__":
    main()
