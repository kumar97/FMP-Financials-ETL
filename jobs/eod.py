"""Daily incremental EOD update.

Call cost for a 250-symbol run:

    before   4 requests per symbol (eod, historical-market-cap, profile,
             shares-float), no rate limiting, no retries        = 1,000 requests
    after    1 request per symbol (eod, date-windowed)
             + 1 market-capitalization-batch request
             + 0-1 screener requests   (cached across runs)
             + 0-8 shares-float pages  (cached across runs)     = ~252 requests

Each symbol resumes from its own watermark. The previous implementation used
``min()`` across all symbols, so one stale ticker re-downloaded years of
history for the entire universe on every run.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Sequence

import pandas as pd

from data_pullv2.fmp import endpoints
from data_pullv2.fmp.client import FMPClient
from data_pullv2.fmp.reference import ReferenceDataProvider
from data_pullv2.fmp.universe import UniverseProvider
from data_pullv2.jobs.base import JobContext, RunReport, chunked
from data_pullv2.settings import Settings
from data_pullv2.transform.fields import SRC_EOD, STOCKS_DAILY

log = logging.getLogger(__name__)


def is_trading_day(day: date) -> bool:
    """Weekday check. US market holidays are not modelled -- a holiday run
    simply finds no new rows and upserts nothing."""
    return day.weekday() < 5


async def run_eod(
    settings: Settings,
    symbols: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
    force: bool = False,
) -> RunReport:
    today = date.today()
    report = RunReport(dry_run=dry_run)

    if not force and not is_trading_day(today):
        log.info("%s is not a trading day - skipping", today)
        return report

    ctx = JobContext(settings, dry_run=dry_run)
    to_date = today.isoformat()

    try:
        async with FMPClient(settings.fmp) as client:
            universe = UniverseProvider(client, settings.run, ctx.cache)
            reference = ReferenceDataProvider(client, ctx.cache)

            working_set = await universe.get_universe(limit=limit, symbols=symbols)
            report.symbols_requested = len(working_set)
            if not working_set:
                log.error("no symbols to process")
                return report

            # Per-symbol resume points.
            default_from = today - timedelta(days=settings.run.eod_lookback_days)
            if dry_run:
                from_dates = {s: default_from for s in working_set}
            else:
                ctx.repo.ensure_schema()
                from_dates = ctx.repo.resume_dates(
                    STOCKS_DAILY.name,
                    working_set,
                    default_from=default_from,
                    stale_after_days=settings.run.eod_stale_after_days,
                )

            pending = [s for s in working_set if from_dates[s] <= today]
            skipped = len(working_set) - len(pending)
            if skipped:
                log.info("%d symbol(s) already current - skipping", skipped)
            if not pending:
                log.info("database already up to date")
                return report

            window_start = min(from_dates[s] for s in pending)
            log.info(
                "updating %d symbols; earliest resume %s -> %s",
                len(pending), window_start, to_date,
            )

            # Shared reference data: cached, so usually zero requests.
            profiles = await universe.get_profiles()
            shares = await reference.get_shares_float(pending)
            # Not cached -- market cap changes daily. 1 request per ~500 symbols.
            market_caps = await reference.get_latest_market_caps(pending)

            chunk_size = settings.run.symbol_chunk
            total_chunks = (len(pending) + chunk_size - 1) // chunk_size

            for index, chunk in enumerate(chunked(pending, chunk_size), start=1):
                log.info("chunk %d/%d - %d symbols", index, total_chunks, len(chunk))

                raw = await client.fetch_symbol_endpoints(
                    chunk,
                    endpoints.EOD_PER_SYMBOL,
                    extra_params={"to": to_date},
                    per_symbol_params=lambda s, spec: {"from": from_dates[s].isoformat()},
                )

                frames: List[pd.DataFrame] = []
                for symbol, by_endpoint in raw.items():
                    payload = by_endpoint.get(endpoints.EOD_FULL.name)
                    if payload is None:
                        report.symbols_failed += 1
                        continue
                    df = ctx.processor.build_stocks_daily(
                        {SRC_EOD: payload},
                        profile=profiles.get(symbol),
                        market_cap=market_caps.get(symbol),
                        shares_outstanding=shares.get(symbol),
                    )
                    if not df.empty:
                        report.symbols_with_data += 1
                        frames.append(df)

                if not frames:
                    continue

                combined = pd.concat(frames, ignore_index=True)
                if dry_run:
                    report.rows_written[STOCKS_DAILY.name] = (
                        report.rows_written.get(STOCKS_DAILY.name, 0) + len(combined)
                    )
                else:
                    results = ctx.writer.upsert_many({STOCKS_DAILY.name: combined})
                    report.add_writes(results)

            stats = client.stats.summary()
            report.api_requests = stats["total_requests"]
            report.api_failures = stats["failed"]
            if client.stats.failed:
                log.warning("failure reasons: %s", client.stats.failures_by_reason())

    finally:
        ctx.close()

    report.log_summary("EOD update")
    return report


def main(**kwargs) -> RunReport:
    from data_pullv2.core.logging_setup import configure
    from data_pullv2.settings import load_settings

    settings = load_settings()
    configure(settings.run.log_level)
    return asyncio.run(run_eod(settings, **kwargs))


if __name__ == "__main__":
    main()
