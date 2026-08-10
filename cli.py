"""Single entrypoint for every job.

    python cli.py eod                       # daily incremental update
    python cli.py eod --dry-run             # fetch + transform, write nothing
    python cli.py backfill --limit 50       # full history for 50 symbols
    python cli.py backfill --symbols AAPL,MSFT
    python cli.py status                    # row counts per table
    python cli.py plan --limit 250          # request-count estimate, no calls
    python cli.py cache --clear

Replaces ``app.py``, which executed its entire pipeline at import time, so
merely importing it fired hundreds of API requests.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from typing import List, Optional

# Run as a script (``python cli.py``, ``python eod_update.py``) the working
# directory is this package, not its parent, so the ``data_pullv2.*`` imports
# the modules use would not resolve. Put the parent on the path first.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_pullv2.core.logging_setup import configure
from data_pullv2.settings import ConfigError, load_settings

log = logging.getLogger("cli")


def _symbols(value: Optional[str]) -> Optional[List[str]]:
    if not value:
        return None
    return [s.strip().upper() for s in value.split(",") if s.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cli.py", description="FMP -> PostgreSQL pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p):
        p.add_argument("--symbols", help="explicit comma-separated symbols")
        p.add_argument("--limit", type=int, help="cap the universe size")
        p.add_argument("--dry-run", action="store_true", help="no database writes")
        p.add_argument("--log-level", default=None)

    eod = sub.add_parser("eod", help="incremental daily price update")
    common(eod)
    eod.add_argument("--force", action="store_true", help="run even on a weekend")

    backfill = sub.add_parser("backfill", help="full fundamentals + price history")
    common(backfill)
    backfill.add_argument("--export-dir", help="also write CSVs to this directory")

    sub.add_parser("status", help="row counts and date ranges per table")

    plan = sub.add_parser("plan", help="estimate request counts without calling the API")
    plan.add_argument("--limit", type=int, default=None)

    cache = sub.add_parser("cache", help="inspect or clear the reference-data cache")
    cache.add_argument("--clear", action="store_true")

    return parser


def cmd_status(settings) -> int:
    from data_pullv2.storage.repo import Repository, make_engine
    from data_pullv2.transform.fields import ALL_TABLES

    repo = Repository(make_engine(settings.db))
    try:
        for spec in ALL_TABLES:
            try:
                stats = repo.table_stats(spec.name)
                log.info(
                    "%-18s rows=%-9s symbols=%-7s %s .. %s",
                    spec.name, stats["total_rows"], stats["unique_symbols"],
                    stats["earliest_date"], stats["latest_date"],
                )
            except Exception as exc:
                log.error("%-18s unavailable: %s", spec.name, exc)
    finally:
        repo.close()
    return 0


def cmd_plan(settings, limit: Optional[int]) -> int:
    """Estimate request counts for both jobs. Makes no API calls."""
    from data_pullv2.fmp import endpoints

    n = limit or settings.run.max_symbols
    per_symbol_backfill = len(endpoints.BACKFILL_PER_SYMBOL)
    batch_calls = -(-n // settings.fmp.batch_symbol_chunk)

    old_backfill = n * 8
    new_backfill = n * per_symbol_backfill + 1 + 8
    old_eod = n * 4
    new_eod = n * 1 + batch_calls  # screener + shares-float usually cached

    rate = settings.fmp.calls_per_minute

    log.info("request plan for %d symbols (plan limit %d/min)", n, rate)
    log.info("  backfill : %6d -> %6d requests  (-%.0f%%),  ~%.1f min at limit",
             old_backfill, new_backfill,
             (1 - new_backfill / old_backfill) * 100, new_backfill / rate)
    log.info("  eod      : %6d -> %6d requests  (-%.0f%%),  ~%.1f min at limit",
             old_eod, new_eod,
             (1 - new_eod / old_eod) * 100, new_eod / rate)
    log.info("  per-symbol backfill endpoints: %s",
             ", ".join(e.name for e in endpoints.BACKFILL_PER_SYMBOL))
    return 0


def cmd_cache(settings, clear: bool) -> int:
    from data_pullv2.core.cache import JsonCache

    cache = JsonCache(settings.run.cache_dir, settings.run.cache_ttl_hours,
                      settings.run.cache_enabled)
    if clear:
        log.info("cleared %d cache entr(ies) from %s", cache.clear(), cache.directory)
        return 0
    if not cache.directory.exists():
        log.info("cache directory %s does not exist yet", cache.directory)
        return 0
    entries = list(cache.directory.glob("*.json.gz"))
    log.info("cache %s (ttl=%dh): %d entr(ies)",
             cache.directory, settings.run.cache_ttl_hours, len(entries))
    for path in entries:
        import time
        age_h = (time.time() - path.stat().st_mtime) / 3600
        log.info("  %-24s %7.1f KB  %5.1fh old%s", path.stem,
                 path.stat().st_size / 1024, age_h,
                 "  (STALE)" if age_h > settings.run.cache_ttl_hours else "")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    try:
        settings = load_settings()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    configure(getattr(args, "log_level", None) or settings.run.log_level)

    if args.command == "status":
        return cmd_status(settings)
    if args.command == "plan":
        return cmd_plan(settings, args.limit)
    if args.command == "cache":
        return cmd_cache(settings, args.clear)

    if args.command == "eod":
        from data_pullv2.jobs.eod import run_eod

        report = asyncio.run(run_eod(
            settings, symbols=_symbols(args.symbols), limit=args.limit,
            dry_run=args.dry_run, force=args.force,
        ))
    elif args.command == "backfill":
        from data_pullv2.jobs.backfill import run_backfill

        report = asyncio.run(run_backfill(
            settings, symbols=_symbols(args.symbols), limit=args.limit,
            dry_run=args.dry_run, export_dir=args.export_dir,
        ))
    else:  # pragma: no cover - argparse enforces the choices
        return 2

    # Non-zero exit when most of the run failed, so a scheduled task that
    # half-succeeded is distinguishable from one that worked.
    if report.symbols_requested and report.failure_rate > 50:
        log.error("failure rate %.1f%% exceeds threshold", report.failure_rate)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
