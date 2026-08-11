"""EOD (End-of-Day) stock data update.

Convenience wrapper around ``jobs/eod.py`` for schedulers that want to invoke
a single target. ``python -m data_pullv2.cli eod`` does exactly the same thing.

Exits non-zero when more than half the symbols fail, so a half-broken run is
distinguishable from a clean one by anything watching the exit code.

--- HOW TO RUN ---
  python -m data_pullv2.eod_update                   # from the repository root
  python -m data_pullv2.cli eod --dry-run --force    # preview without writing
  python -m data_pullv2.cli eod --limit 50           # smaller run

Extra arguments are forwarded, so ``--dry-run``, ``--limit`` and ``--help``
work on this module too.

--- IF YOU SCHEDULE IT (Windows Task Scheduler) ---
   Program:   python
   Arguments: -m data_pullv2.eod_update
   Start in:  <repository root -- the directory containing data_pullv2/>

``Start in`` must be the repository root: that is what puts the package on
sys.path. Trigger after the close plus a settlement delay, e.g. 5:30 PM ET.

For a scheduler that does not depend on this machine being on, see
``.github/workflows/eod.yml`` -- it needs a non-local database.

--- CONFIGURATION (.env) ---
  EOD_MAX_SYMBOLS / MAX_SYMBOLS : symbols per run          (default 250)
  EOD_LOOKBACK_DAYS             : lookback when no history (default 5)
  EOD_STALE_AFTER_DAYS          : treat older rows as delisted (default 30)
  FMP_CONCURRENCY               : concurrent requests      (default 20)
  FMP_CALLS_PER_MINUTE          : plan rate limit          (default 300)
  SYMBOL_CHUNK                  : symbols per commit       (default 100)
  CACHE_TTL_HOURS               : reference-data cache TTL (default 24)
"""

import sys

from data_pullv2.cli import main

if __name__ == "__main__":
    # Forward any extra arguments, so --dry-run/--limit/--help behave as
    # expected. Without this, `eod_update.py --help` silently starts a real
    # production run instead of printing usage.
    sys.exit(main(["eod", *sys.argv[1:]]))
