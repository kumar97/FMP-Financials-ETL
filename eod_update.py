"""EOD (End-of-Day) stock data update.

Thin wrapper kept so the existing Windows Task Scheduler entry keeps working.
The implementation now lives in ``jobs/eod.py``; this file only forwards to it.

--- HOW TO SCHEDULE (Windows Task Scheduler) ---
1. Open Task Scheduler -> Create Basic Task
2. Trigger: Daily at 5:30 PM ET (after market close + settlement delay)
3. Action: Start a program
   Program:   python
   Arguments: C:\\Users\\kumar\\PycharmProjects\\StockData\\data_pullv2\\eod_update.py
   Start in:  C:\\Users\\kumar\\PycharmProjects\\StockData\\data_pullv2
4. Conditions: check "Start only if network connection is available"
5. Settings: check "Run task as soon as possible after a scheduled start is missed"

The task now exits non-zero when more than half the symbols fail, so a
half-broken run shows up as a failure in Task Scheduler instead of looking
identical to a clean one.

--- HOW TO RUN MANUALLY ---
  python eod_update.py
  python cli.py eod --dry-run --force        # preview without writing
  python cli.py eod --limit 50               # smaller run

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

from cli import main

if __name__ == "__main__":
    sys.exit(main(["eod"]))
