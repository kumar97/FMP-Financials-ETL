"""Full backfill entrypoint.

Thin wrapper around ``jobs/backfill.py``.

The previous version of this file ran the entire pipeline at module scope, so
importing it -- for a test, a REPL session, or a tooling import -- fired
hundreds of API requests as a side effect. The work now happens inside a
guarded ``main()``.

  python app.py                        # backfill using .env settings
  python -m data_pullv2.app                          # backfill via .env
  python -m data_pullv2.cli backfill --limit 50      # smaller run
  python -m data_pullv2.cli backfill --dry-run       # no writes
  python -m data_pullv2.cli backfill --export-dir out/   # also dump CSVs
"""

import sys

from data_pullv2.cli import main

if __name__ == "__main__":
    # Forward any extra arguments, so --dry-run/--limit/--help behave as
    # expected rather than silently starting a real backfill.
    sys.exit(main(["backfill", *sys.argv[1:]]))
