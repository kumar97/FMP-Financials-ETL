"""Full backfill entrypoint.

Thin wrapper around ``jobs/backfill.py``.

The previous version of this file ran the entire pipeline at module scope, so
importing it -- for a test, a REPL session, or a tooling import -- fired
hundreds of API requests as a side effect. The work now happens inside a
guarded ``main()``.

  python app.py                        # backfill using .env settings
  python cli.py backfill --limit 50    # smaller run
  python cli.py backfill --dry-run     # fetch + transform, write nothing
  python cli.py backfill --export-dir out/   # also dump CSVs
"""

import sys

from cli import main

if __name__ == "__main__":
    sys.exit(main(["backfill"]))
