"""Consistent logging so a half-failed scheduled run is distinguishable."""

from __future__ import annotations

import logging
import sys


def configure(level: str = "INFO") -> None:
    root = logging.getLogger()
    if root.handlers:  # idempotent under repeated CLI invocation
        root.setLevel(getattr(logging, level, logging.INFO))
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(levelname)-7s %(name)-22s %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    root.addHandler(handler)
    root.setLevel(getattr(logging, level, logging.INFO))

    # aiohttp is noisy at DEBUG and drowns out pipeline progress.
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
