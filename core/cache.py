"""TTL disk cache for slow-moving reference data.

Company profiles (sector / industry / exchange) and shares-float snapshots
change on the order of weeks, but the previous pipeline re-fetched them on
every single run -- once per ticker. Caching them turns repeat-run cost for
that data into zero API calls.
"""

from __future__ import annotations

import gzip
import json
import logging
import time
from pathlib import Path
from typing import Any, Callable, Optional

log = logging.getLogger(__name__)


class JsonCache:
    """Gzipped JSON blobs on disk, keyed by name, expired by mtime."""

    def __init__(self, directory: Path, ttl_hours: int = 24, enabled: bool = True):
        self.directory = Path(directory)
        self.ttl_seconds = ttl_hours * 3600
        self.enabled = enabled
        if self.enabled:
            self.directory.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in key)
        return self.directory / f"{safe}.json.gz"

    def get(self, key: str) -> Optional[Any]:
        if not self.enabled:
            return None
        path = self._path(key)
        if not path.exists():
            return None
        age = time.time() - path.stat().st_mtime
        if age > self.ttl_seconds:
            log.debug("cache miss (stale, %.1fh) for %s", age / 3600, key)
            return None
        try:
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                payload = json.load(fh)
            log.debug("cache hit (%.1fh old) for %s", age / 3600, key)
            return payload
        except Exception as exc:  # corrupt cache must never break a run
            log.warning("discarding unreadable cache entry %s: %s", key, exc)
            try:
                path.unlink()
            except OSError:
                pass
            return None

    def set(self, key: str, value: Any) -> None:
        if not self.enabled:
            return
        path = self._path(key)
        tmp = path.with_suffix(".tmp")
        try:
            with gzip.open(tmp, "wt", encoding="utf-8") as fh:
                json.dump(value, fh)
            tmp.replace(path)  # atomic swap
        except Exception as exc:
            log.warning("could not write cache entry %s: %s", key, exc)
            tmp.unlink(missing_ok=True)

    async def get_or_fetch(self, key: str, producer: Callable[[], Any]) -> Any:
        """Return the cached value, otherwise await ``producer()`` and store it."""
        cached = self.get(key)
        if cached is not None:
            return cached
        value = await producer()
        if value:
            self.set(key, value)
        return value

    def clear(self) -> int:
        removed = 0
        if not self.directory.exists():
            return 0
        for path in self.directory.glob("*.json.gz"):
            try:
                path.unlink()
                removed += 1
            except OSError:
                pass
        return removed
