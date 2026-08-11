"""Shared job scaffolding: context wiring and chunked streaming."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Sequence

from data_pullv2.core.cache import JsonCache
from data_pullv2.core.models import FetchStats, WriteResult
from data_pullv2.fmp.client import FMPClient
from data_pullv2.fmp.reference import ReferenceDataProvider
from data_pullv2.fmp.universe import UniverseProvider
from data_pullv2.settings import Settings
from data_pullv2.storage.repo import Repository, make_engine
from data_pullv2.storage.writer import TableWriter
from data_pullv2.transform.processor import Processor

log = logging.getLogger(__name__)


def chunked(items: Sequence[str], size: int) -> Iterator[List[str]]:
    for i in range(0, len(items), size):
        yield list(items[i : i + size])


@dataclass
class RunReport:
    """What a job actually did -- used for logging and the process exit code."""

    symbols_requested: int = 0
    symbols_with_data: int = 0
    symbols_failed: int = 0
    rows_written: Dict[str, int] = field(default_factory=dict)
    api_requests: int = 0
    api_failures: int = 0
    dry_run: bool = False

    def add_writes(self, results: Dict[str, WriteResult]) -> None:
        for name, res in results.items():
            self.rows_written[name] = self.rows_written.get(name, 0) + res.rows_written

    @property
    def failure_rate(self) -> float:
        total = self.symbols_requested
        return (self.symbols_failed / total * 100.0) if total else 0.0

    def log_summary(self, title: str) -> None:
        log.info("=" * 64)
        log.info("%s summary", title)
        log.info("  symbols requested : %d", self.symbols_requested)
        log.info("  symbols with data : %d", self.symbols_with_data)
        log.info("  symbols failed    : %d (%.1f%%)", self.symbols_failed, self.failure_rate)
        log.info("  API requests      : %d (%d failed)", self.api_requests, self.api_failures)
        for name, rows in sorted(self.rows_written.items()):
            log.info("  %-18s: %d rows", name, rows)
        if self.dry_run:
            log.info("  DRY RUN -- nothing was written to the database")
        log.info("=" * 64)


class JobContext:
    """Owns the engine, client and providers for the lifetime of a job."""

    def __init__(self, settings: Settings, dry_run: bool = False):
        self.settings = settings
        self.dry_run = dry_run
        self.cache = JsonCache(
            settings.run.cache_dir,
            ttl_hours=settings.run.cache_ttl_hours,
            enabled=settings.run.cache_enabled,
        )
        self.processor = Processor()
        self._engine = None
        self._repo: Optional[Repository] = None
        self._writer: Optional[TableWriter] = None

    @property
    def repo(self) -> Repository:
        if self._repo is None:
            self._engine = make_engine(self.settings.db)
            self._repo = Repository(self._engine)
        return self._repo

    @property
    def writer(self) -> TableWriter:
        if self._writer is None:
            _ = self.repo  # ensure engine
            self._writer = TableWriter(
                self._engine, self.settings.run.max_rows_per_statement
            )
        return self._writer

    def close(self) -> None:
        if self._repo is not None:
            self._repo.close()
            self._repo = None
            self._engine = None
