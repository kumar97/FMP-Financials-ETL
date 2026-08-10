"""Validated, single-source configuration.

Everything tuneable lives here and is read from the environment exactly once.
Missing or malformed values fail fast at startup with an actionable message
rather than an AttributeError halfway through a run.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent

# Load .env relative to this file, not the working directory, so scheduled
# tasks and ad-hoc scripts resolve the same configuration.
load_dotenv(PROJECT_ROOT / ".env")


class ConfigError(RuntimeError):
    """Raised when required configuration is missing or invalid."""


def _require(name: str) -> str:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        raise ConfigError(
            f"Required environment variable {name!r} is missing or empty. "
            f"Add it to {PROJECT_ROOT / '.env'}"
        )
    return raw.strip()


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError as exc:
        raise ConfigError(f"Environment variable {name!r}={raw!r} is not an integer") from exc


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class FMPSettings:
    """Financial Modeling Prep API settings."""

    api_key: str
    base_url: str = "https://financialmodelingprep.com/stable"

    # Plan limit. The token bucket in core.ratelimit enforces this globally
    # across every concurrent request, which the previous code did not do.
    calls_per_minute: int = 300

    # How many requests may fire back-to-back before the bucket throttles.
    # Defaults to the full per-minute allowance, which is correct for a fixed
    # per-minute window. Lower it (e.g. FMP_BURST=60) if the API turns out to
    # enforce a sliding window and starts returning 429 on the opening burst.
    burst: Optional[int] = None

    # Concurrency is capped independently of the rate limit: the limiter
    # controls throughput, the semaphore controls how many sockets are open.
    max_concurrent: int = 20

    timeout_seconds: int = 30
    max_retries: int = 4
    backoff_base_seconds: float = 0.5
    backoff_max_seconds: float = 30.0

    # market-capitalization-batch accepts ~1000 symbols; 500 keeps the URL
    # comfortably under common 8 KB request-line limits.
    batch_symbol_chunk: int = 500

    # shares-float-all page size (server accepts up to 5000).
    page_size: int = 5000


@dataclass(frozen=True)
class DatabaseSettings:
    """PostgreSQL connection settings."""

    host: str
    port: int
    database: str
    user: str
    password: str
    sslmode: str = "disable"
    pool_size: int = 5
    max_overflow: int = 10

    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+pg8000://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    def connect_args(self) -> dict:
        """Real SSL wiring for pg8000.

        The previous implementation built a ``connect_args`` dict and then
        discarded it, so ``sslmode`` was silently a no-op.
        """
        if self.sslmode in {"disable", ""}:
            return {}
        import ssl

        ctx = ssl.create_default_context()
        if self.sslmode in {"allow", "prefer", "require"}:
            # These modes encrypt but do not verify the server certificate.
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        return {"ssl_context": ctx}


@dataclass(frozen=True)
class RunSettings:
    """Job-level behaviour knobs."""

    # Universe
    max_symbols: int = 250
    exchanges: tuple = ("NASDAQ", "NYSE", "AMEX")
    include_etfs: bool = False
    include_funds: bool = False

    # Batching: how many symbols are fetched, transformed and written as one
    # restartable unit. Bounds peak memory regardless of universe size.
    symbol_chunk: int = 100

    # Rows per INSERT statement is derived from the column count in
    # storage.writer; this is an upper bound applied on top of it.
    max_rows_per_statement: int = 2000

    # EOD
    eod_lookback_days: int = 5
    # Symbols whose last row is older than this are treated as delisted and
    # excluded from the incremental window calculation, so one stale ticker
    # cannot drag every symbol's from_date back by years.
    eod_stale_after_days: int = 30

    # Caching for slow-moving reference data (profiles, shares float).
    cache_dir: Path = field(default=PROJECT_ROOT / ".cache")
    cache_ttl_hours: int = 24
    cache_enabled: bool = True

    log_level: str = "INFO"


@dataclass(frozen=True)
class Settings:
    fmp: FMPSettings
    db: DatabaseSettings
    run: RunSettings


def load_settings() -> Settings:
    """Build settings from the environment, validating as we go."""
    fmp = FMPSettings(
        api_key=_require("API_KEY"),
        calls_per_minute=_int("FMP_CALLS_PER_MINUTE", 300),
        burst=_int("FMP_BURST", 0) or None,
        max_concurrent=_int("FMP_CONCURRENCY", 20),
        timeout_seconds=_int("FMP_TIMEOUT", 30),
        max_retries=_int("FMP_MAX_RETRIES", 4),
    )
    db = DatabaseSettings(
        host=_require("host"),
        port=_int("port", 5432),
        database=_require("database"),
        user=_require("user"),
        password=_require("password"),
        sslmode=os.getenv("sslmode", "disable").strip() or "disable",
    )
    run = RunSettings(
        max_symbols=_int("MAX_SYMBOLS", _int("EOD_MAX_SYMBOLS", 250)),
        symbol_chunk=_int("SYMBOL_CHUNK", 100),
        eod_lookback_days=_int("EOD_LOOKBACK_DAYS", 5),
        eod_stale_after_days=_int("EOD_STALE_AFTER_DAYS", 30),
        cache_ttl_hours=_int("CACHE_TTL_HOURS", 24),
        cache_enabled=_bool("CACHE_ENABLED", True),
        log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO",
    )
    return Settings(fmp=fmp, db=db, run=run)
