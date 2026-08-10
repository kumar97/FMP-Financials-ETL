"""The single async FMP client.

Replaces the two divergent fetch implementations (``client.AsyncFMPClient``
and the private ``_get``/``fetch_ticker`` pair inside ``eod_update.py``), which
had drifted to the point that the daily job applied no rate limiting at all.

Behaviour added here that neither original had:
  * global token-bucket rate limiting shared by every in-flight request
  * retry with exponential backoff + jitter on 429 / 5xx / timeouts
  * honours ``Retry-After`` and drains the bucket so the whole fan-out backs off
  * records failures as well as successes, so stats are truthful
  * one shared TCP connector instead of a fresh session per call site
"""

from __future__ import annotations

import asyncio
import logging
import random
import sys
import time
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import aiohttp

from core.models import FetchResult, FetchStats
from core.ratelimit import AsyncRateLimiter
from fmp.endpoints import EndpointSpec
from settings import FMPSettings

log = logging.getLogger(__name__)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}


class FMPClient:
    """Async client with rate limiting, retries and honest accounting."""

    def __init__(self, settings: FMPSettings):
        self.settings = settings
        self.stats = FetchStats()
        self._limiter = AsyncRateLimiter(settings.calls_per_minute, burst=settings.burst)
        self._semaphore = asyncio.Semaphore(settings.max_concurrent)
        self._session: Optional[aiohttp.ClientSession] = None

    # -- lifecycle -----------------------------------------------------------

    async def __aenter__(self) -> "FMPClient":
        connector = aiohttp.TCPConnector(
            limit=self.settings.max_concurrent,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=self.settings.timeout_seconds),
            headers={"Accept-Encoding": "gzip"},
        )
        return self

    async def __aexit__(self, *exc) -> None:
        if self._session is not None:
            await self._session.close()
            # Give the transports a moment to close cleanly on Windows.
            await asyncio.sleep(0)
            self._session = None

    # -- request primitives --------------------------------------------------

    def _build_url(self, spec: EndpointSpec, params: Dict[str, Any]) -> str:
        query = dict(params)
        query["apikey"] = self.settings.api_key
        parts = "&".join(f"{k}={v}" for k, v in query.items() if v is not None)
        return f"{self.settings.base_url}/{spec.path}?{parts}"

    async def _sleep_backoff(self, attempt: int, retry_after: Optional[float]) -> None:
        if retry_after is not None:
            delay = retry_after
        else:
            delay = min(
                self.settings.backoff_max_seconds,
                self.settings.backoff_base_seconds * (2 ** attempt),
            )
        # Full jitter avoids a synchronised retry stampede across the fan-out.
        delay = random.uniform(0, delay) if retry_after is None else delay
        await asyncio.sleep(delay)

    async def fetch(
        self,
        spec: EndpointSpec,
        params: Dict[str, Any],
        key: Optional[str] = None,
    ) -> FetchResult:
        """Fetch one URL with rate limiting and retries. Never raises."""
        url = self._build_url(spec, params)
        key = key or f"{params.get('symbol', 'batch')}:{spec.name}"
        started = time.monotonic()
        last_error = "unknown"
        last_status: Optional[int] = None

        for attempt in range(self.settings.max_retries + 1):
            async with self._semaphore:
                await self._limiter.acquire()
                try:
                    assert self._session is not None, "use FMPClient as an async context manager"
                    async with self._session.get(url) as response:
                        last_status = response.status

                        if response.status == 200:
                            data = await response.json(content_type=None)
                            result = FetchResult(
                                key=key,
                                endpoint=spec.name,
                                data=data,
                                success=True,
                                status=200,
                                attempts=attempt + 1,
                                fetch_time=time.monotonic() - started,
                            )
                            self.stats.add(result)
                            return result

                        body = (await response.text())[:200]

                        if response.status == 429:
                            retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                            wait = retry_after if retry_after is not None else 5.0
                            log.warning(
                                "429 rate limited on %s; backing off %.1fs (attempt %d)",
                                key, wait, attempt + 1,
                            )
                            # Throttle every other in-flight request too.
                            await self._limiter.pause(wait)
                            last_error = f"429 rate limited: {body}"
                        elif response.status in RETRYABLE_STATUS:
                            last_error = f"{response.status}: {body}"
                        else:
                            # 401/402/403/404 will not improve with retries.
                            result = FetchResult(
                                key=key, endpoint=spec.name, data=None, success=False,
                                error=f"{response.status}: {body}", status=response.status,
                                attempts=attempt + 1, fetch_time=time.monotonic() - started,
                            )
                            self.stats.add(result)
                            return result

                except asyncio.TimeoutError:
                    last_error = f"timeout after {self.settings.timeout_seconds}s"
                except aiohttp.ClientError as exc:
                    last_error = f"{type(exc).__name__}: {exc}"
                except Exception as exc:  # noqa: BLE001 - must never kill the run
                    last_error = f"{type(exc).__name__}: {exc}"

            if attempt < self.settings.max_retries:
                await self._sleep_backoff(attempt, None)

        result = FetchResult(
            key=key, endpoint=spec.name, data=None, success=False,
            error=last_error, status=last_status,
            attempts=self.settings.max_retries + 1,
            fetch_time=time.monotonic() - started,
        )
        self.stats.add(result)
        log.debug("giving up on %s after %d attempts: %s", key, result.attempts, last_error)
        return result

    # -- fan-out helpers -----------------------------------------------------

    async def fetch_many(
        self,
        requests: Sequence[Tuple[EndpointSpec, Dict[str, Any], str]],
        progress: Optional[Callable[[int, int], None]] = None,
    ) -> List[FetchResult]:
        """Run many requests concurrently; concurrency is bounded internally."""
        total = len(requests)
        done = 0
        out: List[FetchResult] = []

        tasks = [
            asyncio.ensure_future(self.fetch(spec, params, key))
            for spec, params, key in requests
        ]
        for coro in asyncio.as_completed(tasks):
            out.append(await coro)
            done += 1
            if progress:
                progress(done, total)
        return out

    async def fetch_symbol_endpoints(
        self,
        symbols: Iterable[str],
        specs: Sequence[EndpointSpec],
        extra_params: Optional[Dict[str, Any]] = None,
        per_symbol_params: Optional[Callable[[str, EndpointSpec], Dict[str, Any]]] = None,
        progress: Optional[Callable[[int, int], None]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Fetch every ``spec`` for every symbol -> ``{symbol: {endpoint: data}}``.

        Failed requests yield ``None`` for that endpoint rather than being
        dropped, so downstream code can tell "no data" from "never asked".
        """
        requests: List[Tuple[EndpointSpec, Dict[str, Any], str]] = []
        for symbol in symbols:
            for spec in specs:
                params: Dict[str, Any] = {"symbol": symbol}
                if extra_params:
                    params.update(extra_params)
                if per_symbol_params:
                    params.update(per_symbol_params(symbol, spec) or {})
                requests.append((spec, params, f"{symbol}:{spec.name}"))

        results = await self.fetch_many(requests, progress=progress)

        organised: Dict[str, Dict[str, Any]] = {}
        for result in results:
            symbol = result.key.split(":", 1)[0]
            organised.setdefault(symbol, {})[result.endpoint] = (
                result.data if result.success else None
            )
        return organised

    async def fetch_batched_symbols(
        self,
        spec: EndpointSpec,
        symbols: Sequence[str],
        chunk_size: Optional[int] = None,
    ) -> List[Any]:
        """Call a BATCH endpoint over ``symbols``, flattening the responses."""
        size = chunk_size or self.settings.batch_symbol_chunk
        chunks = [symbols[i : i + size] for i in range(0, len(symbols), size)]
        requests = [
            (spec, {"symbols": ",".join(chunk)}, f"batch{i}:{spec.name}")
            for i, chunk in enumerate(chunks)
        ]
        results = await self.fetch_many(requests)

        rows: List[Any] = []
        for result in results:
            if result.success and isinstance(result.data, list):
                rows.extend(result.data)
        log.info(
            "%s: %d symbols in %d request(s) -> %d rows",
            spec.name, len(symbols), len(chunks), len(rows),
        )
        return rows

    async def fetch_paginated(
        self,
        spec: EndpointSpec,
        params: Optional[Dict[str, Any]] = None,
        page_size: Optional[int] = None,
        max_pages: int = 50,
    ) -> List[Any]:
        """Walk a UNIVERSE endpoint's pages until it returns a short page."""
        size = page_size or self.settings.page_size
        base = dict(params or {})
        rows: List[Any] = []
        for page in range(max_pages):
            query = dict(base, page=page, limit=size)
            result = await self.fetch(spec, query, key=f"page{page}:{spec.name}")
            if not result.success or not isinstance(result.data, list) or not result.data:
                break
            rows.extend(result.data)
            if len(result.data) < size:
                break
        else:
            log.warning("%s hit max_pages=%d; results may be truncated", spec.name, max_pages)
        return rows


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None
