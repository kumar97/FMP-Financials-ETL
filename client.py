"""Async client for fetching data from FMP API"""

import asyncio
import aiohttp
import time
from typing import List, Dict, Any, Optional, Callable
from config import FMPConfig
from models import FetchResult
import sys


class RateLimiter:
    """Token bucket rate limiter for async use"""

    def __init__(self, calls_per_minute: int):
        self._rate = calls_per_minute / 60.0  # tokens per second
        self._tokens = float(calls_per_minute)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens += elapsed * self._rate
            self._tokens = min(self._tokens, self._rate * 60)
            self._last_refill = now

            if self._tokens < 1:
                wait = (1 - self._tokens) / self._rate
                await asyncio.sleep(wait)
                self._tokens = 0
            else:
                self._tokens -= 1

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class AsyncFMPClient:
    """Async client for fetching data from FMP API"""

    def __init__(self, config: FMPConfig):
        self.config = config
        self.results: List[FetchResult] = []
        self._rate_limiter = RateLimiter(config.calls_per_minute)

    async def fetch_endpoint(
            self,
            session: aiohttp.ClientSession,
            semaphore: asyncio.Semaphore(),
            endpoint: str,
            ticker: str
    ) -> FetchResult:
        """Fetch a single endpoint for a ticker"""
        if endpoint == 'historical-price-eod':
            url = f"{self.config.base_url}/historical-price-eod/full?symbol={ticker}&apikey={self.config.api_key}"
        else:
            url = f"{self.config.base_url}/{endpoint}?symbol={ticker}&apikey={self.config.api_key}"
        start_time = time.time()

        async with semaphore:  # Use the passed semaphore
            await self._rate_limiter.acquire()
            try:
                async with session.get(
                        url,
                        timeout=aiohttp.ClientTimeout(total=self.config.timeout_seconds)
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    fetch_time = time.time() - start_time

                    result = FetchResult(
                        ticker=ticker,
                        endpoint=endpoint,
                        data=data,
                        success=True,
                        fetch_time=fetch_time
                    )
                    self.results.append(result)
                    return result

            except asyncio.TimeoutError:
                error_msg = f"Timeout after {self.config.timeout_seconds}s"
                return FetchResult(
                    ticker=ticker,
                    endpoint=endpoint,
                    data=None,
                    success=False,
                    error=error_msg,
                    fetch_time=time.time() - start_time
                )
            except Exception as e:
                return FetchResult(
                    ticker=ticker,
                    endpoint=endpoint,
                    data=None,
                    success=False,
                    error=str(e),
                    fetch_time=time.time() - start_time
                )

    async def fetch_ticker_all_endpoints(
            self,
            session: aiohttp.ClientSession,
            semaphore: asyncio.Semaphore,
            ticker: str,
            endpoints: List[str]
    ) -> List[FetchResult]:
        """Fetch all endpoints for a single ticker concurrently"""
        tasks = [
            self.fetch_endpoint(session, semaphore, endpoint, ticker)
            for endpoint in endpoints
        ]
        return await asyncio.gather(*tasks)

    async def fetch_all_async(
            self,
            tickers: List[str],
            endpoints: List[str],
            progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch all tickers and endpoints concurrently

        Args:
            tickers: List of stock symbols
            endpoints: List of FMP endpoints
            progress_callback: Optional function(completed, total) for progress tracking

        Returns:
            Dict with structure: {ticker: {endpoint: data}}
        """
        self.results = []  # Reset results
        total_requests = len(tickers) * len(endpoints)
        completed = 0

        semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)

        async with aiohttp.ClientSession() as session:
            tasks = []

            for ticker in tickers:
                task = self.fetch_ticker_all_endpoints(session, semaphore, ticker, endpoints)
                tasks.append(task)

            # Gather with progress tracking
            organized = {}

            for coro in asyncio.as_completed(tasks):
                ticker_results = await coro

                # Organize by ticker
                for result in ticker_results:
                    if result.ticker not in organized:
                        organized[result.ticker] = {}
                    organized[result.ticker][result.endpoint] = result.data

                    completed += 1
                    if progress_callback:
                        progress_callback(completed, total_requests)

            return organized

    def fetch_all(
            self,
            tickers: List[str],
            endpoints: List[str],
            progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Synchronous wrapper for async fetch"""
        return asyncio.run(self.fetch_all_async(tickers, endpoints, progress_callback))

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the last fetch operation"""
        if not self.results:
            return {}

        successful = [r for r in self.results if r.success]
        failed = [r for r in self.results if not r.success]

        return {
            'total_requests': len(self.results),
            'successful': len(successful),
            'failed': len(failed),
            'success_rate': len(successful) / len(self.results) * 100 if self.results else 0,
            'avg_fetch_time': sum(r.fetch_time for r in self.results) / len(self.results) if self.results else 0,
            'total_time': sum(r.fetch_time for r in self.results),
            'failed_requests': [
                {'ticker': r.ticker, 'endpoint': r.endpoint, 'error': r.error}
                for r in failed
            ]
        }
