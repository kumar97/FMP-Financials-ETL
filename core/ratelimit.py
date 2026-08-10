"""Async token-bucket rate limiter.

Fixes two defects in the previous implementation:

1. It held the lock across ``await asyncio.sleep(...)``, so every waiter queued
   behind the sleeper and the bucket degenerated into a convoy.
2. After sleeping it set ``_tokens = 0`` without advancing ``_last_refill``,
   so the next refill counted the sleep interval a second time and handed out
   a burst above the configured rate.
"""

from __future__ import annotations

import asyncio
import time


class AsyncRateLimiter:
    """Allows at most ``rate_per_minute`` acquisitions per rolling minute."""

    def __init__(self, rate_per_minute: int, burst: int | None = None):
        if rate_per_minute <= 0:
            raise ValueError("rate_per_minute must be positive")
        self._capacity = float(burst if burst is not None else rate_per_minute)
        self._tokens_per_second = rate_per_minute / 60.0
        self._tokens = self._capacity
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._updated
        if elapsed > 0:
            self._tokens = min(
                self._capacity, self._tokens + elapsed * self._tokens_per_second
            )
            self._updated = now

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                deficit = 1.0 - self._tokens
                wait = deficit / self._tokens_per_second
            # Sleep outside the lock so other coroutines can still refill-check.
            await asyncio.sleep(wait)

    async def pause(self, seconds: float) -> None:
        """Drain the bucket for ``seconds`` after a 429.

        Prevents the rest of the in-flight fan-out from immediately hammering
        the API again while the server is asking us to back off.
        """
        async with self._lock:
            self._refill()
            self._tokens = 0.0
            self._updated = time.monotonic() + max(0.0, seconds)
