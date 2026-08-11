"""Shared value objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class FetchResult:
    """Outcome of a single HTTP request, success or failure."""

    key: str          # logical identity, e.g. "AAPL:ratios" or "batch:mcap:0"
    endpoint: str
    data: Optional[Any] = None
    success: bool = False
    error: Optional[str] = None
    status: Optional[int] = None
    attempts: int = 1
    fetch_time: float = 0.0

    @property
    def symbol(self) -> Optional[str]:
        return self.key.split(":", 1)[0] if ":" in self.key else None


@dataclass
class FetchStats:
    """Accurate request accounting.

    The previous client appended only successful results, so ``failed`` was
    always 0 and ``success_rate`` always 100%.
    """

    results: List[FetchResult] = field(default_factory=list)

    def add(self, result: FetchResult) -> None:
        self.results.append(result)

    @property
    def successful(self) -> List[FetchResult]:
        return [r for r in self.results if r.success]

    @property
    def failed(self) -> List[FetchResult]:
        return [r for r in self.results if not r.success]

    def summary(self) -> Dict[str, Any]:
        total = len(self.results)
        ok = len(self.successful)
        bad = total - ok
        times = [r.fetch_time for r in self.results] or [0.0]
        retried = sum(1 for r in self.results if r.attempts > 1)
        return {
            "total_requests": total,
            "successful": ok,
            "failed": bad,
            "retried": retried,
            "success_rate": (ok / total * 100.0) if total else 0.0,
            "avg_fetch_time": sum(times) / len(times),
            "wall_time_saved_note": "avg is per-request, not wall clock",
        }

    def failures_by_reason(self, limit: int = 10) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for r in self.failed:
            reason = r.error or "unknown"
            counts[reason] = counts.get(reason, 0) + 1
        return dict(sorted(counts.items(), key=lambda kv: -kv[1])[:limit])


@dataclass
class WriteResult:
    """Rows written per table for one chunk or one run."""

    table: str
    rows_in: int = 0
    rows_written: int = 0
    duplicates_dropped: int = 0
    statements: int = 0
