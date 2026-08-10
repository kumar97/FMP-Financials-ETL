from dataclasses import dataclass
from typing import Optional, Any


@dataclass
class FetchResult:
    """Container for fetch results with metadata"""
    ticker: str
    endpoint: str
    data: Optional[Any]
    success: bool
    error: Optional[str] = None
    fetch_time: float = 0.0