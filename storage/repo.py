"""Read-side queries and engine construction."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Sequence

import pandas as pd
from sqlalchemy import bindparam, create_engine, func, select, text
from sqlalchemy.engine import Engine

from data_pullv2.settings import DatabaseSettings
from data_pullv2.storage.schema import TABLES, create_all

log = logging.getLogger(__name__)


def make_engine(db: DatabaseSettings) -> Engine:
    return create_engine(
        db.sqlalchemy_url(),
        connect_args=db.connect_args(),
        pool_pre_ping=True,
        pool_size=db.pool_size,
        max_overflow=db.max_overflow,
        future=True,
    )


class Repository:
    """Everything the jobs need to read from PostgreSQL."""

    def __init__(self, engine: Engine):
        self.engine = engine

    def ensure_schema(self) -> None:
        create_all(self.engine)

    def latest_dates(self, table_name: str) -> Dict[str, datetime]:
        """``{symbol: max(date)}`` -- the per-symbol ingest watermark."""
        table = TABLES[table_name]
        stmt = select(table.c.symbol, func.max(table.c.date)).group_by(table.c.symbol)
        with self.engine.connect() as conn:
            return {row[0]: row[1] for row in conn.execute(stmt)}

    def resume_dates(
        self,
        table_name: str,
        symbols: Sequence[str],
        default_from: date,
        stale_after_days: int = 30,
    ) -> Dict[str, date]:
        """Per-symbol ``from`` date for an incremental fetch.

        The previous logic took ``min()`` across every symbol's watermark, so a
        single delisted ticker whose last row was years old forced every symbol
        to re-download its entire history. Each symbol now resumes from its own
        watermark; symbols that are merely stale fall back to ``default_from``
        rather than dragging the window back.
        """
        watermarks = self.latest_dates(table_name)
        today = date.today()
        stale_cutoff = today - timedelta(days=stale_after_days)

        out: Dict[str, date] = {}
        for symbol in symbols:
            last = watermarks.get(symbol)
            if last is None:
                out[symbol] = default_from
                continue
            last_date = last.date() if isinstance(last, datetime) else last
            if last_date < stale_cutoff:
                out[symbol] = default_from
            else:
                out[symbol] = last_date + timedelta(days=1)
        return out

    def table_stats(self, table_name: str) -> Dict[str, object]:
        table = TABLES[table_name]
        stmt = select(
            func.count().label("total_rows"),
            func.count(func.distinct(table.c.symbol)).label("unique_symbols"),
            func.min(table.c.date).label("earliest_date"),
            func.max(table.c.date).label("latest_date"),
        )
        with self.engine.connect() as conn:
            row = conn.execute(stmt).mappings().one()
        return dict(row)

    def load(
        self,
        table_name: str,
        symbols: Optional[Sequence[str]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """Load rows with real bound parameters.

        The previous implementation interpolated ``%s`` placeholders into a raw
        string and passed a positional list, which pg8000 does not bind.
        """
        table = TABLES[table_name]
        stmt = select(table)
        if symbols:
            stmt = stmt.where(table.c.symbol.in_(list(symbols)))
        if start_date is not None:
            stmt = stmt.where(table.c.date >= start_date)
        if end_date is not None:
            stmt = stmt.where(table.c.date <= end_date)
        stmt = stmt.order_by(table.c.symbol, table.c.date.desc())
        with self.engine.connect() as conn:
            return pd.DataFrame(conn.execute(stmt).mappings().all())

    def close(self) -> None:
        self.engine.dispose()
