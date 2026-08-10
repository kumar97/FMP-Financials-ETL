"""Chunked, deduplicated, parameter-safe upserts.

The previous ``_upsert_data`` built a single INSERT containing every row:

    stmt = insert(table).values(records)     # all rows, one statement

PostgreSQL's wire protocol caps a statement at 65,535 bind parameters. With
16 columns that is ~4,095 rows. Measured against the live API, two tickers of
EOD history already produce 35,112 parameters; 250 tickers produce ~4.4M and
5,939 produce ~104M. The statement cannot be sent at all.

Two further failure modes are handled here:

  * ``ON CONFLICT DO UPDATE command cannot affect row a second time`` --
    PostgreSQL rejects a statement whose own payload contains the conflict key
    twice. Duplicate (symbol, date) pairs are collapsed before sending.
  * Unknown columns raise a CompileError at execute time. Columns are
    intersected with the table definition first.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence

import pandas as pd
from sqlalchemy import Table
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine

from core.models import WriteResult
from storage.schema import TABLES

log = logging.getLogger(__name__)

# PostgreSQL protocol limit; leave headroom for the statement itself.
PG_MAX_BIND_PARAMS = 65535
SAFETY_MARGIN = 0.9


def rows_per_statement(column_count: int, hard_cap: int = 2000) -> int:
    """Largest row count that stays under the bind-parameter ceiling."""
    if column_count <= 0:
        return hard_cap
    allowed = int((PG_MAX_BIND_PARAMS * SAFETY_MARGIN) // column_count)
    return max(1, min(hard_cap, allowed))


class TableWriter:
    """Upserts DataFrames into one of the managed tables."""

    def __init__(self, engine: Engine, max_rows_per_statement: int = 2000):
        self.engine = engine
        self.hard_cap = max_rows_per_statement

    def _prepare(self, table: Table, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """Align a DataFrame to the table: drop unknowns, dedupe, stamp."""
        known = set(table.columns.keys()) - {"id"}
        unknown = [c for c in df.columns if c not in known]
        if unknown:
            log.debug("dropping %d column(s) not in %s: %s",
                      len(unknown), table.name, unknown)
        df = df[[c for c in df.columns if c in known]].copy()

        key = [c for c in ("symbol", "date") if c in df.columns]
        dropped = 0
        if key:
            df = df.dropna(subset=key)
            before = len(df)
            # keep='last' matches upsert semantics: newest wins.
            df = df.drop_duplicates(subset=key, keep="last")
            dropped = before - len(df)
            if dropped:
                log.debug("collapsed %d duplicate %s row(s) in %s",
                          dropped, key, table.name)

        df["created_at"] = datetime.now()

        # NaN/NaT are not valid bind values for pg8000; NULL is.
        df = df.astype(object).where(pd.notna(df), None)
        return df, dropped

    def upsert(self, table_name: str, df: pd.DataFrame) -> WriteResult:
        """Insert-or-update ``df`` into ``table_name``, chunked as needed."""
        result = WriteResult(table=table_name, rows_in=len(df))
        if df is None or df.empty:
            return result

        table = TABLES[table_name]
        prepared, dropped = self._prepare(table, df)
        result.duplicates_dropped = dropped
        if prepared.empty:
            return result

        columns = list(prepared.columns)
        chunk = rows_per_statement(len(columns), self.hard_cap)
        records = prepared.to_dict("records")

        update_columns = [
            c.name for c in table.columns
            if c.name not in ("id", "symbol", "date") and c.name in columns
        ]

        written = 0
        statements = 0
        # One transaction per table write: a failure rolls back this table's
        # chunk set only, leaving previously committed chunks intact.
        with self.engine.begin() as conn:
            for start in range(0, len(records), chunk):
                batch = records[start : start + chunk]
                stmt = insert(table).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["symbol", "date"],
                    set_={c: stmt.excluded[c] for c in update_columns},
                )
                conn.execute(stmt)
                written += len(batch)
                statements += 1

        result.rows_written = written
        result.statements = statements
        log.info(
            "%-18s %6d rows in %3d statement(s) (chunk=%d, %d dup dropped)",
            table_name, written, statements, chunk, dropped,
        )
        return result

    def upsert_many(self, tables: Dict[str, pd.DataFrame]) -> Dict[str, WriteResult]:
        out: Dict[str, WriteResult] = {}
        for name, df in tables.items():
            if df is None or df.empty:
                out[name] = WriteResult(table=name)
                continue
            try:
                out[name] = self.upsert(name, df)
            except Exception as exc:
                log.error("write to %s failed: %s", name, exc)
                out[name] = WriteResult(table=name, rows_in=len(df))
        return out
