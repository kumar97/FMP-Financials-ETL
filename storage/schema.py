"""Table definitions derived from ``transform.fields``.

The DDL produced here is column-for-column identical to the previous
``DatabaseManager.TABLE_SCHEMAS``, including the columns that are deliberately
never populated, so ``create_all`` remains a no-op against the existing
database. The difference is that the column list is now generated from the
same declaration the transforms read, so the two cannot drift apart.
"""

from __future__ import annotations

import logging
from typing import Dict

from sqlalchemy import Column, Index, Integer, MetaData, Table
from sqlalchemy.engine import Engine

from transform.fields import ALL_TABLES, TableSpec

log = logging.getLogger(__name__)


def build_metadata() -> MetaData:
    metadata = MetaData()
    for spec in ALL_TABLES:
        _build_table(metadata, spec)
    return metadata


def _build_table(metadata: MetaData, spec: TableSpec) -> Table:
    table = Table(
        spec.name,
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        *[Column(f.db_column, f.dtype) for f in spec.fields],
        extend_existing=True,
    )
    Index(f"idx_{spec.name}_symbol", table.c.symbol)
    Index(f"idx_{spec.name}_date", table.c.date)
    # The upsert's ON CONFLICT target. Must exist for _upsert to work.
    Index(f"idx_{spec.name}_symbol_date", table.c.symbol, table.c.date, unique=True)
    return table


METADATA = build_metadata()
TABLES: Dict[str, Table] = {name: METADATA.tables[name] for name in METADATA.tables}


def create_all(engine: Engine) -> None:
    """Create any missing tables and indexes. Safe to call repeatedly."""
    METADATA.create_all(engine)
    log.info("schema ensured for %d tables", len(ALL_TABLES))
