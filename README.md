# data_pullv2

Pulls equity fundamentals and daily price data from [Financial Modeling Prep](https://financialmodelingprep.com)
into PostgreSQL.

Five tables: `liquidity_ratios`, `earnings_ratios`, `financial_growth`,
`profit_margins`, `stocks_daily` — all upserted on `(symbol, date)`.

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt

cp .env.example .env            # then fill in API_KEY and DB credentials
```

## Usage

```bash
python cli.py plan --limit 250        # request estimate; makes no API calls
python cli.py eod                     # daily incremental price update
python cli.py eod --dry-run --force   # preview, writes nothing
python cli.py backfill --limit 50     # full history for 50 symbols
python cli.py backfill --symbols AAPL,MSFT
python cli.py status                  # row counts and date ranges
python cli.py cache --clear
```

`--dry-run` fetches and transforms but never writes to the database.
`--force` overrides the weekend skip.

Run the offline tests (no network, no database):

```bash
python tests/test_offline.py
```

## Scheduling

`eod_update.py` is a wrapper kept for the existing Windows Task Scheduler
entry. It exits non-zero when more than half the symbols fail, so a
half-broken run registers as a failure rather than looking identical to a
clean one.

```
Program:   python
Arguments: C:\path\to\data_pullv2\eod_update.py
Start in:  C:\path\to\data_pullv2
```

Suggested trigger: daily, 5:30 PM ET (after close plus settlement delay).

## Design

See [ARCHITECTURE.md](ARCHITECTURE.md) for the layer breakdown, request-cost
analysis, and the list of columns that are deliberately left NULL.

Short version — dependencies point one way, `jobs → {fmp, transform, storage} → core`:

| | |
|---|---|
| `cli.py` | entrypoint |
| `settings.py` | validated config |
| `core/` | rate limiting, caching, logging, models |
| `fmp/` | API client, endpoint registry, universe, reference data |
| `transform/` | `fields.py` is the single source of truth for API→DB mapping |
| `storage/` | schema (generated from `fields.py`), chunked writer, queries |
| `jobs/` | `backfill.py`, `eod.py` |

`transform/fields.py` declares each column once; both the DDL and the
transforms are generated from it, so the two cannot drift.

## Configuration

All knobs live in `.env` — see `.env.example` for the annotated list, and
ARCHITECTURE.md for the full table.

## Legacy files

`client.py`, `pipeline.py`, `processor.py`, `db_manager.py`, `models.py` and
`config.py` are the previous implementation. Nothing imports them; they are
retained for reference and can be deleted.

Note three names collide with new modules — `client.py` / `fmp/client.py`,
`models.py` / `core/models.py`, `processor.py` / `transform/processor.py`.
All new imports are package-qualified, so there is no ambiguity, but a bare
`import client` resolves to the old file.
