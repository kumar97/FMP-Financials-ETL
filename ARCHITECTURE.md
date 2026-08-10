# data_pullv2 — architecture

FMP → PostgreSQL ingestion for equity fundamentals and daily prices.

## Layout

```
cli.py            single entrypoint: eod | backfill | status | plan | cache
settings.py       validated config; every tuneable lives here

core/             infrastructure, no domain knowledge
  models.py         FetchResult, FetchStats, WriteResult
  ratelimit.py      async token bucket
  cache.py          TTL disk cache for slow-moving reference data
  logging_setup.py

fmp/              everything that talks to the API
  endpoints.py      endpoint registry + cardinality
  client.py         the only HTTP client: rate limit, retry, 429, stats
  universe.py       symbol universe + profiles (screener-backed)
  reference.py      shares float + market caps (batch-backed)

transform/        payload → DataFrame
  fields.py         SINGLE SOURCE OF TRUTH: api field → db column
  processor.py      pure transforms driven by fields.py

storage/          everything that talks to PostgreSQL
  schema.py         DDL generated from fields.py
  writer.py         chunked, deduped, param-safe upserts
  repo.py           reads: watermarks, stats, engine construction

jobs/             orchestration
  base.py           JobContext, RunReport, chunking
  backfill.py       full history
  eod.py            daily incremental

tests/test_offline.py   no network, no database
```

Dependencies point one way: `jobs → {fmp, transform, storage} → core`.

## The load-bearing idea: one field map

`transform/fields.py` declares every column once:

```python
FieldSpec("revenue_growth", Float, SRC_FIN_GROWTH, "revenueGrowth", scale=100.0)
```

`storage/schema.py` generates the DDL from it, `transform/processor.py` generates
the transforms from it. Previously this mapping lived in three places — the
`TABLE_SCHEMAS` dict, hardcoded column lists in `processor.py`, and a camelCase
regex in `_normalize_column_names` — and nothing raised when they drifted:
`filter_columns` silently dropped unknown API fields and the column just stayed
NULL. That is how seven columns ended up 100% NULL in the live database.

## Request cost

Cardinality is the axis that matters. Verified against this subscription:
bulk endpoints (`batch-eod`, `profile-bulk`, `*-ttm-bulk`) return HTTP 402, and
per-symbol endpoints ignore comma-separated symbols — so `PER_SYMBOL` really is
irreducible, not an oversight.

| | before | after |
|---|---|---|
| **backfill** (per symbol) | 8 | 5 |
| **EOD** (per symbol) | 4 | 1 |
| **EOD**, 250 symbols | 1,000 | ~252 |
| **backfill**, 250 symbols | 2,000 | ~1,259 |

What changed:

- `income-statement-growth` **dropped**. Its only consumed field, `growthEBITDA`,
  is bit-identical to `financial-growth.ebitdaGrowth` (verified across
  AAPL/MSFT/JPM/XOM).
- per-symbol `profile` → **`company-screener`**, one request for the whole
  universe, carrying sector/industry/exchange. Cached 24h.
- per-symbol `shares-float` → **`shares-float-all`**, paginated. Cached 24h.
  (It was previously fetched per ticker and then discarded — the merge in the
  old `process_stocks_daily` is commented out.)
- per-symbol `historical-market-capitalization` → **`market-capitalization-batch`**
  for the daily job (~500 symbols/request). Backfill still uses the per-symbol
  historical series, because the batch endpoint returns only the latest value.

## Correctness fixes

**Bind-parameter overflow.** The old `_upsert_data` built one INSERT for every
row. PostgreSQL caps a statement at 65,535 bind parameters; stocks_daily has 16
columns, so ~4,095 rows. Two tickers of history already produce 35,112
parameters and 250 tickers produce ~4.4M — unsendable. `storage/writer.py`
derives a chunk size from the column count and commits in batches.

**Duplicate conflict keys.** PostgreSQL rejects `ON CONFLICT DO UPDATE` when one
statement carries the same key twice. Rows are deduped on `(symbol, date)`,
`keep="last"`, before sending.

**Watermarks.** The old EOD job used `min()` across all symbols' last dates, so
a single delisted ticker with a stale row dragged every symbol's `from` date
back by years. Each symbol now resumes from its own watermark;
symbols older than `EOD_STALE_AFTER_DAYS` fall back to the lookback default
rather than widening the window for everyone.

**Rate limiting.** `eod_update.py` had none at all. There is now one global
token bucket shared by every in-flight request, plus retry with exponential
backoff and jitter on 429/5xx/timeouts. A 429 drains the bucket so the whole
fan-out backs off, not just the one request. The old limiter also held its lock
across `await sleep` (convoy) and double-counted elapsed time after sleeping
(bursts above the configured rate).

**Honest stats.** The old client appended only successful results, so
`success_rate` was always 100% and `failed_requests` always empty.

**Memory and restartability.** Symbols stream through in chunks
(`SYMBOL_CHUNK`, default 100), each committed independently. Peak memory is
bounded by chunk size instead of universe × history, and a crash costs one
chunk instead of the whole run.

**Import side effects.** `app.py` ran the full pipeline at module scope —
importing it fired hundreds of requests. Work now sits behind `main()`.

## Known-NULL columns (deliberately left alone)

These are NULL for every row in the live DB because the old code requested API
field names that don't exist. Correct names are recorded in `fields.py` but
**disabled**, so this refactor does not change what lands in your tables.

| column | status |
|---|---|
| `interest_coverage` | recoverable → `ratios.interestCoverageRatio` |
| `ebit_growth` | recoverable → `financial-growth.ebitgrowth` (lowercase g) |
| `eps_growth` | recoverable → `financial-growth.epsgrowth` (lowercase g) |
| `shares_outstanding` | recoverable → `shares-float-all.outstandingShares` |
| `cash_ratio` | no equivalent field on this plan |
| `price_earnings_ratio` | duplicate of `price_to_earnings_ratio` |
| `change_pct` | duplicate of `change_percent` |

Set `CORRECTED_FIELDS=1` to populate the four recoverable ones. **No DDL change
is needed** — those columns already exist. Backfill the history afterwards to
fill older rows.

## Usage

```bash
python cli.py plan --limit 250          # request estimate, makes no API calls
python cli.py eod                       # daily incremental
python cli.py eod --dry-run --force     # preview, writes nothing
python cli.py backfill --limit 50
python cli.py backfill --export-dir out/
python cli.py status                    # row counts per table
python cli.py cache --clear
python tests/test_offline.py
```

`app.py` and `eod_update.py` remain as thin wrappers so the existing Task
Scheduler entry keeps working. Both exit non-zero when >50% of symbols fail, so
a half-broken run is distinguishable from a clean one.

## Configuration (`.env`)

| key | default | meaning |
|---|---|---|
| `API_KEY`, `host`, `port`, `database`, `user`, `password` | — | required |
| `MAX_SYMBOLS` | 250 | universe cap |
| `SYMBOL_CHUNK` | 100 | symbols per commit |
| `FMP_CALLS_PER_MINUTE` | 300 | plan rate limit |
| `FMP_BURST` | = rate | back-to-back requests before throttling |
| `FMP_CONCURRENCY` | 20 | open sockets |
| `FMP_MAX_RETRIES` | 4 | retries on 429/5xx/timeout |
| `EOD_LOOKBACK_DAYS` | 5 | window when a symbol has no history |
| `EOD_STALE_AFTER_DAYS` | 30 | older watermark ⇒ treat as delisted |
| `CACHE_TTL_HOURS` | 24 | reference-data cache lifetime |
| `CACHE_ENABLED` | true | |
| `CORRECTED_FIELDS` | false | populate the recoverable NULL columns |
| `LOG_LEVEL` | INFO | |

## Superseded files

Left on disk (this directory is not under version control, so nothing was
deleted) and no longer imported by any entrypoint. Safe to remove once you're
satisfied: `client.py`, `pipeline.py`, `processor.py`, `db_manager.py`,
`models.py`, `config.py`.

`config.py` is still imported by the old `db_manager.py` only. Nothing in the
new package reads it.
