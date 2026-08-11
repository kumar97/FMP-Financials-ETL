# data_pullv2 — architecture

FMP → PostgreSQL ingestion for equity fundamentals and daily prices.

## Layout

The package is nested one level below the repository root, so the import name
`data_pullv2` is a directory *inside* the repo rather than the repo itself.
That keeps imports working whatever the clone is named. There is no install
step: running from the repository root is what puts the package on
`sys.path`.

```
requirements.txt
.env              gitignored; resolved from the repo root (see settings.py)
.github/workflows/eod.yml   scheduled run; needs a non-local database
tests/            no network, no database

data_pullv2/      the package -- this name is what imports bind to
cli.py            single entrypoint: eod | backfill | status | plan | cache
settings.py       validated config; every tuneable lives here
app.py            thin wrapper -> backfill
eod_update.py     thin wrapper -> eod (Task Scheduler entry)

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
```

Dependencies point one way: `jobs → {fmp, transform, storage} → core`.

## The load-bearing idea: one field map

`transform/fields.py` declares every column once:

```python
FieldSpec("revenue_growth", Float, SRC_FIN_GROWTH, "revenueGrowth", scale=100.0)
```

`storage/schema.py` generates the DDL from it, `transform/processor.py` generates
the transforms from it.

A column that has no single API field behind it declares `inputs` and a
`compute` function instead of an `api_field`, and stays in the same one map:

```python
FieldSpec("cash_ratio", Float, SRC_BALANCE_SHEET, None,
          inputs=("cashAndCashEquivalents", "totalCurrentLiabilities"),
          compute=cash_ratio)
```

`compute` receives one numeric Series per name in `inputs`, in order.

Previously this mapping lived in three places — the
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
| **backfill** (per symbol) | 8 | 6 |
| **EOD** (per symbol) | 4 | 1 |
| **EOD**, 250 symbols | 1,000 | ~252 |
| **backfill**, 250 symbols | 2,000 | ~1,509 |

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
- `balance-sheet-statement` **added** to the backfill (+1 per symbol), solely to
  derive `liquidity_ratios.cash_ratio`. It returns the same 5 annual periods on
  the same dates as `ratios`/`key-metrics`, so it merges without inflating the
  row count. The EOD job does not touch fundamentals and is unaffected.

## Correctness fixes

**Bind-parameter overflow.** The old `_upsert_data` built one INSERT for every
row. PostgreSQL caps a statement at 65,535 bind parameters; at stocks_daily's
15 columns that is ~4,369 rows. Two tickers of history already exceed it and
250 tickers produce ~4.7M parameters — unsendable. `storage/writer.py` derives
a chunk size from the column count and commits in batches.

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

Set `CORRECTED_FIELDS=1` to populate them. **No DDL change is needed** — those
columns already exist. Backfill the history afterwards to fill older rows.

Three columns have since left that list:

- `cash_ratio` — there is still no `cashRatio` field on this plan, but the
  inputs are on `balance-sheet-statement`, so it is now computed (see below)
  and populates on every backfill, no flag required.
- `price_earnings_ratio` — **dropped**. Named after FMP's v3 field
  `priceEarningsRatio`, which the stable `/ratios` endpoint does not return, so
  it could never be anything but NULL. `price_to_earnings_ratio` holds the value.
- `change_pct` — **dropped**. Dead duplicate of `change_percent`.

The two drops are the only deliberate divergence from the legacy schema. They
were carried while the original tables were live so that `create_all` could not
alter them; those tables have since been dropped and rebuilt from `fields.py`.
`test_no_column_is_declared_without_a_source` now fails the build if a column
is declared with nothing behind it.

## Derived column: `cash_ratio`

```
cashAndCashEquivalents / totalCurrentLiabilities
```

Both from `balance-sheet-statement`. A zero denominator yields NULL rather than
infinity. The formula lives in `cash_ratio()` in `transform/fields.py`.

## Usage

Run as a module from the repository root:

```bash
python -m data_pullv2.cli plan --limit 250   # request estimate, no API calls
python -m data_pullv2.cli eod                # daily incremental
python -m data_pullv2.cli eod --dry-run --force   # preview, writes nothing
python -m data_pullv2.cli backfill --limit 50
python -m data_pullv2.cli backfill --export-dir out/
python -m data_pullv2.cli status             # row counts per table
python -m data_pullv2.cli cache --clear
python tests/test_offline.py
```

`app.py` and `eod_update.py` remain as thin wrappers so the Task Scheduler
entry keeps working — its arguments become `-m data_pullv2.eod_update`, with
`Start in` set to the repository root. Both forward extra arguments, so
`--dry-run` and `--help` behave as expected instead of silently launching a
production run. Both exit non-zero when >50% of symbols fail, so a half-broken
run is distinguishable from a clean one.

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
| `ENV_FILE` | — | explicit path to `.env`, overriding the search |
| `LOG_LEVEL` | INFO | |

## Superseded files

The original implementation — `client.py`, `pipeline.py`, `processor.py`,
`db_manager.py`, `models.py` and `config.py` — has been deleted. It remains in
git history up to the restructure commit if you need to refer back to it.
