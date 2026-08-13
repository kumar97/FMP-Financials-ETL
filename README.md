# data_pullv2

Pulls equity fundamentals and daily price data from [Financial Modeling Prep](https://financialmodelingprep.com)
into PostgreSQL.

Five tables: `liquidity_ratios`, `earnings_ratios`, `financial_growth`,
`profit_margins`, `stocks_daily` — all upserted on `(symbol, date)`.

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate            # Windows
pip install -r requirements.txt

copy .env.example .env            # then fill in API_KEY and DB credentials
```

There is no install step. The package is imported as `data_pullv2`, and
running from the repository root is what puts it on `sys.path`.

## Usage

Run as a module from the repository root:

```bash
python -m data_pullv2.cli plan --limit 250   # request estimate; no API calls
python -m data_pullv2.cli eod                # daily incremental price update
python -m data_pullv2.cli eod --dry-run --force
python -m data_pullv2.cli backfill --limit 50
python -m data_pullv2.cli backfill --symbols AAPL,MSFT
python -m data_pullv2.cli status             # row counts and date ranges
python -m data_pullv2.cli cache --clear
```

`--dry-run` fetches and transforms but never writes to the database.
`--force` overrides the weekend skip.

Run the offline tests (no network, no database):

```bash
python tests/test_offline.py      # or: python -m pytest -q
```

## Scheduling

Two options: a local scheduler, or GitHub Actions (see [Hosting](#hosting),
which needs a non-local database).

For a local scheduled run, `data_pullv2/eod_update.py` is a single-target
wrapper — `python -m data_pullv2.cli eod` is equivalent. Either exits non-zero
when more than half the symbols fail, so a half-broken run registers as a
failure rather than looking identical to a clean one.

```
Program:   python
Arguments: -m data_pullv2.eod_update
Start in:  <repository root — the directory containing data_pullv2/>
```

`Start in` must be the repository root — the directory containing the
`data_pullv2/` package — because that is what puts the package on `sys.path`.
This is the one thing that will silently break the scheduled task if wrong.

Suggested trigger: daily, 5:30 PM ET (after close plus settlement delay).

Both wrappers forward extra arguments, so `python -m data_pullv2.eod_update
--dry-run` previews without writing.

## Layout

```
.                          repository root
├── requirements.txt
├── .env                   gitignored; see .env.example
├── .github/workflows/     scheduled EOD run (see "Hosting")
├── tests/
└── data_pullv2/           the package
    ├── cli.py             entrypoint
    ├── settings.py        validated config
    ├── core/              rate limiting, caching, logging, models
    ├── fmp/               API client, endpoint registry, universe, reference
    ├── transform/         fields.py = single source of truth for API→DB
    ├── storage/           schema, chunked writer, queries
    └── jobs/              backfill.py, eod.py
```

Dependencies point one way: `jobs → {fmp, transform, storage} → core`.

`transform/fields.py` declares each column once; both the DDL and the
transforms are generated from it, so the two cannot drift.

## Hosting

`.github/workflows/eod.yml` runs the daily update on a GitHub Actions cron.
There is nothing to install or build — the workflow checks out the repo, pips
`requirements.txt`, and runs `python -m data_pullv2.cli eod` from the
repository root.

Trigger it manually any time via Actions → EOD update → Run workflow, which
also accepts `dry_run`, `force` and `limit` inputs. Manual runs ignore
`EOD_ENABLED`, so you can test before enabling the schedule.

The built-in cron is `0 23 * * 1-5` — 6:00 PM EST / 7:00 PM EDT, after the
close in both halves of the year with no DST handling required.

Two things to know about Actions crons: they are best-effort and can lag by
tens of minutes under load, and GitHub disables scheduled workflows in
repositories with no commits for 60 days. Neither is fatal here — the job is
idempotent on `(symbol, date)`, so a late, skipped or repeated run is harmless
and the next run backfills the gap from each symbol's watermark.

### Triggering externally from cron-job.org

If you want punctual, minute-accurate scheduling, drive the workflow from
[cron-job.org](https://cron-job.org) instead. 

**Leave `EOD_ENABLED` unset** in that setup. The built-in schedule then stays
off and cron-job.org is the only trigger, so you don't get two runs a day.

Create a cron-job.org job pointing at the workflow dispatch endpoint:

```
Method : POST
URL    : https://api.github.com/repos/<owner>/<repo>/actions/workflows/eod.yml/dispatches
Body   : {"ref":"main"}
Headers:
  Accept: application/vnd.github+json
  Authorization: Bearer <token>
  X-GitHub-Api-Version: 2022-11-28
  Content-Type: application/json
```

Equivalent curl, useful for testing before you paste it in:

```bash
curl -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer $GH_TOKEN" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  https://api.github.com/repos/<owner>/<repo>/actions/workflows/eod.yml/dispatches \
  -d '{"ref":"main"}'
```

A successful dispatch returns **204 No Content** with an empty body.
cron-job.org treats non-2xx as a failure, so enable its failure notifications.

**Token scope.** Use a *fine-grained* personal access token limited to this
one repository with **Actions: read and write** — that is the minimum needed,
and it only permits triggering and cancelling workflows. Do not use a token
with `Contents: write` (which `repository_dispatch` would require): that
allows pushing code, so a leaked token could rewrite the workflow itself.

⚠️ Fine-grained tokens expire (1 year maximum). When it lapses the dispatch
starts returning 401 and the job silently stops running — which is why the
failure notifications matter. Set a calendar reminder to rotate it.

Optional inputs can be passed in the body, e.g. a smaller preview run:

```json
{"ref": "main", "inputs": {"dry_run": "true", "limit": "25"}}
```

## Design

See [ARCHITECTURE.md](ARCHITECTURE.md) for the layer breakdown, request-cost
analysis, and the list of columns that are deliberately left NULL.

## Configuration

All knobs live in `.env` — see `.env.example` for the annotated list, and
ARCHITECTURE.md for the full table.

`.env` is looked up at the repository root, then inside the package, then the
working directory. Set `ENV_FILE=/path/to/.env` to override.
