# weekly-abr-dataset

A weekly-refreshed Parquet and SQLite mirror of the [Australian Business Register (ABR) bulk extract](https://data.gov.au/data/dataset/abn-bulk-extract).

Source data is published weekly by the ABR as XML on data.gov.au under CC-BY. This repo runs a scheduled pipeline that downloads the latest extract, parses the ~14M-record dataset, and republishes it as queryable Parquet + a single-file SQLite for direct analytical use.

## Why

The ABR publishes the bulk extract only as gzipped XML. Working with it requires either:

- A custom XML parser per project, **or**
- A static third-party dump that goes stale (`joelkoen/simple-abns` last refreshed Dec 2024).

This project fills the gap: a fresh, weekly Parquet drop that any tool with `pyarrow` / `polars` / `duckdb` can read directly off R2.

## Data

Files are published to a public Cloudflare R2 bucket at `https://pub-24e45cc5c0384bf1b367427d9777b0a8.r2.dev/`. Direct URLs:

```
https://pub-24e45cc5c0384bf1b367427d9777b0a8.r2.dev/abn-main-latest.parquet
https://pub-24e45cc5c0384bf1b367427d9777b0a8.r2.dev/abn-trading-names-latest.parquet
https://pub-24e45cc5c0384bf1b367427d9777b0a8.r2.dev/abn-dgr-latest.parquet
https://pub-24e45cc5c0384bf1b367427d9777b0a8.r2.dev/abr-extract-latest.sqlite
```

Each refresh also writes a date-stamped copy: `abn-main-2026-05-07.parquet`, etc.

A `manifest.json` at the bucket root records the source extract date, row counts, and SHA-256 hashes.

### Schema

Three tables:

| Table | Cardinality | Key fields |
|---|---|---|
| `abn_main` | 1 row per ABN (~14M) | abn, abn_status, status_from_date, last_updated, entity_type_ind, entity_type_text, entity_name, state, postcode, asic_number, gst_status, gst_status_from_date |
| `abn_trading_names` | 1:N (multiple per ABN) | abn, name, name_type |
| `abn_dgr` | 1:N (most ABNs have 0) | abn, dgr_status_from_date, dgr_text |

Field naming and types follow the convention in `iangow/abn_lookup`'s XSLT transforms (the most rigorous existing schema map).

## Usage

```python
import polars as pl

df = pl.read_parquet("https://pub-24e45cc5c0384bf1b367427d9777b0a8.r2.dev/abn-main-latest.parquet")
```

```sql
-- DuckDB
SELECT * FROM read_parquet('https://pub-24e45cc5c0384bf1b367427d9777b0a8.r2.dev/abn-main-latest.parquet')
WHERE state = 'VIC' AND gst_status = 'ACT'
LIMIT 10;
```

## Refresh schedule

Pipeline runs weekly on GitHub Actions (Sundays UTC, after the ABR's typical mid-week publish). Idempotent — if the source extract is unchanged from the last run, the pipeline exits early without re-uploading.

## Local development

Requires Python 3.11+ and [uv](https://github.com/astral-sh/uv).

```bash
uv sync
uv run abr-extract run --output-dir ./work
```

## Prior art and credits

This project stands on shoulders. Specific borrowings:

- **[iangow/abn_lookup](https://github.com/iangow/abn_lookup)** (Ian Gow, Melbourne Business School): the three-table normalisation (main / trading-names / DGR) and most of the snake_case column naming convention (`abn_status`, `abn_status_from_date`, `record_last_updated`, etc.) come directly from his XSLT stylesheets, which remain the most rigorous declarative field-map for the ABR XML. We don't run XSLT at runtime, but his `.xsl` files were used as a schema spec.
- **[joelkoen/simple-abns](https://github.com/joelkoen/simple-abns)** (Joel Koen): the philosophy of stream-parse-and-flatten into a small, queryable schema is downstream of his Rust pipeline, and his static dataset is what proved the use-case before we built a refresh-on-cadence version. His EntityType enum was a useful reference for the full ~150-code taxonomy.
- **The ABR's own XSD** (`docs/bulkextract.xsd`, downloaded from data.gov.au) is the authoritative source for the field set and is checked into this repo for reference.

Earlier surveys of the wider ABN tooling ecosystem on GitHub also informed scope decisions — what to deliberately *not* build (no validators, no API wrappers, no scrapers).

## License

This project is MIT.

The ABR bulk extract data is published by the Australian Business Register under [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/). Attribution: "Source: Australian Business Register".
