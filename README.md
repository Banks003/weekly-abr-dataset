# weekly-abr-dataset

A weekly-refreshed Parquet and SQLite mirror of the [Australian Business Register (ABR) bulk extract](https://data.gov.au/data/dataset/abn-bulk-extract).

Source data is published weekly by the ABR as XML on data.gov.au under CC-BY. This repo runs a scheduled pipeline that downloads the latest extract, parses the ~14M-record dataset, and republishes it as queryable Parquet + a single-file SQLite for direct analytical use.

## Why

The ABR publishes the bulk extract only as gzipped XML. Working with it requires either:

- A custom XML parser per project, **or**
- A static third-party dump that goes stale (`joelkoen/simple-abns` last refreshed Dec 2024).

This project fills the gap: a fresh, weekly Parquet drop that any tool with `pyarrow` / `polars` / `duckdb` can read directly off R2.

## Data

Files are published to a public Cloudflare R2 bucket. Direct URLs:

```
https://<r2-public-domain>/abn-main-latest.parquet
https://<r2-public-domain>/abn-trading-names-latest.parquet
https://<r2-public-domain>/abn-dgr-latest.parquet
https://<r2-public-domain>/abr-extract-latest.sqlite
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

df = pl.read_parquet("https://<r2-public-domain>/abn-main-latest.parquet")
```

```sql
-- DuckDB
SELECT * FROM read_parquet('https://<r2-public-domain>/abn-main-latest.parquet')
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

## License

This project is MIT.

The ABR bulk extract data is published by the Australian Business Register under [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/). Attribution: "Source: Australian Business Register".
