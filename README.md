# weekly-abr-dataset

A weekly-refreshed Parquet and SQLite mirror of the [Australian Business Register (ABR) bulk extract](https://data.gov.au/data/dataset/abn-bulk-extract).

Source data is published weekly by the ABR as XML on data.gov.au under CC-BY. This repo runs a scheduled pipeline that downloads the latest extract, parses the ~14M-record dataset, and republishes it as queryable Parquet + a single-file SQLite for direct analytical use.

## Why

The ABR publishes the bulk extract only as gzipped XML. Working with it requires either:

- A custom XML parser per project, **or**
- A static third-party dump that goes stale (`joelkoen/simple-abns` last refreshed Dec 2024).

This project fills the gap: a fresh, weekly Parquet drop that any tool with `pyarrow` / `polars` / `duckdb` can read directly off R2.

## Data

The canonical dataset lives in **Apache Iceberg** on a Cloudflare R2 Data Catalog. Two well-known JSON files at the bucket root expose what's current:

- `https://gazetteer.au/manifest.json` — extract metadata (timestamp, row counts, run id).
- `https://gazetteer.au/iceberg-snapshot.json` — current Iceberg snapshot id + the public URLs of the live data files for each history table. Read this if you want to query the Iceberg-managed history directly with DuckDB or another reader.

A precomputed search index for keystroke-fast filtering is also published:

- `https://gazetteer.au/abn-search-latest.parquet` — one row per ABN with display columns and a denormalised, lowercased `search_text`. Used by the frontend for the search filter; useful for any consumer that just wants name lookups without joining main + trading.

### Legacy static dumps (removed 2026-05-08)

The pre-Iceberg `abn-main-latest.parquet`, `abn-trading-names-latest.parquet`, `abn-dgr-latest.parquet`, and `abr-extract-latest.sqlite` URLs at the bucket root **no longer resolve**. The `manifest.json` + `iceberg-snapshot.json` pointers above (plus the search index) are the supported interface going forward.

If you depended on those URLs, the iceberg + search-index path covers the same use cases — see Usage below — and `joelkoen/simple-abns`-style consumers can read the same columns out of the iceberg data files. Discussion: [#13](https://github.com/Banks003/weekly-abr-dataset/issues/13).

Each refresh writes a date-stamped audit copy of `manifest.json`, `iceberg-snapshot.json`, and `abn-search-latest.parquet` (e.g. `manifest-2026-05-08.json`) at the bucket root.

### Schema

Three tables:

| Table | Cardinality | Key fields |
|---|---|---|
| `abn_main` | 1 row per ABN (~14M) | abn, abn_status, status_from_date, last_updated, entity_type_ind, entity_type_text, entity_name, state, postcode, asic_number, gst_status, gst_status_from_date |
| `abn_trading_names` | 1:N (multiple per ABN) | abn, name, name_type |
| `abn_dgr` | 1:N (most ABNs have 0) | abn, dgr_status_from_date, dgr_text |

Field naming and types follow the convention in `iangow/abn_lookup`'s XSLT transforms (the most rigorous existing schema map).

## Usage

### Quick name search (recommended)

```sql
-- DuckDB
SELECT abn, name, state, postcode, abn_status
FROM read_parquet('https://gazetteer.au/abn-search-latest.parquet')
WHERE search_text ILIKE '%acme%'
LIMIT 10;
```

The search index carries display fields + a pre-lowercased `search_text` column. One ILIKE replaces the multi-relation join the old static dumps required.

### Querying the live Iceberg dataset

```python
import json, urllib.request
import duckdb

snap = json.load(urllib.request.urlopen("https://gazetteer.au/iceberg-snapshot.json"))
main_files = snap["tables"]["abn_main_history"]["data_files"]

con = duckdb.connect()
# valid_to IS NULL filters to the currently-valid SCD2 row per ABN.
df = con.execute(f"""
    SELECT abn, main_name, state, gst_status
    FROM read_parquet({main_files})
    WHERE valid_to IS NULL AND state = 'VIC' AND gst_status = 'ACT'
    LIMIT 10
""").fetchdf()
```

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for the full system map: data flow per refresh, R2 bucket layout, producer/consumer matrix, growth model, and the open work register. The companion issue tracker mirroring open items is [#21](https://github.com/Banks003/weekly-abr-dataset/issues/21).

## Refresh schedule

Pipeline runs weekly on GitHub Actions (Sundays UTC, after the ABR's typical mid-week publish). Idempotent — if the source extract is unchanged from the last run, the pipeline exits early without re-uploading.

The CLI prints stage markers like `[stage] iceberg.abn_main_history (update) starting (rss=2.34 GB)` to stderr around each major step. Failed runs in CI also produce a `refresh-debug-{run_id}` artifact with kernel logs, memory state, and partial work-dir contents.

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
