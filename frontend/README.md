# Frontend — searchable ABN table on gazetteer.au

Single-file static HTML at `index.html`. DuckDB-WASM in the browser queries the published Parquets directly via HTTPS.

## Deploy

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\deploy_frontend.ps1
```

Lands at `https://gazetteer.au/abns/index.html` (and `https://gazetteer.au/abns/` if R2's directory-index serving is enabled).

## Local preview

Open `frontend/index.html` directly in a browser. The page fetches data from `gazetteer.au` over HTTPS, so it works as a `file://` URL too — no local server needed.

## What it does

- Search across `main_name`, `individual_*`, and `trading_names` (substring match).
- Filters: state, ABN status, entity type.
- Configurable page size; pagination via `LIMIT`/`OFFSET`.
- Click a row → expands to show the full profile (ABN, ASIC/ACN, all trading names, all DGR endorsements).
- Exact 11-digit ABN entered into the search box → direct lookup (sub-second).

## Data source

For v1 the page reads `gazetteer.au/abn-main-latest.parquet` etc. directly. These were generated alongside Iceberg from the same source data; equivalent rows.

For v2 (when wired): the pipeline will publish an `iceberg-current.json` manifest listing the data-file URLs from the current Iceberg snapshot, and the frontend will read that — actual time-travel via Iceberg becomes possible from there.

## Performance notes

- DuckDB-WASM cold-start: ~1–2s first query
- ABN exact lookup: <1s after cold start (Parquet row-group skip)
- Substring name search: 5–15s (full main_name scan over 770 MB)
- Aggregation (entity-type populate on load): scans the full main parquet once on first load — cached after that

The CF edge cache absorbs repeat range reads, so subsequent users on the same edge see faster responses.
