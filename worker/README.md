# abr-api Worker

Cloudflare Worker exposing the published dataset as JSON HTTP endpoints.

## Status

**Scaffolded.** The URL routes and response shapes are settled; query logic via DuckDB-WASM lands in a follow-up. Today the routes return placeholder responses with a `placeholder: true` field so callers can test the contract.

## Endpoints

| Route | Description |
|---|---|
| `GET /` | API discovery |
| `GET /manifest` | Latest `manifest.json`, cached 5 min |
| `GET /abn/:abn` | Full profile for one ABN (placeholder) |
| `GET /search?q=&in=&limit=` | Search by name (placeholder) |
| `GET /trends/:metric?since=&by=` | Pre-baked aggregations (placeholder) |

## Local development

```bash
cd worker
npm install
npm run dev   # local dev server on http://localhost:8787
```

## Deploy

```bash
npx wrangler deploy
```

Wrangler reads `wrangler.toml` for the R2 binding and the public data URL.

## R2 binding

`DATA_BUCKET` is bound to `weekly-abr-dataset` for direct R2 reads (used once DuckDB-WASM lands). Until then the placeholder routes use the public r2.dev URL via `DATA_BASE_URL`.

## Plan for query layer

- **For `/abn/:abn` (point lookup):** DuckDB-WASM with predicate pushdown. ABN is the natural sort key candidate, expecting <500 ms after warm cold-start.
- **For `/search`:** full main-name scan — 2–5s. Hot queries cached in CF cache.
- **For `/trends/:metric`:** pre-baked aggregations are tiny (KB) and load instantly from cache. Other metrics computed on demand.

When R2 Data Catalog + DuckDB-Wasm Iceberg reads are stable, we switch the source from snapshot Parquets to the Iceberg history tables for `--at-date` time-travel.
