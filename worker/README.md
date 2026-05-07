# abr-api Worker

Cloudflare Worker exposing the published dataset as JSON HTTP endpoints. DuckDB-WASM running inside the Worker reads parquets over HTTPS at `gazetteer.au`; the CF edge cache absorbs repeat range reads.

## Endpoints

| Route | Description | Cache |
|---|---|---|
| `GET /` | API discovery | — |
| `GET /manifest` | Latest `manifest.json` proxy | 5 min |
| `GET /abn/:abn` | Full profile for one ABN | 1 hr |
| `GET /search?q=&in=&limit=` | Search by name | 10 min |
| `GET /trends/:metric?since=&by=` | Pre-baked aggregations | 1 day |

Response shapes match the Python CLI (`abr-extract profile/search/trends`) — same SQL, same JSON.

## Local development

```bash
cd worker
npm install
npm run dev   # local dev server on http://localhost:8787
```

## Deploy

```bash
cd worker
npx wrangler login    # one-time
npx wrangler deploy
```

Wrangler reads `wrangler.toml` for the R2 binding and the public data URL. The Worker code reads parquets via HTTPS at `DATA_BASE_URL`, not via the R2 binding (the binding is configured for the future Iceberg integration).

## Cold start

DuckDB-WASM initializes once per Worker isolate (~1–2s for the first request). Subsequent requests on the same isolate reuse the connection. CF edge cache means hot keys (e.g. popular ABNs) respond from cache without hitting the Worker at all.

## Bundle size note

`@duckdb/duckdb-wasm` ships a few MB of WASM. If the bundle exceeds the Workers free-tier 1 MB compressed limit, the Workers paid plan ($5/mo) raises that to 10 MB. Bundle size is checked at `wrangler deploy` time.

## What's not here yet

- Iceberg time-travel queries (`?at=2025-06-30`) — wired once `R2_CATALOG_TOKEN` lands and the Iceberg pipeline run is verified.
- Authenticated/paid tiers — currently anonymous, public, rate-limited only by Workers' built-in protections.
