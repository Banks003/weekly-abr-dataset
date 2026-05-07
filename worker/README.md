# abr-api Worker

Cloudflare Worker exposing the published dataset as JSON HTTP endpoints.

**Status:** URL contract is live. Query engine is **deferred** — DuckDB-WASM was the planned backend but it can't run in Cloudflare Workers (CF Workers don't support nested Web Workers, which DuckDB-WASM requires). Routes that read the parquet currently return `{placeholder: true, ...}`. Real queries are served by the Python CLI (`abr-extract profile/search/trends`, optionally with `--cache-dir` for fast repeat use).

The two query-engine paths under consideration:
- **hyparquet** (pure-JS Parquet reader): no SQL, ~150 LOC of handcrafted JS reduce loops per endpoint. Works in CF Workers.
- **Cloudflare Containers** (released 2025): full Node, can run real `duckdb-node`. Paid only.

## Endpoints (REST v1)

| Route | Description | Cache |
|---|---|---|
| `GET /` | API discovery | — |
| `GET /manifest` | Latest `manifest.json` proxy | 5 min |
| `GET /v1/abns?q=&in=&limit=` | Search by name | 10 min |
| `GET /v1/abns/:abn` | Full profile for one ABN | 1 hr |
| `GET /v1/states` | All states with active-ABN counts | 1 day |
| `GET /v1/states/:code` | State detail (active/cancelled/total) | 1 hr |
| `GET /v1/states/:code/abns?status=&limit=&offset=` | Paginated ABNs in state | 10 min |
| `GET /v1/states/:code/registrations?since=&by=` | State-scoped registration time series | 1 day |
| `GET /v1/states/:code/cancellations?since=&by=` | State-scoped cancellation time series | 1 day |
| `GET /v1/entity-types` | All entity types with active-ABN counts | 1 day |
| `GET /v1/entity-types/:code` | Entity type detail | 1 hr |
| `GET /v1/entity-types/:code/abns?state=&status=&limit=&offset=` | Paginated ABNs by entity type | 10 min |
| `GET /v1/aggregations/registrations?state=&entity_type=&since=&by=` | Filtered registration time series | 1 day |
| `GET /v1/aggregations/cancellations?state=&entity_type=&since=&by=` | Filtered cancellation time series | 1 day |

State codes: NSW VIC QLD SA WA TAS NT ACT AAT. Entity-type codes are the 3-letter ABR codes (PUB, PRV, IND, SMF, …).

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
