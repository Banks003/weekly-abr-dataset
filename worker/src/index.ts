/**
 * ABR API — Cloudflare Worker exposing per-ABN lookups, search, and trends
 * over the published R2 dataset.
 *
 * Status: scaffolded. Routes are wired to Hono with response shapes that
 * match what the Python CLI produces (see src/abr_extract/query.py).
 * Query logic uses DuckDB-WASM in a follow-up commit; until then the
 * routes return clearly-marked placeholder responses so the URL contract
 * is testable end-to-end.
 */

import { Hono } from 'hono';

type Env = {
  DATA_BASE_URL: string;
  DATA_BUCKET: R2Bucket;
};

const app = new Hono<{ Bindings: Env }>();

app.get('/', (c) =>
  c.json({
    name: 'abr-api',
    version: '0.1.0',
    description: 'Australian Business Register query API on top of weekly-abr-dataset',
    endpoints: {
      'GET /abn/:abn': 'Full profile for one ABN',
      'GET /search?q=&in=&limit=': 'Search by name (main, trading, individual, or all)',
      'GET /trends/:metric?since=&by=': 'Pre-baked aggregations',
      'GET /manifest': 'Latest dataset manifest passthrough',
    },
    source: '{DATA_BASE_URL}',
  })
);

app.get('/manifest', async (c) => {
  // Proxy the published manifest.json from the R2 bucket via its public URL.
  const url = `${c.env.DATA_BASE_URL}/manifest.json`;
  const upstream = await fetch(url, { cf: { cacheTtl: 300, cacheEverything: true } });
  return new Response(upstream.body, {
    status: upstream.status,
    headers: { 'content-type': 'application/json', 'cache-control': 'max-age=300' },
  });
});

app.get('/abn/:abn', async (c) => {
  const abn = c.req.param('abn');
  if (!/^\d{11}$/.test(abn)) {
    return c.json({ error: 'abn must be 11 digits' }, 400);
  }
  return c.json({
    placeholder: true,
    abn,
    note: 'DuckDB-WASM query layer not yet wired. Use the Python CLI: abr-extract profile <abn>.',
  });
});

app.get('/search', async (c) => {
  const q = c.req.query('q');
  if (!q) return c.json({ error: 'q is required' }, 400);
  const limit = Math.min(parseInt(c.req.query('limit') ?? '20', 10), 100);
  const searchIn = c.req.query('in') ?? 'all';
  return c.json({
    placeholder: true,
    query: q,
    in: searchIn,
    limit,
    note: 'DuckDB-WASM query layer not yet wired. Use the Python CLI: abr-extract search <q>.',
  });
});

app.get('/trends/:metric', async (c) => {
  const metric = c.req.param('metric');
  const validMetrics = ['registrations', 'cancellations', 'by_state', 'by_entity_type'];
  if (!validMetrics.includes(metric)) {
    return c.json({ error: `metric must be one of ${validMetrics.join(', ')}` }, 400);
  }
  return c.json({
    placeholder: true,
    metric,
    since: c.req.query('since') ?? '2020-01-01',
    by: c.req.query('by') ?? 'month',
    note: 'DuckDB-WASM query layer not yet wired. Use the Python CLI: abr-extract trends <metric>.',
  });
});

export default app;
