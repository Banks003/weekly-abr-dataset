/**
 * ABR API — Cloudflare Worker exposing per-ABN lookups, search, and trends
 * over the published R2 dataset.
 *
 * Backed by DuckDB-WASM running inside the Worker isolate. Parquet files are
 * read over HTTPS from the custom domain via DuckDB's HTTPFS; range requests
 * + CF edge cache absorb the repeat-read cost.
 */

import { Hono } from 'hono';
import { profileAbn, searchAbns, trendsByMetric } from './queries';

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
    source: c.env.DATA_BASE_URL,
  })
);

app.get('/manifest', async (c) => {
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
  try {
    const profile = await profileAbn(c.env.DATA_BASE_URL, abn);
    if (!profile) return c.json({ error: `ABN ${abn} not found` }, 404);
    return c.json(profile, 200, {
      'cache-control': 'public, max-age=3600',
    });
  } catch (err: any) {
    return c.json({ error: 'query failed', detail: String(err?.message ?? err) }, 500);
  }
});

app.get('/search', async (c) => {
  const q = c.req.query('q');
  if (!q) return c.json({ error: 'q is required' }, 400);
  const limit = Math.min(parseInt(c.req.query('limit') ?? '20', 10), 100);
  const searchIn = (c.req.query('in') ?? 'all') as 'all' | 'main' | 'trading' | 'individual';

  try {
    const rows = await searchAbns(c.env.DATA_BASE_URL, q, { searchIn, limit });
    return c.json({ q, in: searchIn, limit, results: rows }, 200, {
      'cache-control': 'public, max-age=600',
    });
  } catch (err: any) {
    return c.json({ error: 'query failed', detail: String(err?.message ?? err) }, 500);
  }
});

app.get('/trends/:metric', async (c) => {
  const metric = c.req.param('metric') as
    | 'registrations'
    | 'cancellations'
    | 'by_state'
    | 'by_entity_type';
  const validMetrics = ['registrations', 'cancellations', 'by_state', 'by_entity_type'];
  if (!validMetrics.includes(metric)) {
    return c.json({ error: `metric must be one of ${validMetrics.join(', ')}` }, 400);
  }
  const since = c.req.query('since') ?? '2020-01-01';
  const groupBy = (c.req.query('by') ?? 'month') as 'month' | 'year';

  try {
    const rows = await trendsByMetric(c.env.DATA_BASE_URL, metric, { since, groupBy });
    return c.json({ metric, since, by: groupBy, results: rows }, 200, {
      'cache-control': 'public, max-age=86400',
    });
  } catch (err: any) {
    return c.json({ error: 'query failed', detail: String(err?.message ?? err) }, 500);
  }
});

export default app;
