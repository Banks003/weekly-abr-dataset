/**
 * ABR API — RESTful v1 endpoints over the published dataset.
 *
 * Resources:
 *   /v1/abns            — search and per-ABN profile
 *   /v1/states          — Australian states/territories
 *   /v1/entity-types    — ABR entity-type taxonomy
 *   /v1/aggregations    — cross-cutting time-series with filters
 *   /manifest           — dataset metadata (unversioned)
 *
 * Backed by DuckDB-WASM. Parquet files read over HTTPS at DATA_BASE_URL.
 */

import { Hono } from 'hono';
import {
  aggregateCancellations,
  aggregateRegistrations,
  getEntityType,
  getState,
  isValidState,
  listAbnsForEntityType,
  listAbnsInState,
  listEntityTypes,
  listStates,
  profileAbn,
  searchAbns,
} from './queries';

type Env = {
  DATA_BASE_URL: string;
  DATA_BUCKET: R2Bucket;
};

const app = new Hono<{ Bindings: Env }>();

// --- root + dataset metadata ------------------------------------------

app.get('/', (c) =>
  c.json({
    name: 'abr-api',
    version: '0.1.0',
    description: 'Australian Business Register query API on top of weekly-abr-dataset',
    base: c.env.DATA_BASE_URL,
    endpoints: {
      manifest: 'GET /manifest',
      abns: {
        search: 'GET /v1/abns?q=&in=all|main|trading|individual&limit=',
        profile: 'GET /v1/abns/:abn',
      },
      states: {
        list: 'GET /v1/states',
        detail: 'GET /v1/states/:code',
        listAbns: 'GET /v1/states/:code/abns?status=ACT|CAN&limit=&offset=',
        registrations: 'GET /v1/states/:code/registrations?since=&by=month|year',
        cancellations: 'GET /v1/states/:code/cancellations?since=&by=month|year',
      },
      entityTypes: {
        list: 'GET /v1/entity-types',
        detail: 'GET /v1/entity-types/:code',
        listAbns:
          'GET /v1/entity-types/:code/abns?state=&status=ACT|CAN&limit=&offset=',
      },
      aggregations: {
        registrations:
          'GET /v1/aggregations/registrations?state=&entity_type=&since=&by=',
        cancellations:
          'GET /v1/aggregations/cancellations?state=&entity_type=&since=&by=',
      },
    },
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

// --- /v1/abns ---------------------------------------------------------

app.get('/v1/abns', async (c) => {
  const q = c.req.query('q');
  if (!q) return c.json({ error: 'q is required' }, 400);
  const limit = Math.min(parseInt(c.req.query('limit') ?? '20', 10), 100);
  const searchIn = (c.req.query('in') ?? 'all') as 'all' | 'main' | 'trading' | 'individual';
  return runQuery(c, () => searchAbns(c.env.DATA_BASE_URL, q, { searchIn, limit }), {
    cacheControl: 'public, max-age=600',
  });
});

app.get('/v1/abns/:abn', async (c) => {
  const abn = c.req.param('abn');
  if (!/^\d{11}$/.test(abn)) return c.json({ error: 'abn must be 11 digits' }, 400);
  try {
    const profile = await profileAbn(c.env.DATA_BASE_URL, abn);
    if (!profile) return c.json({ error: `ABN ${abn} not found` }, 404);
    return c.json(profile, 200, { 'cache-control': 'public, max-age=3600' });
  } catch (err: any) {
    return c.json({ error: 'query failed', detail: String(err?.message ?? err) }, 500);
  }
});

// --- /v1/states -------------------------------------------------------

app.get('/v1/states', async (c) =>
  runQuery(c, () => listStates(c.env.DATA_BASE_URL), { cacheControl: 'public, max-age=86400' })
);

app.get('/v1/states/:code', async (c) => {
  const code = c.req.param('code');
  if (!isValidState(code)) return c.json({ error: `unknown state code: ${code}` }, 400);
  try {
    const detail = await getState(c.env.DATA_BASE_URL, code);
    if (!detail) return c.json({ error: `state ${code} has no records` }, 404);
    return c.json(detail, 200, { 'cache-control': 'public, max-age=3600' });
  } catch (err: any) {
    return c.json({ error: 'query failed', detail: String(err?.message ?? err) }, 500);
  }
});

app.get('/v1/states/:code/abns', async (c) => {
  const code = c.req.param('code');
  if (!isValidState(code)) return c.json({ error: `unknown state code: ${code}` }, 400);
  const status = c.req.query('status') as 'ACT' | 'CAN' | undefined;
  const limit = Math.min(parseInt(c.req.query('limit') ?? '50', 10), 500);
  const offset = Math.max(parseInt(c.req.query('offset') ?? '0', 10), 0);
  return runQuery(
    c,
    () => listAbnsInState(c.env.DATA_BASE_URL, code, { status, limit, offset }),
    { cacheControl: 'public, max-age=600' }
  );
});

app.get('/v1/states/:code/registrations', async (c) => {
  const code = c.req.param('code');
  if (!isValidState(code)) return c.json({ error: `unknown state code: ${code}` }, 400);
  const since = c.req.query('since') ?? '2020-01-01';
  const by = (c.req.query('by') ?? 'month') as 'month' | 'year';
  return runQuery(
    c,
    () => aggregateRegistrations(c.env.DATA_BASE_URL, { state: code, since, groupBy: by }),
    { cacheControl: 'public, max-age=86400' }
  );
});

app.get('/v1/states/:code/cancellations', async (c) => {
  const code = c.req.param('code');
  if (!isValidState(code)) return c.json({ error: `unknown state code: ${code}` }, 400);
  const since = c.req.query('since') ?? '2020-01-01';
  const by = (c.req.query('by') ?? 'month') as 'month' | 'year';
  return runQuery(
    c,
    () => aggregateCancellations(c.env.DATA_BASE_URL, { state: code, since, groupBy: by }),
    { cacheControl: 'public, max-age=86400' }
  );
});

// --- /v1/entity-types -------------------------------------------------

app.get('/v1/entity-types', async (c) =>
  runQuery(c, () => listEntityTypes(c.env.DATA_BASE_URL), {
    cacheControl: 'public, max-age=86400',
  })
);

app.get('/v1/entity-types/:code', async (c) => {
  const code = c.req.param('code');
  try {
    const detail = await getEntityType(c.env.DATA_BASE_URL, code);
    if (!detail) return c.json({ error: `entity type ${code} not found` }, 404);
    return c.json(detail, 200, { 'cache-control': 'public, max-age=3600' });
  } catch (err: any) {
    return c.json({ error: 'query failed', detail: String(err?.message ?? err) }, 500);
  }
});

app.get('/v1/entity-types/:code/abns', async (c) => {
  const code = c.req.param('code');
  const state = c.req.query('state') ?? undefined;
  const status = c.req.query('status') as 'ACT' | 'CAN' | undefined;
  const limit = Math.min(parseInt(c.req.query('limit') ?? '50', 10), 500);
  const offset = Math.max(parseInt(c.req.query('offset') ?? '0', 10), 0);
  return runQuery(
    c,
    () =>
      listAbnsForEntityType(c.env.DATA_BASE_URL, code, { state, status, limit, offset }),
    { cacheControl: 'public, max-age=600' }
  );
});

// --- /v1/aggregations -------------------------------------------------

app.get('/v1/aggregations/registrations', async (c) => {
  const opts = {
    state: c.req.query('state') ?? undefined,
    entityType: c.req.query('entity_type') ?? undefined,
    since: c.req.query('since') ?? '2020-01-01',
    groupBy: (c.req.query('by') ?? 'month') as 'month' | 'year',
  };
  return runQuery(c, () => aggregateRegistrations(c.env.DATA_BASE_URL, opts), {
    cacheControl: 'public, max-age=86400',
  });
});

app.get('/v1/aggregations/cancellations', async (c) => {
  const opts = {
    state: c.req.query('state') ?? undefined,
    entityType: c.req.query('entity_type') ?? undefined,
    since: c.req.query('since') ?? '2020-01-01',
    groupBy: (c.req.query('by') ?? 'month') as 'month' | 'year',
  };
  return runQuery(c, () => aggregateCancellations(c.env.DATA_BASE_URL, opts), {
    cacheControl: 'public, max-age=86400',
  });
});

// --- shared error wrapping --------------------------------------------

async function runQuery<T>(
  c: any,
  fn: () => Promise<T>,
  opts: { cacheControl?: string } = {}
) {
  try {
    const result = await fn();
    return c.json(result, 200, {
      'cache-control': opts.cacheControl ?? 'public, max-age=600',
    });
  } catch (err: any) {
    return c.json({ error: 'query failed', detail: String(err?.message ?? err) }, 500);
  }
}

export default app;
