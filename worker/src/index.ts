/**
 * ABR API — RESTful v1 endpoints over the published dataset.
 *
 * Status: URL contract is final, query engine is deferred. Routes that
 * need to read the parquet return `{placeholder: true, ...}` until we
 * pick a CF-Workers-compatible query engine (DuckDB-WASM doesn't work
 * because CF Workers don't support nested Web Workers — see worker/README).
 *
 * What works today:
 *   - GET /              — discovery
 *   - GET /manifest      — proxies the R2 manifest.json (5 min cache)
 *
 * What's stubbed (URL contract live, response is placeholder):
 *   - GET /v1/abns?q=&in=&limit=
 *   - GET /v1/abns/:abn
 *   - GET /v1/states
 *   - GET /v1/states/:code
 *   - GET /v1/states/:code/abns?status=&limit=&offset=
 *   - GET /v1/states/:code/{registrations,cancellations}?since=&by=
 *   - GET /v1/entity-types
 *   - GET /v1/entity-types/:code
 *   - GET /v1/entity-types/:code/abns?state=&status=&limit=&offset=
 *   - GET /v1/aggregations/{registrations,cancellations}?...
 */

import { Hono } from 'hono';

type Env = {
  DATA_BASE_URL: string;
  DATA_BUCKET: R2Bucket;
};

const app = new Hono<{ Bindings: Env }>();

const VALID_STATE_CODES = new Set([
  'NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT', 'AAT',
]);

function notImplemented(extra: Record<string, unknown> = {}) {
  return {
    placeholder: true,
    note: 'Query engine not yet wired in this worker. Use the Python CLI: '
      + 'abr-extract profile / search / trends. URL contract is stable.',
    ...extra,
  };
}

// --- root + dataset metadata ------------------------------------------

app.get('/', (c) =>
  c.json({
    name: 'abr-api',
    version: '0.1.0',
    description: 'Australian Business Register query API on top of weekly-abr-dataset',
    base: c.env.DATA_BASE_URL,
    status: 'URL contract live; query engine deferred (see worker/README.md)',
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

app.get('/v1/abns', (c) => {
  const q = c.req.query('q');
  if (!q) return c.json({ error: 'q is required' }, 400);
  return c.json(notImplemented({ q, in: c.req.query('in') ?? 'all', limit: c.req.query('limit') ?? '20' }));
});

app.get('/v1/abns/:abn', (c) => {
  const abn = c.req.param('abn');
  if (!/^\d{11}$/.test(abn)) return c.json({ error: 'abn must be 11 digits' }, 400);
  return c.json(notImplemented({ abn }));
});

// --- /v1/states -------------------------------------------------------

app.get('/v1/states', (c) =>
  c.json({
    states: [...VALID_STATE_CODES].map((code) => ({ code, active_abns: null })),
    note: 'state codes are stable; counts will populate when the query engine lands',
  })
);

app.get('/v1/states/:code', (c) => {
  const code = c.req.param('code').toUpperCase();
  if (!VALID_STATE_CODES.has(code)) return c.json({ error: `unknown state code: ${code}` }, 400);
  return c.json(notImplemented({ code }));
});

app.get('/v1/states/:code/abns', (c) => {
  const code = c.req.param('code').toUpperCase();
  if (!VALID_STATE_CODES.has(code)) return c.json({ error: `unknown state code: ${code}` }, 400);
  return c.json(notImplemented({ code, status: c.req.query('status'), limit: c.req.query('limit') ?? '50', offset: c.req.query('offset') ?? '0' }));
});

app.get('/v1/states/:code/registrations', (c) => {
  const code = c.req.param('code').toUpperCase();
  if (!VALID_STATE_CODES.has(code)) return c.json({ error: `unknown state code: ${code}` }, 400);
  return c.json(notImplemented({ code, since: c.req.query('since') ?? '2020-01-01', by: c.req.query('by') ?? 'month' }));
});

app.get('/v1/states/:code/cancellations', (c) => {
  const code = c.req.param('code').toUpperCase();
  if (!VALID_STATE_CODES.has(code)) return c.json({ error: `unknown state code: ${code}` }, 400);
  return c.json(notImplemented({ code, since: c.req.query('since') ?? '2020-01-01', by: c.req.query('by') ?? 'month' }));
});

// --- /v1/entity-types -------------------------------------------------

app.get('/v1/entity-types', (c) =>
  c.json(notImplemented({}))
);

app.get('/v1/entity-types/:code', (c) => {
  const code = c.req.param('code').toUpperCase();
  return c.json(notImplemented({ code }));
});

app.get('/v1/entity-types/:code/abns', (c) => {
  const code = c.req.param('code').toUpperCase();
  return c.json(notImplemented({
    code,
    state: c.req.query('state'),
    status: c.req.query('status'),
    limit: c.req.query('limit') ?? '50',
    offset: c.req.query('offset') ?? '0',
  }));
});

// --- /v1/aggregations -------------------------------------------------

app.get('/v1/aggregations/registrations', (c) =>
  c.json(notImplemented({
    state: c.req.query('state'),
    entity_type: c.req.query('entity_type'),
    since: c.req.query('since') ?? '2020-01-01',
    by: c.req.query('by') ?? 'month',
  }))
);

app.get('/v1/aggregations/cancellations', (c) =>
  c.json(notImplemented({
    state: c.req.query('state'),
    entity_type: c.req.query('entity_type'),
    since: c.req.query('since') ?? '2020-01-01',
    by: c.req.query('by') ?? 'month',
  }))
);

export default app;
