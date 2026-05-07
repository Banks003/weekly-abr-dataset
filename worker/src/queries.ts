/**
 * SQL query helpers backing the v1 REST endpoints. All read parquet over
 * HTTPS at DATA_BASE_URL via DuckDB-WASM's HTTPFS.
 */

import { query } from './duckdb';

const FILES = {
  main: 'abn-main-latest.parquet',
  trading: 'abn-trading-names-latest.parquet',
  dgr: 'abn-dgr-latest.parquet',
};

function urls(base: string) {
  return {
    main: `${base}/${FILES.main}`,
    trading: `${base}/${FILES.trading}`,
    dgr: `${base}/${FILES.dgr}`,
  };
}

const VALID_STATE_CODES = new Set([
  'NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT', 'AAT',
]);

export function isValidState(code: string): boolean {
  return VALID_STATE_CODES.has(code.toUpperCase());
}

// --- /v1/abns ----------------------------------------------------------

export async function searchAbns(
  base: string,
  q: string,
  opts: { searchIn?: 'all' | 'main' | 'trading' | 'individual'; limit?: number } = {}
): Promise<unknown[]> {
  const u = urls(base);
  const pattern = `%${q}%`;
  const limit = Math.min(opts.limit ?? 20, 100);
  const searchIn = opts.searchIn ?? 'all';

  const parts: string[] = [];
  const params: unknown[] = [];

  if (searchIn === 'all' || searchIn === 'main') {
    parts.push(`
      SELECT abn, main_name AS matched_name, 'main' AS matched_in,
             entity_type_text, state, postcode, abn_status, gst_status
      FROM read_parquet('${u.main}')
      WHERE main_name ILIKE ?
    `);
    params.push(pattern);
  }
  if (searchIn === 'all' || searchIn === 'individual') {
    parts.push(`
      SELECT abn,
             concat_ws(' ', individual_title, individual_given_names, individual_family_name) AS matched_name,
             'individual' AS matched_in,
             entity_type_text, state, postcode, abn_status, gst_status
      FROM read_parquet('${u.main}')
      WHERE individual_family_name ILIKE ? OR individual_given_names ILIKE ?
    `);
    params.push(pattern, pattern);
  }
  if (searchIn === 'all' || searchIn === 'trading') {
    parts.push(`
      SELECT m.abn, t.name AS matched_name, 'trading' AS matched_in,
             m.entity_type_text, m.state, m.postcode, m.abn_status, m.gst_status
      FROM read_parquet('${u.trading}') t
      JOIN read_parquet('${u.main}') m USING (abn)
      WHERE t.name ILIKE ?
    `);
    params.push(pattern);
  }
  params.push(limit);

  const sql = `SELECT * FROM (${parts.join(' UNION ALL ')}) ORDER BY matched_name LIMIT ?`;
  return query(sql, params);
}

export async function profileAbn(base: string, abn: string): Promise<Record<string, any> | null> {
  const u = urls(base);
  const main = (await query(
    `SELECT * FROM read_parquet('${u.main}') WHERE abn = ?`,
    [abn]
  )) as any[];
  if (!main.length) return null;
  const m = main[0];

  const trading = (await query(
    `SELECT name, name_type FROM read_parquet('${u.trading}') WHERE abn = ? ORDER BY name_type, name`,
    [abn]
  )) as any[];

  const dgr = (await query(
    `SELECT dgr_status_from_date AS "from", dgr_status AS status, dgr_name AS name
     FROM read_parquet('${u.dgr}') WHERE abn = ?`,
    [abn]
  )) as any[];

  const individual =
    m.entity_kind === 'legal' && m.individual_family_name
      ? {
          title: m.individual_title,
          given_names: m.individual_given_names,
          family_name: m.individual_family_name,
          name_type: m.individual_name_type,
        }
      : null;

  return {
    abn: m.abn,
    main_name: m.main_name,
    individual,
    entity_type: { code: m.entity_type_ind, text: m.entity_type_text },
    address: { state: m.state, postcode: m.postcode },
    abn_status: { status: m.abn_status, from: m.abn_status_from_date },
    gst: m.gst_status ? { status: m.gst_status, from: m.gst_status_from_date } : null,
    asic_number: m.asic_number,
    asic_number_type: m.asic_number_type,
    record_last_updated: m.record_last_updated,
    replaced: m.replaced,
    trading_names: trading.map((t) => ({ name: t.name, type: t.name_type })),
    dgrs: dgr.map((d) => ({ name: d.name, status: d.status, from: d.from })),
  };
}

// --- /v1/states --------------------------------------------------------

export async function listStates(base: string): Promise<unknown[]> {
  const u = urls(base);
  return query(`
    SELECT state AS code, COUNT(*) AS active_abns
    FROM read_parquet('${u.main}')
    WHERE abn_status = 'ACT' AND state IS NOT NULL
    GROUP BY state
    ORDER BY active_abns DESC
  `);
}

export async function getState(base: string, code: string): Promise<Record<string, any> | null> {
  const u = urls(base);
  const c = code.toUpperCase();
  const rows = (await query(
    `
    SELECT
      ? AS code,
      COUNT(*) FILTER (WHERE abn_status = 'ACT') AS active,
      COUNT(*) FILTER (WHERE abn_status = 'CAN') AS cancelled,
      COUNT(*) AS total
    FROM read_parquet('${u.main}')
    WHERE state = ?
    `,
    [c, c]
  )) as any[];
  if (!rows.length || rows[0].total === 0) return null;
  return { code: c, counts: { active: rows[0].active, cancelled: rows[0].cancelled, total: rows[0].total } };
}

export async function listAbnsInState(
  base: string,
  code: string,
  opts: { status?: 'ACT' | 'CAN'; limit?: number; offset?: number } = {}
): Promise<{ total: number; abns: unknown[] }> {
  const u = urls(base);
  const c = code.toUpperCase();
  const limit = Math.min(opts.limit ?? 50, 500);
  const offset = Math.max(opts.offset ?? 0, 0);
  const statusFilter = opts.status
    ? `AND abn_status = '${opts.status === 'ACT' ? 'ACT' : 'CAN'}'`
    : '';

  const totalRows = (await query(
    `SELECT COUNT(*) AS n FROM read_parquet('${u.main}') WHERE state = ? ${statusFilter}`,
    [c]
  )) as any[];

  const abns = await query(
    `SELECT abn, main_name, entity_type_ind, entity_type_text, postcode, abn_status, gst_status
     FROM read_parquet('${u.main}')
     WHERE state = ? ${statusFilter}
     ORDER BY abn
     LIMIT ? OFFSET ?`,
    [c, limit, offset]
  );
  return { total: Number(totalRows[0]?.n ?? 0), abns };
}

// --- /v1/entity-types --------------------------------------------------

export async function listEntityTypes(base: string): Promise<unknown[]> {
  const u = urls(base);
  return query(`
    SELECT entity_type_ind AS code, entity_type_text AS text, COUNT(*) AS active_abns
    FROM read_parquet('${u.main}')
    WHERE abn_status = 'ACT'
    GROUP BY 1, 2
    ORDER BY active_abns DESC
  `);
}

export async function getEntityType(
  base: string,
  code: string
): Promise<Record<string, any> | null> {
  const u = urls(base);
  const c = code.toUpperCase();
  const rows = (await query(
    `
    SELECT
      entity_type_ind AS code,
      MAX(entity_type_text) AS text,
      COUNT(*) FILTER (WHERE abn_status = 'ACT') AS active,
      COUNT(*) FILTER (WHERE abn_status = 'CAN') AS cancelled,
      COUNT(*) AS total
    FROM read_parquet('${u.main}')
    WHERE entity_type_ind = ?
    GROUP BY 1
    `,
    [c]
  )) as any[];
  if (!rows.length) return null;
  return {
    code: c,
    text: rows[0].text,
    counts: { active: rows[0].active, cancelled: rows[0].cancelled, total: rows[0].total },
  };
}

export async function listAbnsForEntityType(
  base: string,
  code: string,
  opts: { state?: string; status?: 'ACT' | 'CAN'; limit?: number; offset?: number } = {}
): Promise<{ total: number; abns: unknown[] }> {
  const u = urls(base);
  const c = code.toUpperCase();
  const limit = Math.min(opts.limit ?? 50, 500);
  const offset = Math.max(opts.offset ?? 0, 0);

  const filters: string[] = ['entity_type_ind = ?'];
  const filterParams: unknown[] = [c];
  if (opts.state) {
    filters.push('state = ?');
    filterParams.push(opts.state.toUpperCase());
  }
  if (opts.status) {
    filters.push('abn_status = ?');
    filterParams.push(opts.status);
  }
  const where = filters.join(' AND ');

  const totalRows = (await query(
    `SELECT COUNT(*) AS n FROM read_parquet('${u.main}') WHERE ${where}`,
    filterParams
  )) as any[];

  const abns = await query(
    `SELECT abn, main_name, state, postcode, abn_status, gst_status
     FROM read_parquet('${u.main}')
     WHERE ${where}
     ORDER BY abn
     LIMIT ? OFFSET ?`,
    [...filterParams, limit, offset]
  );
  return { total: Number(totalRows[0]?.n ?? 0), abns };
}

// --- /v1/aggregations -------------------------------------------------

type AggOpts = {
  state?: string;
  entityType?: string;
  since?: string;
  groupBy?: 'month' | 'year';
};

function buildAggregation(metric: 'registrations' | 'cancellations', opts: AggOpts) {
  const since = opts.since ?? '2020-01-01';
  const groupBy = opts.groupBy ?? 'month';
  const fmt = groupBy === 'month' ? '%Y-%m' : '%Y';
  const bucket = groupBy === 'month' ? 'month' : 'year';
  const filters: string[] = [
    'abn_status_from_date >= ?',
    'abn_status_from_date IS NOT NULL',
  ];
  const params: unknown[] = [since];
  if (metric === 'cancellations') filters.push("abn_status = 'CAN'");
  if (opts.state) {
    filters.push('state = ?');
    params.push(opts.state.toUpperCase());
  }
  if (opts.entityType) {
    filters.push('entity_type_ind = ?');
    params.push(opts.entityType.toUpperCase());
  }
  return { fmt, bucket, where: filters.join(' AND '), params };
}

export async function aggregateRegistrations(base: string, opts: AggOpts = {}): Promise<unknown[]> {
  const u = urls(base);
  const a = buildAggregation('registrations', opts);
  return query(
    `SELECT strftime(abn_status_from_date, '${a.fmt}') AS ${a.bucket}, COUNT(*) AS n
     FROM read_parquet('${u.main}')
     WHERE ${a.where}
     GROUP BY 1 ORDER BY 1`,
    a.params
  );
}

export async function aggregateCancellations(base: string, opts: AggOpts = {}): Promise<unknown[]> {
  const u = urls(base);
  const a = buildAggregation('cancellations', opts);
  return query(
    `SELECT strftime(abn_status_from_date, '${a.fmt}') AS ${a.bucket}, COUNT(*) AS n
     FROM read_parquet('${u.main}')
     WHERE ${a.where}
     GROUP BY 1 ORDER BY 1`,
    a.params
  );
}
