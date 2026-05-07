/**
 * SQL templates that mirror the Python query.py functions. Same shape of
 * results — when the Worker is healthy, the CLI and Worker should produce
 * identical JSON for the same inputs.
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
  const main = (await query(`SELECT * FROM read_parquet('${u.main}') WHERE abn = ?`, [abn])) as any[];
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

export async function trendsByMetric(
  base: string,
  metric: 'registrations' | 'cancellations' | 'by_state' | 'by_entity_type',
  opts: { since?: string; groupBy?: 'month' | 'year' } = {}
): Promise<unknown[]> {
  const u = urls(base);
  const since = opts.since ?? '2020-01-01';
  const groupBy = opts.groupBy ?? 'month';

  if (metric === 'registrations' || metric === 'cancellations') {
    const fmt = groupBy === 'month' ? '%Y-%m' : '%Y';
    const bucket = groupBy === 'month' ? 'month' : 'year';
    const statusFilter = metric === 'cancellations' ? "abn_status = 'CAN'" : 'TRUE';
    return query(
      `SELECT strftime(abn_status_from_date, '${fmt}') AS ${bucket}, COUNT(*) AS n
       FROM read_parquet('${u.main}')
       WHERE abn_status_from_date >= ? AND ${statusFilter} AND abn_status_from_date IS NOT NULL
       GROUP BY 1 ORDER BY 1`,
      [since]
    );
  }

  if (metric === 'by_state') {
    return query(
      `SELECT state, COUNT(*) AS n
       FROM read_parquet('${u.main}')
       WHERE abn_status = 'ACT' AND state IS NOT NULL
       GROUP BY state ORDER BY n DESC`
    );
  }

  if (metric === 'by_entity_type') {
    return query(
      `SELECT entity_type_ind AS code, entity_type_text AS text, COUNT(*) AS n
       FROM read_parquet('${u.main}')
       WHERE abn_status = 'ACT'
       GROUP BY 1, 2 ORDER BY n DESC`
    );
  }

  throw new Error(`Unknown metric: ${metric}`);
}
