/**
 * DuckDB-WASM bootstrap for Cloudflare Workers.
 *
 * One-time initialization per Worker isolate. Subsequent requests reuse the
 * connected database. The HTTPFS extension lets us range-read the published
 * parquets directly from the custom domain (gazetteer.au) — no R2 binding
 * needed at this layer because the Worker fetch() API + CF edge cache
 * absorbs repeat range reads automatically.
 */

import * as duckdb from '@duckdb/duckdb-wasm';

let dbPromise: Promise<duckdb.AsyncDuckDB> | null = null;
let connPromise: Promise<duckdb.AsyncDuckDBConnection> | null = null;

async function initDb(): Promise<duckdb.AsyncDuckDB> {
  // The 'eh' (exception handling) build is the smallest one that supports
  // HTTPFS — required to read parquets over HTTPS.
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);

  const worker = await duckdb.createWorker(bundle.mainWorker!);
  const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  return db;
}

export async function getConnection(): Promise<duckdb.AsyncDuckDBConnection> {
  if (!dbPromise) dbPromise = initDb();
  if (!connPromise) {
    connPromise = (async () => {
      const db = await dbPromise!;
      const conn = await db.connect();
      // HTTPFS is registered automatically in recent versions; INSTALL is a
      // no-op safety net.
      await conn.query("INSTALL httpfs; LOAD httpfs;");
      return conn;
    })();
  }
  return connPromise;
}

/** Run a parameter-bound SQL query, return rows as plain JS objects. */
export async function query(sql: string, params: unknown[] = []): Promise<unknown[]> {
  const conn = await getConnection();
  const stmt = await conn.prepare(sql);
  const result = await stmt.query(...params);
  await stmt.close();
  return result.toArray().map((r: any) => r.toJSON());
}
