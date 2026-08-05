#!/usr/bin/env node
// ⚠️  LEGACY — One-shot migration script from legacy DuckDB to market-split
//    SQLite. Already executed; do not re-run without verifying the source
//    DuckDB still exists and has not been garbage-collected.
//    See docs/current/canonical-pipeline.md for the canonical architecture.
// One-shot migration of raw_batches from the legacy DuckDB file to
// market-split SQLite WAL files. The source is never modified.

import duckdb from 'duckdb';
import { DatabaseSync } from 'node:sqlite';
import { mkdir, readdir } from 'node:fs/promises';
import path from 'node:path';

function arg(name, fallback) {
  const prefix = `--${name}=`;
  const inline = process.argv.find((value) => value.startsWith(prefix));
  if (inline) return inline.slice(prefix.length);
  const index = process.argv.indexOf(`--${name}`);
  return index >= 0 && process.argv[index + 1] ? process.argv[index + 1] : fallback;
}

const sourcePath = path.resolve(arg('duckdb', 'data/agg-btc-receiver.duckdb'));
const targetDir = path.resolve(arg('sqlite-dir', 'data/sqlite'));

function openDuckDb(databasePath) {
  return new Promise((resolve, reject) => {
    const db = new duckdb.Database(databasePath, (error) => error ? reject(error) : resolve(db));
  });
}

function connectDuckDb(db) {
  return db.connect();
}

function all(connection, sql, ...params) {
  return new Promise((resolve, reject) => {
    connection.all(sql, ...params, (error, rows) => error ? reject(error) : resolve(rows));
  });
}

function configureTarget(db) {
  db.exec(`
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;
    PRAGMA busy_timeout=5000;
    PRAGMA wal_autocheckpoint=1000;
    CREATE TABLE IF NOT EXISTS raw_batches (
      batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
      schema TEXT NOT NULL,
      market TEXT NOT NULL,
      stream TEXT NOT NULL,
      first_event_ts_ms INTEGER,
      last_event_ts_ms INTEGER,
      first_recv_ts_ms INTEGER NOT NULL,
      last_recv_ts_ms INTEGER NOT NULL,
      row_count INTEGER NOT NULL,
      raw_gzip BLOB NOT NULL,
      raw_bytes INTEGER NOT NULL,
      written_at_ms INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS raw_batches_recv_idx
      ON raw_batches(last_recv_ts_ms);
    CREATE INDEX IF NOT EXISTS raw_batches_stream_idx
      ON raw_batches(stream, last_recv_ts_ms);
  `);
}

function safeMarketName(market) {
  return String(market).replace(/[^a-zA-Z0-9_-]/g, '_');
}

function openTarget(targets, market) {
  if (targets.has(market)) return targets.get(market);
  const db = new DatabaseSync(path.join(targetDir, `${safeMarketName(market)}.sqlite`));
  configureTarget(db);
  const entry = {
    db,
    insert: db.prepare(`
      INSERT INTO raw_batches (
        batch_id, schema, market, stream, first_event_ts_ms, last_event_ts_ms,
        first_recv_ts_ms, last_recv_ts_ms, row_count, raw_gzip, raw_bytes,
        written_at_ms
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `),
    pending: 0,
  };
  targets.set(market, entry);
  return entry;
}

function closeTargets(targets) {
  for (const { db } of targets.values()) db.close();
}

async function main() {
  const entries = await readdir(targetDir, { withFileTypes: true }).catch(() => []);
  const existing = entries.filter((entry) => entry.isFile() && entry.name.endsWith('.sqlite'));
  if (existing.length) {
    throw new Error(`target directory is not empty of SQLite files: ${targetDir}`);
  }
  await mkdir(targetDir, { recursive: true });

  const sourceDb = await openDuckDb(sourcePath);
  const source = connectDuckDb(sourceDb);
  const sourceStats = await all(source, `
    SELECT market, count(*) AS batch_count, coalesce(sum(row_count), 0) AS raw_count,
           coalesce(sum(raw_bytes), 0) AS raw_bytes,
           min(first_recv_ts_ms) AS first_recv_ts_ms,
           max(last_recv_ts_ms) AS last_recv_ts_ms
    FROM raw_batches GROUP BY market ORDER BY market
  `);
  const targets = new Map();
  const batchSize = 5_000;
  let migrated = 0;

  try {
    // Process one market at a time. This keeps the native DuckDB result stream
    // bounded and avoids the long-lived callback path in duckdb-node.
    for (const sourceRow of sourceStats) {
      const market = sourceRow.market;
      const target = openTarget(targets, market);
      for await (const row of source.stream(`
        SELECT batch_id, schema, market, stream, first_event_ts_ms, last_event_ts_ms,
               first_recv_ts_ms, last_recv_ts_ms, row_count, raw_gzip, raw_bytes,
               written_at_ms
        FROM raw_batches WHERE market = ? ORDER BY batch_id
      `, market)) {
        if (target.pending === 0) target.db.exec('BEGIN IMMEDIATE');
        const rawGzip = Buffer.from(row.raw_gzip);
        target.insert.run(
          Number(row.batch_id), row.schema, row.market, row.stream,
          row.first_event_ts_ms === null ? null : Number(row.first_event_ts_ms),
          row.last_event_ts_ms === null ? null : Number(row.last_event_ts_ms),
          Number(row.first_recv_ts_ms), Number(row.last_recv_ts_ms),
          Number(row.row_count), rawGzip, Number(row.raw_bytes), Number(row.written_at_ms),
        );
        target.pending += 1;
        if (target.pending >= batchSize) {
          target.db.exec('COMMIT');
          target.pending = 0;
        }
        migrated += 1;
        if (migrated % 100_000 === 0) console.log(`[migrate] batches=${migrated}`);
      }
      if (target.pending > 0) target.db.exec('COMMIT');
      target.pending = 0;
    }
    for (const target of targets.values()) {
      if (target.pending > 0) target.db.exec('COMMIT');
      target.pending = 0;
    }
  } finally {
    closeTargets(targets);
    await new Promise((resolve) => source.close(() => resolve()));
    await new Promise((resolve) => sourceDb.close(() => resolve()));
  }

  const targetStats = [];
  for (const sourceRow of sourceStats) {
    const db = new DatabaseSync(path.join(targetDir, `${safeMarketName(sourceRow.market)}.sqlite`), { readOnly: true });
    const targetRow = db.prepare(`
      SELECT count(*) AS batch_count, coalesce(sum(row_count), 0) AS raw_count,
             coalesce(sum(raw_bytes), 0) AS raw_bytes,
             min(first_recv_ts_ms) AS first_recv_ts_ms,
             max(last_recv_ts_ms) AS last_recv_ts_ms
      FROM raw_batches
    `).get();
    db.close();
    const actual = Object.fromEntries(Object.entries(targetRow).map(([key, value]) => [key, Number(value)]));
    const expected = Object.fromEntries(Object.entries(sourceRow).map(([key, value]) => [key, Number(value)]));
    for (const key of ['batch_count', 'raw_count', 'raw_bytes', 'first_recv_ts_ms', 'last_recv_ts_ms']) {
      if (actual[key] !== expected[key]) {
        throw new Error(`mismatch market=${sourceRow.market} field=${key} expected=${expected[key]} actual=${actual[key]}`);
      }
    }
    targetStats.push({ market: sourceRow.market, ...actual });
  }
  console.log(JSON.stringify({ source: sourcePath, target: targetDir, migrated_batches: migrated, markets: targetStats }, null, 2));
}

main().catch((error) => {
  console.error(`[migrate] failed: ${error.stack || error.message}`);
  process.exitCode = 1;
});
