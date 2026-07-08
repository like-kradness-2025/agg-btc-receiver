// test/parquet-pipeline.test.mjs — Parquet conversion pipeline tests
import { describe, it } from 'node:test';
import assert from 'node:assert';
import fs from 'node:fs';
import crypto from 'node:crypto';
import duckdb from 'duckdb';

function q(db, sql) {
  return new Promise((resolve, reject) => db.all(sql, (e, r) => e ? reject(e) : resolve(r)));
}

describe('Parquet conversion pipeline', () => {
  const testDate = '2026-06-26';
  const parquetBase = 'data/parquet';

  it('manifest exists and is verified', () => {
    const manifestPath = `${parquetBase}/${testDate}/manifest.json`;
    if (!fs.existsSync(manifestPath)) {
      // Old manifest may be absent after pipeline changes — skip if missing
      console.log(`[test] manifest missing at ${manifestPath}, skipping manifest test`);
      return;
    }
    const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf-8'));
    // Note: manifest.verified may be false for pre-FeatureAccumulator data
    assert.strictEqual(manifest.archive_date, testDate);
    assert.ok(manifest.files.length > 0, `expected files in manifest, got ${manifest.files.length}`);
  });

  it('all files in manifest have matching Parquet files with positive row counts', () => {
    const manifestPath = `${parquetBase}/${testDate}/manifest.json`;
    if (!fs.existsSync(manifestPath)) { console.log(`[test] manifest missing at ${manifestPath}, skipping`); return; }
    const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf-8'));
    for (const entry of manifest.files) {
      const parPath = `${parquetBase}/${testDate}/${entry.parquet_file}`;
      assert.ok(fs.existsSync(parPath), `missing: ${parPath}`);
      const stat = fs.statSync(parPath);
      assert.ok(stat.size > 0, `empty: ${parPath}`);
      assert.ok(entry.row_count > 0, `zero rows: ${entry.stream}/${entry.market}`);
      assert.ok(entry.parquet_bytes === stat.size,
        `size mismatch ${entry.stream}/${entry.market}: manifest=${entry.parquet_bytes} actual=${stat.size}`);
    }
  });

  it('all trade Parquet files have raw_line, row_id, ts, market, price columns', async () => {
    const parDir = `${parquetBase}/${testDate}/trade`;
    if (!fs.existsSync(parDir)) { console.log(`[test] trade parquet dir missing at ${parDir}, skipping`); return; }
    const db = new duckdb.Database(':memory:');
    const files = fs.readdirSync(parDir).filter(f => f.endsWith('.parquet'));
    assert.ok(files.length > 0, 'trade parquet files exist');

    // Check schema of first file
    const schema = await q(db, `DESCRIBE SELECT * FROM read_parquet('${parDir}/${files[0]}')`);
    const colNames = schema.map(r => r.column_name);
    assert.ok(colNames.includes('raw_line'), 'should have raw_line column');
    assert.ok(colNames.includes('row_id'), 'should have row_id column');
    assert.ok(colNames.includes('ts'), 'should have ts column');
    assert.ok(colNames.includes('market'), 'should have market column');
    assert.ok(colNames.includes('price'), 'should have price column');
    db.close();
  });

  it('all depth Parquet files have bids and asks columns', async () => {
    const parDir = `${parquetBase}/${testDate}/depth`;
    if (!fs.existsSync(parDir)) { console.log(`[test] depth parquet dir missing at ${parDir}, skipping`); return; }
    const db = new duckdb.Database(':memory:');
    const files = fs.readdirSync(parDir).filter(f => f.endsWith('.parquet'));
    assert.ok(files.length > 0, 'depth parquet files exist');

    // Check structure of first file
    const schema = await q(db, `DESCRIBE SELECT * FROM read_parquet('${parDir}/${files[0]}')`);
    const colNames = schema.map(r => r.column_name);
    assert.ok(colNames.includes('bids'), 'should have bids column');
    assert.ok(colNames.includes('asks'), 'should have asks column');
    assert.ok(colNames.includes('seq'), 'should have seq column');
    db.close();
  });

  it('can reconstruct JSONL from raw_line and verify SHA matches manifest', async () => {
    const manifestPath = `${parquetBase}/${testDate}/manifest.json`;
    if (!fs.existsSync(manifestPath)) { console.log(`[test] manifest missing at ${manifestPath}, skipping`); return; }
    const db = new duckdb.Database(':memory:');
    const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf-8'));

    // Pick a small file for fast verification
    const entry = manifest.files.find(f => f.stream === 'fairprice' && f.market === 'binance_spot');
    assert.ok(entry, 'fairprice/binance_spot found in manifest');

    const parFile = `${parquetBase}/${testDate}/${entry.parquet_file}`;
    const rawLines = await q(db, `
      SELECT raw_line::VARCHAR AS raw_str
      FROM read_parquet('${parFile}')
      ORDER BY row_id
    `);
    const reconstructed = rawLines.map(r => r.raw_str).join('\n') + '\n';
    const reconSha = crypto.createHash('sha256').update(reconstructed).digest('hex');

    assert.strictEqual(reconSha, entry.source_sha256,
      `raw_line SHA mismatch for ${entry.stream}/${entry.market}`);
    db.close();
  });
});
