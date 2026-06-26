// test_convert_v2.mjs — DuckDB JSONL → Parquet via temp JSON bridge
import duckdb from 'duckdb';
import fs from 'node:fs';
import crypto from 'node:crypto';

function q(db, sql) {
  return new Promise((resolve, reject) => {
    db.all(sql, (err, rows) => err ? reject(err) : resolve(rows));
  });
}
function e(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (err) => err ? reject(err) : resolve());
  });
}

const db = new duckdb.Database(':memory:');

try {
  const srcPath = 'data/raw_hot/2026-06-26/trade/binance_spot.jsonl';

  // SHA-256 of source file
  const srcBuf = fs.readFileSync(srcPath);
  const srcSha256 = crypto.createHash('sha256').update(srcBuf).digest('hex');
  const rowCount = srcBuf.toString().trim().split('\n').length;

  // Step 1: Read all lines, parse + keep raw
  const lines = srcBuf.toString().split('\n').filter(Boolean);
  console.log(`Source: ${lines.length} rows, ${srcBuf.length} bytes`);

  // Step 2: Create temp structured data as JSON array (for DuckDB)
  // Trade schema extraction
  const structured = lines.map(line => {
    const d = JSON.parse(line);
    return {
      raw_line: Buffer.from(line).toString('base64'), // BLOB-safe: encode as base64
      ts: d.ts ?? null,
      market: d.market ?? null,
      price: d.price ?? null,
      qty: d.qty ?? null,
      side: d.side ?? null,
      trade_id: d.tradeId ?? d.trade_id ?? null,
      type: d.type ?? null
    };
  });

  // Write temp JSON
  const tmpJson = '/tmp/convert_bridge.json';
  fs.writeFileSync(tmpJson, JSON.stringify(structured));
  console.log(`Temp JSON: ${tmpJson} (${fs.statSync(tmpJson).size} bytes)`);

  // Step 3: DuckDB reads temp JSON → Parquet
  const outPath = '/tmp/test_convert_v2.parquet';
  await e(db, `
    CREATE TABLE parquet_out AS
    SELECT 
      decode(raw_line) AS raw_line,
      ts::BIGINT,
      market,
      price::DOUBLE,
      qty::DOUBLE,
      side,
      trade_id,
      type
    FROM read_json_auto('${tmpJson}')
  `);
  
  await e(db, `COPY parquet_out TO '${outPath}' (FORMAT PARQUET, COMPRESSION ZSTD)`);
  console.log(`Parquet written: ${outPath} (${fs.statSync(outPath).size} bytes)`);

  // Step 4: Verify — read back raw_line and compare hash
  const verify = await q(db, `SELECT count(*)::VARCHAR AS cnt FROM read_parquet('${outPath}')`);
  console.log(`Parquet row count: ${verify[0]?.cnt}`);

  // Reconstruct JSONL from raw_line and hash
  const rawCheck = await q(db, `
    SELECT raw_line FROM read_parquet('${outPath}') ORDER BY ts LIMIT 2
  `);
  for (const r of rawCheck) {
    const buf = r.raw_line;
    console.log(`raw_line type: ${typeof buf}, isBuffer: ${buf instanceof Buffer}, length: ${buf?.length}`);
    if (buf instanceof Buffer) {
      console.log(`  content: ${buf.toString().slice(0, 80)}`);
    } else if (buf?.constructor?.name === 'Uint8Array') {
      console.log(`  content: ${Buffer.from(buf).toString().slice(0, 80)}`);
    }
  }

  // Full reconstruction: extract all raw_line, join, hash, compare
  // DuckDB BigInt-safe count
  const fullVerify = await q(db, `
    SELECT 
      hash( (SELECT string_agg(raw_line::VARCHAR, chr(10)) FROM read_parquet('${outPath}') ORDER BY ts) ) AS reconstructed_hash
  `);
  // Actually let's do it simpler: read all raw_line from parquet via Node
  const allRaw = await q(db, `SELECT raw_line FROM read_parquet('${outPath}') ORDER BY ts`);
  const reconstructed = allRaw.map(r => {
    const buf = r.raw_line;
    if (buf instanceof Buffer) return buf.toString();
    if (buf?.constructor?.name === 'Uint8Array') return Buffer.from(buf).toString();
    return String(buf);
  }).join('\n');
  const reconSha256 = crypto.createHash('sha256').update(reconstructed).digest('hex');

  console.log(`\nSource SHA-256:      ${srcSha256}`);
  console.log(`Reconstructed SHA-256: ${reconSha256}`);
  console.log(`Match: ${srcSha256 === reconSha256 ? '✅ PASS' : '❌ FAIL'}`);

  // Cleanup
  fs.rmSync(tmpJson);
  fs.rmSync(outPath);
  console.log('\nCleanup done.');

} catch (err) {
  console.error('ERROR:', err.message);
  console.error(err.stack);
} finally {
  db.close();
}
