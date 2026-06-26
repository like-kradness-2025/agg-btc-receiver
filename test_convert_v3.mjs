// test_convert_v3.mjs — JSONL → Parquet via NDJSON bridge (simple, no base64)
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
  const market = 'binance_spot';
  const srcPath = `data/raw_hot/2026-06-26/trade/${market}.jsonl`;

  // SHA-256 of source
  const srcBuf = fs.readFileSync(srcPath);
  const srcSha256 = crypto.createHash('sha256').update(srcBuf).digest('hex');
  const lines = srcBuf.toString().split('\n').filter(Boolean);
  console.log(`Source: ${lines.length} rows, ${(srcBuf.length/1024/1024).toFixed(1)}MB`);

  // Build NDJSON: each line = {raw_line, row_id, ts, market, price, ...}
  const ndjsonLines = lines.map((line, i) => {
    const d = JSON.parse(line);
    return JSON.stringify({
      raw_line: line,
      row_id: i,
      ts: d.ts ?? null,
      market: d.market ?? null,
      price: d.price ?? null,
      qty: d.qty ?? null,
      side: d.side ?? null,
      trade_id: d.tradeId ?? d.trade_id ?? null
    });
  });

  const tmpNdjson = '/tmp/convert_bridge.ndjson';
  fs.writeFileSync(tmpNdjson, ndjsonLines.join('\n'));
  console.log(`NDJSON: ${(fs.statSync(tmpNdjson).size/1024/1024).toFixed(1)}MB`);

  // DuckDB: read NDJSON → write Parquet
  const outPath = '/tmp/test_convert_v3.parquet';
  await e(db, `
    COPY (
      SELECT 
        raw_line,
        row_id::BIGINT AS row_id,
        ts::BIGINT AS ts,
        market,
        price::DOUBLE AS price,
        qty::DOUBLE AS qty,
        side,
        trade_id
      FROM read_json_auto('${tmpNdjson}', format='newline_delimited')
    ) TO '${outPath}' (FORMAT PARQUET, COMPRESSION ZSTD)
  `);

  const parqSize = fs.statSync(outPath).size;
  console.log(`Parquet: ${(parqSize/1024/1024).toFixed(1)}MB (ratio: ${(srcBuf.length/parqSize).toFixed(1)}x)`);

  // Verify row count
  const cnt = await q(db, `SELECT count(*)::VARCHAR AS n FROM read_parquet('${outPath}')`);
  console.log(`Rows: ${cnt[0]?.n}`);

  // Verify raw_line reconstruction
  const allRows = await q(db, `
    SELECT raw_line::VARCHAR AS raw_str FROM read_parquet('${outPath}') ORDER BY row_id
  `);
  const reconstructed = allRows.map(r => r.raw_str).join('\n') + '\n';
  const reconSha256 = crypto.createHash('sha256').update(reconstructed).digest('hex');

  console.log(`\nSource SHA-256:       ${srcSha256}`);
  console.log(`Reconstructed SHA-256: ${reconSha256}`);
  console.log(`Match: ${srcSha256 === reconSha256 ? '✅ PASS' : '❌ FAIL'}`);

  // Cleanup
  fs.rmSync(tmpNdjson);
  fs.rmSync(outPath);

} catch (err) {
  console.error('ERROR:', err.message);
  console.error(err.stack);
} finally {
  db.close();
}
