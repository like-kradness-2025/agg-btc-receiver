// debug_recon2.mjs — debug SHA mismatch between convert script and direct test
import duckdb from 'duckdb';
import fs from 'fs';
import crypto from 'crypto';

const srcPath = 'data/raw_hot/2026-06-26/trade/binance_perp.jsonl';
const parPath = '/tmp/debug_binance_perp.parquet';

function q(db, sql) {
  return new Promise((resolve, reject) => db.all(sql, (e, r) => e ? reject(e) : resolve(r)));
}
function e(db, sql) {
  return new Promise((resolve, reject) => db.exec(sql, (e) => e ? reject(e) : resolve()));
}

// Source
const srcBuf = fs.readFileSync(srcPath);
const lines = srcBuf.toString().split('\n').filter(Boolean);
const origContent = lines.join('\n') + '\n';
const origSha = crypto.createHash('sha256').update(origContent).digest('hex');
console.log('Source lines:', lines.length, 'SHA:', origSha);

// NDJSON — same as convert script
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
    trade_id: d.tradeId ?? d.trade_id ?? null,
  });
});

const tmpFile = '/tmp/debug_binance_perp.ndjson';
fs.writeFileSync(tmpFile, ndjsonLines.join('\n'));

// Convert to Parquet
const db = new duckdb.Database(':memory:');
await e(db, `
  COPY (
    SELECT * FROM read_json_auto('${tmpFile}', format='newline_delimited')
  ) TO '${parPath}' (FORMAT PARQUET, COMPRESSION ZSTD)
`);

// Read back via DuckDB
const rawDb = await q(db, `
  SELECT raw_line::VARCHAR AS raw_str, row_id::BIGINT AS rid
  FROM read_parquet('${parPath}')
  ORDER BY row_id
`);
console.log('Parquet rows:', rawDb.length);

// Reconstruct
const reconstructed = rawDb.map(r => r.raw_str).join('\n') + '\n';
const reconSha = crypto.createHash('sha256').update(reconstructed).digest('hex');
console.log('Recon SHA:', reconSha);
console.log('Match:', origSha === reconSha ? 'PASS' : 'FAIL');

if (origSha !== reconSha) {
  // Find differences using a different method: compare byte by byte in Node
  const origLines = lines;
  const reconLines = rawDb.map(r => r.raw_str);
  console.log('Line counts: orig=', origLines.length, 'recon=', reconLines.length);

  for (let i = 0; i < Math.min(10, origLines.length, reconLines.length); i++) {
    const o = origLines[i];
    const r = reconLines[i];
    if (o !== r) {
      console.log(`Diff at line ${i}:`);
      console.log('  orig len:', o.length, 'recon len:', r.length);
      // Find byte-level diff
      for (let j = 0; j < Math.min(o.length, r.length, 200); j++) {
        if (o.charCodeAt(j) !== r.charCodeAt(j)) {
          console.log(`  byte ${j}: orig=0x${o.charCodeAt(j).toString(16)}(${o[j]}) recon=0x${r.charCodeAt(j).toString(16)}(${r[j]})`);
          console.log(`  orig context: ${o.slice(Math.max(0,j-20), j+20)}`);
          console.log(`  recon context: ${r.slice(Math.max(0,j-20), j+20)}`);
          break;
        }
      }
      break;
    }
  }

  // Also check SHA of raw NDJSON lines vs parquet raw_line
  const ndjsonParsed = ndjsonLines.map(l => JSON.parse(l).raw_line);
  if (ndjsonParsed.length === reconLines.length) {
    let ndjsonMatch = 0;
    for (let i = 0; i < ndjsonParsed.length; i++) {
      if (ndjsonParsed[i] === reconLines[i]) ndjsonMatch++;
    }
    console.log(`\nNDJSON→Parquet raw_line match: ${ndjsonMatch}/${ndjsonParsed.length}`);
  }
}

// Cleanup
fs.rmSync(tmpFile);
fs.rmSync(parPath);
db.close();
