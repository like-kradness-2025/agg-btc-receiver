// debug_recon.mjs — debug SHA-256 mismatch in raw_line reconstruction
import duckdb from 'duckdb';
import fs from 'fs';
import crypto from 'crypto';

const srcPath = process.argv[2] || 'data/raw_hot/2026-06-26/trade/binance_spot.jsonl';
const parPath = process.argv[3] || '/tmp/debug_recon.parquet';

function q(db, sql) {
  return new Promise((resolve, reject) => db.all(sql, (e, r) => e ? reject(e) : resolve(r)));
}
function e(db, sql) {
  return new Promise((resolve, reject) => db.exec(sql, (e) => e ? reject(e) : resolve()));
}

const db = new duckdb.Database(':memory:');

// Source
const srcBuf = fs.readFileSync(srcPath);
const lines = srcBuf.toString().split('\n').filter(Boolean);
const origContent = lines.join('\n') + '\n';
const origSha = crypto.createHash('sha256').update(origContent).digest('hex');

console.log('Source:', srcPath);
console.log('  lines:', lines.length, 'bytes:', srcBuf.length, 'SHA:', origSha);

// Build NDJSON (same as convert script)
const ndjsonLines = lines.map((line, i) => {
  const d = JSON.parse(line);
  return JSON.stringify({
    raw_line: line,
    row_id: i,
    ts: d.ts ?? null,
    market: d.market ?? null,
  });
});

const tmpFile = '/tmp/debug_recon.ndjson';
fs.writeFileSync(tmpFile, ndjsonLines.join('\n'));

// Convert to Parquet (same SQL as convert script)
await e(db, `
  COPY (
    SELECT * FROM read_json_auto('${tmpFile}', format='newline_delimited')
  ) TO '${parPath}' (FORMAT PARQUET, COMPRESSION ZSTD)
`);
console.log('Parquet:', parPath, fs.statSync(parPath).size, 'bytes');

// Read back and reconstruct
const raw = await q(db, `
  SELECT raw_line::VARCHAR AS raw_str, row_id::BIGINT AS rid
  FROM read_parquet('${parPath}')
  ORDER BY row_id
`);
console.log('  parquet rows:', raw.length);

const reconstructed = raw.map(r => r.raw_str).join('\n') + '\n';
const reconSha = crypto.createHash('sha256').update(reconstructed).digest('hex');

console.log('  Recon SHA:', reconSha);
console.log('  Match:', origSha === reconSha ? '✅ PASS' : '❌ FAIL');

if (origSha !== reconSha) {
  // Find first mismatch
  console.log('\nDebugging mismatch...');
  for (let i = 0; i < Math.min(lines.length, raw.length); i++) {
    if (lines[i] !== raw[i].raw_str) {
      console.log(`First mismatch at line ${i}:`);
      console.log('  src:  ', lines[i].slice(0, 120));
      console.log('  recon:', raw[i].raw_str.slice(0, 120));
      console.log('  src len:', lines[i].length, 'recon len:', raw[i].raw_str.length);
      break;
    }
  }
}

// Cleanup
fs.rmSync(tmpFile);
fs.rmSync(parPath);
db.close();
