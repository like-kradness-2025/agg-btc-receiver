// scripts/convert-to-parquet.mjs — JSONL → Parquet conversion with raw_line + manifest
//
// Usage:
//   node scripts/convert-to-parquet.mjs                    # convert today's date
//   node scripts/convert-to-parquet.mjs --date 2026-06-25  # convert specific date
//   node scripts/convert-to-parquet.mjs --dry-run           # list files without converting
//   node scripts/convert-to-parquet.mjs --stream depth      # only depth files
//   node scripts/convert-to-parquet.mjs --market binance_spot  # single market
//
// Output:
//   data/parquet/{date}/{stream}/{market}.parquet
//   data/parquet/{date}/manifest.json

import duckdb from 'duckdb';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

const RAW_HOT_BASE = 'data/raw_hot';
const PARQUET_BASE = 'data/parquet';
const MANIFEST_NAME = 'manifest.json';

// ── helpers ────────────────────────────────────────────────────────────

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

/** SHA-256 of a file */
function sha256File(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    const stream = fs.createReadStream(filePath);
    stream.on('data', d => hash.update(d));
    stream.on('end', () => resolve(hash.digest('hex')));
    stream.on('error', reject);
  });
}

/** Sorted list of JSONL files for a date partition, grouped by stream. */
function scanDatePartition(dateStr, streamFilter = '', marketFilter = '') {
  const dateDir = path.join(RAW_HOT_BASE, dateStr);
  if (!fs.existsSync(dateDir)) return [];

  const results = [];
  const streams = fs.readdirSync(dateDir, {withFileTypes: true})
    .filter(d => d.isDirectory())
    .map(d => d.name);

  for (const stream of streams) {
    if (streamFilter && stream !== streamFilter) continue;
    const streamDir = path.join(dateDir, stream);
    const files = fs.readdirSync(streamDir)
      .filter(f => f.endsWith('.jsonl'))
      .map(f => f.replace('.jsonl', ''))
      .sort();
    for (const market of files) {
      if (marketFilter && market !== marketFilter) continue;
      results.push({ stream, market, path: path.join(streamDir, `${market}.jsonl`) });
    }
  }
  return results;
}

/** Determine output stream name (snapshot → snapshot, depth → depth, etc.) */
function outputStream(stream) {
  return stream === 'lsratio' ? 'ls_ratio' : stream;
}

// ── Per-file conversion ────────────────────────────────────────────────

async function convertFile(srcBuf, stream, market, outDir, dateStr) {
  const db = new duckdb.Database(':memory:');

  const srcSize = srcBuf.length;
  const lines = srcBuf.toString().split('\n').filter(Boolean);
  if (lines.length === 0) { db.close(); return { rows: 0, srcSize, rowCount: 0, parSize: 0 }; }

  // Write temp CSV with line numbers using TAB separator so DuckDB
  // deterministically reads row_id and raw_line.
  const SEP = '\t';
  const tmpFile = path.join('/tmp', `convert_${dateStr}_${stream}_${market}.csv`);
  const tmpLines = lines.map((line, i) => `${i}${SEP}${line}`);
  fs.writeFileSync(tmpFile, tmpLines.join('\n'));

  const outFile = path.join(outDir, `${market}.parquet`);
  fs.mkdirSync(outDir, { recursive: true });

  const csvOpts = `header=false, columns={'row_id': 'BIGINT', 'line': 'VARCHAR'}, sep='${SEP}', quote='', auto_detect=false`;

  // Build column list for DuckDB SQL — common fields first, then stream-specific
  let cols = `line::VARCHAR AS raw_line,
    row_id,
    json_extract_string(line, '$.ts')::BIGINT AS ts,
    json_extract_string(line, '$.market') AS market`;

  if (stream === 'trade') {
    cols += `,
    json_extract_string(line, '$.price')::DOUBLE AS price,
    json_extract_string(line, '$.qty')::DOUBLE AS qty,
    json_extract_string(line, '$.side') AS side,
    coalesce(json_extract_string(line, '$.tradeId'), json_extract_string(line, '$.trade_id')) AS trade_id`;
  } else if (stream === 'depth' || stream === 'snapshot') {
    cols += `,
    json_extract_string(line, '$.schemaVersion') AS schema_version,
    json_extract_string(line, '$.stream') AS stream_name,
    json_extract_string(line, '$.type') AS type,
    json_extract_string(line, '$.recvTs')::BIGINT AS recv_ts,
    json_extract_string(line, '$.exchange') AS exchange,
    json_extract_string(line, '$.seq')::BIGINT AS seq,
    json_extract_string(line, '$.prevSeq')::BIGINT AS prev_seq,
    json_extract_string(line, '$.reason') AS reason,
    json_extract_string(line, '$.bidLevelCount')::BIGINT AS bid_level_count,
    json_extract_string(line, '$.askLevelCount')::BIGINT AS ask_level_count,
    json_extract_string(line, '$.bids') AS bids,
    json_extract_string(line, '$.asks') AS asks`;
  } else if (stream === 'fairprice') {
    cols += `,
    json_extract_string(line, '$.fair_price')::DOUBLE AS fair_price,
    json_extract_string(line, '$.fair_price_source') AS fair_price_source,
    json_extract_string(line, '$.mark_price')::DOUBLE AS mark_price,
    json_extract_string(line, '$.book_mid')::DOUBLE AS book_mid,
    json_extract_string(line, '$.last_price')::DOUBLE AS last_price,
    json_extract_string(line, '$.type') AS type`;
  }

  try {
    await e(db, `
      COPY (
        SELECT ${cols}
        FROM read_csv_auto('${tmpFile}', ${csvOpts})
      ) TO '${outFile}' (FORMAT PARQUET, COMPRESSION ZSTD)
    `);
  } catch (err) {
    db.close();
    throw err;
  }

  const parSize = fs.statSync(outFile).size;
  fs.rmSync(tmpFile);
  db.close();
  return { rows: lines.length, srcSize, rowCount: lines.length, parSize };
}

// ── Main ───────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const dateStr = args.find(a => a.startsWith('--date='))?.split('=')[1]
    || new Date().toISOString().slice(0, 10);
  const streamFilter = args.find(a => a.startsWith('--stream='))?.split('=')[1] || '';
  const marketFilter = args.find(a => a.startsWith('--market='))?.split('=')[1] || '';
  const dryRun = args.includes('--dry-run');

  const files = scanDatePartition(dateStr, streamFilter, marketFilter);
  if (files.length === 0) {
    console.log(`[convert] No files found for ${dateStr}`);
    return;
  }

  const outBase = path.join(PARQUET_BASE, dateStr);
  fs.mkdirSync(outBase, { recursive: true });

  console.log(`[convert] ${dateStr}: ${files.length} files to process${dryRun ? ' (DRY RUN)' : ''}`);

  if (dryRun) {
    for (const { stream, market, path: srcPath } of files) {
      const stats = fs.statSync(srcPath);
      console.log(`  ${stream}/${market}.jsonl  ${(stats.size/1024/1024).toFixed(1)}MB`);
    }
    return;
  }

  const manifest = {
    archive_date: dateStr,
    encoding: 'UTF-8',
    newline: 'LF',
    schema: 'v1',
    files: [],
    conversion_ts: new Date().toISOString(),
    total_source_bytes: 0,
    total_source_rows: 0,
    total_parquet_bytes: 0,
    verified: false,
  };

  for (const { stream, market, path: srcPath } of files) {
    try {
      const outDir = path.join(outBase, outputStream(stream));
      const srcStat = fs.statSync(srcPath);
      if (srcStat.size === 0) {
        console.log(`  ${stream}/${market}: empty, SKIP`);
        continue;
      }
      // Atomic read: compute SHA from the exact same buffer used for conversion
      const srcBuf = fs.readFileSync(srcPath);
      const srcSha256 = crypto.createHash('sha256').update(srcBuf).digest('hex');
      const result = await convertFile(srcBuf, stream, market, outDir, dateStr);

      if (result.rows === 0) {
        console.log(`  ${stream}/${market}: 0 rows, SKIP`);
        continue;
      }

      const parFile = path.join(outDir, `${market}.parquet`);
      const parSha256 = await sha256File(parFile);

      manifest.files.push({
        stream,
        market,
        source_file: `${stream}/${market}.jsonl`,
        source_sha256: srcSha256,
        source_bytes: result.srcSize,
        row_count: result.rows,
        parquet_file: `${outputStream(stream)}/${market}.parquet`,
        parquet_sha256: parSha256,
        parquet_bytes: result.parSize,
      });
      manifest.total_source_bytes += result.srcSize;
      manifest.total_source_rows += result.rows;
      manifest.total_parquet_bytes += result.parSize;

      const pct = ((1 - result.parSize / result.srcSize) * 100).toFixed(0);
      console.log(`  ${stream}/${market}: ${result.rows} rows, ${(result.srcSize/1024/1024).toFixed(1)}MB → ${(result.parSize/1024/1024).toFixed(1)}MB (${pct}% reduction)`);

    } catch (err) {
      console.error(`  ${stream}/${market}: ERROR: ${err.message}`);
    }
  }

  // Verify: one fresh DuckDB per file, compare reconstructed raw_lines against source SHA
  let allVerified = true;
  for (const entry of manifest.files) {
    try {
      const parFile = path.join(outBase, entry.parquet_file);
      const vDb = new duckdb.Database(':memory:');
      const rawLines = await q(vDb, `
        SELECT raw_line::VARCHAR AS raw_str
        FROM read_parquet('${parFile}')
        ORDER BY row_id
      `);
      vDb.close();
      const reconstructed = rawLines.map(r => r.raw_str).join('\n') + '\n';
      const reconSha = crypto.createHash('sha256').update(reconstructed).digest('hex');
      if (reconSha !== entry.source_sha256) {
        console.error(`  ${entry.stream}/${entry.market}: SHA-256 MISMATCH`);
        allVerified = false;
      }
    } catch (err) {
      console.error(`  ${entry.stream}/${entry.market}: verify error: ${err.message}`);
      allVerified = false;
    }
  }

  manifest.verified = allVerified;

  // Write manifest
  const manifestPath = path.join(outBase, MANIFEST_NAME);
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));
  console.log(`\n[convert] Manifest: ${manifestPath}`);
  console.log(`[convert] Total: ${manifest.total_source_rows} rows, ${(manifest.total_source_bytes/1024/1024).toFixed(0)}MB → ${(manifest.total_parquet_bytes/1024/1024).toFixed(0)}MB`);
  console.log(`[convert] Verified: ${allVerified ? '✅ ALL PASS' : '❌ FAIL'}`);
}

main().catch(err => {
  console.error('FATAL:', err.message);
  process.exit(1);
});
