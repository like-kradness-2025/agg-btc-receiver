#!/usr/bin/env node
// ⚠️  LEGACY — Daily raw-v4 JSONL → Parquet archive. Receiver does not use
//    v4 JSONL segments in production. See docs/current/canonical-pipeline.md
//    for the canonical architecture.
//
// Daily raw-v4 archive: closed JSONL -> verified Parquet -> safe raw delete.
// No feature generation lives here. This is storage housekeeping only.

import duckdb from 'duckdb';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';

const RAW_KINDS = ['trades', 'book_updates', 'liquidations', 'snapshots'];
const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_RAW_RETENTION_HOURS = 24;
const DEFAULT_ARCHIVE_RETENTION_DAYS = 90;
const DELIM = String.fromCharCode(1);
const QUOTE = String.fromCharCode(2);

function sqlString(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

function q(db, sql) {
  return new Promise((resolve, reject) => db.all(sql, (err, rows) => err ? reject(err) : resolve(rows)));
}

function exec(db, sql) {
  return new Promise((resolve, reject) => db.exec(sql, err => err ? reject(err) : resolve()));
}

function closeDb(db) {
  try { db.close(); } catch (_) { /* already closed */ }
}

function parseArgs(argv = process.argv.slice(2)) {
  const opts = {
    data: 'data/live_v4',
    archive: 'data/archive/raw_v4',
    manifests: 'data/archive/manifests',
    rawRetentionHours: DEFAULT_RAW_RETENTION_HOURS,
    archiveRetentionDays: DEFAULT_ARCHIVE_RETENTION_DAYS,
    date: null,
    kind: null,
    market: null,
    dryRun: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = () => argv[++i];
    if (arg === '--data') opts.data = next();
    else if (arg === '--archive') opts.archive = next();
    else if (arg === '--manifests') opts.manifests = next();
    else if (arg === '--raw-retention-hours') opts.rawRetentionHours = Number(next());
    else if (arg === '--archive-retention-days') opts.archiveRetentionDays = Number(next());
    else if (arg === '--date') opts.date = next();
    else if (arg === '--kind') opts.kind = next();
    else if (arg === '--market') opts.market = next();
    else if (arg === '--dry-run') opts.dryRun = true;
    else if (arg === '--help') {
      console.log(`Usage: node scripts/archive-raw-v4.mjs [options]

Archives closed raw-v4 JSONL after 24 hours and keeps Parquet for 90 days.

  --data PATH                       raw root (default: data/live_v4)
  --archive PATH                    Parquet root (default: data/archive/raw_v4)
  --manifests PATH                  manifest root (default: data/archive/manifests)
  --raw-retention-hours N           raw grace period (default: 24)
  --archive-retention-days N        Parquet retention (default: 90)
  --date YYYY-MM-DD                 process one date only
  --kind KIND                       process one raw kind only
  --market MARKET                   process one market only
  --dry-run                         inspect without converting or deleting`);
      process.exit(0);
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (!Number.isFinite(opts.rawRetentionHours) || opts.rawRetentionHours < 0) {
    throw new Error('--raw-retention-hours must be a non-negative number');
  }
  if (!Number.isInteger(opts.archiveRetentionDays) || opts.archiveRetentionDays < 1) {
    throw new Error('--archive-retention-days must be a positive integer');
  }
  if (opts.kind && !RAW_KINDS.includes(opts.kind)) {
    throw new Error(`--kind must be one of: ${RAW_KINDS.join(', ')}`);
  }
  if (opts.date && !/^\d{4}-\d{2}-\d{2}$/.test(opts.date)) {
    throw new Error('--date must use YYYY-MM-DD');
  }
  return opts;
}

function utcDate(ms) {
  return new Date(ms).toISOString().slice(0, 10);
}

function dateMs(date) {
  const ms = Date.parse(`${date}T00:00:00.000Z`);
  if (!Number.isFinite(ms)) throw new Error(`invalid date: ${date}`);
  return ms;
}

function eligibleDate(nowMs, rawRetentionHours) {
  // A date is eligible only after the whole UTC day plus the grace period.
  return utcDate(nowMs - rawRetentionHours * 60 * 60 * 1000 - DAY_MS);
}

async function listDirs(dir) {
  try {
    return (await fsp.readdir(dir, { withFileTypes: true }))
      .filter(entry => entry.isDirectory())
      .map(entry => entry.name)
      .sort();
  } catch (error) {
    if (error.code === 'ENOENT') return [];
    throw error;
  }
}

async function listFiles(dir) {
  try {
    return (await fsp.readdir(dir, { withFileTypes: true }))
      .filter(entry => entry.isFile())
      .map(entry => entry.name)
      .sort();
  } catch (error) {
    if (error.code === 'ENOENT') return [];
    throw error;
  }
}

async function discoverSegments(dataRoot, date, { kind = null, market = null } = {}) {
  const segments = [];
  const kinds = kind ? [kind] : RAW_KINDS;
  for (const stream of kinds) {
    const streamRoot = path.join(dataRoot, stream);
    for (const marketName of await listDirs(streamRoot)) {
      if (market && marketName !== market) continue;
      const dateRoot = path.join(streamRoot, marketName, date);
      for (const name of await listFiles(dateRoot)) {
        if (!name.endsWith('.jsonl') || name.endsWith('.jsonl.active')) continue;
        segments.push({
          kind: stream,
          market: marketName,
          date,
          name,
          sourcePath: path.join(dateRoot, name),
          sourceRel: path.relative(dataRoot, path.join(dateRoot, name)),
        });
      }
    }
  }
  return segments;
}

async function hasActiveFiles(dataRoot, date, { kind = null, market = null } = {}) {
  const kinds = kind ? [kind] : RAW_KINDS;
  let count = 0;
  for (const stream of kinds) {
    const streamRoot = path.join(dataRoot, stream);
    for (const marketName of await listDirs(streamRoot)) {
      if (market && marketName !== market) continue;
      const dateRoot = path.join(streamRoot, marketName, date);
      count += (await listFiles(dateRoot)).filter(name => name.endsWith('.jsonl.active')).length;
    }
  }
  return count;
}

async function discoverDates(dataRoot, { kind = null, market = null } = {}) {
  const dates = new Set();
  const kinds = kind ? [kind] : RAW_KINDS;
  for (const stream of kinds) {
    for (const marketName of await listDirs(path.join(dataRoot, stream))) {
      if (market && marketName !== market) continue;
      for (const date of await listDirs(path.join(dataRoot, stream, marketName))) {
        if (/^\d{4}-\d{2}-\d{2}$/.test(date)) dates.add(date);
      }
    }
  }
  return [...dates].sort();
}

async function hashAndCount(filePath) {
  const hash = crypto.createHash('sha256');
  let rows = 0;
  let lastByte = 0;
  const stream = fs.createReadStream(filePath);
  for await (const chunk of stream) {
    hash.update(chunk);
    for (const byte of chunk) if (byte === 0x0a) rows += 1;
    if (chunk.length) lastByte = chunk[chunk.length - 1];
  }
  const size = (await fsp.stat(filePath)).size;
  if (size > 0 && lastByte !== 0x0a) rows += 1;
  return { sha256: hash.digest('hex'), rows, bytes: size };
}

function outputPath(archiveRoot, segment) {
  return path.join(
    archiveRoot,
    segment.kind,
    `market=${segment.market}`,
    `date=${segment.date}`,
    segment.name.replace(/\.jsonl$/, '.parquet'),
  );
}

function archiveSelect(sourcePath, sourceRel) {
  const raw = 'raw_line::VARCHAR AS raw_line';
  return `
    SELECT
      row_number() OVER () - 1 AS row_id,
      ${raw},
      json_extract_string(raw_line, '$.schema') AS schema,
      coalesce(json_extract_string(raw_line, '$.market'), json_extract_string(raw_line, '$.payload.market')) AS market,
      coalesce(json_extract_string(raw_line, '$.stream'), json_extract_string(raw_line, '$.payload.stream')) AS stream,
      coalesce(
        try_cast(json_extract_string(raw_line, '$.event_ts_ms') AS BIGINT),
        try_cast(json_extract_string(raw_line, '$.ts') AS BIGINT),
        try_cast(json_extract_string(raw_line, '$.payload.ts') AS BIGINT)
      ) AS event_ts_ms,
      coalesce(
        try_cast(json_extract_string(raw_line, '$.recv_ts_ms') AS BIGINT),
        try_cast(json_extract_string(raw_line, '$.recvTs') AS BIGINT)
      ) AS recv_ts_ms,
      json_extract_string(raw_line, '$.writer_session_id') AS writer_session_id,
      json_extract_string(raw_line, '$.source_id') AS source_id,
      json_extract(raw_line, '$.payload')::VARCHAR AS payload_json,
      ${sqlString(sourceRel)} AS source_path
    FROM read_csv(${sqlString(sourcePath)},
      columns={'raw_line': 'VARCHAR'},
      delim=${sqlString(DELIM)},
      quote=${sqlString(QUOTE)},
      header=false,
      auto_detect=false)
    WHERE raw_line <> ''`;
}

async function convertSegment(segment, archiveRoot, sourceInfo) {
  const finalPath = outputPath(archiveRoot, segment);
  const tmpPath = `${finalPath}.tmp-${process.pid}-${Date.now()}`;
  await fsp.mkdir(path.dirname(finalPath), { recursive: true });
  const db = new duckdb.Database(':memory:');
  try {
    await exec(db, `COPY (${archiveSelect(segment.sourcePath, segment.sourceRel)}) TO ${sqlString(tmpPath)} (FORMAT PARQUET, COMPRESSION ZSTD)`);
    const rows = await q(db, `SELECT count(*) AS count, min(row_id) AS first_id, max(row_id) AS last_id FROM read_parquet(${sqlString(tmpPath)})`);
    const rowCount = Number(rows[0]?.count ?? 0);
    if (rowCount !== sourceInfo.rows) throw new Error(`row count mismatch source=${sourceInfo.rows} parquet=${rowCount}`);
    if (rowCount > 0 && (Number(rows[0].first_id) !== 0 || Number(rows[0].last_id) !== rowCount - 1)) {
      throw new Error('row_id sequence is not contiguous');
    }
    const newline = sqlString('\n');
    const reconstructed = await q(db, `
      SELECT sha256(string_agg(CAST(raw_line AS VARCHAR), ${newline} ORDER BY row_id) || ${newline}) AS sha256
      FROM read_parquet(${sqlString(tmpPath)})`);
    if (rowCount > 0 && reconstructed[0]?.sha256 !== sourceInfo.sha256) {
      throw new Error('raw_line SHA-256 mismatch after Parquet conversion');
    }
  } finally {
    closeDb(db);
  }

  const outputInfo = await hashAndCount(tmpPath);
  await fsp.rename(tmpPath, finalPath);
  return { finalPath, rows: sourceInfo.rows, outputBytes: outputInfo.bytes, outputSha256: outputInfo.sha256 };
}

async function readManifest(manifestPath, date) {
  try {
    const value = JSON.parse(await fsp.readFile(manifestPath, 'utf8'));
    if (value.archive_date !== date || !value.entries || typeof value.entries !== 'object') throw new Error('unsupported manifest');
    return value;
  } catch (error) {
    if (error.code === 'ENOENT') {
      return { schema_version: 'raw_v4_archive_v1', archive_date: date, status: 'partial', entries: {} };
    }
    throw new Error(`${manifestPath}: ${error.message}`);
  }
}

async function writeManifest(manifestPath, manifest) {
  manifest.updated_at = new Date().toISOString();
  const tmpPath = `${manifestPath}.tmp-${process.pid}`;
  await fsp.mkdir(path.dirname(manifestPath), { recursive: true });
  await fsp.writeFile(tmpPath, `${JSON.stringify(manifest, null, 2)}\n`);
  await fsp.rename(tmpPath, manifestPath);
}

async function archiveDate(opts, date) {
  const segments = await discoverSegments(opts.data, date, opts);
  const activeFiles = await hasActiveFiles(opts.data, date, opts);
  const manifestPath = path.join(opts.manifests, `${date}.json`);
  const manifest = await readManifest(manifestPath, date);
  const entries = manifest.entries;

  if (opts.dryRun) {
    const totalBytes = (await Promise.all(segments.map(segment => fsp.stat(segment.sourcePath).then(stat => stat.size)))).reduce((a, b) => a + b, 0);
    console.log(`[archive] ${date}: ${segments.length} closed segments, ${(totalBytes / 1024 / 1024 / 1024).toFixed(2)} GiB raw, active=${activeFiles}`);
    return { date, segments: segments.length, activeFiles, converted: 0, deleted: 0, rawBytes: totalBytes };
  }

  let converted = 0;
  let deleted = 0;
  for (const segment of segments) {
    const key = segment.sourceRel;
    const sourceInfo = await hashAndCount(segment.sourcePath);
    const previous = entries[key];
    const finalPath = outputPath(opts.archive, segment);

    if (previous?.status === 'verified' && previous.source_sha256 === sourceInfo.sha256 && fs.existsSync(finalPath)) {
      const currentOutput = await hashAndCount(finalPath);
      if (currentOutput.sha256 !== previous.parquet_sha256) {
        throw new Error(`Parquet hash conflict: ${finalPath}`);
      }
      entries[key] = previous;
    } else {
      if (previous?.status === 'verified' && previous.source_sha256 !== sourceInfo.sha256 && fs.existsSync(finalPath)) {
        throw new Error(`source hash conflict for existing archive: ${key}`);
      }
      if (fs.existsSync(finalPath) && !previous) {
        throw new Error(`refusing untracked output collision: ${finalPath}`);
      }
      const result = await convertSegment(segment, opts.archive, sourceInfo);
      entries[key] = {
        status: 'verified',
        kind: segment.kind,
        market: segment.market,
        source_path: key,
        source_sha256: sourceInfo.sha256,
        source_bytes: sourceInfo.bytes,
        source_rows: sourceInfo.rows,
        parquet_path: path.relative(opts.archive, result.finalPath),
        parquet_sha256: result.outputSha256,
        parquet_bytes: result.outputBytes,
        converted_at: new Date().toISOString(),
      };
      converted += 1;
      console.log(`[archive] converted ${key}: ${sourceInfo.rows} rows, ${(sourceInfo.bytes / 1024 / 1024).toFixed(1)} MiB -> ${(result.outputBytes / 1024 / 1024).toFixed(1)} MiB`);
      await writeManifest(manifestPath, manifest);
    }

    const currentSource = await hashAndCount(segment.sourcePath);
    if (currentSource.sha256 !== entries[key].source_sha256) throw new Error(`source changed during archive: ${key}`);
    await fsp.unlink(segment.sourcePath);
    entries[key].source_deleted_at = new Date().toISOString();
    deleted += 1;
    await writeManifest(manifestPath, manifest);
  }

  const remaining = await discoverSegments(opts.data, date, opts);
  manifest.status = remaining.length === 0 && activeFiles === 0 ? 'committed' : 'partial';
  manifest.archive_date = date;
  manifest.retention = { raw_hours: opts.rawRetentionHours, archive_days: opts.archiveRetentionDays };
  await writeManifest(manifestPath, manifest);
  console.log(`[archive] ${date}: status=${manifest.status}, converted=${converted}, deleted=${deleted}, active=${activeFiles}`);
  return { date, segments: segments.length, activeFiles, converted, deleted };
}

async function cleanupExpiredArchives(opts, nowMs) {
  const boundary = utcDate(nowMs - opts.archiveRetentionDays * DAY_MS);
  const dates = (await listFiles(opts.manifests))
    .filter(name => /^\d{4}-\d{2}-\d{2}\.json$/.test(name))
    .map(name => name.slice(0, 10))
    .filter(date => date < boundary)
    .sort();

  let deleted = 0;
  for (const date of dates) {
    const manifestPath = path.join(opts.manifests, `${date}.json`);
    const manifest = await readManifest(manifestPath, date);
    if (manifest.status !== 'committed') {
      console.log(`[archive] retain expired ${date}: manifest=${manifest.status}`);
      continue;
    }
    const entries = Object.values(manifest.entries);
    if (opts.dryRun) {
      console.log(`[archive] would delete expired archive ${date}: ${entries.length} Parquet files`);
      continue;
    }
    for (const entry of entries) {
      const filePath = path.join(opts.archive, entry.parquet_path);
      if (fs.existsSync(filePath)) {
        await fsp.unlink(filePath);
        deleted += 1;
      }
    }
    await fsp.unlink(manifestPath);
    console.log(`[archive] deleted expired archive ${date}: ${entries.length} Parquet files`);
  }
  return deleted;
}

async function main(argv = process.argv.slice(2), nowMs = Date.now()) {
  const opts = parseArgs(argv);
  opts.data = path.resolve(opts.data);
  opts.archive = path.resolve(opts.archive);
  opts.manifests = path.resolve(opts.manifests);
  const eligible = eligibleDate(nowMs, opts.rawRetentionHours);
  const dates = opts.date ? [opts.date] : (await discoverDates(opts.data, opts)).filter(date => date <= eligible);

  if (opts.date && opts.date > eligible) {
    console.log(`[archive] ${opts.date}: not eligible yet (eligible through ${eligible})`);
  } else {
    if (dates.length === 0) console.log(`[archive] no eligible raw dates through ${eligible}`);
    for (const date of dates) await archiveDate(opts, date);
  }
  const expiredDeleted = await cleanupExpiredArchives(opts, nowMs);
  console.log(`[archive] complete: eligible_through=${eligible}, expired_deleted=${expiredDeleted}${opts.dryRun ? ' (dry-run)' : ''}`);
  return { eligible, dates, expiredDeleted };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(error => {
    console.error(`[archive] FATAL: ${error.message}`);
    process.exit(1);
  });
}

export {
  RAW_KINDS,
  parseArgs,
  eligibleDate,
  discoverSegments,
  outputPath,
  archiveDate,
  cleanupExpiredArchives,
  main,
};
