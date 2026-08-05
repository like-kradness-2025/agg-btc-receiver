#!/usr/bin/env node
// ⚠️  LEGACY — Materialize strict 1s Book Snapshot v2 rows from old JSONL-based
//    raw blocks. The live production pipeline is:
//      Receiver SQLite → agg-btc-downstream-live → agg-btc-orderheatmap-publisher
//    See docs/current/canonical-pipeline.md for the canonical architecture.
//
// Materialize strict 1s Book Snapshot v2 rows from finalized raw book blocks.

import { createHash } from 'node:crypto';
import { createReadStream, statSync } from 'node:fs';
import { existsSync, mkdirSync, readFileSync, readdirSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { join, basename, dirname, relative, resolve, sep } from 'node:path';
import { materializeBookSnapshots } from '../lib/book-snapshot-materializer.mjs';
import { stateAtDetailed } from '../lib/book-state-machine.mjs';
import { toCanonicalBookEnvelope } from '../lib/book-updates-adapter.mjs';
import { commitDerived } from '../lib/downstream/derived-commit.mjs';
import { loadCursor, readIncrementalJsonl, saveCursor, writeAtomicJson } from '../lib/downstream/incremental-cursor.mjs';
import {
  RAW_V4_SEGMENT_CURSOR_SCHEMA_VERSION,
  RawV4SegmentReader,
} from '../lib/downstream/raw-v4-segment-reader.mjs';

const BLOCK_MS = 30000;
const V4_CURSOR_SCHEMA = 'book_snapshot_v4_cursor_v1';

function parseArgs() {
  const args = process.argv.slice(2);
  const out = { data: null, outputRoot: 'data/derived/burst_features_v1/book_snapshots_v2', markets: null, from: null, to: null, force: false, rawLayout: 'v3', incremental: false };
  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--data': out.data = args[++i]; break;
      case '--output-root': out.outputRoot = args[++i]; break;
      case '--markets': out.markets = args[++i].split(',').filter(Boolean); break;
      case '--from': out.from = Number(args[++i]); break;
      case '--to': out.to = Number(args[++i]); break;
      case '--force': out.force = true; break;
      case '--raw-layout':
        if (!['v3', 'v4'].includes(args[i + 1])) {
          console.error(`Invalid --raw-layout: ${args[i + 1]} (expected v3 or v4)`);
          process.exit(1);
        }
        out.rawLayout = args[++i];
        break;
      case '--incremental': out.incremental = true; break;
      case '--help':
        console.log('Usage: node scripts/materialize-book-snapshots.mjs [--raw-layout v3|v4] [--data path] [--markets m1,m2] [--from ms] [--to ms] [--force] [--incremental]');
        process.exit(0);
    }
  }
  return out;
}

function blockStartFromPath(filePath) {
  const match = basename(filePath).match(/^(\d{2})-(\d{2})-(\d{2})\.jsonl$/);
  const date = filePath.split('/').slice(-2, -1)[0];
  if (!match || !/^\d{4}-\d{2}-\d{2}$/.test(date)) return null;
  const [hh, mm, ss] = match.slice(1).map(Number);
  const [year, month, day] = date.split('-').map(Number);
  return Date.UTC(year, month - 1, day, hh, mm, ss);
}

function listBlocks(dataDir, market, from, to) {
  const root = join(dataDir, 'book_updates', market);
  if (!existsSync(root)) return [];
  const files = [];
  for (const date of readdirSync(root).sort()) {
    const dateDir = join(root, date);
    if (!existsSync(dateDir)) continue;
    for (const name of readdirSync(dateDir).filter((n) => n.endsWith('.jsonl')).sort()) {
      const path = join(dateDir, name);
      const ms = blockStartFromPath(path);
      if (ms == null || (from != null && ms < from) || (to != null && ms >= to)) continue;
      files.push({ path, ms });
    }
  }
  return files;
}

function readCanonicalEvents(path) {
  const events = [];
  const lines = readFileSync(path, 'utf8').split('\n');
  for (let lineNo = 0; lineNo < lines.length; lineNo++) {
    const line = lines[lineNo].trim();
    if (!line) continue;
    let raw;
    try { raw = JSON.parse(line); } catch (error) {
      throw new Error(`${path}:${lineNo + 1}: invalid JSON: ${error.message}`);
    }
    const result = toCanonicalBookEnvelope({ ...raw, path, line_no: lineNo + 1 });
    if (!result.valid) throw new Error(`${path}:${lineNo + 1}: ${result.errors.join('; ')}`);
    events.push(result.envelope);
  }
  return events;
}

function orderSnapshotBufferedEvents(events) {
  const snapshotIndex = events.reduce(
    (last, event, index) => event.type === 'snapshot' ? index : last,
    -1,
  );
  if (snapshotIndex < 0) return events;

  const snapshotTs = events[snapshotIndex].event_ts_ms;
  return events.map((event, index) => {
    if (index > snapshotIndex && event.type === 'update' && event.event_ts_ms < snapshotTs) {
      // WS buffered updates can carry an exchange timestamp a few ms before
      // the local snapshot creation time. Keep their raw order after the
      // snapshot so a valid bridge is not turned into a false gap.
      return { ...event, event_ts_ms: snapshotTs };
    }
    return event;
  });
}

function findLatestSeed(dataDir, market, blockStartMs) {
  const root = join(dataDir, 'snapshots', market);
  if (!existsSync(root)) return null;
  let latest = null;
  for (const date of readdirSync(root).sort()) {
    const dateDir = join(root, date);
    if (!existsSync(dateDir)) continue;
    for (const name of readdirSync(dateDir).filter((n) => n.endsWith('.jsonl')).sort()) {
      const path = join(dateDir, name);
      try {
        const raw = JSON.parse(readFileSync(path, 'utf8').split('\n').find(Boolean));
        // A receiver restart can emit the seed inside this 30s block. Include
        // it so updates after the seed in the same block are replayed instead
        // of becoming an artificial sequence gap at the next block.
        if (raw?.market !== market || raw.type !== 'snapshot' || raw.ts >= blockStartMs + BLOCK_MS) continue;
        if (!latest || raw.ts > latest.raw.ts) latest = { path, raw };
      } catch { /* corrupt seed is ignored; current block remains fail-closed */ }
    }
  }
  return latest;
}

function canonicalizeExternalV4Snapshot(raw, sourcePath) {
  const isBookEvent = raw?.type === 'snapshot';
  const isStoredSnapshot = raw?.stream === 'snapshot' || raw?.schemaVersion != null;
  if (!isBookEvent && !isStoredSnapshot) return null;

  const result = toCanonicalBookEnvelope({
    ...raw,
    type: 'snapshot',
    ts: raw?.ts ?? raw?.event_ts_ms,
    seq: raw?.seq ?? null,
    source: {
      ...(raw?.source || {}),
      snapshot_origin: 'external_snapshot',
      snapshot_path: sourcePath,
    },
    path: sourcePath,
    line_no: 1,
  });
  if (!result.valid) throw new Error(`${sourcePath}: ${result.errors.join('; ')}`);
  return result.envelope;
}

function findLatestExternalV4Snapshot(dataDir, market, targetMs) {
  const root = join(dataDir, 'snapshots', market);
  if (!existsSync(root)) return null;
  let latest = null;
  for (const date of readdirSync(root).sort()) {
    const dateDir = join(root, date);
    if (!existsSync(dateDir)) continue;
    for (const name of readdirSync(dateDir).filter((n) => n.endsWith('.jsonl')).sort()) {
      const sourcePath = join(dateDir, name);
      let raw;
      try {
        raw = JSON.parse(readFileSync(sourcePath, 'utf8').split('\n').find(Boolean));
      } catch {
        continue;
      }
      if (raw?.market !== market) continue;
      const ts = raw?.ts ?? raw?.event_ts_ms;
      if (typeof ts !== 'number' || !Number.isFinite(ts) || ts > targetMs) continue;
      if (!latest || ts > latest.ts || (ts === latest.ts && sourcePath > latest.path)) {
        latest = { path: sourcePath, ts, raw };
      }
    }
  }
  return latest;
}

function writeAtomic(path, rows, force) {
  if (existsSync(path) && !force) return false;
  mkdirSync(join(path, '..'), { recursive: true });
  const tmp = `${path}.tmp-${process.pid}`;
  writeFileSync(tmp, rows.map((row) => JSON.stringify(row)).join('\n') + '\n', 'utf8');
  renameSync(tmp, path);
  return true;
}

function formatBlockPath(root, market, blockStartMs) {
  const iso = new Date(blockStartMs).toISOString();
  return join(root, market, iso.slice(0, 10), `${iso.slice(11, 19).replaceAll(':', '-')}.jsonl`);
}

function outputPath(root, market, blockStartMs) {
  const iso = new Date(blockStartMs).toISOString();
  return join(root, `market=${market}`, `date=${iso.slice(0, 10)}`, `${iso.slice(11, 19).replaceAll(':', '-')}.jsonl`);
}

function nextBlockSeed(events, market, blockStartMs) {
  const result = stateAtDetailed(events, blockStartMs + BLOCK_MS, { includeLevels: true });
  const state = result.state;
  if (!state?.seeded || result.quarantined) return null;
  return {
    schema_version: 'book_updates_v1',
    market,
    type: 'snapshot',
    event_ts_ms: blockStartMs + BLOCK_MS - 1,
    ts: blockStartMs + BLOCK_MS - 1,
    seq: state.last_seq,
    prev_seq: null,
    bids: state.bids || [],
    asks: state.asks || [],
    qty_unit: 'BTC',
    snapshot_origin: 'derived_block_boundary',
  };
}

function cursorPath(root, market) {
  return join(root, '.incremental-cursors', `${market}.json`);
}

function v4CursorPath(root, market) {
  return join(root, '.v4-cursors', `${market}.json`);
}

function stageCursor(checkpointPath, sourcePath, current) {
  const staged = `${checkpointPath}.staged-${process.pid}`;
  mkdirSync(dirname(staged), { recursive: true });
  const initial = current && current.source_path === sourcePath
    ? current
    : { source_path: sourcePath, byte_offset: 0, partial_line: '', partial_line_base64: '', line_number: 0 };
  saveCursor(staged, initial);
  return staged;
}

function promoteCursor(staged, checkpointPath) {
  renameSync(staged, checkpointPath);
}

/** Process at most one ready raw block without enumerating the data horizon. */
export async function materializeBookSnapshotsIncremental(options = {}) {
  const opts = { ...parseArgsDefaults(), ...options };
  const dataDir = opts.data || 'data/live_v3';
  const markets = opts.markets || [];
  if (!markets.length) throw new Error('incremental book snapshots requires --markets');
  if (opts.force) throw new Error('--force cannot be combined with --incremental');
  if (opts.from == null && markets.some((market) => !existsSync(cursorPath(opts.outputRoot, market)))) {
    throw new Error('incremental book snapshots requires --from on the first run for each market');
  }

  let written = 0;
  let blocked = 0;
  for (const market of markets.slice().sort()) {
    const checkpoint = cursorPath(opts.outputRoot, market);
    const current = existsSync(checkpoint) ? loadCursor(checkpoint) : null;
    const blockMs = current?.next_block_ms ?? opts.from;
    if (blockMs == null || (opts.to != null && blockMs >= opts.to)) {
      blocked += 1;
      continue;
    }
    const sourcePath = formatBlockPath(join(dataDir, 'book_updates'), market, blockMs);
    if (!existsSync(sourcePath)) {
      blocked += 1;
      continue;
    }

    const staged = stageCursor(checkpoint, sourcePath, current);
    try {
      const blockEvents = [];
      const read = readIncrementalJsonl({
        sourcePath,
        checkpointPath: staged,
        onLine(raw, info) {
          const result = toCanonicalBookEnvelope({ ...raw, path: sourcePath, line_no: info.line_number });
          if (!result.valid) throw new Error(`${sourcePath}:${info.line_number}: ${result.errors.join('; ')}`);
          blockEvents.push(result.envelope);
        },
      });
      if (read.partial) {
        blocked += 1;
        continue;
      }
      const events = orderSnapshotBufferedEvents([
        ...(current?.carry_seed ? [current.carry_seed] : []),
        ...blockEvents,
      ]);
      const rows = materializeBookSnapshots(events, blockMs).map((row) => ({ ...row, market }));
      const output = outputPath(opts.outputRoot, market, blockMs);
      const content = rows.map((row) => JSON.stringify(row)).join('\n') + '\n';
      const result = await commitDerived({
        outputPath: output,
        content,
        source: { kind: 'book_updates', market, block_start_ms: blockMs, input_path: sourcePath, input_cursor: read.cursor.byte_offset },
      });
      if (!['committed', 'idempotent'].includes(result.status)) throw new Error(`derived commit ${result.status}: ${result.reason}`);
      saveCursor(staged, {
        ...read.cursor,
        next_block_ms: blockMs + BLOCK_MS,
        carry_seed: nextBlockSeed(events, market, blockMs),
      });
      promoteCursor(staged, checkpoint);
      written += 1;
    } finally {
      if (existsSync(staged)) unlinkSync(staged);
    }
  }
  return { written_blocks: written, blocked_markets: blocked, markets };
}

function loadV4Checkpoint(path) {
  if (!existsSync(path)) return null;
  const value = JSON.parse(readFileSync(path, 'utf8'));
  if (value.schema_version !== V4_CURSOR_SCHEMA) {
    throw new Error(`unsupported v4 cursor schema at ${path}: ${value.schema_version}`);
  }
  return value;
}

function canonicalizeV4Record(record, sourcePath) {
  if (!record || typeof record !== 'object' || record.payload === undefined) {
    throw new Error(`malformed raw_v4 record: ${JSON.stringify(record).slice(0, 200)}`);
  }
  const result = toCanonicalBookEnvelope({ ...record.payload, path: sourcePath, line_no: 0 });
  if (!result.valid) {
    throw new Error(`raw_v4 payload invalid: ${result.errors.join('; ')}`);
  }
  return result.envelope;
}

function cursorAfterV4Record(record, fallback) {
  const meta = record?._meta;
  const separator = meta?.logical_id?.indexOf('/');
  const byteOffset = meta?.byte_offset + meta?.byte_length;
  if (separator == null || separator < 1
      || !Number.isSafeInteger(meta.byte_offset) || !Number.isSafeInteger(meta.byte_length)
      || !Number.isSafeInteger(byteOffset)) {
    return fallback;
  }
  return JSON.stringify({
    schema_version: RAW_V4_SEGMENT_CURSOR_SCHEMA_VERSION,
    date: meta.logical_id.slice(0, separator),
    segment: meta.logical_id.slice(separator + 1),
    byte_offset: byteOffset,
  });
}

function addV4SegmentRange(ranges, record) {
  const meta = record?._meta;
  const start = meta?.byte_offset;
  const end = start + meta?.byte_length;
  if (typeof meta?.logical_id !== 'string' || !Number.isSafeInteger(start)
      || !Number.isSafeInteger(meta.byte_length) || !Number.isSafeInteger(end)) {
    throw new Error(`raw_v4 record metadata missing byte range: ${JSON.stringify(meta)}`);
  }
  const current = ranges.get(meta.logical_id);
  if (current) {
    current.byteOffsetStart = Math.min(current.byteOffsetStart, start);
    current.byteOffsetEnd = Math.max(current.byteOffsetEnd, end);
  } else {
    ranges.set(meta.logical_id, {
      segmentLogicalId: meta.logical_id,
      byteOffsetStart: start,
      byteOffsetEnd: end,
    });
  }
}

function blockStartForV4Record(record, sourcePath) {
  if (!Number.isSafeInteger(record?.event_ts_ms) || record.event_ts_ms < 0) {
    throw new Error(`${sourcePath}: invalid raw_v4 event_ts_ms: ${record?.event_ts_ms}`);
  }
  return Math.floor(record.event_ts_ms / BLOCK_MS) * BLOCK_MS;
}

function sha256File(filePath) {
  return new Promise((resolveHash, reject) => {
    const hash = createHash('sha256');
    const stream = createReadStream(filePath);
    stream.on('data', (chunk) => hash.update(chunk));
    stream.on('end', () => resolveHash(hash.digest('hex')));
    stream.on('error', reject);
  });
}

function sha256FilePrefix(filePath, prefixSize) {
  if (!Number.isSafeInteger(prefixSize) || prefixSize < 0) {
    return Promise.reject(new TypeError(`invalid raw_v4 prefix size: ${prefixSize}`));
  }
  if (prefixSize === 0) return Promise.resolve(createHash('sha256').update('').digest('hex'));
  return new Promise((resolveHash, reject) => {
    const hash = createHash('sha256');
    let bytesRead = 0;
    const stream = createReadStream(filePath, { start: 0, end: prefixSize - 1 });
    stream.on('data', (chunk) => {
      bytesRead += chunk.length;
      hash.update(chunk);
    });
    stream.on('end', () => {
      if (bytesRead !== prefixSize) {
        reject(new Error(`raw_v4 source prefix truncated: expected ${prefixSize}, got ${bytesRead}`));
        return;
      }
      resolveHash(hash.digest('hex'));
    });
    stream.on('error', reject);
  });
}

function segmentFile(dataDir, market, logicalId) {
  const separator = logicalId.indexOf('/');
  if (separator < 1 || separator === logicalId.length - 1) {
    throw new Error(`invalid raw_v4 logical segment id: ${logicalId}`);
  }
  const date = logicalId.slice(0, separator);
  const segment = logicalId.slice(separator + 1);
  const dir = join(resolve(dataDir), 'book_updates', market, date);
  const closed = join(dir, `${segment}.jsonl`);
  const active = `${closed}.active`;
  if (existsSync(closed)) return { path: closed, active: false };
  if (existsSync(active)) return { path: active, active: true };
  throw new Error(`raw_v4 segment disappeared: ${logicalId}`);
}

async function materializeV4SegmentProof(dataDir, market, ranges, cache) {
  const proof = [];
  for (const range of ranges.values()) {
    const file = segmentFile(dataDir, market, range.segmentLogicalId);
    const sourceSize = statSync(file.path).size;
    const cached = cache.get(range.segmentLogicalId);
    const sourceHash = cached && cached.path === file.path && cached.size === sourceSize && !file.active
      ? cached.hash
      : await sha256File(file.path);
    const sourcePrefixSize = range.byteOffsetEnd;
    const sourcePrefixHash = await sha256FilePrefix(file.path, sourcePrefixSize);
    if (!file.active) cache.set(range.segmentLogicalId, { path: file.path, size: sourceSize, hash: sourceHash });

    const sourceLogicalPath = relative(resolve(dataDir), file.path)
      .replaceAll(sep, '/')
      .replace(/\.active$/, '');
    const sourcePath = resolve(dataDir, sourceLogicalPath);
    proof.push({
      segmentLogicalId: range.segmentLogicalId,
      byteOffsetStart: range.byteOffsetStart,
      byteOffsetEnd: range.byteOffsetEnd,
      sourceLogicalPath,
      sourcePath,
      sourceSize,
      sourceSha256: sourceHash,
      source_path: sourcePath,
      source_size: sourceSize,
      source_hash: sourceHash,
      sourcePrefixSize,
      sourcePrefixSha256: sourcePrefixHash,
      source_prefix_size: sourcePrefixSize,
      source_prefix_sha256: sourcePrefixHash,
      source_prefix_hash: sourcePrefixHash,
      byte_offset: range.byteOffsetEnd,
      byte_offset_end: range.byteOffsetEnd,
      active: file.active,
      status: file.active ? 'active' : 'committed',
    });
  }
  return proof;
}

async function commitV4Block({ blockMs, bucket, segmentRanges, cursor, checkpoint, market, outputRoot, carrySeed, dataDir, sourcePath, segmentCache }) {
  const events = orderSnapshotBufferedEvents(bucket.splice(0, bucket.length));
  const replay = carrySeed ? [carrySeed, ...events] : events;
  const rawV4SegmentProof = await materializeV4SegmentProof(dataDir, market, segmentRanges, segmentCache);
  const proofStatus = rawV4SegmentProof.every((item) => item.status === 'committed') ? 'committed' : 'active';
  const rows = materializeBookSnapshots(replay, blockMs).map((row) => ({ ...row, market }));
  const output = outputPath(outputRoot, market, blockMs);
  const content = rows.map((row) => JSON.stringify(row)).join('\n') + '\n';
  const result = await commitDerived({
    outputPath: output,
    content,
    source: {
      kind: 'book_updates_v4',
      market,
      block_start_ms: blockMs,
      raw_v4_cursor: cursor,
      input_path: sourcePath,
      raw_v4_segment_proof: rawV4SegmentProof,
      status: proofStatus,
      committed: proofStatus === 'committed',
    },
  });
  if (!['committed', 'idempotent'].includes(result.status)) {
    throw new Error(`derived commit ${result.status}: ${result.reason}`);
  }
  const nextCarry = nextBlockSeed(replay, market, blockMs);
  writeAtomicJson(checkpoint, {
    schema_version: V4_CURSOR_SCHEMA,
    market,
    raw_v4_cursor: cursor,
    next_block_ms: blockMs + BLOCK_MS,
    carry_seed: nextCarry,
    raw_v4_segment_proof: rawV4SegmentProof,
  });
  return { carrySeed: nextCarry, rawV4SegmentProof };
}

async function closeV4Block(blockMs, bucket, segmentRanges, cursor, checkpoint, market, outputRoot, carrySeed, dataDir, sourcePath, nextBlockMs, segmentCache) {
  if (blockMs >= nextBlockMs) {
    const committed = await commitV4Block({ blockMs, bucket, segmentRanges, cursor, checkpoint, market, outputRoot, carrySeed, dataDir, sourcePath, segmentCache });
    segmentRanges.clear();
    return { carrySeed: committed.carrySeed, rawV4SegmentProof: committed.rawV4SegmentProof, written: 1 };
  }
  const events = bucket.splice(0, bucket.length);
  const replay = carrySeed ? [carrySeed, ...events] : events;
  const nextCarry = nextBlockSeed(replay, market, blockMs);
  segmentRanges.clear();
  return { carrySeed: nextCarry, rawV4SegmentProof: null, written: 0 };
}

/** Incremental v4 materializer: durable byte cursor, 30s event-time buckets,
 *  only closed / sufficiently old windows are atomically committed. */
export async function materializeBookSnapshotsV4(options = {}) {
  const opts = { ...parseArgsDefaults(), ...options };
  const dataDir = opts.data || 'data/live_v4';
  const markets = opts.markets || [];
  if (!markets.length) throw new Error('v4 book snapshots requires --markets');
  if (opts.force) throw new Error('--force cannot be combined with v4 incremental mode');

  let written = 0;
  let blocked = 0;
  let activePartial = 0;
  for (const market of markets.slice().sort()) {
    const checkpoint = v4CursorPath(opts.outputRoot, market);
    const current = loadV4Checkpoint(checkpoint);
    const nextBlockMs = current?.next_block_ms ?? opts.from;
    if (nextBlockMs == null) {
      blocked += 1;
      continue;
    }
    if (opts.to != null && nextBlockMs >= opts.to) {
      blocked += 1;
      continue;
    }

    let currentBlockMs = null;
    let carrySeed = current?.carry_seed || null;
    let externalSeed = null;
    if (!carrySeed) {
      const seed = findLatestExternalV4Snapshot(dataDir, market, nextBlockMs);
      if (seed) externalSeed = canonicalizeExternalV4Snapshot(seed.raw, seed.path);
    }
    let prevCursor = current?.raw_v4_cursor || null;
    let committedCursor = prevCursor;
    let committedSegmentProof = current?.raw_v4_segment_proof || null;
    let sourcePath = dataDir;
    const bucket = [];
    const segmentRanges = new Map();
    const segmentCache = new Map();
    let lastDone = false;

    const reader = new RawV4SegmentReader({
      root: dataDir,
      market,
      kind: 'book_updates',
      cursor: current?.raw_v4_cursor || null,
    });

    try {
      await reader.open();
      while (true) {
        const read = await reader.read({ maxRecords: 1 });
        if (read.eof && read.records.length === 0) break;
        if (read.records.length === 0) {
          if (read.done) lastDone = true;
          continue;
        }

        const record = read.records[0];
        const blockStart = blockStartForV4Record(record, sourcePath);

        if (opts.to != null && blockStart >= opts.to) {
          while (currentBlockMs != null && currentBlockMs < opts.to && currentBlockMs < blockStart) {
            const closed = await closeV4Block(currentBlockMs, bucket, segmentRanges, prevCursor, checkpoint, market, opts.outputRoot, carrySeed, dataDir, sourcePath, nextBlockMs, segmentCache);
            carrySeed = closed.carrySeed;
            written += closed.written;
            committedCursor = prevCursor;
            if (closed.written) committedSegmentProof = closed.rawV4SegmentProof;
            currentBlockMs += BLOCK_MS;
          }
          break;
        }

        if (currentBlockMs == null) {
          currentBlockMs = blockStart;
          if (!carrySeed && externalSeed) bucket.push(externalSeed);
        }

        // Close any complete windows using the wrapper timestamp before
        // validating the payload. This ensures an invalid next-block record
        // cannot prevent the previous closed block from being committed.
        if (blockStart > currentBlockMs) {
          while (currentBlockMs < blockStart) {
            const closed = await closeV4Block(currentBlockMs, bucket, segmentRanges, prevCursor, checkpoint, market, opts.outputRoot, carrySeed, dataDir, sourcePath, nextBlockMs, segmentCache);
            carrySeed = closed.carrySeed;
            written += closed.written;
            committedCursor = prevCursor;
            if (closed.written) committedSegmentProof = closed.rawV4SegmentProof;
            currentBlockMs += BLOCK_MS;
          }
        }

        // Validate payload only after boundary decisions; on failure the byte
        // cursor stays at the last committed/valid record.
        const event = canonicalizeV4Record(record, sourcePath);
        const recordCursor = cursorAfterV4Record(record, read.cursor);
        sourcePath = record?._meta?.source_path || sourcePath;

        if (blockStart < currentBlockMs) {
          // Already-committed horizon; validate before consuming bytes, but do not reprocess.
          prevCursor = recordCursor;
          continue;
        }

        if (blockStart === currentBlockMs) {
          bucket.push(event);
          addV4SegmentRange(segmentRanges, record);
          prevCursor = recordCursor;
        }
        if (read.done) {
          lastDone = true;
          break;
        }
      }

      if (lastDone && bucket.length > 0 && currentBlockMs != null && (opts.to == null || currentBlockMs < opts.to)) {
        const closed = await closeV4Block(currentBlockMs, bucket, segmentRanges, prevCursor, checkpoint, market, opts.outputRoot, carrySeed, dataDir, sourcePath, nextBlockMs, segmentCache);
        carrySeed = closed.carrySeed;
        written += closed.written;
        committedCursor = prevCursor;
        if (closed.written) committedSegmentProof = closed.rawV4SegmentProof;
        currentBlockMs += BLOCK_MS;
      }
    } finally {
      await reader.close();
    }

    if (currentBlockMs == null) {
      // No new data for this market.
      blocked += 1;
    } else if (bucket.length > 0) {
      // Active partial left uncommitted.
      activePartial += 1;
    }

    // Persist the durable byte cursor at the last committed boundary.
    // Active partial bytes remain unconsumed so the next run replays them.
    writeAtomicJson(checkpoint, {
      schema_version: V4_CURSOR_SCHEMA,
      market,
      raw_v4_cursor: committedCursor,
      next_block_ms: currentBlockMs ?? nextBlockMs,
      carry_seed: carrySeed,
      raw_v4_segment_proof: committedSegmentProof,
    });
  }
  return { written_blocks: written, blocked_markets: blocked, active_partial_markets: activePartial, markets };
}

function parseArgsDefaults() {
  return { data: null, outputRoot: 'data/derived/burst_features_v1/book_snapshots_v2', markets: null, from: null, to: null, force: false, rawLayout: 'v3', incremental: false };
}

async function main() {
  const opts = parseArgs();
  const dataDir = opts.data || (opts.rawLayout === 'v4' ? 'data/live_v4' : 'data/live_v3');

  if (opts.rawLayout === 'v4') {
    console.log(JSON.stringify(await materializeBookSnapshotsV4({ ...opts, data: dataDir })));
    return;
  }

  if (opts.incremental) {
    console.log(JSON.stringify(await materializeBookSnapshotsIncremental({ ...opts, data: dataDir })));
    return;
  }

  const bookRoot = join(dataDir, 'book_updates');
  const markets = opts.markets || (existsSync(bookRoot) ? readdirSync(bookRoot).filter((name) => existsSync(join(bookRoot, name))) : []);
  let written = 0;
  let skipped = 0;
  for (const market of markets.sort()) {
    let carrySeed = null;
    for (const block of listBlocks(dataDir, market, opts.from, opts.to)) {
      const events = [];
      const seed = carrySeed || findLatestSeed(dataDir, market, block.ms);
      if (seed) {
        if (carrySeed) {
          events.push(carrySeed);
        } else {
          const seedResult = toCanonicalBookEnvelope({ ...seed.raw, path: seed.path, line_no: 1 });
          if (!seedResult.valid) throw new Error(`${seed.path}: ${seedResult.errors.join('; ')}`);
          events.push(seedResult.envelope);
        }
      }
      const blockEvents = readCanonicalEvents(block.path);
      let replayEvents = blockEvents;
      if (!carrySeed && seed) {
        const seedIndex = blockEvents.reduce(
          (last, event, index) => event.type === 'snapshot' && event.event_ts_ms === seed.raw.ts ? index : last,
          -1,
        );
        if (seedIndex >= 0) replayEvents = blockEvents.slice(seedIndex);
      }
      events.push(...orderSnapshotBufferedEvents(replayEvents));
      const rows = materializeBookSnapshots(events, block.ms).map((row) => ({ ...row, market }));
      if (writeAtomic(outputPath(opts.outputRoot, market, block.ms), rows, opts.force)) written += 1;
      else skipped += 1;
      carrySeed = nextBlockSeed(events, market, block.ms);
    }
  }
  console.log(JSON.stringify({ written_blocks: written, skipped_blocks: skipped, markets }));
}

if (process.argv[1] && process.argv[1].endsWith('/materialize-book-snapshots.mjs')) {
  main().catch((error) => { console.error(error.stack || error.message); process.exit(1); });
}

export { parseArgs, listBlocks, outputPath, v4CursorPath };
