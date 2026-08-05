#!/usr/bin/env node
// Recover one v4 Book cursor to a verified full raw snapshot.
// The old checkpoint is preserved beside the live cursor before replacement.

import { copyFileSync, existsSync, readFileSync } from 'node:fs';
import { basename, dirname } from 'node:path';
import { writeAtomicJson } from '../lib/downstream/incremental-cursor.mjs';

const CURSOR_SCHEMA = 'book_snapshot_v4_cursor_v1';
const RAW_CURSOR_SCHEMA = 'raw_v4_segment_cursor_v1';
const BLOCK_MS = 30_000;

function args() {
  const out = {};
  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--market') out.market = argv[++i];
    else if (argv[i] === '--cursor') out.cursor = argv[++i];
    else if (argv[i] === '--raw') out.raw = argv[++i];
    else if (argv[i] === '--dry-run') out.dryRun = true;
  }
  if (!out.market || !out.cursor || !out.raw) throw new Error('usage: --market M --cursor CURSOR --raw RAW [--dry-run]');
  return out;
}

function findLatestFullSnapshot(rawPath) {
  const bytes = readFileSync(rawPath);
  let start = 0;
  let latest = null;
  while (start < bytes.length) {
    const newline = bytes.indexOf(0x0a, start);
    const end = newline < 0 ? bytes.length : newline;
    const line = bytes.subarray(start, end);
    if (line.length) {
      try {
        const record = JSON.parse(line.toString('utf8'));
        const payload = record?.payload || record;
        if (payload?.type === 'snapshot'
            && Array.isArray(payload.bids) && payload.bids.length > 0
            && Array.isArray(payload.asks) && payload.asks.length > 0
            && Number.isFinite(Number(record.event_ts_ms))) {
          latest = { offset: start, eventTsMs: Number(record.event_ts_ms), bids: payload.bids.length, asks: payload.asks.length };
        }
      } catch {
        // The receiver only appends JSONL; ignore an incomplete final line.
      }
    }
    start = newline < 0 ? bytes.length : newline + 1;
  }
  if (!latest) throw new Error(`no complete two-sided snapshot in ${rawPath}`);
  return latest;
}

const opts = args();
const snapshot = findLatestFullSnapshot(opts.raw);
const rawName = basename(opts.raw).replace(/\.jsonl(?:\.active)?$/, '');
const date = opts.raw.split('/').at(-2);
const rawCursor = JSON.stringify({
  schema_version: RAW_CURSOR_SCHEMA,
  date,
  segment: rawName,
  byte_offset: snapshot.offset,
});
const checkpoint = {
  schema_version: CURSOR_SCHEMA,
  market: opts.market,
  raw_v4_cursor: rawCursor,
  next_block_ms: Math.floor(snapshot.eventTsMs / BLOCK_MS) * BLOCK_MS,
  carry_seed: null,
  raw_v4_segment_proof: null,
  recovery: {
    reason: 'rewind_to_latest_verified_full_snapshot',
    source_path: opts.raw,
    snapshot_event_ts_ms: snapshot.eventTsMs,
    snapshot_byte_offset: snapshot.offset,
    snapshot_levels: { bids: snapshot.bids, asks: snapshot.asks },
  },
};

if (opts.dryRun) {
  console.log(JSON.stringify({ cursor: opts.cursor, checkpoint }, null, 2));
} else {
  if (existsSync(opts.cursor)) {
    const backup = `${opts.cursor}.bak-${Date.now()}`;
    copyFileSync(opts.cursor, backup);
    checkpoint.recovery.previous_cursor_backup = backup;
  }
  writeAtomicJson(opts.cursor, checkpoint);
  console.log(JSON.stringify({ cursor: opts.cursor, checkpoint }, null, 2));
}
