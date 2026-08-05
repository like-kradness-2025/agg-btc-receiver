#!/usr/bin/env node
// Materialize market-level OrderHeatmap rows from strict Book Snapshot v2.
//
// ⚠️  LEGACY — This script operates on the old JSONL-based path
//    (data/derived/burst_features_v1/).  The live production pipeline is:
//      Receiver SQLite → agg-btc-downstream-live → agg-btc-orderheatmap-publisher
//    See docs/canonical-pipeline.md for the canonical architecture.

import { existsSync, mkdirSync, readFileSync, readdirSync, renameSync, writeFileSync } from 'node:fs';
import { join, basename } from 'node:path';
import { materializeOrderHeatmapBlock } from '../lib/orderheatmap-materializer.mjs';
import { writeAtomicJson } from '../lib/downstream/incremental-cursor.mjs';

const BLOCK_MS = 30_000;
const CURSOR_SCHEMA = 'orderheatmap_cursor_v1';
const SNAPSHOT_SCHEMA = 'book_snapshot_1s_v2';

function parseArgs() {
  const args = process.argv.slice(2);
  const out = {
    snapshotRoot: 'data/derived/burst_features_v1/book_snapshots_v2',
    outputRoot: 'data/derived/burst_features_v1/orderheatmap_1s',
    markets: null, from: null, to: null, force: false, incremental: false,
    skipInitialUnseeded: false,
    continueOnMarketError: false,
  };
  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--snapshot-root': out.snapshotRoot = args[++i]; break;
      case '--output-root': out.outputRoot = args[++i]; break;
      case '--markets': out.markets = args[++i].split(',').filter(Boolean); break;
      case '--from': out.from = Number(args[++i]); break;
      case '--to': out.to = Number(args[++i]); break;
      case '--force': out.force = true; break;
      case '--incremental': out.incremental = true; break;
      case '--skip-initial-unseeded': out.skipInitialUnseeded = true; break;
      case '--continue-on-market-error': out.continueOnMarketError = true; break;
      case '--help':
        console.log('Usage: node scripts/materialize-orderheatmap.mjs [--markets m1,m2] [--from ms] [--to ms] [--force] [--incremental] [--skip-initial-unseeded] [--continue-on-market-error]');
        process.exit(0);
    }
  }
  return out;
}

function blockFromPath(filePath) {
  const match = basename(filePath).match(/^(\d{2})-(\d{2})-(\d{2})\.jsonl$/);
  const parts = filePath.split('/');
  const datePart = parts.at(-2);
  const date = datePart?.startsWith('date=') ? datePart.slice(5) : datePart;
  if (!match || !/^\d{4}-\d{2}-\d{2}$/.test(date)) return null;
  const [hh, mm, ss] = match.slice(1).map(Number);
  const [year, month, day] = date.split('-').map(Number);
  return Date.UTC(year, month - 1, day, hh, mm, ss);
}

function listSnapshotFiles(root, market, from, to) {
  const marketRoot = join(root, `market=${market}`);
  if (!existsSync(marketRoot)) return [];
  const files = [];
  for (const date of readdirSync(marketRoot).sort()) {
    const dateRoot = join(marketRoot, date);
    if (!existsSync(dateRoot)) continue;
    for (const name of readdirSync(dateRoot).filter((n) => n.endsWith('.jsonl')).sort()) {
      const path = join(dateRoot, name);
      const ms = blockFromPath(path);
      if (ms == null || (from != null && ms < from) || (to != null && ms >= to)) continue;
      files.push({ path, ms });
    }
  }
  return files;
}

function readRows(path) {
  return readFileSync(path, 'utf8').split('\n').filter(Boolean).map((line, index) => {
    try { return JSON.parse(line); } catch (error) { throw new Error(`${path}:${index + 1}: ${error.message}`); }
  });
}

function outputPath(root, market, blockStartMs) {
  const iso = new Date(blockStartMs).toISOString();
  return join(root, `market=${market}`, `date=${iso.slice(0, 10)}`, `${iso.slice(11, 19).replaceAll(':', '-')}.jsonl`);
}

function snapshotPath(root, market, blockStartMs) {
  const iso = new Date(blockStartMs).toISOString();
  return join(root, `market=${market}`, `date=${iso.slice(0, 10)}`, `${iso.slice(11, 19).replaceAll(':', '-')}.jsonl`);
}

function cursorPath(root, market) {
  return join(root, '.incremental-cursors', `${market}.json`);
}

function loadCursor(path) {
  if (!existsSync(path)) return null;
  const value = JSON.parse(readFileSync(path, 'utf8'));
  if (value.schema_version !== CURSOR_SCHEMA) {
    throw new Error(`unsupported orderheatmap cursor schema at ${path}: ${value.schema_version}`);
  }
  return value;
}

const USABLE_BOOK_STATUSES = new Set(['seeded', 'unsequenced']);

function alignBlockCeil(value) {
  return value == null ? value : Math.ceil(value / BLOCK_MS) * BLOCK_MS;
}

function validateSnapshotBlock(rows, blockStartMs, path) {
  if (rows.length !== 30) {
    throw new Error(`invalid snapshot block ${path}: expected 30 rows, got ${rows.length}`);
  }
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const expectedTs = blockStartMs + i * 1000;
    if (row?.schema_version !== SNAPSHOT_SCHEMA) {
      throw new Error(`invalid snapshot block ${path}: row ${i} schema_version=${row?.schema_version} (expected ${SNAPSHOT_SCHEMA})`);
    }
    if (row?.ts !== expectedTs) {
      throw new Error(`invalid snapshot block ${path}: row ${i} ts ${row?.ts} != ${expectedTs}`);
    }
    if (row.finalized !== true) {
      throw new Error(`invalid snapshot block ${path}: row ${i} finalized=${row.finalized} (expected true)`);
    }
    if (row.seeded !== true) {
      throw new Error(`invalid snapshot block ${path}: row ${i} seeded=${row.seeded} (expected true)`);
    }
    if (row.gap !== false) {
      throw new Error(`invalid snapshot block ${path}: row ${i} gap=${row.gap} (expected false)`);
    }
    if (row.crossed !== false) {
      throw new Error(`invalid snapshot block ${path}: row ${i} crossed=${row.crossed} (expected false)`);
    }
    if (row.stale !== false) {
      throw new Error(`invalid snapshot block ${path}: row ${i} stale=${row.stale} (expected false)`);
    }
    if (!USABLE_BOOK_STATUSES.has(row.book_status)) {
      throw new Error(`invalid snapshot block ${path}: row ${i} book_status=${row.book_status} (expected seeded or unsequenced)`);
    }
  }
}

function writeAtomic(path, rows, force) {
  if (existsSync(path) && !force) return false;
  mkdirSync(join(path, '..'), { recursive: true });
  const tmp = `${path}.tmp-${process.pid}`;
  writeFileSync(tmp, rows.map((row) => JSON.stringify(row)).join('\n') + '\n', 'utf8');
  renameSync(tmp, path);
  return true;
}

/** Incremental OrderHeatmap: process only new complete 30-row snapshot blocks
 *  using a durable cursor. Invalid blocks do not advance the cursor (fail-closed). */
export async function materializeOrderHeatmapIncremental(options = {}) {
  const opts = { ...parseArgsDefaults(), ...options };
  const markets = opts.markets || [];
  if (!markets.length) throw new Error('incremental orderheatmap requires --markets');
  if (opts.force) throw new Error('--force cannot be combined with --incremental');

  let written = 0;
  let skipped = 0;
  let blocked = 0;
  const failedMarkets = [];
  for (const market of markets.slice().sort()) {
    const checkpoint = cursorPath(opts.outputRoot, market);
    const current = loadCursor(checkpoint);
    // A live horizon is based on wall-clock milliseconds and is almost never
    // exactly 30s-aligned. Snapshot filenames are block-aligned, so only a
    // brand-new cursor may normalize its starting boundary. Once persisted,
    // the cursor remains strict and may never jump over a missing block.
    let nextBlockMs = current?.next_block_ms ?? alignBlockCeil(opts.from);
    if (nextBlockMs == null) {
      blocked += 1;
      continue;
    }
    if (opts.to != null && nextBlockMs >= opts.to) {
      blocked += 1;
      continue;
    }

    let marketBlocked = false;
    let processedBlocks = 0;
    let initialUnseededStartMs = null;
    let initialUnseededUntilMs = null;
    while (opts.to == null || nextBlockMs < opts.to) {
      // Probe only the cursor's expected file. Never enumerate or jump over
      // a missing block; the next run retries from the unchanged boundary.
      const path = snapshotPath(opts.snapshotRoot, market, nextBlockMs);
      if (!existsSync(path)) {
        marketBlocked = true;
        break;
      }
      const snapshots = readRows(path);
      if (!current && opts.skipInitialUnseeded
          && snapshots.length === 30
          && snapshots.every((row) => row.seeded !== true
            && row.finalized !== true)) {
        initialUnseededStartMs ??= nextBlockMs;
        nextBlockMs += BLOCK_MS;
        initialUnseededUntilMs = nextBlockMs;
        writeAtomicJson(checkpoint, {
          schema_version: CURSOR_SCHEMA,
          market,
          next_block_ms: nextBlockMs,
          initial_unseeded_gap: {
            start_ms: initialUnseededStartMs,
            end_ms: initialUnseededUntilMs,
          },
        });
        continue;
      }
      try {
        validateSnapshotBlock(snapshots, nextBlockMs, path);
        const rows = materializeOrderHeatmapBlock(snapshots);
        const output = outputPath(opts.outputRoot, market, nextBlockMs);
        if (writeAtomic(output, rows, false)) written += 1;
        else skipped += 1;
        processedBlocks += 1;
        nextBlockMs += BLOCK_MS;
        writeAtomicJson(checkpoint, {
          schema_version: CURSOR_SCHEMA,
          market,
          next_block_ms: nextBlockMs,
          ...(initialUnseededStartMs == null ? {} : {
            initial_unseeded_gap: {
              start_ms: initialUnseededStartMs,
              end_ms: initialUnseededUntilMs,
            },
          }),
        });
      } catch (error) {
        if (!opts.continueOnMarketError) throw error;
        marketBlocked = true;
        failedMarkets.push({ market, path, reason: error.message });
        break;
      }
    }
    if (marketBlocked && processedBlocks === 0) blocked += 1;
  }
  return {
    written_blocks: written,
    skipped_blocks: skipped,
    blocked_markets: blocked,
    failed_markets: failedMarkets,
    markets,
  };
}

function parseArgsDefaults() {
  return {
    snapshotRoot: 'data/derived/burst_features_v1/book_snapshots_v2',
    outputRoot: 'data/derived/burst_features_v1/orderheatmap_1s',
    markets: null, from: null, to: null, force: false, incremental: false,
    skipInitialUnseeded: false,
    continueOnMarketError: false,
  };
}

async function main() {
  const opts = parseArgs();
  if (opts.incremental) {
    const result = await materializeOrderHeatmapIncremental(opts);
    console.log(JSON.stringify(result));
    if (result.failed_markets.length > 0) process.exitCode = 1;
    return;
  }

  const root = opts.snapshotRoot;
  const markets = opts.markets || (existsSync(root)
    ? readdirSync(root).filter((name) => name.startsWith('market='))
      .map((name) => name.slice('market='.length))
    : []);
  let written = 0;
  let skipped = 0;
  let invalid = 0;
  for (const market of markets.sort()) {
    for (const file of listSnapshotFiles(root, market, opts.from, opts.to)) {
      const snapshots = readRows(file.path);
      if (snapshots.length !== 30 || snapshots.some((row, index) => row.ts !== file.ms + index * 1000)) {
        invalid += 1;
        continue;
      }
      const rows = materializeOrderHeatmapBlock(snapshots);
      if (writeAtomic(outputPath(opts.outputRoot, market, file.ms), rows, opts.force)) written += 1;
      else skipped += 1;
    }
  }
  console.log(JSON.stringify({ written_blocks: written, skipped_blocks: skipped, invalid_blocks: invalid, markets }));
}

if (process.argv[1] && process.argv[1].endsWith('/materialize-orderheatmap.mjs')) {
  console.error('[LEGACY] This materializer (JSONL-based) is NOT used in production.');
  console.error('[LEGACY] Live pipeline: Receiver SQLite → agg-btc-downstream-live → agg-btc-orderheatmap-publisher');
  console.error('[LEGACY] See docs/canonical-pipeline.md for the canonical architecture.\n');
  main().catch((error) => { console.error(error.stack || error.message); process.exit(1); });
}

export { parseArgs, listSnapshotFiles, outputPath, snapshotPath, cursorPath };
