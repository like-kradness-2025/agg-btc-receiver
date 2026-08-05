#!/usr/bin/env node
// Repair existing OrderHeatmap artifacts after a materializer rule change.
// Conflicting outputs/manifests are moved to a recoverable backup tree.

import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { mkdir, rename } from 'node:fs/promises';
import { join, relative } from 'node:path';
import { materializeOrderHeatmapBlock } from '../lib/orderheatmap-materializer.mjs';
import { commitDerived, hashBytes } from '../lib/downstream/derived-commit.mjs';

const BLOCK_MS = 30_000;
const SNAPSHOT_SCHEMA = 'book_snapshot_1s_v2';

function arg(name, fallback) {
  const i = process.argv.indexOf(name);
  return i < 0 ? fallback : process.argv[i + 1];
}

const snapshotRoot = arg('--snapshot-root', 'data/derived/burst_features_v2/book_snapshots_v2');
const outputRoot = arg('--output-root', 'data/derived/burst_features_v2/orderheatmap_1s');
const markets = (arg('--markets', '') || '').split(',').filter(Boolean).sort();
const fromMs = arg('--from-ms', null) == null ? null : Number(arg('--from-ms', null));
const toMs = arg('--to-ms', null) == null ? null : Number(arg('--to-ms', null));
if (!markets.length) throw new Error('--markets is required');

function blockMs(filePath) {
  const parts = filePath.split('/');
  const date = parts.at(-2)?.replace(/^date=/, '');
  const match = parts.at(-1)?.match(/^(\d{2})-(\d{2})-(\d{2})\.jsonl$/);
  if (!date || !match) return null;
  const [y, mo, d] = date.split('-').map(Number);
  const [hh, mm, ss] = match.slice(1).map(Number);
  return Date.UTC(y, mo - 1, d, hh, mm, ss);
}

function listSnapshotFiles(market) {
  const root = join(snapshotRoot, `market=${market}`);
  if (!existsSync(root)) return [];
  const out = [];
  for (const date of readdirSync(root).sort()) {
    const dir = join(root, date);
    if (!existsSync(dir)) continue;
    for (const name of readdirSync(dir).filter((item) => item.endsWith('.jsonl')).sort()) {
      const path = join(dir, name);
      const ms = blockMs(path);
      if (ms != null && (fromMs == null || ms >= fromMs) && (toMs == null || ms < toMs)) out.push({ path, ms });
    }
  }
  return out;
}

function outputPath(market, ms) {
  const iso = new Date(ms).toISOString();
  return join(outputRoot, `market=${market}`, `date=${iso.slice(0, 10)}`, `${iso.slice(11, 19).replaceAll(':', '-')}.jsonl`);
}

function validBlock(rows, ms) {
  return rows.length === 30 && rows.every((row, i) => row?.schema_version === SNAPSHOT_SCHEMA
    && row.ts === ms + i * 1000);
}

const backupRoot = join(outputRoot, `.repair-backup-${Date.now()}`);
let scanned = 0;
let repaired = 0;
let unchanged = 0;
let skipped = 0;

for (const market of markets) {
  for (const file of listSnapshotFiles(market)) {
    scanned++;
    const rows = readFileSync(file.path, 'utf8').split('\n').filter(Boolean).map(JSON.parse);
    if (!validBlock(rows, file.ms)) {
      skipped++;
      continue;
    }
    const content = `${materializeOrderHeatmapBlock(rows).map((row) => JSON.stringify(row)).join('\n')}\n`;
    const output = outputPath(market, file.ms);
    const existing = existsSync(output) ? readFileSync(output) : null;
    if (existing && hashBytes(existing) === hashBytes(Buffer.from(content))) {
      unchanged++;
      continue;
    }

    const backup = join(backupRoot, relative(outputRoot, output));
    const backupManifest = `${backup}.manifest.json`;
    const manifest = `${output}.manifest.json`;
    await mkdir(join(backup, '..'), { recursive: true });
    if (existsSync(output)) await rename(output, backup);
    if (existsSync(manifest)) await rename(manifest, backupManifest);
    try {
      const result = await commitDerived({
        outputPath: output,
        content,
        source: {
          kind: 'book_snapshots_v2_repair',
          market,
          block_start_ms: file.ms,
          reason: 'enforce_orderheatmap_depth_limit_usd_10000',
        },
      });
      if (!['committed', 'idempotent'].includes(result.status)) throw new Error(`${output}: ${result.status} ${result.reason}`);
      repaired++;
    } catch (error) {
      if (existsSync(backup)) await rename(backup, output);
      if (existsSync(backupManifest)) await rename(backupManifest, manifest);
      throw error;
    }
    // This repair intentionally handles many large JSONL blocks. When the
    // caller enables --expose-gc, release per-block buffers before the next
    // artifact so the bounded maintenance cgroup remains effective.
    if (global.gc && scanned % 10 === 0) global.gc();
  }
}

console.log(JSON.stringify({ scanned, repaired, unchanged, skipped, backup_root: repaired ? backupRoot : null }));
