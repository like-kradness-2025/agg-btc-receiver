#!/usr/bin/env node
// Move one OrderHeatmap cursor past an explicitly unavailable Book horizon.
// The previous cursor is preserved beside the live checkpoint.

import { copyFileSync, existsSync, readFileSync } from 'node:fs';
import { writeAtomicJson } from '../lib/downstream/incremental-cursor.mjs';

const SCHEMA = 'orderheatmap_cursor_v1';
const argv = process.argv.slice(2);
const value = (name) => {
  const index = argv.indexOf(name);
  return index < 0 ? null : argv[index + 1];
};

const market = value('--market');
const cursor = value('--cursor');
const nextBlockMs = Number(value('--next-block-ms'));
const gapStartMs = Number(value('--gap-start-ms'));
if (!market || !cursor || !Number.isSafeInteger(nextBlockMs) || !Number.isSafeInteger(gapStartMs)) {
  throw new Error('usage: --market M --cursor CURSOR --next-block-ms MS --gap-start-ms MS');
}

const checkpoint = {
  schema_version: SCHEMA,
  market,
  next_block_ms: nextBlockMs,
  initial_unseeded_gap: { start_ms: gapStartMs, end_ms: nextBlockMs },
  recovery: {
    reason: 'advance_past_verified_unavailable_book_horizon',
    previous_next_block_ms: existsSync(cursor) ? JSON.parse(readFileSync(cursor, 'utf8')).next_block_ms : null,
  },
};
if (existsSync(cursor)) {
  const backup = `${cursor}.bak-${Date.now()}`;
  copyFileSync(cursor, backup);
  checkpoint.recovery.previous_cursor_backup = backup;
}
writeAtomicJson(cursor, checkpoint);
console.log(JSON.stringify({ cursor, checkpoint }, null, 2));
