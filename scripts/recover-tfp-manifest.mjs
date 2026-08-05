#!/usr/bin/env node
// Move a TFP manifest checkpoint to a verified raw start boundary.
// The previous manifest is retained beside the live file for audit/recovery.

import { copyFileSync, existsSync, readFileSync, renameSync, writeFileSync } from 'node:fs';

const argv = process.argv.slice(2);
const value = (name) => {
  const index = argv.indexOf(name);
  return index < 0 ? null : argv[index + 1];
};

const manifest = value('--manifest');
const fromMs = Number(value('--from-ms'));
if (!manifest || !Number.isSafeInteger(fromMs) || !existsSync(manifest)) {
  throw new Error('usage: --manifest PATH --from-ms EPOCH_MS');
}

const current = JSON.parse(readFileSync(manifest, 'utf8'));
const previous = Number(current.last_checkpoint_block_start);
if (!Number.isSafeInteger(previous)) throw new Error(`manifest has no valid checkpoint: ${manifest}`);
if (previous >= fromMs) {
  console.log(JSON.stringify({ changed: false, manifest, previous, from_ms: fromMs }));
  process.exit(0);
}

const backup = `${manifest}.bak-${Date.now()}`;
copyFileSync(manifest, backup);
const next = {
  ...current,
  last_checkpoint_block_start: fromMs - 30_000,
  recovery: {
    reason: 'reset_before_verified_earliest_raw_segment',
    previous_last_checkpoint_block_start: previous,
    earliest_raw_segment_start_ms: fromMs,
    backup,
  },
};
const temp = `${manifest}.tmp-${process.pid}`;
writeFileSync(temp, `${JSON.stringify(next)}\n`, 'utf8');
renameSync(temp, manifest);
console.log(JSON.stringify({ changed: true, manifest, previous, next: next.last_checkpoint_block_start, backup }));
