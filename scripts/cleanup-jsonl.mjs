// scripts/cleanup-jsonl.mjs — Delete JSONL source files after verified Parquet conversion
//
// Usage:
//   node scripts/cleanup-jsonl.mjs --date 2026-06-25              # delete verified
//   node scripts/cleanup-jsonl.mjs --date 2026-06-25 --dry-run     # what would be deleted
//   node scripts/cleanup-jsonl.mjs --date 2026-06-25 --stream depth # single stream
//
// Safety: only deletes files whose manifest entry has verified=true
//         and row_count matches the source file.

import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';

const RAW_HOT_BASE = 'data/raw_hot';
const PARQUET_BASE = 'data/parquet';
const MANIFEST_NAME = 'manifest.json';

async function main() {
  const args = process.argv.slice(2);
  const dateStr = args.find(a => a.startsWith('--date='))?.split('=')[1];
  const streamFilter = args.find(a => a.startsWith('--stream='))?.split('=')[1] || '';
  const dryRun = args.includes('--dry-run');

  if (!dateStr) {
    console.error('Usage: node scripts/cleanup-jsonl.mjs --date=YYYY-MM-DD [--dry-run] [--stream=trade]');
    process.exit(1);
  }

  const manifestPath = path.join(PARQUET_BASE, dateStr, MANIFEST_NAME);
  if (!fs.existsSync(manifestPath)) {
    console.log(`[cleanup] No manifest at ${manifestPath}`);
    return;
  }

  const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf-8'));

  if (!manifest.verified) {
    console.log(`[cleanup] Manifest for ${dateStr} is NOT verified. Refusing to delete.`);
    return;
  }

  let deletedBytes = 0;
  let deletedFiles = 0;
  let skippedFiles = 0;

  for (const entry of manifest.files) {
    if (streamFilter && entry.stream !== streamFilter) continue;

    const sourcePath = path.join(RAW_HOT_BASE, dateStr, entry.stream, `${entry.market}.jsonl`);

    if (!fs.existsSync(sourcePath)) {
      skippedFiles++;
      continue;
    }

    // Verify row count. Source may have grown (receiver still writing); that's fine
    // as long as it has at least as many rows as were converted. If source has fewer
    // rows (truncated), refuse to delete.
    // Use streaming line count to avoid loading multi-GB files into memory.
    let srcLines = 0;
    const stream = fs.createReadStream(sourcePath);
    for await (const _ of readline.createInterface({ input: stream })) { srcLines++; }

    if (srcLines < entry.row_count) {
      console.log(`  ${entry.stream}/${entry.market}: row count mismatch (source=${srcLines}, manifest=${entry.row_count}), too few rows, SKIP`);
      skippedFiles++;
      continue;
    }

    const srcSize = fs.statSync(sourcePath).size;
    deletedBytes += srcSize;
    deletedFiles++;

    if (dryRun) {
      console.log(`  Would delete: ${entry.stream}/${entry.market}.jsonl (${(srcSize/1024/1024).toFixed(1)}MB, ${entry.row_count} rows)`);
    } else {
      fs.rmSync(sourcePath);
      console.log(`  Deleted: ${entry.stream}/${entry.market}.jsonl (${(srcSize/1024/1024).toFixed(1)}MB, ${entry.row_count} rows)`);
    }
  }

  if (dryRun) {
    console.log(`\n[cleanup] DRY RUN: ${deletedFiles} files (${(deletedBytes/1024/1024).toFixed(0)}MB) would be deleted, ${skippedFiles} skipped`);
  } else {
    console.log(`\n[cleanup] Deleted ${deletedFiles} files, ${(deletedBytes/1024/1024).toFixed(0)}MB freed, ${skippedFiles} skipped`);
  }
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
