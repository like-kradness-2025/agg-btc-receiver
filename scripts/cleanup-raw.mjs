#!/usr/bin/env node
// scripts/cleanup-raw.mjs — safe cleanup of raw trade/book files
//
// After the downstream aggregator writes 1s features to data/1s_features/<date>/<market>.jsonl,
// this script safely deletes the corresponding raw 30-second window files
// (trades, book_updates) from data/live_v3/.
//
// Safety guarantees:
//   1. 300-second safety margin — only files whose mtime is >300s old are candidates
//   2. 30-second file granularity — entire HH-MM-SS.jsonl deleted only when ALL
//      30 contained 1-second rows exist in the features output
//   3. Verification — each second's ts is confirmed present in 1s_features before delete
//   4. Dry-run mode — preview what would be deleted without actually deleting
//
// Usage:
//   node scripts/cleanup-raw.mjs [options]
//
// Options:
//   --data <path>       Raw data directory (default: data/live_v3)
//   --features <path>   1s features directory (default: data/1s_features)
//   --safety-margin <s> Seconds before a file is eligible (default: 300)
//   --dry-run           Show what would be deleted without actually deleting
//   --help              Show this help

import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';

// ── Constants ──────────────────────────────────────────────────────────

const WIN_MS = 30000; // 30-second window
const SEC_MS = 1000;
const DEFAULT_DATA_DIR = 'data/live_v3';
const DEFAULT_FEATURES_DIR = 'data/1s_features';
const DEFAULT_SAFETY_MARGIN_SEC = 300;

const RAW_KINDS = ['trades', 'book_updates'];

// ── Argument parsing ───────────────────────────────────────────────────

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = {
    data: DEFAULT_DATA_DIR,
    features: DEFAULT_FEATURES_DIR,
    safetyMarginSec: DEFAULT_SAFETY_MARGIN_SEC,
    dryRun: false,
    verbose: false,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    const next = args[i + 1];
    switch (arg) {
      case '--data':
        opts.data = next;
        i++;
        break;
      case '--features':
        opts.features = next;
        i++;
        break;
      case '--safety-margin':
        opts.safetyMarginSec = parseInt(next, 10);
        i++;
        break;
      case '--dry-run':
        opts.dryRun = true;
        break;
      case '--verbose':
        opts.verbose = true;
        break;
      case '--help':
        console.log(`
cleanup-raw.mjs — safely delete raw trade/book files
                    after 1s features have been accumulated.

Usage:
  node scripts/cleanup-raw.mjs [options]

Options:
  --data <path>          Raw data directory (default: ${DEFAULT_DATA_DIR})
  --features <path>      1s features directory (default: ${DEFAULT_FEATURES_DIR})
  --safety-margin <s>    Seconds before a file is eligible for deletion (default: ${DEFAULT_SAFETY_MARGIN_SEC})
  --dry-run              Show what would be deleted without actually deleting
  --verbose              Print per-file skip/delete decisions
  --help                 Show this help

How it works:
  1. Scans ${DEFAULT_DATA_DIR}/{trades,book_updates}/ for 30s window files
  2. For each file, checks that its mtime is at least --safety-margin seconds old
  3. Verifies ALL 30 seconds in that window exist in the corresponding
     ${DEFAULT_FEATURES_DIR}/<date>/<market>.jsonl file
  4. If all 30 seconds are confirmed present → deletes the raw file
`);
        process.exit(0);
      default:
        if (arg.startsWith('--')) {
          console.error(`Unknown option: ${arg}`);
          process.exit(1);
        }
    }
  }

  return opts;
}

// ── Helpers ────────────────────────────────────────────────────────────

/**
 * Convert a 30s window start ms to UTC date dir + file base.
 * Matches the convention in raw-rotation-writer.mjs.
 */
function windowMsToParts(windowMs) {
  const d = new Date(windowMs);
  const Y = String(d.getUTCFullYear()).padStart(4, '0');
  const M = String(d.getUTCMonth() + 1).padStart(2, '0');
  const D = String(d.getUTCDate()).padStart(2, '0');
  const h = String(d.getUTCHours()).padStart(2, '0');
  const m = String(d.getUTCMinutes()).padStart(2, '0');
  const s = String(d.getUTCSeconds()).padStart(2, '0');
  return { dateDir: `${Y}-${M}-${D}`, fileBase: `${h}-${m}-${s}` };
}

/**
 * Parse a HH-MM-SS.jsonl filename back to window start ms.
 * @param {string} filename - e.g. "07-48-00.jsonl"
 * @param {string} dateDir - e.g. "2026-07-08"
 * @returns {number} window start ms (UTC)
 */
function filenameToWindowMs(filename, dateDir) {
  const base = filename.replace(/\.jsonl(?:\.open)?$/, '');
  const [h, m, s] = base.split('-').map(Number);
  const [Y, M, D] = dateDir.split('-').map(Number);
  return Date.UTC(Y, M - 1, D, h, m, s);
}

/**
 * Load all second timestamps present in a 1s_features JSONL file.
 * Returns a Set of integer millisecond timestamps.
 *
 * @param {string} filePath - path to <market>.jsonl in 1s_features
 * @returns {Promise<Set<number>>} set of ts values (integer ms)
 */
async function loadFeatureTimestamps(filePath) {
  const timestamps = new Set();
  if (!fs.existsSync(filePath)) return timestamps;

  const rl = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: 'utf8' }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    if (!line.trim()) continue;
    try {
      const obj = JSON.parse(line);
      if (typeof obj.ts === 'number') {
        timestamps.add(obj.ts);
      }
    } catch (_) {
      // Skip malformed lines
    }
  }

  return timestamps;
}

// ── Cache for loaded feature timestamps ────────────────────────────────

/** @type {Map<string, Set<number>>} key: "dateDir/market" */
const featureCache = new Map();

/**
 * Get the Set of ts values for a given date+market from 1s_features.
 * Caches results to avoid re-reading the same file.
 */
async function getFeatureTimestamps(featuresDir, dateDir, market) {
  const key = `${dateDir}/${market}`;
  let cached = featureCache.get(key);
  if (cached !== undefined) return cached;

  const filePath = path.join(featuresDir, dateDir, `${market}.jsonl`);
  const tsSet = await loadFeatureTimestamps(filePath);
  featureCache.set(key, tsSet);
  return tsSet;
}

// ── Discovery ──────────────────────────────────────────────────────────

/**
 * Discover all 30s raw window files for a given kind and market.
 *
 * @returns {Array<{ fullPath: string, market: string, kind: string, dateDir: string, fileBase: string, windowMs: number }>}
 */
function discoverRawFiles(dataDir, kind, market) {
  const results = [];
  const marketDir = path.join(dataDir, kind, market);

  if (!fs.existsSync(marketDir)) return results;

  // Direct child entries: could be date dirs (YYYY-MM-DD) or stray .jsonl files
  const dateEntries = fs.readdirSync(marketDir, { withFileTypes: true });

  for (const entry of dateEntries) {
    if (!entry.isDirectory()) continue;
    const dateDir = entry.name;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateDir)) continue;

    const datePath = path.join(marketDir, dateDir);
    let timeFiles;
    try {
      timeFiles = fs.readdirSync(datePath);
    } catch (_) {
      continue;
    }

    for (const tf of timeFiles) {
      // Match HH-MM-SS.jsonl or HH-MM-SS.jsonl.open
      if (!/^\d{2}-\d{2}-\d{2}\.jsonl(?:\.open)?$/.test(tf)) continue;

      const windowMs = filenameToWindowMs(tf, dateDir);
      results.push({
        fullPath: path.join(datePath, tf),
        market,
        kind,
        dateDir,
        fileBase: tf.replace(/\.jsonl(?:\.open)?$/, ''),
        windowMs,
      });
    }
  }

  return results;
}

/**
 * Discover all markets present across all raw kinds.
 */
function discoverMarkets(dataDir) {
  const markets = new Set();
  for (const kind of RAW_KINDS) {
    const kindDir = path.join(dataDir, kind);
    if (!fs.existsSync(kindDir)) continue;
    const entries = fs.readdirSync(kindDir, { withFileTypes: true });
    for (const entry of entries) {
      if (entry.isDirectory()) {
        markets.add(entry.name);
      }
    }
  }
  return Array.from(markets).sort();
}

// ── Main ───────────────────────────────────────────────────────────────

async function main() {
  const opts = parseArgs();
  const dataDir = path.resolve(opts.data);
  const featuresDir = path.resolve(opts.features);
  const safetyMarginMs = opts.safetyMarginSec * 1000;
  const nowMs = Date.now();
  const cutoffMs = nowMs - safetyMarginMs;

  if (!fs.existsSync(dataDir)) {
    console.error(`Data directory not found: ${dataDir}`);
    process.exit(1);
  }

  console.log(`Data dir:       ${dataDir}`);
  console.log(`Features dir:   ${featuresDir}`);
  console.log(`Safety margin:  ${opts.safetyMarginSec}s`);
  console.log(`Dry run:        ${opts.dryRun}`);
  console.log(`Verbose:        ${opts.verbose}`);
  console.log(`Cutoff time:    ${new Date(cutoffMs).toISOString()}`);
  console.log('');

  const markets = discoverMarkets(dataDir);
  console.log(`Markets found: ${markets.length} (${markets.join(', ')})`);
  console.log('');

  let totalDeleted = 0;
  let totalSkipped = 0;
  let totalAgeSkipped = 0;
  let totalMissingFeatures = 0;

  for (const market of markets) {
    const marketStats = { deleted: 0, skipped: 0, ageSkipped: 0, missingFeatures: 0 };

    for (const kind of RAW_KINDS) {
      const files = discoverRawFiles(dataDir, kind, market);
      if (files.length === 0) continue;

      for (const file of files) {
        // 1. Check age (safety margin)
        let stat;
        try {
          stat = fs.statSync(file.fullPath);
        } catch (_) {
          // File vanished — skip
          marketStats.skipped++;
          totalSkipped++;
          continue;
        }

        if (stat.mtimeMs > cutoffMs) {
          marketStats.ageSkipped++;
          totalAgeSkipped++;
          continue;
        }

        // 2. Load feature timestamps for this date+market
        const featureTs = await getFeatureTimestamps(featuresDir, file.dateDir, market);

        // 3. Verify ALL 30 seconds exist in features
        let allSecondsPresent = true;
        const missingSeconds = [];

        for (let offset = 0; offset < WIN_MS; offset += SEC_MS) {
          const secondTs = file.windowMs + offset;
          if (!featureTs.has(secondTs)) {
            allSecondsPresent = false;
            missingSeconds.push(new Date(secondTs).toISOString());
          }
        }

        if (!allSecondsPresent) {
          marketStats.missingFeatures++;
          totalMissingFeatures++;
          if (opts.verbose) {
            console.log(`  [SKIP] ${file.fullPath} — missing ${missingSeconds.length}/30 feature rows (first: ${missingSeconds[0]})`);
          }
          continue;
        }

        // 4. Delete
        if (opts.dryRun) {
          if (opts.verbose) {
            console.log(`  [DELETE] ${file.fullPath} (all 30 feature rows confirmed, age OK)`);
          }
          marketStats.deleted++;
          totalDeleted++;
        } else {
          try {
            fs.unlinkSync(file.fullPath);
            if (opts.verbose) {
              console.log(`  [DELETED] ${file.fullPath}`);
            }
            marketStats.deleted++;
            totalDeleted++;
          } catch (err) {
            console.error(`  [ERROR] Failed to delete ${file.fullPath}: ${err.message}`);
            marketStats.skipped++;
            totalSkipped++;
          }
        }
      }
    }

    const ms = marketStats;
    if (ms.deleted > 0 || ms.skipped > 0 || ms.ageSkipped > 0 || ms.missingFeatures > 0) {
      console.log(`  ${market}: deleted=${ms.deleted}, skipped(age)=${ms.ageSkipped}, skipped(missing)=${ms.missingFeatures}, skipped(err)=${ms.skipped}`);
    }
  }

  // 5. Clean up empty directories
  if (!opts.dryRun && totalDeleted > 0) {
    let dirsRemoved = 0;
    for (const market of markets) {
      for (const kind of RAW_KINDS) {
        const marketDir = path.join(dataDir, kind, market);
        if (!fs.existsSync(marketDir)) continue;
        // Walk date dirs
        const dateEntries = fs.readdirSync(marketDir, { withFileTypes: true }).filter(e => e.isDirectory());
        for (const entry of dateEntries) {
          const datePath = path.join(marketDir, entry.name);
          try {
            const remaining = fs.readdirSync(datePath).filter(f => !f.startsWith('.'));
            if (remaining.length === 0) {
              fs.rmdirSync(datePath);
              dirsRemoved++;
            }
          } catch (_) { /* skip */ }
        }
        // Remove market dir if empty
        try {
          const remaining = fs.readdirSync(marketDir).filter(f => !f.startsWith('.'));
          if (remaining.length === 0) {
            fs.rmdirSync(marketDir);
            dirsRemoved++;
          }
        } catch (_) { /* skip */ }
      }
    }
    if (dirsRemoved > 0) {
      console.log(`\nRemoved ${dirsRemoved} empty directories`);
    }
  }

  // 6. Summary
  console.log('');
  console.log('═══════════════════════════════════════');
  console.log(`  Files deleted:     ${totalDeleted}`);
  console.log(`  Files skipped (age < ${opts.safetyMarginSec}s): ${totalAgeSkipped}`);
  console.log(`  Files skipped (missing features): ${totalMissingFeatures}`);
  console.log(`  Files skipped (errors):           ${totalSkipped}`);
  console.log('═══════════════════════════════════════');

  if (opts.dryRun) {
    console.log('\n(Dry run — no files were actually deleted)');
  }
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
