#!/usr/bin/env node
// scripts/cleanup-raw.mjs — safe cleanup of converted raw trade files
//
// TFP mode deletes trade blocks only after committed manifest and finalized
// features_1s verification. Legacy timestamp mode remains available.
//
// Safety guarantees:
//   1. 300-second safety margin — only files whose mtime is >300s old are candidates
//   2. TFP manifest status must be committed and output shard must exist
//   3. Exactly 30 consecutive finalized rows must cover the raw block
//   4. A finalized, internally consistent 5m Footprint must cover the raw block
//   5. Input/output paths must remain inside configured roots
//   6. Dry-run mode — preview what would be deleted without actually deleting
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
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

// ── Constants ──────────────────────────────────────────────────────────

const WIN_MS = 30000; // 30-second window
const FOOTPRINT_MS = 300000; // 5-minute window
const SEC_MS = 1000;
const DEFAULT_DATA_DIR = 'data/live_v3';
const DEFAULT_FEATURES_DIR = 'data/1s_features';
const DEFAULT_DERIVED_DIR = 'data/derived/burst_features_v2';
const DEFAULT_SAFETY_MARGIN_SEC = 300;
const V4_SAFETY_MARGIN_SEC = 24 * 60 * 60;
const SNAPSHOT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const V4_SEGMENT_RE = /^\d{2}-\d+\.jsonl(?:\.active)?$/;
const HASH_CHUNK_BYTES = 64 * 1024; // fixed-size chunk for SHA-256 streaming

const RAW_KINDS = ['trades', 'book_updates'];

// ── Argument parsing ───────────────────────────────────────────────────

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = {
    data: DEFAULT_DATA_DIR,
    features: DEFAULT_FEATURES_DIR,
    safetyMarginSec: DEFAULT_SAFETY_MARGIN_SEC,
    safetyMarginExplicit: false,
    dryRun: false,
    verbose: false,
    derived: DEFAULT_DERIVED_DIR,
    tfpMode: true,
    rawLayout: 'auto',
    rawLayoutExplicit: false,
    consumerCursors: null,
    consumerManifests: null,
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
        opts.tfpMode = false;
        if (!opts.rawLayoutExplicit) opts.rawLayout = 'v3';
        i++;
        break;
      case '--derived':
        opts.derived = next;
        i++;
        break;
      case '--legacy-features':
        opts.tfpMode = false;
        if (!opts.rawLayoutExplicit) opts.rawLayout = 'v3';
        break;
      case '--raw-layout':
        if (!['auto', 'v3', 'v4'].includes(next)) {
          console.error(`Invalid --raw-layout: ${next} (expected auto, v3, or v4)`);
          process.exit(1);
        }
        opts.rawLayout = next;
        opts.rawLayoutExplicit = true;
        i++;
        break;
      case '--v3':
        opts.rawLayout = 'v3';
        opts.rawLayoutExplicit = true;
        break;
      case '--v4':
        opts.rawLayout = 'v4';
        opts.rawLayoutExplicit = true;
        break;
      case '--consumer-cursors':
        opts.consumerCursors = next;
        i++;
        break;
      case '--consumer-manifests':
        opts.consumerManifests = next;
        i++;
        break;
      case '--safety-margin':
        opts.safetyMarginSec = parseInt(next, 10);
        opts.safetyMarginExplicit = true;
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
  --raw-layout <mode>    Raw layout: auto, v3, or v4 (v4 is opt-in; auto detects segments)
  --features <path>      1s features directory (legacy mode, default: ${DEFAULT_FEATURES_DIR})
  --derived <path>       TFP derived root (default: ${DEFAULT_DERIVED_DIR})
  --legacy-features      Use legacy timestamp-only verification
  --consumer-cursors <path>    v4 consumer cursor proof root
  --consumer-manifests <path>  v4 consumer manifest proof root
  --safety-margin <s>    Seconds before a file is eligible for deletion (default: ${DEFAULT_SAFETY_MARGIN_SEC}; v4 default: ${V4_SAFETY_MARGIN_SEC})
  --dry-run              Show what would be deleted without actually deleting
  --verbose              Print per-file skip/delete decisions
  --help                 Show this help

How it works:
  1. Scans ${DEFAULT_DATA_DIR}/{trades,book_updates}/ for 30s window files
  2. For each file, checks that its mtime is at least --safety-margin seconds old
  3. Verifies ALL 30 seconds in that window exist in the corresponding
     ${DEFAULT_FEATURES_DIR}/<date>/<market>.jsonl file
  4. Verifies a finalized 5-minute Footprint durably covers the raw window
  5. If both proofs are present → deletes the raw file
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

function isV4SegmentName(name) {
  return V4_SEGMENT_RE.test(name);
}

/** Detect v4 without changing the v3 default when no v4 segment exists. */
export function detectRawLayout(dataDir) {
  for (const kind of RAW_KINDS) {
    const kindDir = path.join(dataDir, kind);
    if (!fs.existsSync(kindDir)) continue;
    const markets = fs.readdirSync(kindDir, { withFileTypes: true });
    for (const marketEntry of markets) {
      if (!marketEntry.isDirectory()) continue;
      const marketDir = path.join(kindDir, marketEntry.name);
      for (const dateEntry of fs.readdirSync(marketDir, { withFileTypes: true })) {
        if (!dateEntry.isDirectory()) continue;
        if (fs.readdirSync(path.join(marketDir, dateEntry.name)).some(isV4SegmentName)) return 'v4';
      }
    }
  }
  return 'v3';
}

/** Discover immutable and active v4 segments; callers must never delete active ones. */
export function discoverV4Segments(dataDir) {
  const results = [];
  for (const kind of RAW_KINDS) {
    const kindDir = path.join(dataDir, kind);
    if (!fs.existsSync(kindDir)) continue;
    for (const marketEntry of fs.readdirSync(kindDir, { withFileTypes: true })) {
      if (!marketEntry.isDirectory()) continue;
      const marketDir = path.join(kindDir, marketEntry.name);
      for (const dateEntry of fs.readdirSync(marketDir, { withFileTypes: true })) {
        if (!dateEntry.isDirectory() || !/^\d{4}-\d{2}-\d{2}$/.test(dateEntry.name)) continue;
        const dateDir = path.join(marketDir, dateEntry.name);
        for (const name of fs.readdirSync(dateDir)) {
          if (!isV4SegmentName(name)) continue;
          results.push({
            fullPath: path.join(dateDir, name),
            market: marketEntry.name,
            kind,
            dateDir: dateEntry.name,
            segmentName: name,
            active: name.endsWith('.active'),
          });
        }
      }
    }
  }
  return results.sort((a, b) => a.fullPath.localeCompare(b.fullPath));
}

function proofRoots(value) {
  return (Array.isArray(value) ? value : [value]).filter(Boolean).map((root) => path.resolve(root));
}

function sha256File(filePath) {
  let fd;
  try {
    fd = fs.openSync(filePath, 'r');
    const hash = crypto.createHash('sha256');
    const buffer = Buffer.alloc(HASH_CHUNK_BYTES);
    let bytesRead;
    while ((bytesRead = fs.readSync(fd, buffer, 0, HASH_CHUNK_BYTES, null)) > 0) {
      hash.update(buffer.subarray(0, bytesRead));
    }
    return hash.digest('hex');
  } catch (_) {
    return null;
  } finally {
    if (fd !== undefined) {
      try { fs.closeSync(fd); } catch (_) { /* ignore */ }
    }
  }
}

function sha256FilePrefix(filePath, prefixSize) {
  if (!Number.isSafeInteger(prefixSize) || prefixSize < 0) return null;
  let fd;
  try {
    fd = fs.openSync(filePath, 'r');
    const hash = crypto.createHash('sha256');
    const buffer = Buffer.alloc(HASH_CHUNK_BYTES);
    let offset = 0;
    while (offset < prefixSize) {
      const bytesRead = fs.readSync(fd, buffer, 0, Math.min(HASH_CHUNK_BYTES, prefixSize - offset), offset);
      if (bytesRead === 0) return null;
      hash.update(buffer.subarray(0, bytesRead));
      offset += bytesRead;
    }
    return hash.digest('hex');
  } catch (_) {
    return null;
  } finally {
    if (fd !== undefined) {
      try { fs.closeSync(fd); } catch (_) { /* ignore */ }
    }
  }
}

function proofFiles(roots) {
  const files = [];
  const walk = (dir) => {
    if (!fs.existsSync(dir)) return;
    let entries;
    try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch (_) { return; }
    for (const entry of entries) {
      const file = path.join(dir, entry.name);
      if (entry.isDirectory()) walk(file);
      else if (/\.json(?:l)?$/.test(entry.name)) files.push(file);
    }
  };
  for (const root of proofRoots(roots)) {
    if (fs.existsSync(root) && fs.statSync(root).isDirectory()) walk(root);
    else if (fs.existsSync(root)) files.push(root);
  }
  return files;
}

function parseProofFile(filePath) {
  try {
    const value = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    return [{ value, filePath }];
  } catch (_) {
    return [];
  }
}

function sourcePathMatches(value, segment) {
  const expected = path.resolve(segment.fullPath).replaceAll(path.sep, '/');
  const suffix = `/${segment.kind}/${segment.market}/${segment.dateDir}/${segment.segmentName}`;
  if (typeof value !== 'string') return false;
  const normalized = value.replaceAll('\\', '/');
  const stripActive = (name) => name.endsWith('.active') ? name.slice(0, -'.active'.length) : name;
  const normalizedComparable = stripActive(normalized);
  const expectedComparable = stripActive(expected);
  if (normalizedComparable === expectedComparable) return true;
  const logicalSuffix = stripActive(suffix).replace(/^\//, '');
  return normalizedComparable === logicalSuffix || normalizedComparable.endsWith(`/${logicalSuffix}`);
}

function objectHasSource(value, segment, inheritedKey = '') {
  if (!value || typeof value !== 'object') return sourcePathMatches(inheritedKey, segment);
  if (Array.isArray(value)) return value.some((item) => objectHasSource(item, segment, inheritedKey));
  for (const [key, child] of Object.entries(value)) {
    if (/source|input|segment|file|path/i.test(key) && typeof child === 'string' && sourcePathMatches(child, segment)) return true;
    if (objectHasSource(child, segment, key)) return true;
  }
  return sourcePathMatches(inheritedKey, segment);
}

function valuesForKeys(value, pattern, out = []) {
  if (!value || typeof value !== 'object') return out;
  if (Array.isArray(value)) {
    for (const item of value) valuesForKeys(item, pattern, out);
    return out;
  }
  for (const [key, child] of Object.entries(value)) {
    if (pattern.test(key)) out.push(child);
    valuesForKeys(child, pattern, out);
  }
  return out;
}

function proofRecords(roots) {
  return proofFiles(roots).flatMap((filePath) => parseProofFile(filePath));
}

function hashValues(record) {
  return valuesForKeys(record.value, /^(?:source|input|raw|content)?_?sha256$|^(?:source|content|file)_hash$/i)
    .filter((value) => typeof value === 'string' && value.length > 0);
}

function prefixProofs(value, out = []) {
  if (!value || typeof value !== 'object') return out;
  if (Array.isArray(value)) {
    for (const item of value) prefixProofs(item, out);
    return out;
  }
  const size = Object.entries(value).find(([key]) => /^source_?prefix_?(?:size|bytes)$/i.test(key))?.[1];
  const hash = Object.entries(value).find(([key]) => /^source_?prefix_?(?:sha256|hash)$/i.test(key))?.[1];
  const end = Object.entries(value).find(([key]) => /^(?:byte_?)?offset_?end$/i.test(key))?.[1];
  if (size !== undefined && typeof hash === 'string') out.push({ size: Number(size), hash, end: Number(end) });
  for (const child of Object.values(value)) prefixProofs(child, out);
  return out;
}

function cursorRecordCoversSegment(record, segment, byteLength, fileHash) {
  if (!objectHasSource(record.value, segment, path.basename(record.filePath))) return false;
  const offsets = valuesForKeys(record.value, /^(?:byte_)?offset$|^(?:consumed|end|last_byte)_offset$|^(?:consumed|processed)_bytes$/i)
    .map(Number).filter(Number.isSafeInteger);
  if (!offsets.some((offset) => offset >= byteLength)) return false;
  const partials = valuesForKeys(record.value, /^partial_line(?:_base64)?$/i);
  if (!partials.every((partial) => partial === '' || partial === null || partial === undefined)) return false;
  const hashes = hashValues(record);
  if (hashes.length > 0 && !hashes.some((hash) => hash.toLowerCase() === fileHash)) {
    const prefixVerified = prefixProofs(record.value).some(({ size, hash, end }) => (
      Number.isSafeInteger(size)
        && Number.isSafeInteger(end)
        && size === end
        && size >= 0
        && size <= byteLength
        && typeof hash === 'string'
        && sha256FilePrefix(segment.fullPath, size) === hash.toLowerCase()
    ));
    if (!prefixVerified) return false;
  }
  return true;
}

function manifestRecordCommitsSegment(record, segment, byteLength, fileHash) {
  if (!objectHasSource(record.value, segment, path.basename(record.filePath))) return false;
  const statuses = valuesForKeys(record.value, /^status$/i).map((value) => String(value).toLowerCase());
  const verified = valuesForKeys(record.value, /^(?:verified|committed|consumer_committed)$/i);
  if (statuses.some((status) => ['intent', 'pending', 'failed', 'quarantined'].includes(status))) return false;
  if (!statuses.some((status) => ['committed', 'complete', 'completed', 'verified'].includes(status))
      && !verified.some((value) => value === true)) return false;
  if (verified.some((value) => value === false)) return false;
  const sizes = valuesForKeys(record.value, /^(?:source|input|raw)?_?(?:byte_)?(?:length|size|bytes)$/i)
    .map(Number).filter(Number.isSafeInteger);
  const hashes = hashValues(record);
  if (sizes.every((size) => size === byteLength)
      && hashes.length > 0
      && hashes.some((hash) => hash.toLowerCase() === fileHash)) return true;

  return prefixProofs(record.value).some(({ size, hash, end }) => (
    Number.isSafeInteger(size)
      && Number.isSafeInteger(end)
      && size === end
      && size >= 0
      && size <= byteLength
      && typeof hash === 'string'
      && sha256FilePrefix(segment.fullPath, size) === hash.toLowerCase()
  ));
}

/**
 * Verify the v4 deletion evidence without mutating the segment.
 * Both proofs are mandatory: every consumer cursor at EOF and a committed manifest.
 */
export function verifyV4ClosedSegment(segment, { cursorRoots = [], manifestRoots = [], quarantineCheckpointRoots = [] } = {}) {
  const name = segment.segmentName || path.basename(segment.fullPath || '');
  if (!segment.fullPath || name.endsWith('.active')) return { ok: false, reason: 'active-segment' };
  if (!isV4SegmentName(name)) return { ok: false, reason: 'not-a-v4-segment' };
  let stat;
  try { stat = fs.statSync(segment.fullPath); } catch (_) { return { ok: false, reason: 'segment-missing' }; }
  const fileHash = sha256File(segment.fullPath);
  if (!fileHash) return { ok: false, reason: 'segment-hash-unavailable' };

  const cursorRecords = proofRecords(cursorRoots);
  const targetCursors = cursorRecords.filter((record) => objectHasSource(record.value, segment, path.basename(record.filePath)));
  if (targetCursors.length === 0) return { ok: false, reason: 'missing-consumer-cursor-proof' };
  if (!targetCursors.every((record) => cursorRecordCoversSegment(record, segment, stat.size, fileHash))) {
    return { ok: false, reason: 'incomplete-consumer-cursor-proof' };
  }

  const manifestRecords = proofRecords(manifestRoots);
  const targetManifests = manifestRecords.filter((record) => objectHasSource(record.value, segment, path.basename(record.filePath)));
  if (targetManifests.length === 0) return { ok: false, reason: 'missing-consumer-manifest-proof' };
  if (!targetManifests.some((record) => manifestRecordCommitsSegment(record, segment, stat.size, fileHash))) {
    return { ok: false, reason: 'missing-consumer-manifest-proof' };
  }

  const qcRecords = proofRecords(quarantineCheckpointRoots);
  const targetQc = qcRecords.filter((record) => objectHasSource(record.value, segment, path.basename(record.filePath)));
  if (targetQc.length > 0) {
    return { ok: false, reason: 'quarantine-or-checkpoint-reference' };
  }

  return { ok: true, cursor: true, manifest: true, byteLength: stat.size };
}

function loadTfpManifest(derivedDir, market) {
  const manifestPath = path.join(derivedDir, 'manifests', `${market}.json`);
  try {
    const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
    if (!manifest || typeof manifest !== 'object' || !manifest.processed_blocks) return null;
    return manifest;
  } catch (_) { return null; }
}

const rollupManifestCache = new Map();
const footprintProofCache = new Map();

function verifyFootprintWindow(derivedDir, file) {
  const intervalMs = Math.floor(file.windowMs / FOOTPRINT_MS) * FOOTPRINT_MS;
  const { dateDir, fileBase } = windowMsToParts(intervalMs);
  const outputPath = path.join(derivedDir, 'footprint_5m', file.market, dateDir, `${fileBase}.jsonl`);
  const cacheKey = outputPath;
  if (footprintProofCache.has(cacheKey)) return footprintProofCache.get(cacheKey);
  let result;
  try {
    const rows = fs.readFileSync(outputPath, 'utf8').split('\n').filter(Boolean).map((line) => JSON.parse(line));
    const meta = rows[0];
    if (meta?.type !== 'meta' || meta.ts !== intervalMs || meta.market !== file.market) {
      result = { ok: false, reason: 'footprint-meta-mismatch' };
    } else if (meta.finalized !== true || meta.coverage !== 1 || meta.source_seconds !== 300) {
      result = { ok: false, reason: 'footprint-not-finalized' };
    } else {
      const cells = rows.slice(1);
      const tradeCount = cells.reduce((sum, row) =>
        sum + Number(row.buy_count || 0) + Number(row.sell_count || 0), 0);
      const notional = cells.reduce((sum, row) => sum + Number(row.total_notional || 0), 0);
      const expectedCount = Number(meta.source_trade_count);
      const expectedNotional = Number(meta.source_notional);
      const tolerance = Math.max(0.01, Math.abs(expectedNotional) * 1e-9);
      if (!Number.isFinite(expectedCount) || tradeCount !== expectedCount) {
        result = { ok: false, reason: 'footprint-trade-count-mismatch' };
      } else if (!Number.isFinite(expectedNotional) || Math.abs(notional - expectedNotional) > tolerance) {
        result = { ok: false, reason: 'footprint-notional-mismatch' };
      } else {
        result = { ok: true };
      }
    }
  } catch (_) {
    result = { ok: false, reason: 'footprint-missing-or-invalid' };
  }
  footprintProofCache.set(cacheKey, result);
  return result;
}

function loadRollupManifest(derivedDir, market) {
  const cacheKey = `${derivedDir}/${market}`;
  if (rollupManifestCache.has(cacheKey)) return rollupManifestCache.get(cacheKey);
  const manifestPath = path.join(derivedDir, 'manifests', 'features_30s', `${market}.json`);
  let manifest = null;
  try {
    manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
    if (!manifest || typeof manifest !== 'object' || !manifest.processed_windows) manifest = null;
  } catch (_) { /* missing/corrupt manifest */ }
  rollupManifestCache.set(cacheKey, manifest);
  return manifest;
}

function verifyFinalizedRollup(derivedDir, file, sourceManifestKey) {
  const manifest = loadRollupManifest(derivedDir, file.market);
  if (!manifest) return false;
  const matches = Object.values(manifest.processed_windows).filter((record) =>
    record && record.window_start_ms === file.windowMs && record.status === 'committed'
      && (!record.source_manifest_key || record.source_manifest_key === sourceManifestKey));
  if (matches.length !== 1) return false;
  const record = matches[0];
  if (!record.output_path) return false;
  const outputPath = path.resolve(record.output_path);
  if (outputPath !== derivedDir && !outputPath.startsWith(`${derivedDir}${path.sep}`)) return false;
  if (!fs.existsSync(outputPath)) return false;
  try {
    const rows = fs.readFileSync(outputPath, 'utf8').split('\n').filter(Boolean).map((line) => JSON.parse(line));
    const quality = rows[0]?._quality;
    return rows.length === 1
      && rows[0].ts === file.windowMs
      && quality?.source_layer === 'features_1s'
      && quality?.source_window_count === 30
      && quality?.coverage === 1
      && quality?.has_missing_input === false
      && quality?.finalized === true;
  } catch (_) { return false; }
}

/** Verify that exactly one finalized TFP output shard durably represents a raw block. */
function verifyTfpWindow(derivedDir, file) {
  const manifest = loadTfpManifest(derivedDir, file.market);
  if (!manifest) return { ok: false, reason: 'missing-or-corrupt-tfp-manifest' };
  const records = Object.entries(manifest.processed_blocks).filter(([, record]) =>
    record && record.block_start_ms === file.windowMs && record.status === 'committed');
  if (records.length !== 1) return { ok: false, reason: `committed-manifest-records=${records.length}` };
  const [manifestKey, record] = records[0];
  if (record.input_path && path.resolve(record.input_path) !== path.resolve(file.fullPath)) {
    return { ok: false, reason: 'manifest-input-path-mismatch' };
  }
  if (record.output_path) {
    const outputPath = path.resolve(record.output_path);
    if (outputPath !== derivedDir && !outputPath.startsWith(`${derivedDir}${path.sep}`)) {
      return { ok: false, reason: 'manifest-output-path-outside-derived' };
    }
    if (!fs.existsSync(outputPath)) return { ok: false, reason: 'committed-output-missing' };
  }
  const outputPath = record.output_path
    ? path.resolve(record.output_path)
    : path.join(derivedDir, 'features_1s', file.market, file.dateDir, `${file.fileBase}.jsonl`);
  if (!fs.existsSync(outputPath)) return { ok: false, reason: 'features_1s-shard-missing' };
  let rows;
  try {
    rows = fs.readFileSync(outputPath, 'utf8').split('\n').filter(Boolean).map((line) => JSON.parse(line));
  } catch (_) { return { ok: false, reason: 'features_1s-shard-invalid-json' }; }
  if (rows.length !== 30) return { ok: false, reason: `features_1s-rows=${rows.length}` };
  const rowsFinalized = rows.every((row) => row._quality?.finalized === true);
  if (!rowsFinalized && !verifyFinalizedRollup(derivedDir, file, manifestKey)) {
    return { ok: false, reason: 'features_1s-not-finalized' };
  }
  for (let i = 0; i < 30; i++) {
    const row = rows[i];
    if (row.ts !== file.windowMs + i * SEC_MS) return { ok: false, reason: 'features_1s-timestamp-coverage-mismatch' };
    if (rowsFinalized && row._quality?.finalized !== true) return { ok: false, reason: 'features_1s-not-finalized' };
  }
  const footprintProof = verifyFootprintWindow(derivedDir, file);
  if (!footprintProof.ok) return footprintProof;
  return { ok: true, bookSeededRows: rows.filter((row) => row._quality?.book_seeded === true).length };
}

function bookFileContainsSnapshot(filePath) {
  try {
    for (const line of fs.readFileSync(filePath, 'utf8').split('\n')) {
      if (!line.trim()) continue;
      if (JSON.parse(line).type === 'snapshot') return true;
    }
    return false;
  } catch (_) {
    return true;
  }
}

function cleanupSeedSnapshots(dataDir, nowMs, dryRun, verbose) {
  const root = path.join(dataDir, 'snapshots');
  if (!fs.existsSync(root)) return 0;
  const cutoff = nowMs - SNAPSHOT_RETENTION_MS;
  let deleted = 0;
  for (const marketEntry of fs.readdirSync(root, { withFileTypes: true })) {
    if (!marketEntry.isDirectory()) continue;
    const marketDir = path.join(root, marketEntry.name);
    for (const dateEntry of fs.readdirSync(marketDir, { withFileTypes: true })) {
      if (!dateEntry.isDirectory()) continue;
      const dateDir = path.join(marketDir, dateEntry.name);
      for (const name of fs.readdirSync(dateDir)) {
        if (!name.endsWith('.jsonl')) continue;
        const file = path.join(dateDir, name);
        let stat;
        try { stat = fs.statSync(file); } catch { continue; }
        if (stat.mtimeMs >= cutoff) continue;
        if (dryRun) {
          if (verbose) console.log(`  [DELETE] ${file} (seed snapshot retention)`);
        } else {
          try { fs.unlinkSync(file); } catch { continue; }
        }
        deleted++;
      }
      if (!dryRun && fs.readdirSync(dateDir).length === 0) fs.rmdirSync(dateDir);
    }
    if (!dryRun && fs.readdirSync(marketDir).length === 0) fs.rmdirSync(marketDir);
  }
  return deleted;
}

function resolvedProofRoots(opts, dataDir, derivedDir) {
  const cursors = opts.consumerCursors
    ? [path.resolve(opts.consumerCursors)]
    : [path.join(dataDir, 'consumer-cursors'), path.join(dataDir, 'consumers', 'cursors'), path.join(derivedDir, 'consumer-cursors')];
  const manifests = opts.consumerManifests
    ? [path.resolve(opts.consumerManifests)]
    : [path.join(dataDir, 'consumer-manifests'), path.join(dataDir, 'consumers', 'manifests'), path.join(derivedDir, 'consumer-manifests')];
  const quarantineCheckpoint = [
    path.join(dataDir, 'quarantine'),
    path.join(dataDir, 'checkpoints'),
    path.join(derivedDir, 'quarantine'),
    path.join(derivedDir, 'manifests', 'checkpoints'),
  ];
  return { cursors, manifests, quarantineCheckpoint };
}

function resolveRawLayout(opts, dataDir) {
  return opts.rawLayout === 'auto' ? detectRawLayout(dataDir) : opts.rawLayout;
}

function cleanupV4(dataDir, derivedDir, opts) {
  const { cursors, manifests, quarantineCheckpoint } = resolvedProofRoots(opts, dataDir, derivedDir);
  const safetyMarginSec = opts.safetyMarginExplicit ? opts.safetyMarginSec : V4_SAFETY_MARGIN_SEC;
  const cutoffMs = Date.now() - safetyMarginSec * 1000;
  const segments = discoverV4Segments(dataDir);
  const stats = { deleted: 0, skipped: 0, active: 0, ageSkipped: 0, missingProof: 0 };

  console.log(`Data dir:             ${dataDir}`);
  console.log('Raw layout:           v4');
  console.log(`Consumer cursors:     ${cursors.join(', ')}`);
  console.log(`Consumer manifests:   ${manifests.join(', ')}`);
  console.log(`Quarantine/checkpoint roots: ${quarantineCheckpoint.join(', ')}`);
  console.log(`Safety margin:        ${safetyMarginSec}s`);
  console.log(`Dry run:              ${opts.dryRun}`);
  console.log(`Segments found:       ${segments.length}`);
  console.log('');

  for (const segment of segments) {
    if (segment.active) {
      stats.active++;
      if (opts.verbose) console.log(`  [SKIP] ${segment.fullPath} — active-segment`);
      continue;
    }
    let stat;
    try { stat = fs.statSync(segment.fullPath); } catch (_) { stats.skipped++; continue; }
    if (stat.mtimeMs > cutoffMs) {
      stats.ageSkipped++;
      if (opts.verbose) console.log(`  [SKIP] ${segment.fullPath} — safety-margin`);
      continue;
    }
    const proof = verifyV4ClosedSegment(segment, { cursorRoots: cursors, manifestRoots: manifests, quarantineCheckpointRoots: quarantineCheckpoint });
    if (!proof.ok) {
      stats.missingProof++;
      if (opts.verbose) console.log(`  [SKIP] ${segment.fullPath} — ${proof.reason}`);
      continue;
    }
    if (opts.dryRun) {
      console.log(`  [DELETE] ${segment.fullPath} (v4 closed + cursor EOF + manifest committed)`);
      stats.deleted++;
      continue;
    }
    try {
      fs.unlinkSync(segment.fullPath);
      stats.deleted++;
      if (opts.verbose) console.log(`  [DELETED] ${segment.fullPath}`);
    } catch (err) {
      stats.skipped++;
      console.error(`  [ERROR] Failed to delete ${segment.fullPath}: ${err.message}`);
    }
  }

  console.log(`Summary: deleted=${stats.deleted}, active=${stats.active}, skipped(age)=${stats.ageSkipped}, skipped(proof)=${stats.missingProof}, skipped(err)=${stats.skipped}`);
  return stats;
}

// ── Main ───────────────────────────────────────────────────────────────

async function main() {
  const opts = parseArgs();
  const dataDir = path.resolve(opts.data);
  const featuresDir = path.resolve(opts.features);
  const derivedDir = path.resolve(opts.derived);
  const safetyMarginMs = opts.safetyMarginSec * 1000;
  const nowMs = Date.now();
  const cutoffMs = nowMs - safetyMarginMs;

  if (!fs.existsSync(dataDir)) {
    console.error(`Data directory not found: ${dataDir}`);
    process.exit(1);
  }

  if (resolveRawLayout(opts, dataDir) === 'v4') {
    cleanupV4(dataDir, derivedDir, opts);
    return;
  }

  console.log(`Data dir:       ${dataDir}`);
  console.log(`Features dir:   ${featuresDir}`);
  console.log(`Derived dir:    ${derivedDir}`);
  console.log(`Cleanup mode:   ${opts.tfpMode ? 'TFP manifest/finalized' : 'legacy timestamps'}`);
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

    const kinds = opts.tfpMode ? ['trades'] : RAW_KINDS;
    for (const kind of kinds) {
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

        if (opts.tfpMode) {
          const proof = verifyTfpWindow(derivedDir, file);
          if (!proof.ok) {
            marketStats.missingFeatures++;
            totalMissingFeatures++;
            if (opts.verbose) console.log(`  [SKIP] ${file.fullPath} — ${proof.reason}`);
            continue;
          }
          if (kind === 'book_updates') {
            if (bookFileContainsSnapshot(file.fullPath)) {
              marketStats.missingFeatures++; totalMissingFeatures++;
              if (opts.verbose) console.log(`  [SKIP] ${file.fullPath} — snapshot-retained`);
              continue;
            }
            if (proof.bookSeededRows === 0) {
              marketStats.missingFeatures++; totalMissingFeatures++;
              if (opts.verbose) console.log(`  [SKIP] ${file.fullPath} — no-seeded-book-output`);
              continue;
            }
          }
          if (opts.dryRun) {
            console.log(`  [DELETE] ${file.fullPath} (TFP + Footprint finalized proof)`);
            marketStats.deleted++; totalDeleted++;
          } else {
            try { fs.unlinkSync(file.fullPath); marketStats.deleted++; totalDeleted++; if (opts.verbose) console.log(`  [DELETED] ${file.fullPath}`); }
            catch (err) { console.error(`  [ERROR] Failed to delete ${file.fullPath}: ${err.message}`); marketStats.skipped++; totalSkipped++; }
          }
          continue;
        }

        // 2. Load feature timestamps for this date+market (legacy mode)
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

  const deletedSnapshots = cleanupSeedSnapshots(dataDir, nowMs, opts.dryRun, opts.verbose);
  if (deletedSnapshots > 0) {
    console.log(`Seed snapshots ${opts.dryRun ? 'eligible' : 'deleted'}: ${deletedSnapshots} (retention=${SNAPSHOT_RETENTION_MS / 86400000}d)`);
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

if (process.argv[1] && path.resolve(fileURLToPath(import.meta.url)) === path.resolve(process.argv[1])) {
  main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}
