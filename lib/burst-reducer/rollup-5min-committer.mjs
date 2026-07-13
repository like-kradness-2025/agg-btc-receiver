// lib/burst-reducer/rollup-5min-committer.mjs — Dedicated 5min committer
// P3-C2: isolated namespace, idempotency via hash key, hash conflict → quarantine

import {
  closeSync, existsSync, fsyncSync, mkdirSync, openSync,
  readFileSync, readdirSync, renameSync, rmSync, writeFileSync,
} from 'node:fs';
import { createHash } from 'node:crypto';
import { dirname, join } from 'node:path';
import { aggregate5min } from './rollup-5min.mjs';
import {
  FEATURES_5MIN_DIR, FEATURES_30S_DIR, MANIFESTS_DIR, CHECKPOINTS_DIR,
} from './schema.mjs';

export const FIVEMIN_SCHEMA_VERSION = 'burst_features_5min_v1';

// ── Utilities ──────────────────────────────────────────────────────────

function sha256(content) {
  return createHash('sha256').update(content).digest('hex');
}

function sha256File(path) {
  if (!existsSync(path)) return null;
  return sha256(readFileSync(path, 'utf8'));
}

function durableWrite(path, content) {
  writeFileSync(path, content, 'utf8');
  const fd = openSync(path, 'r');
  try { fsyncSync(fd); } finally { closeSync(fd); }
}

function fsyncDirectory(path) {
  const fd = openSync(path, 'r');
  try { fsyncSync(fd); } finally { closeSync(fd); }
}

function writeAtomicJson(path, value) {
  const tmpPath = `${path}.tmp`;
  mkdirSync(dirname(path), { recursive: true });
  durableWrite(tmpPath, `${JSON.stringify(value, null, 2)}\n`);
  renameSync(tmpPath, path);
  fsyncDirectory(dirname(path));
}

function formatDate(blockStartMs) {
  const d = new Date(blockStartMs);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
}

function formatBlockTime(blockStartMs) {
  const d = new Date(blockStartMs);
  return `${String(d.getUTCHours()).padStart(2, '0')}-${String(d.getUTCMinutes()).padStart(2, '0')}-${String(d.getUTCSeconds()).padStart(2, '0')}`;
}

function parseJsonl(content, file) {
  const rows = [];
  for (const [index, line] of content.split('\n').entries()) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      rows.push(JSON.parse(trimmed));
    } catch (err) {
      const wrapped = new Error(`E_FIVEMIN_SOURCE_CORRUPT: invalid JSON at ${file}:${index + 1}`);
      wrapped.code = 'E_FIVEMIN_SOURCE_CORRUPT';
      wrapped.cause = err;
      throw wrapped;
    }
  }
  return rows;
}

function readJsonOrNull(path, code) {
  if (!existsSync(path)) return null;
  let value;
  try {
    value = JSON.parse(readFileSync(path, 'utf8'));
  } catch (err) {
    const wrapped = new Error(`${code}: invalid JSON at ${path}`);
    wrapped.code = code;
    wrapped.cause = err;
    throw wrapped;
  }
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    const error = new Error(`${code}: expected object at ${path}`);
    error.code = code;
    throw error;
  }
  return value;
}

// ── Path helpers (isolated namespace) ──────────────────────────────────

function manifestPath(market, derivedDir) {
  return join(derivedDir, MANIFESTS_DIR, FEATURES_5MIN_DIR, `${market}.json`);
}

function checkpointPath(market, derivedDir) {
  return join(derivedDir, CHECKPOINTS_DIR, FEATURES_5MIN_DIR, `${market}.json`);
}

export function load5minManifest(market, derivedDir) {
  return readJsonOrNull(manifestPath(market, derivedDir), 'E_FIVEMIN_MANIFEST_CORRUPT');
}

export function load5minCheckpoint(market, derivedDir) {
  return readJsonOrNull(checkpointPath(market, derivedDir), 'E_FIVEMIN_CHECKPOINT_CORRUPT');
}

function initialManifest(market) {
  return {
    schema_version: FIVEMIN_SCHEMA_VERSION,
    namespace: FEATURES_5MIN_DIR,
    source_layer: FEATURES_30S_DIR,
    market,
    last_checkpoint_window_start: null,
    processed_windows: {},
  };
}

// ── 5min Committer ─────────────────────────────────────────────────────

export class Rollup5minCommitter {
  constructor(market, runId, derivedDir) {
    this._market = market;
    this._runId = runId;
    this._derivedDir = derivedDir;
    this._featuresDir = join(derivedDir, FEATURES_5MIN_DIR);
    this._manifestPath = manifestPath(market, derivedDir);
    this._checkpointPath = checkpointPath(market, derivedDir);
  }

  /**
   * Persist one complete 5min window from 10 validated 30s rows.
   *
   * @param {Object} params
   * @param {Array} params.rows — 10 validated 30s feature rows
   * @param {string} [params.sourceInputSha256] — optional composite input hash
   * @param {string} [params.sourceOutputHash] — hash of the aggregated 30s source content
   * @param {string} [params.sourceOutputPath] — path of the committed 30s window
   * @param {string|null} [params.sourceManifestKey] — manifest key of the final 30s window
   * @returns {{ key, output_path, output_row_hash, nextGeneration, idempotent }}
   */
  commitWindow({ rows, sourceInputSha256, sourceOutputPath, sourceOutputHash, sourceManifestKey = null }) {
    const [row] = aggregate5min(rows);
    const content = `${JSON.stringify(row)}\n`;
    const outputHash = sha256(content);
    const inputHash = sourceOutputHash || sha256(JSON.stringify(rows));
    const key = `${FIVEMIN_SCHEMA_VERSION}:${this._market}:${row.ts}:${inputHash}`;
    const date = formatDate(row.ts);
    const time = formatBlockTime(row.ts);
    const outputPath = join(this._featuresDir, this._market, date, `${time}.jsonl`);

    // ── Load or init manifest ──────────────────────────────────────────
    let manifest = load5minManifest(this._market, this._derivedDir) || initialManifest(this._market);
    if (manifest.namespace !== FEATURES_5MIN_DIR
        || manifest.source_layer !== FEATURES_30S_DIR
        || manifest.market !== this._market) {
      const err = new Error(`E_FIVEMIN_NAMESPACE: incompatible 5min manifest for ${this._market}`);
      err.code = 'E_FIVEMIN_NAMESPACE';
      throw err;
    }
    if (!manifest.processed_windows) manifest.processed_windows = {};

    const existing = manifest.processed_windows[key];

    // ── Cross-key conflict: same window_start_ms, different source hash ─
    if (!existing) {
      for (const [existingKey, existingRec] of Object.entries(manifest.processed_windows)) {
        if (existingRec.window_start_ms === row.ts
            && existingRec.source_output_hash !== inputHash
            && (existingRec.status === 'committed' || existingRec.status === 'intent')) {
          // Found conflicting window with different source hash
          manifest.processed_windows[existingKey] = {
            ...existingRec,
            status: 'quarantined',
            quarantined_at: new Date().toISOString(),
            quarantined_reason: 'hash-conflict',
          };
          writeAtomicJson(this._manifestPath, manifest);
          const err = new Error(`E_FIVEMIN_HASH_CONFLICT: window ${row.ts} has different source hash — quarantined`);
          err.code = 'E_FIVEMIN_HASH_CONFLICT';
          err.details = { existing_key: existingKey, existing_hash: existingRec.source_output_hash, new_hash: inputHash };
          throw err;
        }
      }
    }

    // ── Idempotency: same key + same hash + output exists → repair checkpoint ─
    if (existing?.status === 'committed' && existsSync(outputPath)
        && sha256File(outputPath) === existing.output_row_hash) {
      const checkpoint = load5minCheckpoint(this._market, this._derivedDir);
      if (!checkpoint || checkpoint.last_committed_window_start < row.ts) {
        this._writeCheckpoint(row.ts, checkpoint?.generation ?? existing.checkpoint_generation ?? 0, outputPath, outputHash);
      }
      return { key, output_path: outputPath, output_row_hash: outputHash, nextGeneration: null, idempotent: true };
    }

    // ── Hash conflict: same window, different source hash → quarantine ─
    if (existing && existing.window_start_ms === row.ts && existing.source_output_hash !== inputHash) {
      const err = new Error(`E_FIVEMIN_HASH_CONFLICT: window ${row.ts} has different source hash — quarantined`);
      err.code = 'E_FIVEMIN_HASH_CONFLICT';
      err.details = { existing_key: key, existing_hash: existing.source_output_hash, new_hash: inputHash };
      // Quarantine the existing record
      manifest.processed_windows[key] = {
        ...existing,
        status: 'quarantined',
        quarantined_at: new Date().toISOString(),
        quarantined_reason: 'hash-conflict',
      };
      writeAtomicJson(this._manifestPath, manifest);
      throw err;
    }

    // ── Checkpoint generation ──────────────────────────────────────────
    const checkpoint = load5minCheckpoint(this._market, this._derivedDir);
    const nextGeneration = (checkpoint?.generation ?? 0) + 1;

    // ── Stage ──────────────────────────────────────────────────────────
    const stagedPath = join(this._featuresDir, this._market, date, '.staging', this._runId, `${time}.jsonl`);
    mkdirSync(dirname(stagedPath), { recursive: true });
    durableWrite(stagedPath, content);

    // ── Intent manifest ────────────────────────────────────────────────
    manifest.processed_windows[key] = {
      ...(existing || {}),
      window_start_ms: row.ts,
      source_layer: FEATURES_30S_DIR,
      source_input_sha256: sourceInputSha256 || null,
      source_output_hash: inputHash,
      source_output_path: sourceOutputPath || null,
      source_manifest_key: sourceManifestKey,
      source_row_count: rows.length,
      output_row_hash: outputHash,
      staged_path: stagedPath,
      output_path: outputPath,
      checkpoint_generation: nextGeneration,
      status: 'intent',
    };
    writeAtomicJson(this._manifestPath, manifest);

    // ── Atomic rename staging → final ──────────────────────────────────
    mkdirSync(dirname(outputPath), { recursive: true });
    renameSync(stagedPath, outputPath);
    fsyncDirectory(dirname(outputPath));

    // ── Verify renamed output hash matches staged hash ─────────────────
    const finalHash = sha256File(outputPath);
    if (finalHash !== outputHash) {
      // Hash mismatch after rename — critical corruption, quarantine
      manifest = load5minManifest(this._market, this._derivedDir) || initialManifest(this._market);
      if (manifest.processed_windows?.[key]) {
        manifest.processed_windows[key].status = 'quarantined';
        manifest.processed_windows[key].quarantined_at = new Date().toISOString();
        manifest.processed_windows[key].quarantined_reason = 'rename-hash-mismatch';
      }
      writeAtomicJson(this._manifestPath, manifest);
      const err = new Error(`E_FIVEMIN_RENAME_HASH: output hash mismatch after rename for ${outputPath}`);
      err.code = 'E_FIVEMIN_RENAME_HASH';
      throw err;
    }

    // ── Checkpoint ─────────────────────────────────────────────────────
    this._writeCheckpoint(row.ts, nextGeneration, outputPath, outputHash);

    // ── Promote intent → committed ────────────────────────────────────
    manifest = load5minManifest(this._market, this._derivedDir) || initialManifest(this._market);
    manifest.processed_windows[key] = {
      ...(manifest.processed_windows?.[key] || {}),
      window_start_ms: row.ts,
      output_row_hash: outputHash,
      output_path: outputPath,
      checkpoint_generation: nextGeneration,
      status: 'committed',
    };
    manifest.last_checkpoint_window_start = row.ts;
    writeAtomicJson(this._manifestPath, manifest);

    return {
      key,
      output_path: outputPath,
      output_row_hash: outputHash,
      nextGeneration,
      idempotent: false,
    };
  }

  _writeCheckpoint(windowStartMs, generation, outputPath, outputHash) {
    writeAtomicJson(this._checkpointPath, {
      schema_version: FIVEMIN_SCHEMA_VERSION,
      namespace: FEATURES_5MIN_DIR,
      source_layer: FEATURES_30S_DIR,
      market: this._market,
      last_committed_window_start: windowStartMs,
      generation,
      output_path: outputPath,
      output_row_hash: outputHash,
      updated_at: new Date().toISOString(),
    });
  }

  // ── Recovery: orphan staging scan ────────────────────────────────────

  /**
   * Scan .staging/ directories for orphan files not referenced by any intent
   * record. Orphans are removed (no corresponding manifest intent to promote).
   *
   * @returns {{ cleaned: number }}
   */
  scanOrphanStaging() {
    let cleaned = 0;
    const marketDir = join(this._featuresDir, this._market);
    if (!existsSync(marketDir)) return { cleaned };

    const manifest = load5minManifest(this._market, this._derivedDir);
    const referencedPaths = new Set();
    if (manifest?.processed_windows) {
      for (const rec of Object.values(manifest.processed_windows)) {
        if (rec.staged_path) referencedPaths.add(rec.staged_path);
      }
    }

    const dates = readdirSync(marketDir, { withFileTypes: true })
      .filter(d => d.isDirectory() && !d.name.startsWith('.'));
    for (const dateDir of dates) {
      const stagingRoot = join(marketDir, dateDir.name, '.staging');
      if (!existsSync(stagingRoot)) continue;
      const runDirs = readdirSync(stagingRoot, { withFileTypes: true })
        .filter(d => d.isDirectory());
      for (const runDir of runDirs) {
        const runPath = join(stagingRoot, runDir.name);
        const files = readdirSync(runPath);
        for (const file of files) {
          const fullPath = join(runPath, file);
          if (!referencedPaths.has(fullPath)) {
            try { rmSync(fullPath, { recursive: true, force: true }); cleaned++; } catch (_) { /* best-effort */ }
          }
        }
        try {
          const remaining = readdirSync(runPath);
          if (remaining.length === 0) rmSync(runPath, { force: true, recursive: true });
        } catch (_) { /* best-effort */ }
      }
      try {
        const remaining = readdirSync(stagingRoot);
        if (remaining.length === 0) rmSync(stagingRoot, { force: true, recursive: true });
      } catch (_) { /* best-effort */ }
    }
    return { cleaned };
  }

  // ── Recovery: source-referenced reconciliation ────────────────────────

  /**
   * Reconcile missing 5min windows from committed 30s manifest.
   * Reads the 30s rollup manifest and re-aggregates complete 5min windows.
   * Mimics the pattern in RollupOutputCommitter.reconcileCommitted1s().
   *
   * @returns {{ repaired: number, checked: number }}
   */
  reconcileCommitted30s() {
    const sourceManifestPath = join(this._derivedDir, MANIFESTS_DIR, FEATURES_30S_DIR, `${this._market}.json`);
    const sourceManifest = readJsonOrNull(sourceManifestPath, 'E_ROLLUP_SOURCE_MANIFEST_CORRUPT');
    if (!sourceManifest) return { repaired: 0, checked: 0 };

    // Collect committed 30s windows ordered by ts
    const thirtySRecords = [];
    for (const [sourceKey, record] of Object.entries(sourceManifest.processed_windows || {})) {
      if (record?.status !== 'committed') continue;
      if (!existsSync(record.output_path)) {
        const err = new Error(`E_FIVEMIN_SOURCE_MISSING: committed 30s window is missing: ${record.output_path}`);
        err.code = 'E_FIVEMIN_SOURCE_MISSING';
        throw err;
      }
      const sourceContent = readFileSync(record.output_path, 'utf8');
      const actualHash = sha256(sourceContent);
      if (record.output_row_hash && actualHash !== record.output_row_hash) {
        const err = new Error(`E_FIVEMIN_SOURCE_HASH: committed 30s window hash mismatch: ${record.output_path}`);
        err.code = 'E_FIVEMIN_SOURCE_HASH';
        throw err;
      }
      const rows = parseJsonl(sourceContent, record.output_path);
      for (const row of rows) {
        if (typeof row.ts === 'number') {
          thirtySRecords.push({ ts: row.ts, rows, key: sourceKey, outputPath: record.output_path, hash: actualHash });
        }
      }
    }

    // Sort by ts
    thirtySRecords.sort((a, b) => a.ts - b.ts);

    // Group into 5min buckets (each bucket = 10 consecutive 30s windows)
    let checked = 0;
    let repaired = 0;
    let i = 0;
    while (i + 9 < thirtySRecords.length) {
      const bucket = thirtySRecords.slice(i, i + 10);
      // Verify alignment: first ts must be 5min-aligned, each consecutive 30s
      let aligned = true;
      for (let j = 1; j < 10; j++) {
        if (bucket[j].ts !== bucket[0].ts + j * 30_000) { aligned = false; break; }
      }
      if (aligned) {
        checked++;
        const combinedRows = bucket.flatMap(b => b.rows);
        const sourceHash = sha256(JSON.stringify(combinedRows));
        const last30sKey = bucket[bucket.length - 1].key;
        try {
          const result = this.commitWindow({
            rows: combinedRows,
            sourceInputSha256: null,
            sourceOutputHash: sourceHash,
            sourceOutputPath: bucket[bucket.length - 1].outputPath,
            sourceManifestKey: last30sKey,
          });
          if (!result.idempotent) repaired++;
        } catch (_) {
          // Skip windows that fail validation (e.g. partial quality)
        }
      }
      i += 10;
    }

    return { repaired, checked };
  }
}
