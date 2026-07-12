// lib/burst-reducer/rollup-output-committer.mjs — isolated durable 30s output

import {
  closeSync,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  writeFileSync,
} from 'node:fs';
import { createHash } from 'node:crypto';
import { dirname, join } from 'node:path';
import { aggregate30s } from './rollup.mjs';
import { CHECKPOINTS_DIR, FEATURES_30S_DIR, MANIFESTS_DIR } from './schema.mjs';

export const ROLLUP_SCHEMA_VERSION = 'burst_features_30s_v1';

function sha256(content) {
  return createHash('sha256').update(content).digest('hex');
}

function sha256File(path) {
  return sha256(readFileSync(path, 'utf8'));
}

function durableWrite(path, content) {
  writeFileSync(path, content, 'utf8');
  const fd = openSync(path, 'r');
  try {
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
}

function fsyncDirectory(path) {
  const fd = openSync(path, 'r');
  try {
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
}

function writeAtomicJson(path, value) {
  const tmpPath = `${path}.tmp`;
  mkdirSync(dirname(path), { recursive: true });
  durableWrite(tmpPath, `${JSON.stringify(value, null, 2)}\n`);
  renameSync(tmpPath, path);
  fsyncDirectory(dirname(path));
}

function formatDate(blockStartMs) {
  const date = new Date(blockStartMs);
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}-${String(date.getUTCDate()).padStart(2, '0')}`;
}

function formatBlockTime(blockStartMs) {
  const date = new Date(blockStartMs);
  return `${String(date.getUTCHours()).padStart(2, '0')}-${String(date.getUTCMinutes()).padStart(2, '0')}-${String(date.getUTCSeconds()).padStart(2, '0')}`;
}

function parseJsonl(content, path) {
  const rows = [];
  for (const [index, line] of content.split('\n').entries()) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      rows.push(JSON.parse(trimmed));
    } catch (error) {
      const wrapped = new Error(`E_ROLLUP_SOURCE_CORRUPT: invalid JSON at ${path}:${index + 1}`);
      wrapped.code = 'E_ROLLUP_SOURCE_CORRUPT';
      wrapped.cause = error;
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
  } catch (error) {
    const wrapped = new Error(`${code}: invalid JSON at ${path}`);
    wrapped.code = code;
    wrapped.cause = error;
    throw wrapped;
  }
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    const error = new Error(`${code}: expected object at ${path}`);
    error.code = code;
    throw error;
  }
  return value;
}

function ensureSourcePath(path) {
  if (typeof path !== 'string' || path.length === 0 || !path.split(/[\\/]+/).includes('features_1s')) {
    const error = new Error('E_ROLLUP_SOURCE_LAYER: sourceOutputPath must be a features_1s shard');
    error.code = 'E_ROLLUP_SOURCE_LAYER';
    throw error;
  }
}

function manifestPath(market, derivedDir) {
  return join(derivedDir, MANIFESTS_DIR, FEATURES_30S_DIR, `${market}.json`);
}

function checkpointPath(market, derivedDir) {
  return join(derivedDir, CHECKPOINTS_DIR, FEATURES_30S_DIR, `${market}.json`);
}

export function loadRollupManifest(market, derivedDir) {
  return readJsonOrNull(manifestPath(market, derivedDir), 'E_ROLLUP_MANIFEST_CORRUPT');
}

export function loadRollupCheckpoint(market, derivedDir) {
  return readJsonOrNull(checkpointPath(market, derivedDir), 'E_ROLLUP_CHECKPOINT_CORRUPT');
}

function initialManifest(market) {
  return {
    schema_version: ROLLUP_SCHEMA_VERSION,
    namespace: FEATURES_30S_DIR,
    source_layer: FEATURES_1S_DIR,
    market,
    last_checkpoint_window_start: null,
    processed_windows: {},
  };
}

// Kept local so this module has one explicit source namespace and cannot
// accidentally reuse OutputCommitter's features_1s path.
const FEATURES_1S_DIR = 'features_1s';

export class RollupOutputCommitter {
  constructor(market, runId, derivedDir) {
    this._market = market;
    this._runId = runId;
    this._derivedDir = derivedDir;
    this._featuresDir = join(derivedDir, FEATURES_30S_DIR);
    this._manifestPath = manifestPath(market, derivedDir);
    this._checkpointPath = checkpointPath(market, derivedDir);
  }

  /**
   * Persist one complete 30s rollup after its source 1s shard is durable.
   * The source rows are validated by aggregate30s before any output is staged.
   */
  commitWindow({ rows, sourceInputSha256, sourceOutputPath, sourceOutputHash, sourceManifestKey = null }) {
    ensureSourcePath(sourceOutputPath);
    const [row] = aggregate30s(rows);
    const content = `${JSON.stringify(row)}\n`;
    const outputHash = sha256(content);
    const inputHash = sourceOutputHash || sha256(JSON.stringify(rows));
    const key = `${ROLLUP_SCHEMA_VERSION}:${this._market}:${row.ts}:${inputHash}`;
    const date = formatDate(row.ts);
    const time = formatBlockTime(row.ts);
    const outputPath = join(this._featuresDir, this._market, date, `${time}.jsonl`);

    let manifest = loadRollupManifest(this._market, this._derivedDir) || initialManifest(this._market);
    if (manifest.namespace !== FEATURES_30S_DIR || manifest.source_layer !== FEATURES_1S_DIR || manifest.market !== this._market) {
      const error = new Error(`E_ROLLUP_NAMESPACE: incompatible rollup manifest for ${this._market}`);
      error.code = 'E_ROLLUP_NAMESPACE';
      throw error;
    }
    if (!manifest.processed_windows) manifest.processed_windows = {};

    const existing = manifest.processed_windows[key];
    if (existing?.status === 'committed' && existsSync(outputPath) && sha256File(outputPath) === existing.output_row_hash) {
      // Repair a missing checkpoint without rewriting the 30s shard.
      const checkpoint = loadRollupCheckpoint(this._market, this._derivedDir);
      if (!checkpoint || checkpoint.last_committed_window_start < row.ts) {
        this._writeCheckpoint(row.ts, checkpoint?.generation ?? existing.checkpoint_generation ?? 0, outputPath, outputHash);
      }
      return { key, output_path: outputPath, output_row_hash: outputHash, idempotent: true };
    }

    const checkpoint = loadRollupCheckpoint(this._market, this._derivedDir);
    const nextGeneration = (checkpoint?.generation ?? 0) + 1;
    const stagedPath = join(this._featuresDir, this._market, date, '.staging', this._runId, `${time}.jsonl`);
    mkdirSync(dirname(stagedPath), { recursive: true });
    durableWrite(stagedPath, content);

    manifest.processed_windows[key] = {
      ...(existing || {}),
      window_start_ms: row.ts,
      source_layer: FEATURES_1S_DIR,
      source_input_sha256: sourceInputSha256 || null,
      source_output_hash: inputHash,
      source_output_path: sourceOutputPath,
      source_manifest_key: sourceManifestKey,
      source_row_count: rows.length,
      output_row_hash: outputHash,
      staged_path: stagedPath,
      output_path: outputPath,
      checkpoint_generation: nextGeneration,
      status: 'intent',
    };
    writeAtomicJson(this._manifestPath, manifest);

    mkdirSync(dirname(outputPath), { recursive: true });
    renameSync(stagedPath, outputPath);
    fsyncDirectory(dirname(outputPath));

    this._writeCheckpoint(row.ts, nextGeneration, outputPath, outputHash);

    manifest = loadRollupManifest(this._market, this._derivedDir) || initialManifest(this._market);
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
      schema_version: ROLLUP_SCHEMA_VERSION,
      namespace: FEATURES_30S_DIR,
      source_layer: FEATURES_1S_DIR,
      market: this._market,
      last_committed_window_start: windowStartMs,
      generation,
      output_path: outputPath,
      output_row_hash: outputHash,
      updated_at: new Date().toISOString(),
    });
  }

  /**
   * Recover rollup rows that were skipped by a crash after the durable 1s
   * commit. Only committed features_1s manifest records are eligible.
   */
  reconcileCommitted1s() {
    const sourceManifestPath = join(this._derivedDir, MANIFESTS_DIR, `${this._market}.json`);
    const sourceManifest = readJsonOrNull(sourceManifestPath, 'E_ROLLUP_SOURCE_MANIFEST_CORRUPT');
    if (!sourceManifest) return { repaired: 0, checked: 0 };

    let checked = 0;
    let repaired = 0;
    for (const [sourceManifestKey, record] of Object.entries(sourceManifest.processed_blocks || {})) {
      if (record?.status !== 'committed') continue;
      ensureSourcePath(record.output_path);
      if (!existsSync(record.output_path)) {
        const error = new Error(`E_ROLLUP_SOURCE_MISSING: committed 1s shard is missing: ${record.output_path}`);
        error.code = 'E_ROLLUP_SOURCE_MISSING';
        throw error;
      }
      const sourceContent = readFileSync(record.output_path, 'utf8');
      const actualSourceHash = sha256(sourceContent);
      if (record.output_row_hash && actualSourceHash !== record.output_row_hash) {
        const error = new Error(`E_ROLLUP_SOURCE_HASH: committed 1s shard hash mismatch: ${record.output_path}`);
        error.code = 'E_ROLLUP_SOURCE_HASH';
        throw error;
      }
      const rows = parseJsonl(sourceContent, record.output_path);
      checked += 1;
      const result = this.commitWindow({
        rows,
        sourceInputSha256: record.input_sha256 || null,
        sourceOutputPath: record.output_path,
        sourceOutputHash: actualSourceHash,
        sourceManifestKey,
      });
      if (!result.idempotent) repaired += 1;
    }
    return { repaired, checked };
  }
}
