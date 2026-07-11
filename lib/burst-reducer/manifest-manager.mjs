// lib/burst-reducer/manifest-manager.mjs — Manifest read/write with atomic rename
// Follows plan Task 7a

import { existsSync, readFileSync, writeFileSync, renameSync, mkdirSync, openSync, fsyncSync, closeSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { SCHEMA_VERSION, DERIVED_DIR, MANIFESTS_DIR } from './schema.mjs';

/**
 * Sentinel returned by loadManifest when the manifest file is corrupt.
 */
export const MANIFEST_CORRUPT = Symbol('MANIFEST_CORRUPT');
/**
 * Sentinel returned by loadCheckpoint when the checkpoint file is corrupt.
 */
export const CHECKPOINT_CORRUPT = Symbol('CHECKPOINT_CORRUPT');

function writeFileDurable(path, content) {
  writeFileSync(path, content, 'utf8');
  const fd = openSync(path, 'r');
  try {
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
}

function fsyncDirectory(dir) {
  const fd = openSync(dir, 'r');
  try {
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
}

/**
 * Load a manifest file. Returns null if not found.
 * Returns MANIFEST_CORRUPT if the file exists but is empty or invalid JSON.
 * In the corrupt case the original file is renamed to .bak.<timestamp>
 * for evidence preservation.
 * @param {string} market
 * @param {string} [derivedDir]
 * @returns {Object|null|symbol}
 */
export function loadManifest(market, derivedDir = DERIVED_DIR) {
  const path = join(derivedDir, MANIFESTS_DIR, `${market}.json`);
  if (!existsSync(path)) return null;
  let raw;
  try {
    raw = readFileSync(path, 'utf8');
  } catch {
    return null;
  }
  if (raw.trim().length === 0) {
    // Empty file → corrupt. Back up and return sentinel.
    backupCorruptManifest(path);
    return MANIFEST_CORRUPT;
  }
  try {
    return JSON.parse(raw);
  } catch {
    // Invalid JSON → corrupt. Back up and return sentinel.
    backupCorruptManifest(path);
    return MANIFEST_CORRUPT;
  }
}

/**
 * Rename a corrupt manifest file to .bak.<timestamp> for evidence.
 * @param {string} path
 */
function backupCorruptManifest(path) {
  try {
    const bakPath = `${path}.bak.${Date.now()}`;
    renameSync(path, bakPath);
  } catch (_) {
    // If rename fails (permissions, etc.), leave the file in place.
    // The caller must NOT proceed with processing.
  }
}

/**
 * Write a manifest record with atomic rename (.tmp → final).
 * Handles both intent and committed writes.
 * @param {string} market
 * @param {string} key - composite key
 * @param {Object} record - the processed block record
 * @param {'intent'|'committed'} status
 * @param {Object} [existingManifest] - existing manifest to merge into
 */
export function writeManifestRecord(market, key, record, status, existingManifest, derivedDir = DERIVED_DIR) {
  const manifestDir = join(derivedDir, MANIFESTS_DIR);
  mkdirSync(manifestDir, { recursive: true });

  const manifest = existingManifest ? { ...existingManifest } : {
    schema_version: SCHEMA_VERSION,
    market,
    last_checkpoint_block_start: null,
    processed_blocks: {},
  };

  if (!manifest.processed_blocks) manifest.processed_blocks = {};

  manifest.processed_blocks[key] = {
    ...(manifest.processed_blocks[key] || {}),
    ...record,
    status,
  };

  if (status === 'committed') {
    manifest.last_checkpoint_block_start = record.block_start_ms ?? manifest.last_checkpoint_block_start;
  }

  const tmpPath = join(manifestDir, `${market}.json.tmp`);
  const finalPath = join(manifestDir, `${market}.json`);

  writeFileDurable(tmpPath, JSON.stringify(manifest, null, 2) + '\n');
  renameSync(tmpPath, finalPath);
  fsyncDirectory(manifestDir);

  return manifest;
}

/**
 * Load a checkpoint file. Returns null if not found.
 * Returns CHECKPOINT_CORRUPT if the file exists but is empty or invalid JSON.
 * In the corrupt case the original file is renamed to .bak.<timestamp>
 * for evidence preservation.
 * @param {string} market
 * @param {string} [derivedDir]
 * @returns {Object|null|symbol}
 */
export function loadCheckpoint(market, derivedDir = DERIVED_DIR) {
  const path = join(derivedDir, 'manifests/checkpoints', `${market}.json`);
  if (!existsSync(path)) return null;
  let raw;
  try {
    raw = readFileSync(path, 'utf8');
  } catch {
    return null;
  }
  if (raw.trim().length === 0) {
    // Empty file → corrupt. Back up and return sentinel.
    backupCorruptCheckpoint(path);
    return CHECKPOINT_CORRUPT;
  }
  try {
    return JSON.parse(raw);
  } catch {
    // Invalid JSON → corrupt. Back up and return sentinel.
    backupCorruptCheckpoint(path);
    return CHECKPOINT_CORRUPT;
  }
}

/**
 * Rename a corrupt checkpoint file to .bak.<timestamp> for evidence.
 * @param {string} path
 */
function backupCorruptCheckpoint(path) {
  try {
    const bakPath = `${path}.bak.${Date.now()}`;
    renameSync(path, bakPath);
  } catch (_) {
    // If rename fails, leave the file in place.
    // The caller must NOT proceed with processing.
  }
}

/**
 * Write checkpoint with atomic rename.
 * @param {Object} params
 * @param {number} params.last_committed_block_start
 * @param {Object|null} params.pending_block
 * @param {Object|null} params.open_burst
 * @param {number} params.generation
 */
export function writeCheckpoint({ last_committed_block_start, pending_block, open_burst, generation, market: explicitMarket }) {
  const market = explicitMarket || pending_block?.replay_identity?.market || 'unknown';
  const cpDir = join(DERIVED_DIR, 'manifests/checkpoints');
  mkdirSync(cpDir, { recursive: true });

  const checkpoint = {
    schema_version: SCHEMA_VERSION,
    last_committed_block_start,
    pending_block,
    open_burst,
    generation,
    updated_at: new Date().toISOString(),
  };

  const tmpPath = join(cpDir, `${market}.json.tmp`);
  const finalPath = join(cpDir, `${market}.json`);

  writeFileDurable(tmpPath, JSON.stringify(checkpoint, null, 2) + '\n');
  renameSync(tmpPath, finalPath);
  fsyncDirectory(cpDir);

  return checkpoint;
}
