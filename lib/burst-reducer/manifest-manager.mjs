// lib/burst-reducer/manifest-manager.mjs — Manifest read/write with atomic rename
// Follows plan Task 7a

import { existsSync, readFileSync, writeFileSync, renameSync, mkdirSync, openSync, fsyncSync, closeSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { Buffer } from 'node:buffer';
import { SCHEMA_VERSION, DERIVED_DIR, MANIFESTS_DIR, CHECKPOINT_SIZE_WARN, CHECKPOINT_SIZE_HARD_LIMIT } from './schema.mjs';

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
 * @param {'intent'|'committed'|'verified_missing'} status
 * @param {Object} [existingManifest] - existing manifest to merge into
 * @param {string} [derivedDir]
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
 * When kind !== 'trades', the filename is ${market}.${kind}.json.
 * @param {string} market
 * @param {string} [derivedDir]
 * @param {string} [kind='trades'] - 'trades' or 'book_updates'
 * @returns {Object|null|symbol}
 */
export function loadCheckpoint(market, derivedDir = DERIVED_DIR, kind = 'trades') {
  const filename = kind === 'trades' ? `${market}.json` : `${market}.${kind}.json`;
  const path = join(derivedDir, 'manifests/checkpoints', filename);
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
 * Emit structured JSON log line to stderr (mirrors pipeline.mjs pattern).
 * @param {{ level: string, msg: string, [key: string]: any }} record
 */
function emitStructured(record) {
  process.stderr.write(JSON.stringify({ ts: new Date().toISOString(), ...record }) + '\n');
}

/**
 * Write a verified-missing record to the manifest for book_updates tracking.
 * Tracks a book block that was confirmed missing during processing.
 * @param {string} market
 * @param {number} blockStartMs
 * @param {Object} details - { kind, gap_range?, reason? }
 * @param {string} [derivedDir]
 * @returns {Object} updated manifest
 */
export function writeVerifiedMissingRecord(market, blockStartMs, details, derivedDir = DERIVED_DIR) {
  const key = `verified_missing:${market}:${blockStartMs}`;
  const existing = (() => {
    const m = loadManifest(market, derivedDir);
    return (m && m !== MANIFEST_CORRUPT) ? m : null;
  })();
  return writeManifestRecord(market, key, {
    block_start_ms: blockStartMs,
    market,
    reason: 'verified-missing',
    details,
    ts: new Date().toISOString(),
  }, 'verified_missing', existing || undefined, derivedDir);
}

/**
 * Write checkpoint with atomic rename.
 * When kind !== 'trades', the filename is ${market}.${kind}.json.
 * @param {Object} params
 * @param {number} params.last_committed_block_start
 * @param {Object|null} params.pending_block
 * @param {Object|null} params.open_burst
 * @param {number} params.generation
 * @param {string} [params.market] - explicit market (auto-derived from pending_block if omitted)
 * @param {string} [params.kind='trades'] - 'trades' or 'book_updates'
 */
export function writeCheckpoint({ last_committed_block_start, pending_block, open_burst, generation, market: explicitMarket, kind = 'trades', derivedDir }) {
  const market = explicitMarket || pending_block?.replay_identity?.market || 'unknown';
  const cpDir = join(derivedDir || DERIVED_DIR, 'manifests/checkpoints');
  mkdirSync(cpDir, { recursive: true });

  const checkpoint = {
    schema_version: SCHEMA_VERSION,
    kind,
    last_committed_block_start,
    pending_block,
    open_burst: open_burst ?? null,
    generation,
    updated_at: new Date().toISOString(),
  };

  const json = JSON.stringify(checkpoint, null, 2) + '\n';
  const size = Buffer.byteLength(json, 'utf8');

  // P0-2: Checkpoint size boundedness
  if (size >= CHECKPOINT_SIZE_HARD_LIMIT) {
    emitStructured({ level: 'FATAL', msg: 'checkpoint exceeds 1 MiB, refusing to write', size });
    console.error(`E026: checkpoint exceeds 1 MiB (${size} bytes), refusing to write`);
    throw new Error('E026: checkpoint exceeds 1 MiB, refusing to write');
  }

  if (size >= CHECKPOINT_SIZE_WARN) {
    emitStructured({ level: 'WARN', msg: 'checkpoint exceeds 256 KiB', size });
    console.warn(`checkpoint size ${size} bytes exceeds ${CHECKPOINT_SIZE_WARN} byte warn threshold`);
  }

  const filename = kind === 'trades' ? `${market}.json` : `${market}.${kind}.json`;
  const tmpPath = join(cpDir, `${filename}.tmp`);
  const finalPath = join(cpDir, filename);

  writeFileDurable(tmpPath, json);
  renameSync(tmpPath, finalPath);
  fsyncDirectory(cpDir);

  return checkpoint;
}
