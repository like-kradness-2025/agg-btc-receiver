// lib/burst-reducer/output-committer.mjs — Atomic 5-step block shard commit
// Follows plan Task 7b-7e

import { mkdirSync, writeFileSync, renameSync, openSync, fsyncSync, closeSync, existsSync, readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { createHash } from 'node:crypto';
import { SCHEMA_VERSION, FEATURES_1S_DIR } from './schema.mjs';

function sha256(content) {
  return createHash('sha256').update(content).digest('hex');
}

function writeFileDurable(path, content) {
  writeFileSync(path, content, 'utf8');
  const fd = openSync(path, 'r');
  try { fsyncSync(fd); } finally { closeSync(fd); }
}

function fsyncDirectory(dir) {
  const fd = openSync(dir, 'r');
  try { fsyncSync(fd); } finally { closeSync(fd); }
}

function formatBlockTime(blockStartMs) {
  const d = new Date(blockStartMs);
  return `${String(d.getUTCHours()).padStart(2,'0')}-${String(d.getUTCMinutes()).padStart(2,'0')}-${String(d.getUTCSeconds()).padStart(2,'0')}`;
}

function formatDate(blockStartMs) {
  const d = new Date(blockStartMs);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')}`;
}

function compositeKey(market, blockStartMs, inputSha256) {
  return `${SCHEMA_VERSION}:${market}:${blockStartMs}:${inputSha256}`;
}

export class OutputCommitter {
  constructor(market, runId, derivedDir) {
    this._market = market;
    this._runId = runId;
    this._derivedDir = derivedDir;
    this._manifestDir = join(derivedDir, 'manifests');
    this._checkpointDir = join(derivedDir, 'manifests/checkpoints');
    this._features1sDir = join(derivedDir, FEATURES_1S_DIR);
  }

  commitFinalizedBlock(finalizedBlock, nextPendingBlock, nextDetectorState, rows, manifestInputs, checkpointGeneration, commitId, isEofFinalization) {
    const { block_start_ms, input_sha256 } = finalizedBlock;

    if (!isEofFinalization && nextPendingBlock === null) {
      throw new Error('E031: non-EOF commit requires non-null nextPendingBlock');
    }
    if (isEofFinalization && nextPendingBlock !== null) {
      throw new Error('E031: EOF commit requires null nextPendingBlock');
    }

    this._validateRows(rows, block_start_ms);

    const date = formatDate(block_start_ms);
    const time = formatBlockTime(block_start_ms);
    const content = rows.map(r => JSON.stringify(r)).join('\n') + '\n';
    const stagedHash = sha256(content);
    const key = compositeKey(this._market, block_start_ms, input_sha256);

    // Extract audit fields from manifestInputs
    const assumedEmptyInputBlocks = manifestInputs.assumed_empty_input_blocks || [];
    const assumedEmptyGapRanges = manifestInputs.assumed_empty_gap_ranges || [];
    const reorderedInput = manifestInputs.reordered_input === true;
    const timestampInversionCount = typeof manifestInputs.timestamp_inversion_count === 'number'
      ? manifestInputs.timestamp_inversion_count : 0;

    // Stage
    const outputDir = join(this._features1sDir, this._market, date);
    const stagedFile = join(outputDir, '.staging', this._runId, `${time}.jsonl`);
    mkdirSync(dirname(stagedFile), { recursive: true });
    writeFileDurable(stagedFile, content);

    // Intent manifest (inline)
    mkdirSync(this._manifestDir, { recursive: true });
    let manifest = this._loadManifest();
    if (!manifest) {
      manifest = { schema_version: SCHEMA_VERSION, market: this._market, last_checkpoint_block_start: null, processed_blocks: {} };
    }
    if (!manifest.processed_blocks) manifest.processed_blocks = {};
    manifest.processed_blocks[key] = {
      ...(manifest.processed_blocks[key] || {}),
      block_start_ms,
      input_sha256,
      staged_row_hash: stagedHash,
      staged_path: stagedFile,
      output_path: join(this._derivedDir, FEATURES_1S_DIR, this._market, date, `${time}.jsonl`),
      checkpoint_generation: checkpointGeneration,
      commit_id: commitId,
      auxiliary_input_hashes: manifestInputs.auxiliary_input_hashes || {},
      assumed_empty_input_blocks: assumedEmptyInputBlocks,
      assumed_empty_gap_ranges: assumedEmptyGapRanges,
      reordered_input: reorderedInput,
      timestamp_inversion_count: timestampInversionCount,
      status: 'intent',
    };
    this._writeManifest(manifest);

    // Final data shard
    const outputPath = join(outputDir, `${time}.jsonl`);
    mkdirSync(dirname(outputPath), { recursive: true });
    renameSync(stagedFile, outputPath);
    fsyncDirectory(outputDir);
    const finalHash = sha256(content);

    // Checkpoint (inline) — P1-1 minimal state
    mkdirSync(this._checkpointDir, { recursive: true });
    const nextGeneration = checkpointGeneration + 1;
    const cp = {
      schema_version: SCHEMA_VERSION,
      last_committed_block_start: block_start_ms,
      pending_block: isEofFinalization ? null : (() => {
        // P1-1: strip open_burst_before_N1 from pending_block (redundant with checkpoint open_burst)
        if (!nextPendingBlock) return null;
        const { open_burst_before_N1, ...rest } = nextPendingBlock;
        return rest;
      })(),
      open_burst: isEofFinalization ? null : nextDetectorState,  // P1-1: already minimal from detector.getMinimalBurstState()
      generation: nextGeneration,
      updated_at: new Date().toISOString(),
    };
    const cpTmp = join(this._checkpointDir, `${this._market}.json.tmp`);
    const cpFinal = join(this._checkpointDir, `${this._market}.json`);
    writeFileDurable(cpTmp, JSON.stringify(cp, null, 2) + '\n');
    renameSync(cpTmp, cpFinal);
    fsyncDirectory(this._checkpointDir);

    // Committed manifest — merge with intent record, preserve fields
    manifest = this._loadManifest();
    if (!manifest) manifest = { schema_version: SCHEMA_VERSION, market: this._market, last_checkpoint_block_start: null, processed_blocks: {} };
    if (!manifest.processed_blocks) manifest.processed_blocks = {};
    const existingRecord = manifest.processed_blocks[key] || {};
    manifest.processed_blocks[key] = {
      ...existingRecord,  // preserve intent fields (auxiliary_input_hashes, staged_row_hash, etc.)
      block_start_ms,
      output_row_hash: finalHash,
      checkpoint_generation: checkpointGeneration,
      commit_id: commitId,
      status: 'committed',
    };
    manifest.last_checkpoint_block_start = block_start_ms;
    this._writeManifest(manifest);

    return { key, stagedHash, finalHash, nextGeneration, staged_path: stagedFile, output_path: outputPath };
  }

  _loadManifest() {
    const path = join(this._manifestDir, `${this._market}.json`);
    if (!existsSync(path)) return null;
    try { return JSON.parse(readFileSync(path, 'utf8')); } catch { return null; }
  }

  _writeManifest(manifest) {
    const tmpPath = join(this._manifestDir, `${this._market}.json.tmp`);
    const finalPath = join(this._manifestDir, `${this._market}.json`);
    mkdirSync(this._manifestDir, { recursive: true });
    writeFileDurable(tmpPath, JSON.stringify(manifest, null, 2) + '\n');
    renameSync(tmpPath, finalPath);
    fsyncDirectory(this._manifestDir);
  }

  _validateRows(rows, blockStartMs) {
    if (rows.length !== 30) throw new Error(`E030: expected 30 rows, got ${rows.length}`);
    for (let i = 0; i < 30; i++) {
      if (rows[i].ts !== blockStartMs + i * 1000) throw new Error(`E030: row ${i} ts mismatch`);
      if (rows[i].market !== this._market) throw new Error(`E030: row ${i} market mismatch`);
    }
  }
}
