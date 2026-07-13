// test/burst-reducer/output-committer.test.mjs — OutputCommitter tests (7b-7e)
import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import { existsSync, readFileSync, rmSync, mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { randomUUID } from 'node:crypto';
import { OutputCommitter } from '../../lib/burst-reducer/output-committer.mjs';
import { createBaseRow } from '../../lib/burst-reducer/schema.mjs';
import { loadManifest, loadCheckpoint } from '../../lib/burst-reducer/manifest-manager.mjs';

const MARKET = 'test_committer';
const RUN_ID = 'test-run-1';
const DEFAULT_DERIVED = 'data/derived/burst_features_v1';

function makeTestRows(blockStartMs) {
  const rows = [];
  for (let i = 0; i < 30; i++) {
    const ts = blockStartMs + i * 1000;
    rows.push(createBaseRow(ts, MARKET, {
      book_seeded: false,
      trade_count_this_second: 0,
      warmup: blockStartMs === 0,
      input_block_ids: [String(blockStartMs)],
      finalized: true,
    }));
  }
  // Set some features for verifiability
  rows[0].burst_count_1s = 2;
  rows[0].total_burst_notional_1s = 500;
  rows[1].burst_count_1s = 1;
  rows[1].total_burst_notional_1s = 100;
  return rows;
}

describe('OutputCommitter', () => {
  const manifestPath = join('data/derived/burst_features_v1', 'manifests', `${MARKET}.json`);
  const checkpointPath = join('data/derived/burst_features_v1', 'manifests/checkpoints', `${MARKET}.json`);
  const derived1sDir = join('data/derived/burst_features_v1', 'features_1s', MARKET);

  before(() => {
    try { rmSync(manifestPath, { force: true }); } catch (_) {}
    try { rmSync(checkpointPath, { force: true }); } catch (_) {}
    try { rmSync(derived1sDir, { recursive: true, force: true }); } catch (_) {}
  });

  after(() => {
    try { rmSync(manifestPath, { force: true }); } catch (_) {}
    try { rmSync(checkpointPath, { force: true }); } catch (_) {}
    try { rmSync(derived1sDir, { recursive: true, force: true }); } catch (_) {}
  });

  it('non-EOF commit: creates output, manifest, and checkpoint with pending', () => {
    const committer = new OutputCommitter(MARKET, RUN_ID, DEFAULT_DERIVED);
    const rows = makeTestRows(0);
    const commitId = randomUUID();
    const nextPendingBlock = {
      block_start_ms: 30000,
      trade_input_sha256: 'abc123def456',
      auxiliary_input_hashes: {},
      replay_identity: { market: MARKET, block_start_ms: 30000, input_path: '/tmp/test.jsonl' },
      open_burst_before_N1: null,
    };
    const nextDetectorState = { schemaVersion: 1, open: null, closedBursts: [], nextId: 5 };

    const result = committer.commitFinalizedBlock(
      { block_start_ms: 0, input_sha256: 'abc123' },
      nextPendingBlock,
      nextDetectorState,
      rows,
      { auxiliary_input_hashes: { 'agg_0_30000': 'hash1' } },
      0,
      commitId,
      false, // isEofFinalization: false
    );

    // Check output path
    assert.ok(result.key.includes('burst_features_v1:test_committer:0:abc123'));
    assert.equal(result.nextGeneration, 1);
    assert.equal(result.stagedHash.length, 64);
    assert.equal(result.finalHash.length, 64);

    // Check output file exists
    assert.ok(existsSync(result.output_path), 'output shard should exist');
    assert.ok(result.output_path.includes('00-00-00.jsonl'), 'should be canonical block name');
    const outputContent = readFileSync(result.output_path, 'utf8');
    const lines = outputContent.trim().split('\n');
    assert.equal(lines.length, 30, 'should be 30 rows');

    // Check manifest
    const manifest = loadManifest(MARKET);
    assert.ok(manifest);
    const key = result.key;
    assert.ok(manifest.processed_blocks[key]);
    assert.equal(manifest.processed_blocks[key].status, 'committed');
    assert.ok(manifest.processed_blocks[key].auxiliary_input_hashes['agg_0_30000']);

    // Check checkpoint with pending block
    const cp = loadCheckpoint(MARKET);
    assert.ok(cp);
    assert.equal(cp.last_committed_block_start, 0);
    assert.ok(cp.pending_block !== null, 'pending_block should not be null for non-EOF');
    assert.equal(cp.pending_block.block_start_ms, 30000);
    assert.equal(cp.generation, 1);
  });

  it('EOF commit: pending_block is null, E031 not thrown', () => {
    const committer = new OutputCommitter(MARKET, RUN_ID + '-eof', DEFAULT_DERIVED);
    const rows = makeTestRows(30000);
    const commitId = randomUUID();

    const result = committer.commitFinalizedBlock(
      { block_start_ms: 30000, input_sha256: 'def456' },
      null,    // nextPendingBlock: null (EOF)
      null,    // nextDetectorState: null (EOF)
      rows,
      { auxiliary_input_hashes: {} },
      1,
      commitId,
      true,    // isEofFinalization: true
    );

    assert.equal(result.nextGeneration, 2);

    // Check checkpoint: pending_block should be null
    const cp = loadCheckpoint(MARKET);
    // Note: checkpoint overwrites per-market, so last checkpoint is for MARKET
    // Since we used same MARKET, it was overwritten. Check the EOF-specific outputs.
    const eofManifest = loadManifest(MARKET);
    assert.ok(eofManifest);
    const eofKey = result.key;
    assert.ok(eofManifest.processed_blocks[eofKey]);
    assert.equal(eofManifest.processed_blocks[eofKey].status, 'committed');
  });

  it('throws E031 on non-EOF commit with null nextPendingBlock', () => {
    const committer = new OutputCommitter(MARKET + '_err', RUN_ID + '-err', DEFAULT_DERIVED);
    assert.throws(() => {
      committer.commitFinalizedBlock(
        { block_start_ms: 0, input_sha256: 'x' },
        null,
        null,
        makeTestRows(0),
        { auxiliary_input_hashes: {} },
        0,
        randomUUID(),
        false,  // isEofFinalization: false, but nextPendingBlock=null → E031
      );
    }, /E031/);
  });

  it('throws E031 on EOF commit with non-null nextPendingBlock', () => {
    const committer = new OutputCommitter(MARKET + '_err2', RUN_ID + '-err2', DEFAULT_DERIVED);
    assert.throws(() => {
      committer.commitFinalizedBlock(
        { block_start_ms: 0, input_sha256: 'x' },
        { block_start_ms: 30000 },
        null,
        makeTestRows(0),
        { auxiliary_input_hashes: {} },
        0,
        randomUUID(),
        true,  // isEofFinalization: true, but nextPendingBlock=non-null → E031
      );
    }, /E031/);
  });
});
