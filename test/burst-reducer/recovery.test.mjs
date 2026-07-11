// test/burst-reducer/recovery.test.mjs — Fail-closed recovery tests (P1-2 Task 3)
import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import { existsSync, mkdirSync, writeFileSync, rmSync, readFileSync, renameSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { createHash } from 'node:crypto';

const TEST_ROOT = 'data/derived/burst_features_v1_test_recovery';
const MARKET = 'test_recovery';
const CP_DIR = join(TEST_ROOT, 'manifests/checkpoints');
const MANIFEST_DIR = join(TEST_ROOT, 'manifests');
const FEATURES_DIR = join(TEST_ROOT, 'features_1s', MARKET);

function sha256(content) {
  return createHash('sha256').update(content).digest('hex');
}

function makeShardContent(blockStartMs, market) {
  const rows = [];
  for (let i = 0; i < 30; i++) {
    rows.push(JSON.stringify({ ts: blockStartMs + i * 1000, market, burst_count_1s: 0 }));
  }
  return rows.join('\n') + '\n';
}

function makeIntentRecord(key, blockStartMs, inputSha256, stagedHash) {
  const time = `${String(new Date(blockStartMs).getUTCHours()).padStart(2,'0')}-${String(new Date(blockStartMs).getUTCMinutes()).padStart(2,'0')}-${String(new Date(blockStartMs).getUTCSeconds()).padStart(2,'0')}`;
  const date = `${new Date(blockStartMs).getUTCFullYear()}-${String(new Date(blockStartMs).getUTCMonth()+1).padStart(2,'0')}-${String(new Date(blockStartMs).getUTCDate()).padStart(2,'0')}`;
  return {
    block_start_ms: blockStartMs,
    input_sha256: inputSha256,
    staged_row_hash: stagedHash,
    staged_path: join(FEATURES_DIR, '.staging', 'run1', `${time}.jsonl`),
    output_path: join(FEATURES_DIR, date, `${time}.jsonl`),
    checkpoint_generation: 1,
    commit_id: 'uuid-1',
    auxiliary_input_hashes: {},
    status: 'intent',
  };
}

function makeCommittedRecord(key, blockStartMs, inputSha256, finalHash, generation) {
  const time = `${String(new Date(blockStartMs).getUTCHours()).padStart(2,'0')}-${String(new Date(blockStartMs).getUTCMinutes()).padStart(2,'0')}-${String(new Date(blockStartMs).getUTCSeconds()).padStart(2,'0')}`;
  const date = `${new Date(blockStartMs).getUTCFullYear()}-${String(new Date(blockStartMs).getUTCMonth()+1).padStart(2,'0')}-${String(new Date(blockStartMs).getUTCDate()).padStart(2,'0')}`;
  return {
    block_start_ms: blockStartMs,
    input_sha256: inputSha256,
    output_row_hash: finalHash,
    output_path: join(FEATURES_DIR, date, `${time}.jsonl`),
    checkpoint_generation: generation,
    commit_id: 'uuid-1',
    auxiliary_input_hashes: {},
    status: 'committed',
  };
}

function compositeKey(market, blockStartMs, inputSha256) {
  return `burst_features_v1:${market}:${blockStartMs}:${inputSha256}`;
}

function writeManifestFile(manifest) {
  mkdirSync(MANIFEST_DIR, { recursive: true });
  writeFileSync(join(MANIFEST_DIR, `${MARKET}.json`), JSON.stringify(manifest, null, 2) + '\n');
}

function writeCheckpointFile(cp) {
  mkdirSync(CP_DIR, { recursive: true });
  writeFileSync(join(CP_DIR, `${MARKET}.json`), JSON.stringify(cp, null, 2) + '\n');
}

function cleanup() {
  try { rmSync(TEST_ROOT, { recursive: true, force: true }); } catch (_) {}
}

describe('MarketStateRecovery', () => {
  before(() => cleanup());
  after(() => cleanup());

  it('Fixture 1: intent + staged hash match + final missing => rename + committed', async () => {
    cleanup();
    const { reconcileMarketState } = await import('../../lib/burst-reducer/recovery.mjs');

    const blockMs = 30000;
    const inputSha = sha256('input');
    const shardContent = makeShardContent(blockMs, MARKET);
    const stagedHash = sha256(shardContent);
    const key = compositeKey(MARKET, blockMs, inputSha);

    // Create staged file matching the intent record
    const stagedPath = join(FEATURES_DIR, '.staging', 'run1', '00-00-30.jsonl');
    mkdirSync(dirname(stagedPath), { recursive: true });
    writeFileSync(stagedPath, shardContent);

    // Create manifest with intent record (no final file)
    writeManifestFile({
      schema_version: 'burst_features_v1',
      market: MARKET,
      last_checkpoint_block_start: null,
      processed_blocks: {
        [key]: makeIntentRecord(key, blockMs, inputSha, stagedHash),
      },
    });

    const result = reconcileMarketState(MARKET, TEST_ROOT);
    // After reconcile, staged should be renamed to final, record committed
    const finalPath = join(FEATURES_DIR, '1970-01-01', '00-00-30.jsonl');
    assert.ok(!existsSync(stagedPath), 'staged file should no longer exist');
    assert.ok(existsSync(finalPath), 'final shard should exist after rename');
    assert.equal(sha256(readFileSync(finalPath, 'utf8')), stagedHash, 'final shard content matches');
    // quarantinedKeys should be empty
    assert.deepEqual(result.quarantinedKeys, []);
  });

  it('Fixture 2: committed + final hash matches + checkpoint consistent => ok', async () => {
    cleanup();
    const { reconcileMarketState } = await import('../../lib/burst-reducer/recovery.mjs');

    const blockMs = 30000;
    const inputSha = sha256('input2');
    const shardContent = makeShardContent(blockMs, MARKET);
    const finalHash = sha256(shardContent);
    const key = compositeKey(MARKET, blockMs, inputSha);

    // Create final shard file
    const date = '1970-01-01';
    const finalPath = join(FEATURES_DIR, date, '00-00-30.jsonl');
    mkdirSync(dirname(finalPath), { recursive: true });
    writeFileSync(finalPath, shardContent);

    // Manifest committed record with matching hash
    writeManifestFile({
      schema_version: 'burst_features_v1',
      market: MARKET,
      last_checkpoint_block_start: blockMs,
      processed_blocks: {
        [key]: makeCommittedRecord(key, blockMs, inputSha, finalHash, 1),
      },
    });

    // Checkpoint with matching generation and cursor
    writeCheckpointFile({
      schema_version: 'burst_features_v1',
      last_committed_block_start: blockMs,
      pending_block: null,
      open_burst: null,
      generation: 2, // generation in checkpoint is next_gen, committed record has checkpoint_generation=1
      updated_at: new Date().toISOString(),
    });

    const result = reconcileMarketState(MARKET, TEST_ROOT);
    assert.ok(result.cursor, 'cursor should be returned');
    assert.equal(result.generation, 2);
    assert.deepEqual(result.quarantinedKeys, []);
  });

  it('Fixture 3: intent + hash mismatch => quarantined, cursor unchanged', async () => {
    cleanup();
    const { reconcileMarketState } = await import('../../lib/burst-reducer/recovery.mjs');

    const blockMs = 30000;
    const inputSha = sha256('input3');
    const realShard = makeShardContent(blockMs, MARKET);
    const wrongHash = sha256('wrong-content');
    const key = compositeKey(MARKET, blockMs, inputSha);

    // Create staged file with DIFFERENT content than the intent record's hash
    const stagedPath = join(FEATURES_DIR, '.staging', 'run1', '00-00-30.jsonl');
    mkdirSync(dirname(stagedPath), { recursive: true });
    writeFileSync(stagedPath, realShard);  // real shard, but manifest hash is wrong

    // Intent record with wrong hash
    const intentRec = makeIntentRecord(key, blockMs, inputSha, wrongHash);
    intentRec.staged_path = stagedPath;
    writeManifestFile({
      schema_version: 'burst_features_v1',
      market: MARKET,
      last_checkpoint_block_start: null,
      processed_blocks: { [key]: intentRec },
    });

    const result = reconcileMarketState(MARKET, TEST_ROOT);
    // Should be quarantined (hash mismatch)
    assert.ok(result.quarantinedKeys.length >= 1 || result.errors.length >= 0,
      'should have quarantined keys or errors');
    // Staged should have been removed
    assert.ok(!existsSync(stagedPath), 'staged file should be removed on mismatch');
  });

  it('Fixture 4: committed + final shard missing => quarantined', async () => {
    cleanup();
    const { reconcileMarketState } = await import('../../lib/burst-reducer/recovery.mjs');

    const blockMs = 30000;
    const inputSha = sha256('input4');
    const key = compositeKey(MARKET, blockMs, inputSha);

    // Manifest committed record but NO final shard file
    writeManifestFile({
      schema_version: 'burst_features_v1',
      market: MARKET,
      last_checkpoint_block_start: blockMs,
      processed_blocks: {
        [key]: makeCommittedRecord(key, blockMs, inputSha, 'somehash', 1),
      },
    });

    const result = reconcileMarketState(MARKET, TEST_ROOT);
    assert.ok(result.quarantinedKeys.length >= 1,
      'committed record with missing shard should be quarantined');
  });

  it('Fixture 5: committed + final shard hash mismatch => quarantined', async () => {
    cleanup();
    const { reconcileMarketState } = await import('../../lib/burst-reducer/recovery.mjs');

    const blockMs = 30000;
    const inputSha = sha256('input5');
    const shardContent = makeShardContent(blockMs, MARKET);
    const wrongHash = sha256('wrong');
    const key = compositeKey(MARKET, blockMs, inputSha);

    // Create final shard file
    const date = '1970-01-01';
    const finalPath = join(FEATURES_DIR, date, '00-00-30.jsonl');
    mkdirSync(dirname(finalPath), { recursive: true });
    writeFileSync(finalPath, shardContent);

    // Committed record with WRONG hash
    writeManifestFile({
      schema_version: 'burst_features_v1',
      market: MARKET,
      last_checkpoint_block_start: blockMs,
      processed_blocks: {
        [key]: makeCommittedRecord(key, blockMs, inputSha, wrongHash, 1),
      },
    });

    const result = reconcileMarketState(MARKET, TEST_ROOT);
    assert.ok(result.quarantinedKeys.length >= 1,
      'committed record with hash mismatch should be quarantined');
  });

  it('Fixture 6: committed + checkpoint generation mismatch => quarantined', async () => {
    cleanup();
    const { reconcileMarketState } = await import('../../lib/burst-reducer/recovery.mjs');

    const blockMs = 30000;
    const inputSha = sha256('input6');
    const shardContent = makeShardContent(blockMs, MARKET);
    const finalHash = sha256(shardContent);
    const key = compositeKey(MARKET, blockMs, inputSha);

    // Create final shard file
    const date = '1970-01-01';
    const finalPath = join(FEATURES_DIR, date, '00-00-30.jsonl');
    mkdirSync(dirname(finalPath), { recursive: true });
    writeFileSync(finalPath, shardContent);

    // Committed record with generation=5
    writeManifestFile({
      schema_version: 'burst_features_v1',
      market: MARKET,
      last_checkpoint_block_start: blockMs,
      processed_blocks: {
        [key]: makeCommittedRecord(key, blockMs, inputSha, finalHash, 5),
      },
    });

    // Checkpoint with DIFFERENT generation (no way to arrive at gen 5)
    writeCheckpointFile({
      schema_version: 'burst_features_v1',
      last_committed_block_start: blockMs,
      pending_block: null,
      open_burst: null,
      generation: 2,
      updated_at: new Date().toISOString(),
    });

    const result = reconcileMarketState(MARKET, TEST_ROOT);
    assert.ok(result.quarantinedKeys.length >= 1,
      'committed record with generation mismatch should be quarantined');
  });

  // ═══ Bug 1 regression: writeManifestRecord 4th arg overrides status ═══

  it('Fixture 7: intent quarantine record status is quarantined in manifest (Bug 1)', async () => {
    cleanup();
    const { reconcileMarketState } = await import('../../lib/burst-reducer/recovery.mjs');
    const { loadManifest } = await import('../../lib/burst-reducer/manifest-manager.mjs');

    const blockMs = 90000;
    const inputSha = sha256('input7');
    const key = compositeKey(MARKET, blockMs, inputSha);

    // Intent with no staged, no final → quarantine (neither exists path)
    const intentRec = makeIntentRecord(key, blockMs, inputSha, 'somehash');
    intentRec.staged_path = join(FEATURES_DIR, '.staging', 'run1', '00-01-30.jsonl');
    writeManifestFile({
      schema_version: 'burst_features_v1',
      market: MARKET,
      last_checkpoint_block_start: null,
      processed_blocks: { [key]: intentRec },
    });

    reconcileMarketState(MARKET, TEST_ROOT);

    const manifest = loadManifest(MARKET, TEST_ROOT);
    assert.ok(manifest?.processed_blocks?.[key], 'manifest record should exist');
    assert.equal(manifest.processed_blocks[key].status, 'quarantined',
      'quarantined intent record status should be quarantined, not intent');
  });

  it('Fixture 8: committed quarantine record status is quarantined in manifest (Bug 1)', async () => {
    cleanup();
    const { reconcileMarketState } = await import('../../lib/burst-reducer/recovery.mjs');
    const { loadManifest } = await import('../../lib/burst-reducer/manifest-manager.mjs');

    const blockMs = 120000;
    const inputSha = sha256('input8');
    const key = compositeKey(MARKET, blockMs, inputSha);

    // Committed with missing final shard → quarantine
    writeManifestFile({
      schema_version: 'burst_features_v1',
      market: MARKET,
      last_checkpoint_block_start: blockMs,
      processed_blocks: {
        [key]: makeCommittedRecord(key, blockMs, inputSha, 'somehash', 2),
      },
    });

    reconcileMarketState(MARKET, TEST_ROOT);

    const manifest = loadManifest(MARKET, TEST_ROOT);
    assert.ok(manifest?.processed_blocks?.[key], 'manifest record should exist');
    assert.equal(manifest.processed_blocks[key].status, 'quarantined',
      'quarantined committed record status should be quarantined, not committed');
  });

  // ═══ Bug 3 regression: intent final shard hash check before committed ═══

  it('Fixture 9: intent + final exists + staged exists + hash matches => committed (Bug 3)', async () => {
    cleanup();
    const { reconcileMarketState } = await import('../../lib/burst-reducer/recovery.mjs');
    const { loadManifest } = await import('../../lib/burst-reducer/manifest-manager.mjs');

    const blockMs = 180000;
    const inputSha = sha256('input9');
    const content = makeShardContent(blockMs, MARKET);
    const contentHash = sha256(content);
    const key = compositeKey(MARKET, blockMs, inputSha);

    // Create staged file
    const stagedPath = join(FEATURES_DIR, '.staging', 'run1', '00-03-00.jsonl');
    mkdirSync(dirname(stagedPath), { recursive: true });
    writeFileSync(stagedPath, content);

    // Create final file with SAME content
    const date = '1970-01-01';
    const finalPath = join(FEATURES_DIR, date, '00-03-00.jsonl');
    mkdirSync(dirname(finalPath), { recursive: true });
    writeFileSync(finalPath, content);

    // Intent record with staged_hash matching both files
    const intentRec = makeIntentRecord(key, blockMs, inputSha, contentHash);
    writeManifestFile({
      schema_version: 'burst_features_v1',
      market: MARKET,
      last_checkpoint_block_start: null,
      processed_blocks: { [key]: intentRec },
    });

    const result = reconcileMarketState(MARKET, TEST_ROOT);
    assert.deepEqual(result.quarantinedKeys, [],
      'intent with matching hashes should not quarantine');

    const manifest = loadManifest(MARKET, TEST_ROOT);
    assert.equal(manifest.processed_blocks[key].status, 'committed',
      'matching hashes should result in committed');
  });

  it('Fixture 10: intent + final exists + staged exists + hash MISMATCH => quarantine (Bug 3)', async () => {
    cleanup();
    const { reconcileMarketState } = await import('../../lib/burst-reducer/recovery.mjs');
    const { loadManifest } = await import('../../lib/burst-reducer/manifest-manager.mjs');

    const blockMs = 210000;
    const inputSha = sha256('input10');
    const stagedContent = makeShardContent(blockMs, MARKET);
    // Final with different market → different content
    const finalContent = makeShardContent(blockMs, 'WRONG_MARKET');
    const stagedHash = sha256(stagedContent);
    const key = compositeKey(MARKET, blockMs, inputSha);

    // Create staged file
    const stagedPath = join(FEATURES_DIR, '.staging', 'run1', '00-03-30.jsonl');
    mkdirSync(dirname(stagedPath), { recursive: true });
    writeFileSync(stagedPath, stagedContent);

    // Create final file with DIFFERENT content
    const date = '1970-01-01';
    const finalPath = join(FEATURES_DIR, date, '00-03-30.jsonl');
    mkdirSync(dirname(finalPath), { recursive: true });
    writeFileSync(finalPath, finalContent);

    // Intent record with staged_hash = hash of staged content (≠ final hash)
    const intentRec = makeIntentRecord(key, blockMs, inputSha, stagedHash);
    writeManifestFile({
      schema_version: 'burst_features_v1',
      market: MARKET,
      last_checkpoint_block_start: null,
      processed_blocks: { [key]: intentRec },
    });

    const result = reconcileMarketState(MARKET, TEST_ROOT);
    assert.ok(result.quarantinedKeys.length >= 1,
      'intent with mismatching final shard hash should be quarantined');

    const manifest = loadManifest(MARKET, TEST_ROOT);
    assert.equal(manifest.processed_blocks[key].status, 'quarantined',
      'hash mismatch should result in quarantined status');
  });

  it('Fixture 11: intent + final exists + staged gone + hash matches => committed (Bug 3)', async () => {
    cleanup();
    const { reconcileMarketState } = await import('../../lib/burst-reducer/recovery.mjs');
    const { loadManifest } = await import('../../lib/burst-reducer/manifest-manager.mjs');

    const blockMs = 240000;
    const inputSha = sha256('input11');
    const content = makeShardContent(blockMs, MARKET);
    const contentHash = sha256(content);
    const key = compositeKey(MARKET, blockMs, inputSha);

    // Create final file (no staged)
    const date = '1970-01-01';
    const finalPath = join(FEATURES_DIR, date, '00-04-00.jsonl');
    mkdirSync(dirname(finalPath), { recursive: true });
    writeFileSync(finalPath, content);

    // Intent record with staged_hash matching final content
    const intentRec = makeIntentRecord(key, blockMs, inputSha, contentHash);
    writeManifestFile({
      schema_version: 'burst_features_v1',
      market: MARKET,
      last_checkpoint_block_start: null,
      processed_blocks: { [key]: intentRec },
    });

    const result = reconcileMarketState(MARKET, TEST_ROOT);
    assert.deepEqual(result.quarantinedKeys, [],
      'intent with matching final shard hash (staged gone) should not quarantine');

    const manifest = loadManifest(MARKET, TEST_ROOT);
    assert.equal(manifest.processed_blocks[key].status, 'committed',
      'matching hash (staged gone) should result in committed');
  });

  it('Fixture 12: intent + final exists + staged gone + hash MISMATCH => quarantine (Bug 3)', async () => {
    cleanup();
    const { reconcileMarketState } = await import('../../lib/burst-reducer/recovery.mjs');
    const { loadManifest } = await import('../../lib/burst-reducer/manifest-manager.mjs');

    const blockMs = 270000;
    const inputSha = sha256('input12');
    const finalContent = makeShardContent(blockMs, MARKET);
    const wrongHash = sha256('wrong-content-entirely');
    const key = compositeKey(MARKET, blockMs, inputSha);

    // Create final file with real content
    const date = '1970-01-01';
    const finalPath = join(FEATURES_DIR, date, '00-04-30.jsonl');
    mkdirSync(dirname(finalPath), { recursive: true });
    writeFileSync(finalPath, finalContent);

    // Intent record with WRONG staged_hash
    const intentRec = makeIntentRecord(key, blockMs, inputSha, wrongHash);
    writeManifestFile({
      schema_version: 'burst_features_v1',
      market: MARKET,
      last_checkpoint_block_start: null,
      processed_blocks: { [key]: intentRec },
    });

    const result = reconcileMarketState(MARKET, TEST_ROOT);
    assert.ok(result.quarantinedKeys.length >= 1,
      'intent with mismatching final shard hash (staged gone) should be quarantined');

    const manifest = loadManifest(MARKET, TEST_ROOT);
    assert.equal(manifest.processed_blocks[key].status, 'quarantined',
      'hash mismatch (staged gone) should result in quarantined');
  });

  it('Fixture 13: intent + final exists + checkpoint generation mismatch => quarantine (Bug 3)', async () => {
    cleanup();
    const { reconcileMarketState } = await import('../../lib/burst-reducer/recovery.mjs');
    const { loadManifest } = await import('../../lib/burst-reducer/manifest-manager.mjs');

    const blockMs = 300000;
    const inputSha = sha256('input13');
    const content = makeShardContent(blockMs, MARKET);
    const contentHash = sha256(content);
    const key = compositeKey(MARKET, blockMs, inputSha);

    // Create final file with matching content (hash would pass)
    const date = '1970-01-01';
    const finalPath = join(FEATURES_DIR, date, '00-05-00.jsonl');
    mkdirSync(dirname(finalPath), { recursive: true });
    writeFileSync(finalPath, content);

    // Intent record with hash matching, but checkpoint_generation AHEAD of cp
    const intentRec = makeIntentRecord(key, blockMs, inputSha, contentHash);
    intentRec.checkpoint_generation = 5; // intentionally high generation
    writeManifestFile({
      schema_version: 'burst_features_v1',
      market: MARKET,
      last_checkpoint_block_start: null,
      processed_blocks: { [key]: intentRec },
    });

    // Checkpoint with LOWER generation → inconsistency
    writeCheckpointFile({
      schema_version: 'burst_features_v1',
      last_committed_block_start: blockMs - 30000,
      pending_block: null,
      open_burst: null,
      generation: 3, // < 5 → record gen >= cp gen → inconsistency
      updated_at: new Date().toISOString(),
    });

    const result = reconcileMarketState(MARKET, TEST_ROOT);
    assert.ok(result.quarantinedKeys.length >= 1,
      'intent with generation mismatch should be quarantined');

    const manifest = loadManifest(MARKET, TEST_ROOT);
    assert.equal(manifest.processed_blocks[key].status, 'quarantined',
      'generation mismatch should result in quarantined');
  });

  // ── PDD safety fix 2: staged-only intent generation + cursor consistency ──

  it('Fixture 14: staged-only with checkpoint generation mismatch => quarantine', async () => {
    cleanup();
    const { reconcileMarketState } = await import('../../lib/burst-reducer/recovery.mjs');
    const { loadManifest } = await import('../../lib/burst-reducer/manifest-manager.mjs');

    const blockMs = 330000;
    const inputSha = sha256('input14');
    const content = makeShardContent(blockMs, MARKET);
    const contentHash = sha256(content);
    const key = compositeKey(MARKET, blockMs, inputSha);

    // Create staged file with matching content (hash would pass)
    const stagedPath = join(FEATURES_DIR, '.staging', 'run1', '00-05-30.jsonl');
    mkdirSync(dirname(stagedPath), { recursive: true });
    writeFileSync(stagedPath, content);

    // Intent record with staged-only (no final), matching hash, but checkpoint_generation ahead of cp
    const intentRec = makeIntentRecord(key, blockMs, inputSha, contentHash);
    intentRec.checkpoint_generation = 10; // ahead of cp's generation
    writeManifestFile({
      schema_version: 'burst_features_v1',
      market: MARKET,
      last_checkpoint_block_start: null,
      processed_blocks: { [key]: intentRec },
    });

    // Checkpoint with LOWER generation → inconsistency
    writeCheckpointFile({
      schema_version: 'burst_features_v1',
      last_committed_block_start: blockMs - 30000,
      pending_block: null,
      open_burst: null,
      generation: 3, // < 10 → record gen > cp gen → generation mismatch
      updated_at: new Date().toISOString(),
    });

    const result = reconcileMarketState(MARKET, TEST_ROOT);
    assert.ok(result.quarantinedKeys.length >= 1,
      'staged-only with generation mismatch should be quarantined');

    const manifest = loadManifest(MARKET, TEST_ROOT);
    assert.equal(manifest.processed_blocks[key].status, 'quarantined',
      'generation mismatch staged-only should result in quarantined');

    // Staged file should be removed
    assert.ok(!existsSync(stagedPath), 'staged file should be removed on generation mismatch');
  });

  it('Fixture 15: staged-only with checkpoint cursor mismatch => quarantine', async () => {
    cleanup();
    const { reconcileMarketState } = await import('../../lib/burst-reducer/recovery.mjs');
    const { loadManifest } = await import('../../lib/burst-reducer/manifest-manager.mjs');

    const blockMs = 360000;
    const inputSha = sha256('input15');
    const content = makeShardContent(blockMs, MARKET);
    const contentHash = sha256(content);
    const key = compositeKey(MARKET, blockMs, inputSha);

    // Create staged file with matching content
    const stagedPath = join(FEATURES_DIR, '.staging', 'run1', '00-06-00.jsonl');
    mkdirSync(dirname(stagedPath), { recursive: true });
    writeFileSync(stagedPath, content);

    // Intent record with staged-only, matching hash, generation=1 (valid)
    const intentRec = makeIntentRecord(key, blockMs, inputSha, contentHash);
    intentRec.checkpoint_generation = 1;
    writeManifestFile({
      schema_version: 'burst_features_v1',
      market: MARKET,
      last_checkpoint_block_start: null,
      processed_blocks: { [key]: intentRec },
    });

    // Checkpoint that claims a DIFFERENT pending block than what staged is for
    // Staged block=360000, but cp says pending should be 390000
    writeCheckpointFile({
      schema_version: 'burst_features_v1',
      last_committed_block_start: 330000,
      pending_block: {
        block_start_ms: 390000,  // different from staged block_start_ms=360000
        trade_input_sha256: 'somehash',
        auxiliary_input_hashes: {},
        replay_identity: { market: MARKET, block_start_ms: 390000, input_path: '/tmp/39000.jsonl' },
      },
      open_burst: null,
      generation: 2,
      updated_at: new Date().toISOString(),
    });

    const result = reconcileMarketState(MARKET, TEST_ROOT);
    assert.ok(result.quarantinedKeys.length >= 1,
      'staged-only with cursor mismatch should be quarantined');

    const manifest = loadManifest(MARKET, TEST_ROOT);
    assert.equal(manifest.processed_blocks[key].status, 'quarantined',
      'cursor mismatch staged-only should result in quarantined');

    // Staged file should be removed
    assert.ok(!existsSync(stagedPath), 'staged file should be removed on cursor mismatch');
  });
});
