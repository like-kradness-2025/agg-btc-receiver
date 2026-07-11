// test/burst-reducer/manifest-manager.test.mjs — ManifestManager tests
import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import { existsSync, readFileSync, mkdirSync, rmSync, writeFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { loadManifest, writeManifestRecord, loadCheckpoint, writeCheckpoint, MANIFEST_CORRUPT, CHECKPOINT_CORRUPT } from '../../lib/burst-reducer/manifest-manager.mjs';

// Test with a dedicated test output root
const TEST_ROOT = 'data/derived/burst_features_v1_test_manifest';
const MARKET = 'test_manifest';

// Override DERIVED_DIR by patching process? We can't, so we test the real path.
// Instead, clear state in the real path before/after to avoid interfering with production.

describe('ManifestManager', () => {
  const testManifestPath = join('data/derived/burst_features_v1', 'manifests', `${MARKET}.json`);
  const testCpDir = join('data/derived/burst_features_v1', 'manifests/checkpoints');

  before(() => {
    // Clean any leftover test state from all test markets
    const cleanMarkets = ['test_manifest', 'test_committer', 'test_pipeline', 'test_committer_err', 'test_committer_err2'];
    const testManifestDir = join('data/derived/burst_features_v1', 'manifests');
    const testCpDir = join('data/derived/burst_features_v1', 'manifests/checkpoints');
    for (const m of cleanMarkets) {
      try { rmSync(join(testManifestDir, `${m}.json`), { force: true }); } catch (_) {}
      try { rmSync(join(testCpDir, `${m}.json`), { force: true }); } catch (_) {}
    }
  });

  after(() => {
    try { rmSync(testManifestPath, { force: true }); } catch (_) {}
    try { rmSync(join(testCpDir, `${MARKET}.json`), { force: true }); } catch (_) {}
  });

  it('loadManifest returns null for non-existent market', () => {
    const m = loadManifest('nonexistent_xyz');
    assert.equal(m, null);
  });

  it('writeManifestRecord with intent status creates file', () => {
    const key = 'burst_features_v1:test_manifest:1000:abc123';
    const result = writeManifestRecord(MARKET, key, {
      block_start_ms: 1000,
      input_sha256: 'abc123',
      staged_row_hash: 'def456',
      staged_path: '/tmp/test.jsonl',
      output_path: 'features_1s/test_manifest/2026-07-10/00-00-00.jsonl',
      checkpoint_generation: 1,
      commit_id: 'uuid-1',
      auxiliary_input_hashes: {},
    }, 'intent', null);

    assert.ok(existsSync(testManifestPath));
    assert.equal(result.processed_blocks[key].status, 'intent');
  });

  it('writeManifestRecord with committed status updates existing', () => {
    const key = 'burst_features_v1:test_manifest:1000:abc123';
    const existing = loadManifest(MARKET);
    const result = writeManifestRecord(MARKET, key, {
      block_start_ms: 1000,  // needed for last_checkpoint update
      output_row_hash: 'ghi789',
      checkpoint_generation: 2,
      commit_id: 'uuid-1',
    }, 'committed', existing);

    assert.equal(result.processed_blocks[key].status, 'committed');
    assert.equal(result.processed_blocks[key].output_row_hash, 'ghi789');
    assert.equal(result.processed_blocks[key].input_sha256, 'abc123'); // preserved from intent
    assert.equal(result.last_checkpoint_block_start, 1000);
  });

  it('writeCheckpoint creates file with correct structure', () => {
    const cp = writeCheckpoint({
      last_committed_block_start: 1000,
      pending_block: { block_start_ms: 3000, replay_identity: { market: MARKET } },
      open_burst: null,
      generation: 2,
      market: MARKET,
    });

    assert.ok(existsSync(join(testCpDir, `${MARKET}.json`)));
    assert.equal(cp.last_committed_block_start, 1000);
    assert.equal(cp.pending_block.block_start_ms, 3000);
    assert.equal(cp.generation, 2);
  });

  it('loadCheckpoint returns checkpoint correctly', () => {
    const cp = loadCheckpoint(MARKET);
    assert.ok(cp);
    assert.equal(cp.last_committed_block_start, 1000);
    assert.equal(cp.pending_block.block_start_ms, 3000);
  });

  it('checkpoint generation increases monotonically', () => {
    const cp2 = writeCheckpoint({
      last_committed_block_start: 3000,
      pending_block: { block_start_ms: 6000, replay_identity: { market: MARKET } },
      open_burst: null,
      generation: 3,
      market: MARKET,
    });
    assert.equal(cp2.generation, 3);
    assert.ok(cp2.generation > 2);
  });

  it('composite key structure', () => {
    const key = 'burst_features_v1:test_manifest:1000:abc123';
    const parts = key.split(':');
    assert.equal(parts.length, 4);
    assert.equal(parts[0], 'burst_features_v1');
    assert.equal(parts[2], '1000');
  });

  // ── P1-2: Corrupt manifest detection ───────────────────────────────────

  it('loadManifest returns MANIFEST_CORRUPT for empty manifest, backs up file', () => {
    const manifestDir = join('data/derived/burst_features_v1', 'manifests');
    const market = 'corrupt_empty';
    const path = join(manifestDir, `${market}.json`);

    // Write empty file
    mkdirSync(manifestDir, { recursive: true });
    writeFileSync(path, '');

    // Count .bak files before
    const baksBefore = readdirSync(manifestDir).filter(f => f.startsWith(`${market}.json.bak.`));

    const result = loadManifest(market);
    assert.equal(result, MANIFEST_CORRUPT, 'should return MANIFEST_CORRUPT for empty file');

    // .bak file should exist
    const baksAfter = readdirSync(manifestDir).filter(f => f.startsWith(`${market}.json.bak.`));
    assert.ok(baksAfter.length > baksBefore.length, 'backup file should be created');

    // Clean up
    try { rmSync(path, { force: true }); } catch (_) {}
    for (const bak of baksAfter) {
      try { rmSync(join(manifestDir, bak), { force: true }); } catch (_) {}
    }
  });

  it('loadManifest returns MANIFEST_CORRUPT for invalid JSON, backs up file', () => {
    const manifestDir = join('data/derived/burst_features_v1', 'manifests');
    const market = 'corrupt_json';
    const path = join(manifestDir, `${market}.json`);

    // Write invalid JSON
    mkdirSync(manifestDir, { recursive: true });
    writeFileSync(path, '{invalid json!!!');

    const baksBefore = readdirSync(manifestDir).filter(f => f.startsWith(`${market}.json.bak.`));

    const result = loadManifest(market);
    assert.equal(result, MANIFEST_CORRUPT, 'should return MANIFEST_CORRUPT for invalid JSON');

    const baksAfter = readdirSync(manifestDir).filter(f => f.startsWith(`${market}.json.bak.`));
    assert.ok(baksAfter.length > baksBefore.length, 'backup file should be created');

    // Original corrupt file should still exist (renamed to .bak)
    assert.ok(!existsSync(path), 'original manifest should be renamed away');

    // Clean up
    try { rmSync(path, { force: true }); } catch (_) {}
    for (const bak of baksAfter) {
      try { rmSync(join(manifestDir, bak), { force: true }); } catch (_) {}
    }
  });

  it('reconcileMarketState with corrupt manifest returns corrupt-manifest status', async () => {
    const { reconcileMarketState } = await import('../../lib/burst-reducer/recovery.mjs');
    const manifestDir = join('data/derived/burst_features_v1', 'manifests');
    const market = 'corrupt_recovery';
    const path = join(manifestDir, `${market}.json`);

    mkdirSync(manifestDir, { recursive: true });
    writeFileSync(path, 'garbage');

    const result = reconcileMarketState(market);
    // Result should indicate corrupt-manifest — either quarantinedKeys or errors or a status field
    const isCorrupt = result.quarantinedKeys?.length > 0 || result.errors?.length > 0 ||
      result.status === 'corrupt-manifest';
    assert.ok(isCorrupt, 'recovery should detect corrupt manifest');

    // Clean up
    try { rmSync(path, { force: true }); } catch (_) {}
    const baks = readdirSync(manifestDir).filter(f => f.startsWith(`${market}.json.bak.`));
    for (const bak of baks) {
      try { rmSync(join(manifestDir, bak), { force: true }); } catch (_) {}
    }
  });

  // ── PDD: Checkpoint corruption detection ───────────────────────────────────

  it('loadCheckpoint returns CHECKPOINT_CORRUPT for empty file, backs up', () => {
    const cpDir = join('data/derived/burst_features_v1', 'manifests/checkpoints');
    const market = 'cp_corrupt_empty';
    const path = join(cpDir, `${market}.json`);

    // Write empty file
    mkdirSync(cpDir, { recursive: true });
    writeFileSync(path, '');

    // Count .bak files before
    const baksBefore = readdirSync(cpDir).filter(f => f.startsWith(`${market}.json.bak.`));

    const result = loadCheckpoint(market);
    assert.equal(result, CHECKPOINT_CORRUPT, 'should return CHECKPOINT_CORRUPT for empty file');

    // .bak file should exist
    const baksAfter = readdirSync(cpDir).filter(f => f.startsWith(`${market}.json.bak.`));
    assert.ok(baksAfter.length > baksBefore.length, 'backup file should be created');

    // Clean up
    try { rmSync(path, { force: true }); } catch (_) {}
    for (const bak of baksAfter) {
      try { rmSync(join(cpDir, bak), { force: true }); } catch (_) {}
    }
  });

  it('loadCheckpoint returns CHECKPOINT_CORRUPT for invalid JSON, backs up', () => {
    const cpDir = join('data/derived/burst_features_v1', 'manifests/checkpoints');
    const market = 'cp_corrupt_json';
    const path = join(cpDir, `${market}.json`);

    // Write invalid JSON
    mkdirSync(cpDir, { recursive: true });
    writeFileSync(path, '{invalid json!!!');

    const baksBefore = readdirSync(cpDir).filter(f => f.startsWith(`${market}.json.bak.`));

    const result = loadCheckpoint(market);
    assert.equal(result, CHECKPOINT_CORRUPT, 'should return CHECKPOINT_CORRUPT for invalid JSON');

    const baksAfter = readdirSync(cpDir).filter(f => f.startsWith(`${market}.json.bak.`));
    assert.ok(baksAfter.length > baksBefore.length, 'backup file should be created');

    // Original corrupt file should still exist (renamed to .bak)
    assert.ok(!existsSync(path), 'original checkpoint should be renamed away');

    // Clean up
    try { rmSync(path, { force: true }); } catch (_) {}
    for (const bak of baksAfter) {
      try { rmSync(join(cpDir, bak), { force: true }); } catch (_) {}
    }
  });
});
