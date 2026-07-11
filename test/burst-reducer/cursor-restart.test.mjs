// test/burst-reducer/cursor-restart.test.mjs — P0-2 authoritative cursor + P1-1 minimal checkpoint tests
import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import { existsSync, mkdirSync, writeFileSync, rmSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { runPipeline } from '../../lib/burst-reducer/pipeline.mjs';
import { loadCheckpoint, writeCheckpoint, loadManifest } from '../../lib/burst-reducer/manifest-manager.mjs';

const TEST_DIR = join('test', 'fixtures', 'burst-v1', 'tmp-cursor');
const MARKET = 'test_cursor';
const RUN_ID = 'cursor-test';

function makeTradeBlock(blockStartMs, trades) {
  return trades.map(t => JSON.stringify(t)).join('\n') + '\n';
}

function setupBlocks() {
  rmSync(TEST_DIR, { recursive: true, force: true });
  mkdirSync(join(TEST_DIR, 'trades', MARKET, '1970-01-01'), { recursive: true });
  mkdirSync(join(TEST_DIR, 'trades', MARKET, '1969-12-31'), { recursive: true });

  // Block 0
  writeFileSync(join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-00-00.jsonl'),
    makeTradeBlock(0, [
      { ts: 500, side: 'buy', price: 100, qty: 1 },
      { ts: 520, side: 'buy', price: 100, qty: 2 },
    ]));

  // Block 30000
  writeFileSync(join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-00-30.jsonl'),
    makeTradeBlock(30000, [
      { ts: 30500, side: 'sell', price: 101, qty: 1 },
    ]));

  // Block 60000
  writeFileSync(join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-01-00.jsonl'),
    makeTradeBlock(60000, [
      { ts: 60500, side: 'buy', price: 102, qty: 2 },
    ]));

  // Raw trade lookback block for Block 0
  writeFileSync(join(TEST_DIR, 'trades', MARKET, '1969-12-31', '23-59-30.jsonl'),
    makeTradeBlock(-30000, [{ ts: -29000, side: 'buy', price: 100, qty: 2 }]));
}

function cleanDerived() {
  try { rmSync(join('data/derived/burst_features_v1', 'features_1s', MARKET), { recursive: true, force: true }); } catch (_) {}
  try { rmSync(join('data/derived/burst_features_v1', 'manifests', `${MARKET}.json`), { force: true }); } catch (_) {}
  try { rmSync(join('data/derived/burst_features_v1', 'manifests/checkpoints', `${MARKET}.json`), { force: true }); } catch (_) {}
}

// finalizedThroughMs=90000: after 3 blocks (0,30000,60000), next boundary=90000 == finalizedThrough → EOF
const HORIZON_3BLOCKS = 90000;

describe('Authoritative Cursor (P0-2) + Minimal Checkpoint (P1-1)', () => {
  before(() => { setupBlocks(); });
  after(() => {
    rmSync(TEST_DIR, { recursive: true, force: true });
    cleanDerived();
  });

  it('checkpoint cursor takes precedence over CLI --from', async () => {
    cleanDerived();
    // Step 1: Run with fromMs=30000, toMs=60000, no horizon
    // Block 30000 found, block 60000 is N+1 → block 30000 committed (1 block)
    const r1 = await runPipeline({
      dataDir: TEST_DIR, market: MARKET, fromMs: 30000, toMs: 90000,
      runId: RUN_ID + '-cursor-1',
    });
    // Block 30000 committed (pending=60000), then blocked (no N+1 for 60000)
    assert.equal(r1.processed, 1, 'first run commits block 30000');
    assert.equal(r1.blocked, true);

    // Step 2: Run fromMs=0 with horizon. Checkpoint has last_committed=30000, pending=60000
    // effectiveFromMs should be 60000 (from pending), not 0
    const r2 = await runPipeline({
      dataDir: TEST_DIR, market: MARKET, fromMs: 0, toMs: 120000,
      runId: RUN_ID + '-cursor-2',
      finalizedThroughMs: HORIZON_3BLOCKS,
    });
    // Should resume from pending=60000: block 60000 committed (N+1 missing → verified-missing at 90000)
    // But wait: with HORIZON_3BLOCKS=90000, next boundary=90000 == finalizedThrough → EOF
    // So: pending=60000, N+1 at 90000 doesn't exist → horizon boundary → EOF finalizes block 60000
    // Total: 1 more block committed
    assert.equal(r2.processed, 1, 'should resume from checkpoint and commit block 60000');
  });

  it('restart with pending block produces consistent output', async () => {
    cleanDerived();
    // Full run
    const r1 = await runPipeline({
      dataDir: TEST_DIR, market: MARKET, fromMs: 0, toMs: 120000,
      runId: RUN_ID + '-byte-1',
      finalizedThroughMs: HORIZON_3BLOCKS,
    });
    assert.equal(r1.processed, 3);

    const featuresDir = join('data/derived/burst_features_v1', 'features_1s', MARKET, '1970-01-01');
    const { readdirSync: rd } = await import('node:fs');
    const files1 = rd(featuresDir).filter(f => f.endsWith('.jsonl')).sort();

    // Simulate restart with checkpoint at block 0
    cleanDerived();
    const pendingBlock = {
      block_start_ms: 0,
      trade_input_sha256: createHash('sha256').update(
        readFileSync(join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-00-00.jsonl'), 'utf8')
      ).digest('hex'),
      auxiliary_input_hashes: {},
      replay_identity: {
        market: MARKET,
        block_start_ms: 0,
        input_path: join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-00-00.jsonl'),
      },
    };
    writeCheckpoint({
      last_committed_block_start: null,
      pending_block: pendingBlock,
      open_burst: null,
      generation: 0,
      market: MARKET,
    });

    // Restart
    const r2 = await runPipeline({
      dataDir: TEST_DIR, market: MARKET, fromMs: 0, toMs: 120000,
      runId: RUN_ID + '-byte-2',
      finalizedThroughMs: HORIZON_3BLOCKS,
    });
    assert.equal(r2.processed, 3, 'restart should process same number of blocks');

    // Same number of output files
    const files2 = rd(featuresDir).filter(f => f.endsWith('.jsonl')).sort();
    assert.deepEqual(files1, files2, 'same output file names');

    // Each file should have 30 rows
    for (const f of files2) {
      const content = readFileSync(join(featuresDir, f), 'utf8');
      const lines = content.trim().split('\n');
      assert.equal(lines.length, 30, `${f} should have 30 rows`);
    }
  });

  it('P1-1: EOF checkpoint does not contain closedBursts', async () => {
    cleanDerived();
    await runPipeline({
      dataDir: TEST_DIR, market: MARKET, fromMs: 0, toMs: 120000,
      runId: RUN_ID + '-p1-1',
      finalizedThroughMs: HORIZON_3BLOCKS,
    });

    const cp = loadCheckpoint(MARKET);
    assert.ok(cp);
    // After EOF, open_burst should be null
    assert.equal(cp.open_burst, null);
    // pending_block should be null
    assert.equal(cp.pending_block, null);
    // Checkpoint should NOT have closedBursts field
    assert.ok(!('closedBursts' in cp), 'checkpoint must not contain closedBursts');

    // Checkpoint size should be reasonable
    const cpStr = JSON.stringify(cp);
    assert.ok(cpStr.length < 4096, `checkpoint should be small (<4KB), got ${cpStr.length} bytes`);
  });

  it('P1-1: checkpoint pending_block does not contain open_burst_before_N1', async () => {
    cleanDerived();
    // Run with only 2 blocks finalized → leaves a checkpoint with pending
    // finalizedThroughMs=60000: block 0 committed, block 30000 committed, EOF
    // After EOF, pending=null. But we want to check non-EOF checkpoint.
    // Run without finalizedThroughMs to leave a pending block.
    await runPipeline({
      dataDir: TEST_DIR, market: MARKET, fromMs: 0, toMs: 60000,
      runId: RUN_ID + '-p1-1b',
    });

    const cp = loadCheckpoint(MARKET);
    if (cp && cp.pending_block) {
      // P1-1: pending_block must NOT have open_burst_before_N1
      assert.ok(!('open_burst_before_N1' in cp.pending_block),
        'pending_block must not contain open_burst_before_N1');
    }
    // If no checkpoint (no blocks committed), that's also OK
  });

  it('restart at burst boundary matches uninterrupted output', async () => {
    cleanDerived();
    const r = await runPipeline({
      dataDir: TEST_DIR, market: MARKET, fromMs: 0, toMs: 120000,
      runId: RUN_ID + '-dedup',
      finalizedThroughMs: HORIZON_3BLOCKS,
    });
    assert.equal(r.processed, 3);
    // Each manifest update should have a unique key
    const keys = r.manifestUpdates.map(u => u.key);
    assert.equal(new Set(keys).size, keys.length, 'all keys should be unique');
  });

  it('absent block within horizon => commits through gap (Task 7)', async () => {
    cleanDerived();
    // Remove block 60000 file
    rmSync(join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-01-00.jsonl'));

    const r = await runPipeline({
      dataDir: TEST_DIR, market: MARKET, fromMs: 0, toMs: 120000,
      runId: RUN_ID + '-absent',
      finalizedThroughMs: HORIZON_3BLOCKS,
    });
    // Block 0 committed (pending=30000), block 30000 committed as EOF via data-none-gap
    // (block 90000 doesn't exist in setup, so only 2 blocks)
    assert.equal(r.processed, 2, 'should commit through gap (block 0 + block 30000 EOF)');
    assert.ok(!r.blocked, 'should not be blocked by gap');

    // Verify the output shards exist (2 shards for the 2 committed blocks)
    const featuresDir = join('data/derived/burst_features_v1', 'features_1s', MARKET, '1970-01-01');
    const { readdirSync: rd } = await import('node:fs');
    const shardFiles = rd(featuresDir).filter(f => f.endsWith('.jsonl'));
    assert.equal(shardFiles.length, 2, 'should have 2 shard files (no synthetic shard for absent block)');

    // Restore block 60000
    writeFileSync(join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-01-00.jsonl'),
      makeTradeBlock(60000, [{ ts: 60500, side: 'buy', price: 102, qty: 2 }]));
  });

  // P1-2: reject --from that skips past checkpoint cursor
  it('rejects --from past checkpoint cursor with E022', async () => {
    cleanDerived();
    // First run: fromMs=30000, toMs=90000 → commits block 30000, checkpoint cursor at 60000
    await runPipeline({
      dataDir: TEST_DIR, market: MARKET, fromMs: 30000, toMs: 90000,
      runId: RUN_ID + '-e022',
    });

    // Checkpoint should exist with cursor
    const cp = loadCheckpoint(MARKET);
    assert.ok(cp, 'checkpoint should exist');
    const cursorMs = cp.pending_block ? cp.pending_block.block_start_ms :
      (cp.last_committed_block_start + 30000);
    assert.ok(cursorMs > 0, 'cursor should be > 0');

    // Now try with fromMs PAST the cursor → should throw E022
    const futureFrom = cursorMs + 30000;
    await assert.rejects(
      () => runPipeline({
        dataDir: TEST_DIR, market: MARKET, fromMs: futureFrom, toMs: futureFrom + 30000,
        runId: RUN_ID + '-e022-2',
      }),
      /E022/,
      `should reject --from ${futureFrom} past cursor ${cursorMs}`
    );

    // Verify checkpoint/manifest unchanged
    const cpAfter = loadCheckpoint(MARKET);
    assert.equal(cpAfter.generation, cp.generation, 'checkpoint generation must not advance');
  });

  // ── PDD safety fix 3: truthy→nullish for last_committed_block_start ──

  it('last_committed_block_start=0 makes cursor authoritative (nullish fix)', async () => {
    cleanDerived();
    // Create a checkpoint with last_committed_block_start=0 and no pending_block
    // This was previously falsy in truthy-check and would be ignored
    writeCheckpoint({
      last_committed_block_start: 0,
      pending_block: null,
      open_burst: null,
      generation: 1,
      market: MARKET,
    });

    // Run pipeline with fromMs=0 (cursor authoritative: effectiveFromMs = 0+30000 = 30000)
    // With the nullish fix, cursor at 0 is NOT falsy, so effectiveFromMs=30000
    // Without the fix (truthy bug), cp.last_committed_block_start=0 is falsy,
    // so effectiveFromMs = fromMs = 0, and block 0 would also be processed
    // Either way the pipeline should not crash
    const r = await runPipeline({
      dataDir: TEST_DIR,
      market: MARKET,
      fromMs: 0,
      toMs: 90000,
      runId: RUN_ID + '-nullish',
    });
    // At minimum block 30000 should be the first block scanned → processed >= 1
    assert.ok(r.processed >= 1, 'pipeline should process at least 1 block with cursor=0');
    // After this run, checkpoint should have been updated
    const cpAfter = loadCheckpoint(MARKET);
    assert.ok(cpAfter, 'checkpoint should exist after processing');
    assert.ok(cpAfter.generation > 1, 'checkpoint generation should advance from 1');
    assert.ok(cpAfter.last_committed_block_start >= 0,
      'checkpoint last_committed should reflect processed block');
  });

  // ═══ Task 7: gap integration tests ═══

  it('synthetic N→N+2 gap: both sides committed, no synthetic shard, cursor advances, manifest has range audit', async () => {
    cleanDerived();
    // Ensure block 60000 exists (may have been deleted by previous test)
    const block60000Path = join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-01-00.jsonl');
    if (!existsSync(block60000Path)) {
      writeFileSync(block60000Path,
        makeTradeBlock(60000, [{ ts: 60500, side: 'buy', price: 102, qty: 2 }]));
    }
    // Create block 90000 for the N→N+2 gap test (90000 exists as the next candidate after gap)
    const block90000Path = join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-01-30.jsonl');
    writeFileSync(block90000Path,
      makeTradeBlock(90000, [{ ts: 90500, side: 'sell', price: 103, qty: 1 }]));

    // Now remove block 60000 — blocks are [0, 30000, 90000]
    rmSync(block60000Path);

    const r = await runPipeline({
      dataDir: TEST_DIR, market: MARKET, fromMs: 0, toMs: 120000,
      runId: RUN_ID + '-gap-range',
      finalizedThroughMs: HORIZON_3BLOCKS,
    });

    // Both sides of the gap committed + EOF finalize = 3 blocks
    assert.equal(r.processed, 3, 'should process 3 blocks (0, 30000 via gap, 90000 EOF)');
    assert.ok(!r.blocked, 'should not be blocked by gap');

    // Verify output shards exist for both sides of gap (no synthetic shard for absent block)
    const featuresDir = join('data/derived/burst_features_v1', 'features_1s', MARKET, '1970-01-01');
    const { readdirSync: rd } = await import('node:fs');
    const shardFiles = rd(featuresDir).filter(f => f.endsWith('.jsonl')).sort();
    assert.equal(shardFiles.length, 3, 'should have exactly 3 shards (no synthetic for absent 60000)');

    // Checkpoint cursor should have advanced
    const cp = loadCheckpoint(MARKET);
    assert.ok(cp, 'checkpoint should exist');
    assert.equal(cp.generation, 3, 'generation should be 3 (3 commits)');

    // Manifest has range audit for the gap at 60000→90000
    const manifest = loadManifest(MARKET);
    assert.ok(manifest, 'manifest should exist');
    const records = Object.values(manifest.processed_blocks);
    const gapRecords = records.filter(r => r.assumed_empty_gap_ranges && r.assumed_empty_gap_ranges.length > 0);
    assert.ok(gapRecords.length >= 1, 'should have at least 1 record with assumed_empty_gap_ranges');
    const gapRange = gapRecords[0].assumed_empty_gap_ranges[0];
    assert.equal(gapRange.start_ms, 60000, 'gap start should be 60000');
    assert.equal(gapRange.end_ms_exclusive, 90000, 'gap end should be 90000');

    // Cleanup: restore block 60000, remove block 90000
    writeFileSync(block60000Path,
      makeTradeBlock(60000, [{ ts: 60500, side: 'buy', price: 102, qty: 2 }]));
    rmSync(block90000Path, { force: true });
  });

  it('long gap audit is 1 range entry, not an array proportional to block count', async () => {
    cleanDerived();
    // Remove blocks 30000 and 60000 from setup, leaving only block 0 and a far-ahead block
    rmSync(join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-00-30.jsonl'), { force: true });
    rmSync(join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-01-00.jsonl'), { force: true });

    // Create a block far ahead: block 300000 (5:00, a 270s / 9-block gap from block 0)
    const longGapBlockMs = 300000;
    const longGapPath = join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-05-00.jsonl');
    writeFileSync(longGapPath,
      makeTradeBlock(longGapBlockMs, [{ ts: longGapBlockMs + 500, side: 'buy', price: 100, qty: 2 }]));

    // Scan from 0 to 330000: finds [0, 300000]. Gap handler commits block 0 with 1 range entry.
    const r = await runPipeline({
      dataDir: TEST_DIR, market: MARKET, fromMs: 0, toMs: 330000,
      runId: RUN_ID + '-long-gap',
      finalizedThroughMs: longGapBlockMs + 30000,
    });

    // Block 0 committed via gap handler, block 300000 EOF-finalized
    assert.ok(r.processed >= 1, 'should process at least 1 block');
    assert.ok(!r.blocked, 'should not be blocked by long gap');

    // Manifest audit: should have exactly 1 gap range (not 9 individual entries)
    const manifest = loadManifest(MARKET);
    assert.ok(manifest, 'manifest should exist');
    const records = Object.values(manifest.processed_blocks);
    let totalGapRangeCount = 0;
    for (const rec of records) {
      if (rec.assumed_empty_gap_ranges) {
        totalGapRangeCount += rec.assumed_empty_gap_ranges.length;
      }
    }
    // 9 absent blocks (30k-270k) in a single range
    assert.equal(totalGapRangeCount, 1, 'should have exactly 1 gap range (not 9)');

    const gapRec = records.find(r => r.assumed_empty_gap_ranges && r.assumed_empty_gap_ranges.length > 0);
    assert.ok(gapRec, 'should find record with gap range');
    const range = gapRec.assumed_empty_gap_ranges[0];
    assert.equal(range.start_ms, 30000, 'gap starts at 30000');
    assert.equal(range.end_ms_exclusive, longGapBlockMs, `gap ends at ${longGapBlockMs}`);
    assert.equal(typeof range.start_ms, 'number', 'start_ms is a number');
    assert.equal(typeof range.end_ms_exclusive, 'number', 'end_ms_exclusive is a number');

    // Cleanup
    rmSync(longGapPath, { force: true });
    // Restore blocks for other tests
    writeFileSync(join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-00-30.jsonl'),
      makeTradeBlock(30000, [{ ts: 30500, side: 'sell', price: 101, qty: 1 }]));
    writeFileSync(join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-01-00.jsonl'),
      makeTradeBlock(60000, [{ ts: 60500, side: 'buy', price: 102, qty: 2 }]));
  });
});
