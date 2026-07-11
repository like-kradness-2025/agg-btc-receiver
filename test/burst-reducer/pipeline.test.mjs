// test/burst-reducer/pipeline.test.mjs — Pipeline integration tests (basic flow + P0-4 horizon + Task 7 gap burst)
import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import { existsSync, mkdirSync, writeFileSync, rmSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { runPipeline } from '../../lib/burst-reducer/pipeline.mjs';
import { loadManifest, loadCheckpoint } from '../../lib/burst-reducer/manifest-manager.mjs';

function sha256(content) {
  return createHash('sha256').update(content).digest('hex');
}

const TEST_DIR = join('test', 'fixtures', 'burst-v1', 'tmp-pipeline');
const MARKET = 'test_pipeline';
const RUN_ID = 'pipeline-test-1';

function makeTradeBlock(blockStartMs, trades) {
  return trades.map(t => JSON.stringify(t)).join('\n') + '\n';
}

describe('Pipeline', () => {
  before(() => {
    rmSync(TEST_DIR, { recursive: true, force: true });
    mkdirSync(join(TEST_DIR, 'trades', MARKET, '1970-01-01'), { recursive: true });
    mkdirSync(join(TEST_DIR, 'trades', MARKET, '1969-12-31'), { recursive: true });

    // Block 0: 2 trades forming 1 burst (ts=500,520 same side, gap=20 -> 1 burst)
    writeFileSync(join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-00-00.jsonl'),
      makeTradeBlock(0, [
        { ts: 500, side: 'buy', price: 100, qty: 1 },
        { ts: 520, side: 'buy', price: 100, qty: 2 },
      ]));

    // Block 1: 1 trade (ts=30500)
    writeFileSync(join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-00-30.jsonl'),
      makeTradeBlock(30000, [
        { ts: 30500, side: 'sell', price: 101, qty: 1 },
      ]));

    // Raw trade lookback block for Block 0 (date 1969-12-31)
    writeFileSync(join(TEST_DIR, 'trades', MARKET, '1969-12-31', '23-59-30.jsonl'),
      makeTradeBlock(-30000, [
        { ts: -29000, side: 'buy', price: 100, qty: 2 },
      ]));
  });

  after(() => {
    rmSync(TEST_DIR, { recursive: true, force: true });
    // Clean any test artifacts in derived
    try {
      rmSync(join('data/derived/burst_features_v1', 'features_1s', MARKET), { recursive: true, force: true });
    } catch (_) {}
    try {
      rmSync(join('data/derived/burst_features_v1', 'manifests', `${MARKET}.json`), { force: true });
    } catch (_) {}
    try {
      rmSync(join('data/derived/burst_features_v1', 'manifests/checkpoints', `${MARKET}.json`), { force: true });
    } catch (_) {}
  });

  it('processes 2 consecutive blocks (1-block lag) with finalized-through EOF', async () => {
    // P0-4: finalizedThroughMs=60000 is exactly the pending block's end boundary → EOF
    const result = await runPipeline({
      dataDir: TEST_DIR,
      market: MARKET,
      fromMs: 0,
      toMs: 90000,  // covers both blocks
      runId: RUN_ID,
      finalizedThroughMs: 60000,  // P0-4: EOF at 60000 boundary (exactly after block 30000)
    });

    assert.equal(result.processed, 2, 'should process 2 blocks (1 normal + 1 EOF)');
    assert.equal(result.errors, 0);
    assert.equal(result.manifestUpdates.length, 2);

    // Check manifest
    const manifest = loadManifest(MARKET);
    assert.ok(manifest);
    assert.equal(manifest.last_checkpoint_block_start, 30000); // last committed

    // Check checkpoint
    const cp = loadCheckpoint(MARKET);
    assert.ok(cp);
    // After EOF, pending_block should be null
    assert.equal(cp.pending_block, null);
    assert.equal(cp.last_committed_block_start, 30000);
    assert.ok(cp.generation > 0);
  });

  it('blocks EOF without finalized-through (P0-4)', async () => {
    // Clean up first
    try { rmSync(join('data/derived/burst_features_v1', 'features_1s', MARKET), { recursive: true, force: true }); } catch (_) {}
    try { rmSync(join('data/derived/burst_features_v1', 'manifests', `${MARKET}.json`), { force: true }); } catch (_) {}
    try { rmSync(join('data/derived/burst_features_v1', 'manifests/checkpoints', `${MARKET}.json`), { force: true }); } catch (_) {}

    const result = await runPipeline({
      dataDir: TEST_DIR,
      market: MARKET,
      fromMs: 0,
      toMs: 90000,
      runId: RUN_ID + '-blocked',
      // No finalizedThroughMs → EOF blocked
    });

    assert.equal(result.processed, 1, 'should process only 1 block (no EOF without proof)');
    assert.equal(result.blocked, true);
    assert.equal(result.blockedReason, 'no-horizon-proof');
  });

  it('output shards exist and have 30 rows each', async () => {
    // Run with finalizedThrough to get both shards
    try { rmSync(join('data/derived/burst_features_v1', 'features_1s', MARKET), { recursive: true, force: true }); } catch (_) {}
    try { rmSync(join('data/derived/burst_features_v1', 'manifests', `${MARKET}.json`), { force: true }); } catch (_) {}
    try { rmSync(join('data/derived/burst_features_v1', 'manifests/checkpoints', `${MARKET}.json`), { force: true }); } catch (_) {}

    await runPipeline({
      dataDir: TEST_DIR,
      market: MARKET,
      fromMs: 0,
      toMs: 90000,
      runId: RUN_ID + '-shard',
      finalizedThroughMs: 60000,
    });

    const featuresDir = join('data/derived/burst_features_v1', 'features_1s', MARKET, '1970-01-01');
    assert.ok(existsSync(featuresDir), 'features date dir should exist');

    const { readdirSync } = await import('node:fs');
    const files = readdirSync(featuresDir).filter(f => f.endsWith('.jsonl'));
    assert.equal(files.length, 2, 'should have 2 shard files');

    for (const f of files) {
      const content = readFileSync(join(featuresDir, f), 'utf8');
      const lines = content.trim().split('\n');
      assert.equal(lines.length, 30, `${f} should have 30 rows`);
      const firstRow = JSON.parse(lines[0]);
      assert.equal(firstRow.market, MARKET);
      // #13 = null, #14 = 0 per P1 contract
      assert.equal(firstRow.burst_notional_vs_top_depth, null);
      assert.equal(firstRow.burst_mid_move_bps_1s, 0);
    }
  });
});

// ═══ Bug 2 regression: reconcileMarketState errors block pipeline processing ═══

describe('Pipeline Recovery Errors', () => {
  const ERROR_MARKET = 'test_pipeline_error';
  const ERROR_DIR = 'data/derived/burst_features_v1';
  const ERROR_MANIFEST_DIR = join(ERROR_DIR, 'manifests');

  after(() => {
    try { rmSync(join(ERROR_MANIFEST_DIR, `${ERROR_MARKET}.json`), { force: true }); } catch (_) {}
    try { rmSync(join(ERROR_MANIFEST_DIR, 'checkpoints', `${ERROR_MARKET}.json`), { force: true }); } catch (_) {}
    try { rmSync(join(ERROR_DIR, 'features_1s', ERROR_MARKET), { recursive: true, force: true }); } catch (_) {}
  });

  it('throws E023 on corrupt manifest during recovery reconciliation (Bug 2)', async () => {
    // Ensure clean
    try { rmSync(join(ERROR_MANIFEST_DIR, `${ERROR_MARKET}.json`), { force: true }); } catch (_) {}
    try { rmSync(join(ERROR_DIR, 'features_1s', ERROR_MARKET), { recursive: true, force: true }); } catch (_) {}

    // Create empty manifest → MANIFEST_CORRUPT
    mkdirSync(ERROR_MANIFEST_DIR, { recursive: true });
    writeFileSync(join(ERROR_MANIFEST_DIR, `${ERROR_MARKET}.json`), '');

    await assert.rejects(
      async () => {
        await runPipeline({
          dataDir: TEST_DIR,
          market: ERROR_MARKET,
          fromMs: 0,
          toMs: 60000,
          runId: 'pipeline-error-test',
        });
      },
      (err) => {
        assert.ok(err.message.includes('E023'), `Expected E023 error, got: ${err.message}`);
        return true;
      },
      'pipeline should throw E023 on corrupt manifest',
    );
  });

  it('throws E023 on recovery quarantinedKeys preventing pipeline processing (Bug 2)', async () => {
    // Ensure clean
    try { rmSync(join(ERROR_MANIFEST_DIR, `${ERROR_MARKET}.json`), { force: true }); } catch (_) {}
    try { rmSync(join(ERROR_DIR, 'features_1s', ERROR_MARKET), { recursive: true, force: true }); } catch (_) {}
    try { rmSync(join(ERROR_MANIFEST_DIR, 'checkpoints', `${ERROR_MARKET}.json`), { force: true }); } catch (_) {}

    const blockMs = 30000;
    const inputSha = sha256('input-error');
    const key = `burst_features_v1:${ERROR_MARKET}:${blockMs}:${inputSha}`;

    // Create committed record pointing to nonexistent final shard → will quarantine
    mkdirSync(ERROR_MANIFEST_DIR, { recursive: true });
    writeFileSync(join(ERROR_MANIFEST_DIR, `${ERROR_MARKET}.json`), JSON.stringify({
      schema_version: 'burst_features_v1',
      market: ERROR_MARKET,
      last_checkpoint_block_start: blockMs,
      processed_blocks: {
        [key]: {
          block_start_ms: blockMs,
          input_sha256: inputSha,
          output_row_hash: 'hash-of-nonexistent-file',
          output_path: join(ERROR_DIR, 'features_1s', ERROR_MARKET, '1970-01-01', '00-00-30.jsonl'),
          checkpoint_generation: 1,
          commit_id: 'uuid-error',
          auxiliary_input_hashes: {},
          status: 'committed',
        },
      },
    }, null, 2) + '\n');

    await assert.rejects(
      async () => {
        await runPipeline({
          dataDir: TEST_DIR,
          market: ERROR_MARKET,
          fromMs: 0,
          toMs: 60000,
          runId: 'pipeline-error-test-2',
        });
      },
      (err) => {
        assert.ok(err.message.includes('E023'), `Expected E023 error, got: ${err.message}`);
        return true;
      },
      'pipeline should throw E023 on quarantined recovery records',
    );

    // Verify no checkpoint or output was created
    const cp = loadCheckpoint(ERROR_MARKET);
    assert.equal(cp, null, 'no checkpoint should exist after failed recovery');
  });

  it('throws E024 on corrupt checkpoint (PDD safety fix 1)', async () => {
    // Ensure clean
    try { rmSync(join(ERROR_MANIFEST_DIR, `${ERROR_MARKET}.json`), { force: true }); } catch (_) {}
    try { rmSync(join(ERROR_DIR, 'features_1s', ERROR_MARKET), { recursive: true, force: true }); } catch (_) {}
    try { rmSync(join(ERROR_MANIFEST_DIR, 'checkpoints', `${ERROR_MARKET}.json`), { force: true }); } catch (_) {}
    try { rmSync(join(ERROR_MANIFEST_DIR, 'checkpoints', `${ERROR_MARKET}.json.bak.*`), { force: true }); } catch (_) {}

    // Create clean manifest (valid, empty processed_blocks) so manifest passes
    mkdirSync(ERROR_MANIFEST_DIR, { recursive: true });
    writeFileSync(join(ERROR_MANIFEST_DIR, `${ERROR_MARKET}.json`), JSON.stringify({
      schema_version: 'burst_features_v1',
      market: ERROR_MARKET,
      last_checkpoint_block_start: null,
      processed_blocks: {},
    }) + '\n');

    // Create corrupt checkpoint (invalid JSON)
    mkdirSync(join(ERROR_MANIFEST_DIR, 'checkpoints'), { recursive: true });
    writeFileSync(join(ERROR_MANIFEST_DIR, 'checkpoints', `${ERROR_MARKET}.json`), 'garbage!!!');

    // Clean up .bak from any previous runs
    try {
      const { readdirSync: rd, rmSync: rm } = await import('node:fs');
      for (const f of rd(join(ERROR_MANIFEST_DIR, 'checkpoints'))) {
        if (f.startsWith(`${ERROR_MARKET}.json.bak.`)) rm(join(ERROR_MANIFEST_DIR, 'checkpoints', f), { force: true });
      }
    } catch (_) {}

    await assert.rejects(
      async () => {
        await runPipeline({
          dataDir: TEST_DIR,
          market: ERROR_MARKET,
          fromMs: 0,
          toMs: 60000,
          runId: 'pipeline-e024-test',
        });
      },
      (err) => {
        assert.ok(err.message.includes('E024'), `Expected E024 error, got: ${err.message}`);
        return true;
      },
      'pipeline should throw E024 on corrupt checkpoint',
    );

    // Verify no scan/feed/commit happened: no output dirs created
    const featuresDir = join(ERROR_DIR, 'features_1s', ERROR_MARKET);
    assert.ok(!existsSync(featuresDir), 'no features output should exist after corrupt checkpoint');
  });
});

// ═══ Task 7 review blocker: gap branch must feed candidate trades before computing N features ═══

describe('Gap Burst Continuity (Task 7 review blocker)', () => {
  const GAP_BURST_MARKET = 'test_gap_burst';
  const GAP_DIR = join('test', 'fixtures', 'burst-v1', 'tmp-gap-burst');

  function makeBlockContent(trades) {
    return trades.map(t => JSON.stringify(t)).join('\n') + '\n';
  }

  function cleanDerived() {
    try { rmSync(join('data/derived/burst_features_v1', 'features_1s', GAP_BURST_MARKET), { recursive: true, force: true }); } catch (_) {}
    try { rmSync(join('data/derived/burst_features_v1', 'manifests', `${GAP_BURST_MARKET}.json`), { force: true }); } catch (_) {}
    try { rmSync(join('data/derived/burst_features_v1', 'manifests/checkpoints', `${GAP_BURST_MARKET}.json`), { force: true }); } catch (_) {}
  }

  function setupContinuous() {
    rmSync(GAP_DIR, { recursive: true, force: true });
    mkdirSync(join(GAP_DIR, 'trades', GAP_BURST_MARKET, '1969-12-31'), { recursive: true });
    mkdirSync(join(GAP_DIR, 'trades', GAP_BURST_MARKET, '1970-01-01'), { recursive: true });

    // Lookback for #12 denominator
    writeFileSync(join(GAP_DIR, 'trades', GAP_BURST_MARKET, '1969-12-31', '23-59-30.jsonl'),
      makeBlockContent([{ ts: -29000, side: 'buy', price: 100, qty: 1 }]));

    // Block 0: 2 consecutive buy trades at end → starts open burst (gap=3ms ≤ 50ms threshold)
    writeFileSync(join(GAP_DIR, 'trades', GAP_BURST_MARKET, '1970-01-01', '00-00-00.jsonl'),
      makeBlockContent([
        { ts: 29995, side: 'buy', price: 100, qty: 1 },
        { ts: 29998, side: 'buy', price: 100, qty: 2 },
      ]));

    // Block 30000: empty (no trades)
    writeFileSync(join(GAP_DIR, 'trades', GAP_BURST_MARKET, '1970-01-01', '00-00-30.jsonl'), '\n');

    // Block 60000: empty (no trades)
    writeFileSync(join(GAP_DIR, 'trades', GAP_BURST_MARKET, '1970-01-01', '00-01-00.jsonl'), '\n');

    // Block 90000: same-side buy → gap > 50ms from N's last trade, closes N's burst, starts new
    writeFileSync(join(GAP_DIR, 'trades', GAP_BURST_MARKET, '1970-01-01', '00-01-30.jsonl'),
      makeBlockContent([
        { ts: 90001, side: 'buy', price: 100, qty: 3 },
      ]));

    // Block 120000: sell → closes 90000's buy burst
    writeFileSync(join(GAP_DIR, 'trades', GAP_BURST_MARKET, '1970-01-01', '00-02-00.jsonl'),
      makeBlockContent([
        { ts: 120500, side: 'sell', price: 101, qty: 1 },
      ]));
  }

  function setupGap() {
    rmSync(GAP_DIR, { recursive: true, force: true });
    mkdirSync(join(GAP_DIR, 'trades', GAP_BURST_MARKET, '1969-12-31'), { recursive: true });
    mkdirSync(join(GAP_DIR, 'trades', GAP_BURST_MARKET, '1970-01-01'), { recursive: true });

    // Same lookback
    writeFileSync(join(GAP_DIR, 'trades', GAP_BURST_MARKET, '1969-12-31', '23-59-30.jsonl'),
      makeBlockContent([{ ts: -29000, side: 'buy', price: 100, qty: 1 }]));

    // Block 0: same burst-start trades
    writeFileSync(join(GAP_DIR, 'trades', GAP_BURST_MARKET, '1970-01-01', '00-00-00.jsonl'),
      makeBlockContent([
        { ts: 29995, side: 'buy', price: 100, qty: 1 },
        { ts: 29998, side: 'buy', price: 100, qty: 2 },
      ]));

    // NO blocks 30000, 60000 — intentional gap

    // Block 90000: same as continuous
    writeFileSync(join(GAP_DIR, 'trades', GAP_BURST_MARKET, '1970-01-01', '00-01-30.jsonl'),
      makeBlockContent([
        { ts: 90001, side: 'buy', price: 100, qty: 3 },
      ]));

    // Block 120000: same as continuous
    writeFileSync(join(GAP_DIR, 'trades', GAP_BURST_MARKET, '1970-01-01', '00-02-00.jsonl'),
      makeBlockContent([
        { ts: 120500, side: 'sell', price: 101, qty: 1 },
      ]));
  }

  before(() => { cleanDerived(); });
  after(() => {
    rmSync(GAP_DIR, { recursive: true, force: true });
    cleanDerived();
  });

  it('gap-run block N+k burst features match continuous-run (RED: Task 7 review blocker)', async () => {
    // ── Continuous run: all blocks [0, 30k, 60k, 90k, 120k] ──
    setupContinuous();
    cleanDerived();

    const cResult = await runPipeline({
      dataDir: GAP_DIR,
      market: GAP_BURST_MARKET,
      fromMs: 0,
      toMs: 150000,
      runId: 'gap-burst-continuous',
      finalizedThroughMs: 150000,
    });
    assert.equal(cResult.processed, 5, 'continuous should process 5 blocks');

    const featuresDir = join('data/derived/burst_features_v1', 'features_1s', GAP_BURST_MARKET, '1970-01-01');
    const cBlock90Content = readFileSync(join(featuresDir, '00-01-30.jsonl'), 'utf8');
    const cRows = cBlock90Content.trim().split('\n').map(l => JSON.parse(l));

    // Block 90000 row [0] = ts=90000 second bucket: buy at 90001 forms 1 burst
    const cBurstCount = cRows[0].burst_count_1s;
    assert.equal(cBurstCount, 1, `continuous N+k second 30 burst_count_1s should be 1, got ${cBurstCount}`);
    const cBurstNotional = cRows[0].total_burst_notional_1s;
    assert.equal(cBurstNotional, 300, `continuous N+k second 30 total_burst_notional should be 300, got ${cBurstNotional}`);

    // ── Gap run: blocks [0, 90k, 120k] — 30k and 60k absent ──
    setupGap();
    cleanDerived();

    const gResult = await runPipeline({
      dataDir: GAP_DIR,
      market: GAP_BURST_MARKET,
      fromMs: 0,
      toMs: 150000,
      runId: 'gap-burst-gap',
      finalizedThroughMs: 150000,
    });
    assert.equal(gResult.processed, 3, 'gap should process 3 blocks (0, 90k, 120k)');

    const gBlock90Content = readFileSync(join(featuresDir, '00-01-30.jsonl'), 'utf8');
    const gRows = gBlock90Content.trim().split('\n').map(l => JSON.parse(l));
    const gBurstCount = gRows[0].burst_count_1s;
    const gBurstNotional = gRows[0].total_burst_notional_1s;

    // Assert: gap matches continuous
    assert.equal(
      gBurstCount, cBurstCount,
      `gap-run N+k burst_count_1s (${gBurstCount}) must match continuous (${cBurstCount})`
    );
    assert.equal(
      gBurstNotional, cBurstNotional,
      `gap-run N+k total_burst_notional (${gBurstNotional}) must match continuous (${cBurstNotional})`
    );
  });
});

// ═══ §4.2: Pipeline reorder audit integration ═══

describe('Pipeline Reorder Audit (§4.2)', () => {
  const REORDER_MARKET = 'test_reorder_audit';
  const REORDER_DIR = join('test', 'fixtures', 'burst-v1', 'tmp-reorder-audit');

  function makeBlockContent(trades) {
    return trades.map(t => JSON.stringify(t)).join('\n') + '\n';
  }

  function cleanDerived() {
    try { rmSync(join('data/derived/burst_features_v1', 'features_1s', REORDER_MARKET), { recursive: true, force: true }); } catch (_) {}
    try { rmSync(join('data/derived/burst_features_v1', 'manifests', `${REORDER_MARKET}.json`), { force: true }); } catch (_) {}
    try { rmSync(join('data/derived/burst_features_v1', 'manifests/checkpoints', `${REORDER_MARKET}.json`), { force: true }); } catch (_) {}
  }

  after(() => {
    rmSync(REORDER_DIR, { recursive: true, force: true });
    cleanDerived();
  });

  it('reordered input block → manifest audit fields + ASSUMED_REORDERED_INPUT log', async () => {
    // Setup: 2 trade blocks, first has 2ms timestamp inversion
    rmSync(REORDER_DIR, { recursive: true, force: true });
    mkdirSync(join(REORDER_DIR, 'trades', REORDER_MARKET, '1970-01-01'), { recursive: true });

    // Block 0 (ms=0): 2 trades, ts reversed by 2ms
    writeFileSync(join(REORDER_DIR, 'trades', REORDER_MARKET, '1970-01-01', '00-00-00.jsonl'),
      makeBlockContent([
        { ts: 1002, side: 'buy', price: 100, qty: 1 },
        { ts: 1000, side: 'buy', price: 101, qty: 2 },
      ]));

    // Block 1 (ms=30000): 1 normal trade (needed for 1-block lag pipeline)
    writeFileSync(join(REORDER_DIR, 'trades', REORDER_MARKET, '1970-01-01', '00-00-30.jsonl'),
      makeBlockContent([
        { ts: 30500, side: 'sell', price: 102, qty: 1 },
      ]));

    // Raw trade lookback for #12 denominator
    mkdirSync(join(REORDER_DIR, 'trades', REORDER_MARKET, '1969-12-31'), { recursive: true });
    writeFileSync(join(REORDER_DIR, 'trades', REORDER_MARKET, '1969-12-31', '23-59-30.jsonl'),
      makeBlockContent([{ ts: -29000, side: 'buy', price: 100, qty: 1 }]));

    cleanDerived();

    // Capture stderr for structured logs
    const stderrChunks = [];
    const origWrite = process.stderr.write.bind(process.stderr);
    process.stderr.write = (chunk, ...args) => {
      stderrChunks.push(chunk.toString());
      return origWrite(chunk, ...args);
    };

    try {
      const result = await runPipeline({
        dataDir: REORDER_DIR,
        market: REORDER_MARKET,
        fromMs: 0,
        toMs: 60000,
        runId: 'reorder-audit-test',
        finalizedThroughMs: 60000,
      });

      assert.equal(result.processed, 2, 'should process 2 blocks (0 + EOF 30000)');
      assert.equal(result.errors, 0);
    } finally {
      process.stderr.write = origWrite;
    }

    // ── Check manifest ──
    const { loadManifest } = await import('../../lib/burst-reducer/manifest-manager.mjs');
    const m = loadManifest(REORDER_MARKET);
    assert.ok(m, 'manifest should exist');
    assert.ok(m.processed_blocks, 'manifest should have processed_blocks');

    // Find block 0's record (the reordered one)
    const block0Key = Object.keys(m.processed_blocks).find(k =>
      m.processed_blocks[k].block_start_ms === 0);
    assert.ok(block0Key, 'block 0 should exist in manifest');
    const block0Record = m.processed_blocks[block0Key];
    assert.equal(block0Record.reordered_input, true,
      `block 0 reordered_input should be true, got ${block0Record.reordered_input}`);
    assert.equal(block0Record.timestamp_inversion_count, 1,
      `block 0 inversion count should be 1, got ${block0Record.timestamp_inversion_count}`);

    // Block 1 (30000) should NOT be reordered
    const block30Key = Object.keys(m.processed_blocks).find(k =>
      m.processed_blocks[k].block_start_ms === 30000);
    assert.ok(block30Key, 'block 30000 should exist in manifest');
    const block30Record = m.processed_blocks[block30Key];
    assert.equal(block30Record.reordered_input, false,
      `block 30000 reordered_input should be false`);
    assert.equal(block30Record.timestamp_inversion_count, 0,
      `block 30000 inversion count should be 0`);

    // ── Check structured log ──
    const logLines = stderrChunks.join('').trim().split('\n')
      .map(l => { try { return JSON.parse(l); } catch { return null; } })
      .filter(l => l && l.level === 'ASSUMED_REORDERED_INPUT');
    assert.ok(logLines.length >= 1, 'should have at least 1 ASSUMED_REORDERED_INPUT log entry');
    const reorderLog = logLines[0];
    assert.equal(reorderLog.market, REORDER_MARKET);
    assert.equal(reorderLog.block_start_ms, 0);
    assert.equal(reorderLog.timestamp_inversion_count, 1);
    assert.ok(reorderLog.input_sha256, 'log should include input_sha256');
  });
});
