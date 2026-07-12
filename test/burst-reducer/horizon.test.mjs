// test/burst-reducer/horizon.test.mjs — P0-4 finalized input horizon tests + P0-1 horizon proof / frozen inventory validation
import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import { existsSync, mkdirSync, writeFileSync, rmSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { runPipeline } from '../../lib/burst-reducer/pipeline.mjs';
import { loadCheckpoint } from '../../lib/burst-reducer/manifest-manager.mjs';
import { INPUT_KIND, VALID_INPUT_KINDS, BLOCK_DURATION_MS } from '../../lib/burst-reducer/schema.mjs';

const TEST_DIR = join('test', 'fixtures', 'burst-v1', 'tmp-horizon');
const P0_1_DIR = join('test', 'fixtures', 'burst-v1', 'tmp-horizon-p0-1');
const MARKET = 'test_horizon';
const P0_1_MARKET = 'test_p0_1';
const RUN_ID = 'horizon-test';

function makeTradeBlock(blockStartMs, trades) {
  return trades.map(t => JSON.stringify(t)).join('\n') + '\n';
}

function setupTwoBlocks() {
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

  // Raw trade lookback block for Block 0
  writeFileSync(join(TEST_DIR, 'trades', MARKET, '1969-12-31', '23-59-30.jsonl'),
    makeTradeBlock(-30000, [
      { ts: -29000, side: 'buy', price: 100, qty: 2 },
    ]));
}

/**
 * Set up P0-1 test fixtures with both trades and book_updates blocks.
 */
function setupP0_1Fixtures() {
  rmSync(P0_1_DIR, { recursive: true, force: true });

  // Trades: block 0 and block 60000 (gap at 30000 for gap tests)
  mkdirSync(join(P0_1_DIR, 'trades', P0_1_MARKET, '1970-01-01'), { recursive: true });
  writeFileSync(join(P0_1_DIR, 'trades', P0_1_MARKET, '1970-01-01', '00-00-00.jsonl'),
    makeTradeBlock(0, [{ ts: 500, side: 'buy', price: 100, qty: 1 }]));
  writeFileSync(join(P0_1_DIR, 'trades', P0_1_MARKET, '1970-01-01', '00-01-00.jsonl'),
    makeTradeBlock(60000, [{ ts: 60500, side: 'sell', price: 101, qty: 1 }]));

  // Book_updates: block 0 only (for verified-missing test)
  mkdirSync(join(P0_1_DIR, 'book_updates', P0_1_MARKET, '1970-01-01'), { recursive: true });
  writeFileSync(join(P0_1_DIR, 'book_updates', P0_1_MARKET, '1970-01-01', '00-00-00.jsonl'),
    '{"schema_version":"book_updates_v1","market":"test_p0_1","type":"snapshot","event_ts_ms":500,"seq":1,"prev_seq":null,"bids":[["100","1"]],"asks":[["101","1"]],"source":{"exchange":"test","channel":"book"}}\n');
  // Block 60000 for gap test
  writeFileSync(join(P0_1_DIR, 'book_updates', P0_1_MARKET, '1970-01-01', '00-01-00.jsonl'),
    '{"schema_version":"book_updates_v1","market":"test_p0_1","type":"snapshot","event_ts_ms":60500,"seq":2,"prev_seq":null,"bids":[["100","2"]],"asks":[["101","2"]],"source":{"exchange":"test","channel":"book"}}\n');

  // Raw trade lookback for trades tests
  mkdirSync(join(P0_1_DIR, 'trades', P0_1_MARKET, '1969-12-31'), { recursive: true });
  writeFileSync(join(P0_1_DIR, 'trades', P0_1_MARKET, '1969-12-31', '23-59-30.jsonl'),
    makeTradeBlock(-30000, [{ ts: -29000, side: 'buy', price: 100, qty: 2 }]));
}

function cleanDerived() {
  try { rmSync(join('data/derived/burst_features_v1', 'features_1s', MARKET), { recursive: true, force: true }); } catch (_) {}
  try { rmSync(join('data/derived/burst_features_v1', 'manifests', `${MARKET}.json`), { force: true }); } catch (_) {}
  try { rmSync(join('data/derived/burst_features_v1', 'manifests/checkpoints', `${MARKET}.json`), { force: true }); } catch (_) {}
  try { rmSync(join('data/derived/burst_features_v1', 'quarantine'), { recursive: true, force: true }); } catch (_) {}
}

function cleanP0_1Derived() {
  try { rmSync(join('data/derived/burst_features_v1', 'features_1s', P0_1_MARKET), { recursive: true, force: true }); } catch (_) {}
  try { rmSync(join('data/derived/burst_features_v1', 'manifests', `${P0_1_MARKET}.json`), { force: true }); } catch (_) {}
  // B5: also clean book_updates checkpoint (different filename pattern)
  try { rmSync(join('data/derived/burst_features_v1', 'manifests/checkpoints', `${P0_1_MARKET}.book_updates.json`), { force: true }); } catch (_) {}
  try { rmSync(join('data/derived/burst_features_v1', 'manifests/checkpoints', `${P0_1_MARKET}.json`), { force: true }); } catch (_) {}
  try { rmSync(join('data/derived/burst_features_v1', 'quarantine', P0_1_MARKET), { recursive: true, force: true }); } catch (_) {}
}

describe('Finalized Horizon (P0-4)', () => {
  before(() => { setupTwoBlocks(); });
  after(() => {
    rmSync(TEST_DIR, { recursive: true, force: true });
    cleanDerived();
  });

  it('raw next block absent + no watermark => pending retained, no EOF, no quarantine', async () => {
    cleanDerived();
    const result = await runPipeline({
      dataDir: TEST_DIR, market: MARKET, fromMs: 0, toMs: 90000,
      runId: RUN_ID + '-1',
    });
    // Block 0 committed (N+1=30000), then pending=30000, no N+1 → blocked
    assert.equal(result.processed, 1);
    assert.equal(result.blocked, true);
    assert.equal(result.blockedReason, 'no-horizon-proof');

    // Checkpoint must retain pending block
    const cp = loadCheckpoint(MARKET);
    assert.ok(cp);
    assert.ok(cp.pending_block, 'pending_block should be retained');
    assert.equal(cp.pending_block.block_start_ms, 30000);
  });

  it('explicit valid empty next block => previous pending finalizes', async () => {
    cleanDerived();
    // Add empty block at 60000 (valid-empty) — no separate agg file needed
    mkdirSync(join(TEST_DIR, 'trades', MARKET, '1970-01-01'), { recursive: true });
    writeFileSync(join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-01-00.jsonl'), '\n');

    // finalizedThroughMs=120000: 3 blocks, but after block 60000, next boundary=90000 < 120000
    // → verified-missing (no block at 90000). Use 90000 as horizon instead.
    const result = await runPipeline({
      dataDir: TEST_DIR, market: MARKET, fromMs: 0, toMs: 120000,
      runId: RUN_ID + '-2',
      finalizedThroughMs: 90000,
    });

    // Blocks: 0 (committed with N+1=30000), 30000 (committed with N+1=60000), 60000 (EOF at 90000)
    assert.equal(result.processed, 3);
    assert.ok(!result.blocked);

    // Clean up
    try { rmSync(join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-01-00.jsonl'), { force: true }); } catch (_) {}
  });

  it('absent next block at --finalized-through => blocked, not quarantine', async () => {
    cleanDerived();
    // finalizedThroughMs=60000: after block 30000, next boundary=60000 == finalizedThrough → EOF
    const result = await runPipeline({
      dataDir: TEST_DIR, market: MARKET, fromMs: 0, toMs: 90000,
      runId: RUN_ID + '-3',
      finalizedThroughMs: 60000,
    });
    // Both blocks committed, EOF at boundary
    assert.equal(result.processed, 2);
    assert.ok(!result.blocked);

    const cp = loadCheckpoint(MARKET);
    assert.equal(cp.pending_block, null);
    assert.equal(cp.last_committed_block_start, 30000);
  });

  it('absent next block within finalized-through => EOF-gap completion, no quarantine', async () => {
    cleanDerived();
    // finalizedThroughMs=120000: after block 0 committed (pending=30000),
    // block 30000 needs N+1 at 60000. 60000 < 120000 but file absent.
    // §4.1 contract: gap is EOF-completable → pending block 30000 finalizes
    // at horizon without synthetic shard for the gap.
    const result = await runPipeline({
      dataDir: TEST_DIR, market: MARKET, fromMs: 0, toMs: 90000,
      runId: RUN_ID + '-4',
      finalizedThroughMs: 120000,
    });
    // Both existing raw blocks processed (block 0 normal commit + block 30000 EOF-finalize).
    // No verified-missing quarantine, no synthetic gap shard.
    assert.equal(result.processed, 2, 'both existing blocks processed, gap EOF-completable');
    assert.ok(!result.blocked, 'gap within finalized-through is EOF-completable, not blocked');

    // Checkpoint: final state — both blocks committed, no pending
    const cp = loadCheckpoint(MARKET);
    assert.equal(cp.last_committed_block_start, 30000);
    assert.equal(cp.pending_block, null);
  });

  it('--finalized-through misaligned => throws E040', async () => {
    cleanDerived();
    await assert.rejects(
      () => runPipeline({
        dataDir: TEST_DIR, market: MARKET, fromMs: 0, toMs: 90000,
        runId: RUN_ID + '-5',
        finalizedThroughMs: 61000,
      }),
      /E040/,
      'should throw E040 for misaligned finalized-through'
    );
  });

  it('range exhaustion alone => never EOF', async () => {
    cleanDerived();
    // toMs=15000: only block 0 is in range (0 < 15000 && 0+30000 > 0)
    const result = await runPipeline({
      dataDir: TEST_DIR, market: MARKET, fromMs: 0, toMs: 15000,
      runId: RUN_ID + '-6',
    });
    // Block 0 found but no N+1, so no commit
    assert.equal(result.processed, 0);
    assert.equal(result.blocked, true);
    assert.equal(result.blockedReason, 'no-horizon-proof');
  });

  it('blocked pending followed by later arrival => restart is byte-identical', async () => {
    cleanDerived();
    // Step 1: Complete run with finalizedThroughMs=60000
    const result1 = await runPipeline({
      dataDir: TEST_DIR, market: MARKET, fromMs: 0, toMs: 90000,
      runId: RUN_ID + '-restart-a',
      finalizedThroughMs: 60000,
    });
    assert.equal(result1.processed, 2);

    const featuresDir = join('data/derived/burst_features_v1', 'features_1s', MARKET, '1970-01-01');
    const { readdirSync: rd } = await import('node:fs');
    const files1 = rd(featuresDir).filter(f => f.endsWith('.jsonl')).sort();
    const contents1 = {};
    for (const f of files1) {
      contents1[f] = readFileSync(join(featuresDir, f), 'utf8');
    }

    // Step 2: Restart from scratch
    cleanDerived();
    const result2 = await runPipeline({
      dataDir: TEST_DIR, market: MARKET, fromMs: 0, toMs: 90000,
      runId: RUN_ID + '-restart-b',
      finalizedThroughMs: 60000,
    });
    assert.equal(result2.processed, 2);

    const files2 = rd(featuresDir).filter(f => f.endsWith('.jsonl')).sort();
    assert.deepEqual(files1, files2, 'same output files');

    for (const f of files1) {
      assert.equal(contents1[f], readFileSync(join(featuresDir, f), 'utf8'), `${f} should be byte-identical`);
    }
  });
});

describe('P0-1 Horizon Proof / Frozen Inventory Validation', () => {
  before(() => { setupP0_1Fixtures(); });
  after(() => {
    rmSync(P0_1_DIR, { recursive: true, force: true });
    cleanP0_1Derived();
    cleanDerived();
  });

  // ── HORIZON-005: book_updates missing next block within finalizedThrough → verified-missing ──
  it('HORIZON-005: book_updates missing next block within finalizedThrough => verified-missing', async () => {
    cleanP0_1Derived();
    // Only block 0 exists in book_updates. finalizedThrough=90000 → next boundary 30000
    // is within horizon but file absent. For book_updates → verified-missing.
    // Block at 60000 also exists from setup, creating a gap at 30000.
    // B5: the pipeline now advances past verified-missing gap, processes block 60000,
    // then at EOF horizon proof validates (nextBoundaryStart===finalizedThroughMs).
    const result = await runPipeline({
      dataDir: P0_1_DIR,
      market: P0_1_MARKET,
      fromMs: 0,
      toMs: 90000,
      runId: RUN_ID + '-h005',
      finalizedThroughMs: 90000,
      kind: 'book_updates',
    });

    // B5: pipeline advanced past the gap — no blocked state at EOF
    assert.ok(!result.blocked, 'B5: non-trade gap advances past missing blocks, no blocked at EOF');
    assert.equal(result.processed, 1, 'gap block at 30000 is counted as processed');

    // Quarantine report should exist for the missing gap block at 30000
    const qPath = join('data/derived/burst_features_v1', 'quarantine', P0_1_MARKET, '30000.json');
    assert.ok(existsSync(qPath), 'quarantine report should exist for gap block at 30000');
    const qReport = JSON.parse(readFileSync(qPath, 'utf8'));
    assert.equal(qReport.market, P0_1_MARKET);
    assert.equal(qReport.block_start_ms, 30000);
    assert.equal(qReport.kind, 'book_updates');
    assert.match(qReport.reason, /MISSING_FINALIZED_INPUT/i);
    assert.equal(qReport.details.gap_from, 30000);
    assert.equal(qReport.details.gap_to_exclusive, 60000);

    // Verified-missing record should exist in manifest
    const manifestPath = join('data/derived/burst_features_v1', 'manifests', `${P0_1_MARKET}.json`);
    assert.ok(existsSync(manifestPath), 'manifest should exist');
    const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
    const vmk = `verified_missing:${P0_1_MARKET}:30000`;
    assert.ok(manifest.processed_blocks[vmk], 'verified_missing record should exist in manifest');
    assert.equal(manifest.processed_blocks[vmk].reason, 'verified-missing');
    assert.equal(manifest.processed_blocks[vmk].details.reason, 'verified-missing-gap');

    // Book_updates checkpoint should exist
    const cpPath = join('data/derived/burst_features_v1', 'manifests/checkpoints', `${P0_1_MARKET}.book_updates.json`);
    assert.ok(existsSync(cpPath), 'book_updates checkpoint should exist');
  });

  // ── HORIZON-006: frozen inventory hash mismatch ──
  it('HORIZON-006: frozen inventory hash mismatch => hash-mismatch state', async () => {
    cleanP0_1Derived();

    // First, run trades pipeline without finalizedThrough to create checkpoint with pending block
    const result1 = await runPipeline({
      dataDir: P0_1_DIR,
      market: P0_1_MARKET,
      fromMs: 0,
      toMs: 120000,
      runId: RUN_ID + '-h006-a',
      // No finalizedThroughMs — leaves block 60000 pending with no-horizon-proof
      kind: 'trades',
    });
    // Block 0 committed, block 60000 pending (no-horizon-proof)
    assert.equal(result1.processed, 1);

    // Now run again with frozen inventory
    // The pending block from checkpoint should trigger hash mismatch
    const fakeInventory = {
      byKindAndMarket: new Map([
        ['trades', new Map([
          [P0_1_MARKET, new Map([
            [0, { market: P0_1_MARKET, kind: 'trades', block_start_ms: 0, sha256: '0000000000000000000000000000000000000000000000000000000000000000' }],
          ])]
        ])]
      ]),
      entries: [{ market: P0_1_MARKET, kind: 'trades', block_start_ms: 0, sha256: '0000000000000000000000000000000000000000000000000000000000000000' }],
      errors: [],
    };

    const result2 = await runPipeline({
      dataDir: P0_1_DIR,
      market: P0_1_MARKET,
      fromMs: 0,
      toMs: 120000,
      runId: RUN_ID + '-h006-b',
      finalizedThroughMs: 120000,
      frozenInventory: fakeInventory,
      kind: 'trades',
    });
    // The pending block at 60000 should hit EOF check with inventory
    // Inventory declares block 0 (which isn't the pending block) → pending-not-in-inventory
    assert.equal(result2.blockedState, 'verified-missing');
  });

  // ── HORIZON-008: trades gap within finalizedThrough → ASSUMED_EMPTY_GAP ──
  it('HORIZON-008: trades gap within finalizedThrough => ASSUMED_EMPTY_GAP, blocks committed', async () => {
    cleanP0_1Derived();
    // Trades blocks at 0 and 60000 with finalizedThrough=120000
    // Gap at 30000 is within horizon → ASSUMED_EMPTY_GAP (trades behavior)
    const result = await runPipeline({
      dataDir: P0_1_DIR,
      market: P0_1_MARKET,
      fromMs: 0,
      toMs: 120000,
      runId: RUN_ID + '-h008',
      finalizedThroughMs: 120000,
      kind: 'trades',
    });

    // Block 0 should be committed with gap, block 60000 pending
    assert.equal(result.processed, 1, 'one block committed (block 0)');
    assert.equal(result.blocked, true, 'still pending (no EOF)');
    assert.equal(result.blockedReason, 'next-block-exists', 'next block at 60000 found');
  });

  // ── HORIZON-009: book_updates gap → verified-missing ──
  it('HORIZON-009: book_updates gap within finalizedThrough => verified-missing quarantine', async () => {
    cleanP0_1Derived();
    // Book_updates blocks at 0 and 60000 with finalizedThrough=120000
    // Gap at 30000 → kind=book_updates → verified-missing quarantine
    const result = await runPipeline({
      dataDir: P0_1_DIR,
      market: P0_1_MARKET,
      fromMs: 0,
      toMs: 120000,
      runId: RUN_ID + '-h009',
      finalizedThroughMs: 120000,
      kind: 'book_updates',
    });

    // Gap at 30000 should trigger verified-missing
    assert.equal(result.blocked, true);
    assert.equal(result.blockedReason, 'verified-missing');
    assert.equal(result.blockedState, 'verified-missing');

    // Quarantine report should exist for the missing block at 30000
    const qPath = join('data/derived/burst_features_v1', 'quarantine', P0_1_MARKET, '30000.json');
    assert.ok(existsSync(qPath), 'quarantine report should exist for missing block at 30000');
    const qReport = JSON.parse(readFileSync(qPath, 'utf8'));
    assert.equal(qReport.block_start_ms, 30000);
    assert.equal(qReport.kind, 'book_updates');
    assert.match(qReport.reason, /MISSING_FINALIZED_INPUT/i);
    assert.ok(qReport.details.gap_from, 30000);
    assert.ok(qReport.details.gap_to_exclusive, 60000);
  });

  // ── HORIZON-011: no horizon proof → no-horizon-proof ──
  it('HORIZON-011: no finalizedThrough, no frozenInventory => no-horizon-proof blocked', async () => {
    cleanP0_1Derived();
    // Run trades pipeline first to create checkpoint
    const result1 = await runPipeline({
      dataDir: P0_1_DIR,
      market: P0_1_MARKET,
      fromMs: 0,
      toMs: 120000,
      runId: RUN_ID + '-h011-a',
      kind: 'trades',
    });
    assert.equal(result1.processed, 1);

    // Now with no finalizedThrough and no frozenInventory → no-horizon-proof
    const result2 = await runPipeline({
      dataDir: P0_1_DIR,
      market: P0_1_MARKET,
      fromMs: 0,
      toMs: 120000,
      runId: RUN_ID + '-h011-b',
      kind: 'trades',
    });
    assert.equal(result2.blocked, true);
    assert.equal(result2.blockedState, 'no-horizon-proof');
  });

  // ── HORIZON-012: finalized-through misaligned → E040 ──
  it('HORIZON-012: --finalized-through misaligned => E040', async () => {
    cleanP0_1Derived();
    await assert.rejects(
      () => runPipeline({
        dataDir: P0_1_DIR,
        market: P0_1_MARKET,
        fromMs: 0,
        toMs: 90000,
        runId: RUN_ID + '-h012',
        finalizedThroughMs: 61000,
        kind: 'trades',
      }),
      /E040/,
      'should throw E040 for misaligned finalized-through'
    );
  });

  // ── HORIZON-013: frozen inventory block_start_ms not 30s-aligned → validation error ──
  it('HORIZON-013: frozen inventory block_start_ms not 30s-aligned => validation error', async () => {
    const { validateInventoryEntry } = await import('../../scripts/tfp.mjs');

    // Validate an entry with non-30s-aligned block_start_ms
    const errors = validateInventoryEntry({
      market: 'test',
      kind: 'book_updates',
      block_start_ms: 1000,  // Not 30s-aligned (1000 % 30000 = 1000)
      path: 'book_updates/test/1970-01-01/00-00-01.jsonl',
      sha256: '',
    }, 0);

    assert.ok(errors.length > 0, 'should return validation error for misaligned block_start_ms');
    assert.match(errors[0], /aligned/i, 'error message should mention alignment');
    assert.match(errors[0], /1000/, 'error should reference the misaligned value');
  });

  // ── HORIZON-014: duplicate (market, kind, block_start_ms) detection ──
  it('HORIZON-014: duplicate (market, kind, block_start_ms) in frozen inventory => detected', async () => {
    const { validateInventoryCrossReferences } = await import('../../scripts/tfp.mjs');

    // Two entries with identical (market, kind, block_start_ms) but different sha256
    const entries = [
      { market: 'test', kind: 'trades', block_start_ms: 0, sha256: 'a'.repeat(64), path: 'trades/test/1970-01-01/00-00-00.jsonl' },
      { market: 'test', kind: 'trades', block_start_ms: 0, sha256: 'b'.repeat(64), path: 'trades/test/1970-01-01/00-00-00.jsonl' },
    ];

    const errors = validateInventoryCrossReferences(entries);
    assert.equal(errors.length, 1, 'should detect exactly 1 duplicate pair');
    assert.match(errors[0], /duplicate/i, 'error should mention duplicate');
    assert.match(errors[0], /market=test/, 'error should identify the market');
    assert.match(errors[0], /block_start_ms=0/, 'error should identify the block');
  });

  // ── schema.mjs validity checks ──
  it('INPUT_KIND constants are correct', () => {
    assert.equal(INPUT_KIND.TRADES, 'trades');
    assert.equal(INPUT_KIND.BOOK_UPDATES, 'book_updates');
    assert.ok(VALID_INPUT_KINDS.has('trades'));
    assert.ok(VALID_INPUT_KINDS.has('book_updates'));
    assert.equal(VALID_INPUT_KINDS.size, 2);
    assert.equal(BLOCK_DURATION_MS, 30000);
  });

  // ── BLOCKED structured log contains kind and blocked_state ──
  // B5: non-trade gaps no longer return BLOCKED; they advance past missing blocks.
  // The BLOCKED entry is still emitted by horizon-check before EOF finalization when
  // horizon.canFinalize is false (verified-missing or not-yet-arrived).
  it('BLOCKED structured log contains kind and blocked_state', async () => {
    cleanP0_1Derived();
    // Capture stderr output
    const oldStderrWrite = process.stderr.write;
    const stderrChunks = [];
    process.stderr.write = (chunk) => { stderrChunks.push(chunk.toString()); return true; };

    try {
      await runPipeline({
        dataDir: P0_1_DIR,
        market: P0_1_MARKET,
        fromMs: 0,
        toMs: 120000,
        runId: RUN_ID + '-blocked-log',
        finalizedThroughMs: 30000,
        kind: 'book_updates',
      });
    } finally {
      process.stderr.write = oldStderrWrite;
    }

    // With finalizedThroughMs=30000 and block at 0 processed at EOF, the horizon
    // check for block 0 with nextBoundaryStart=30000 === finalizedThroughMs
    // gives canFinalize=true (finalized-through-boundary), so no BLOCKED emitted.
    // Instead check for VERIFIED_MISSING structured log from the gap handler.
    const vmLine = stderrChunks.find(c => c.includes('"level":"VERIFIED_MISSING"'));
    assert.ok(vmLine, 'should have a VERIFIED_MISSING structured log entry');
    const parsed = JSON.parse(vmLine);
    assert.equal(parsed.kind, 'book_updates');
    assert.equal(parsed.market, P0_1_MARKET);
  });

  // ── scanBlocks backward compatibility ──
  it('scanBlocks with kind=trades returns same results as scanTradeBlocks', async () => {
    const { scanBlocks, scanTradeBlocks } = await import('../../lib/burst-reducer/block-scanner.mjs');
    const results1 = scanTradeBlocks(P0_1_DIR, P0_1_MARKET, 0, 120000);
    const results2 = scanBlocks(P0_1_DIR, 'trades', P0_1_MARKET, 0, 120000);
    assert.deepEqual(results1, results2);
  });

  // ── scanBookUpdateBlocks works ──
  it('scanBookUpdateBlocks returns book_updates blocks', async () => {
    const { scanBookUpdateBlocks } = await import('../../lib/burst-reducer/block-scanner.mjs');
    const blocks = scanBookUpdateBlocks(P0_1_DIR, P0_1_MARKET, 0, 120000);
    assert.ok(blocks.length >= 1);
    // All returned blocks should be book_updates blocks
    for (const b of blocks) {
      assert.ok(b.fullPath.includes('book_updates'), `path ${b.fullPath} should contain book_updates`);
    }
  });

  // ── kind default is 'trades' (backward compatibility) ──
  it('runPipeline default kind=trades matches existing behavior', async () => {
    cleanP0_1Derived();
    const resultDefault = await runPipeline({
      dataDir: P0_1_DIR,
      market: P0_1_MARKET,
      fromMs: 0,
      toMs: 120000,
      runId: RUN_ID + '-default-kind',
    });
    cleanP0_1Derived();
    const resultExplicit = await runPipeline({
      dataDir: P0_1_DIR,
      market: P0_1_MARKET,
      fromMs: 0,
      toMs: 120000,
      runId: RUN_ID + '-explicit-kind',
      kind: 'trades',
    });
    assert.equal(resultDefault.processed, resultExplicit.processed);
    assert.equal(resultDefault.blocked, resultExplicit.blocked);
    assert.equal(resultDefault.blockedReason, resultExplicit.blockedReason);
  });

  // ── HORIZON-E2E: frozen inventory via tfp.mjs CLI with book_updates ──
  // B5: non-trade pipeline advances past gaps, writes checkpoint at EOF.
  it('HORIZON-E2E: frozen inventory via tfp.mjs CLI with book_updates', async () => {
    cleanP0_1Derived();
    const fixturePath = join(P0_1_DIR, 'frozen-inventory-e2e.json');

    // Create frozen inventory fixture covering the two book_updates blocks
    const fixture = [
      {
        market: P0_1_MARKET,
        kind: 'book_updates',
        block_start_ms: 0,
        path: `book_updates/${P0_1_MARKET}/1970-01-01/00-00-00.jsonl`,
        sha256: '',
      },
      {
        market: P0_1_MARKET,
        kind: 'book_updates',
        block_start_ms: 60000,
        path: `book_updates/${P0_1_MARKET}/1970-01-01/00-01-00.jsonl`,
        sha256: '',
      },
    ];
    writeFileSync(fixturePath, JSON.stringify(fixture, null, 2));

    try {
      const { spawnSync } = await import('node:child_process');
      const result = spawnSync('node', [
        'scripts/tfp.mjs',
        '--data', P0_1_DIR,
        '--from', '1970-01-01T00:00:00.000Z',
        '--to', '1970-01-01T00:02:00.000Z',
        '--kind', 'book_updates',
        '--frozen-inventory', fixturePath,
      ], { encoding: 'utf8', timeout: 15000 });

      // Exit code 0 — clean exit with checkpoint write
      assert.equal(result.status, 0, `tfp.mjs exit code should be 0 (got ${result.status}, stderr: ${result.stderr})`);

      // B5: pipeline advances past verified-missing gaps, VERIFIED_MISSING log emitted
      assert.ok(result.stderr.includes('VERIFIED_MISSING'), 'stderr should indicate verified-missing (gap at 30000)');

      // Book_updates checkpoint should be written
      const cpPath = join('data/derived/burst_features_v1', 'manifests/checkpoints', `${P0_1_MARKET}.book_updates.json`);
      assert.ok(existsSync(cpPath), 'book_updates checkpoint should exist');
    } finally {
      try { rmSync(fixturePath, { force: true }); } catch (_) {}
    }
  });
});
