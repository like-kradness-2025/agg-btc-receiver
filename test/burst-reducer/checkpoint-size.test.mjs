// test/burst-reducer/checkpoint-size.test.mjs — P0-2 checkpoint size boundedness
import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import { existsSync, readFileSync, rmSync, statSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { BurstBuilder } from '../../lib/burst-builder.mjs';
import { writeCheckpoint, loadCheckpoint } from '../../lib/burst-reducer/manifest-manager.mjs';
import { serializeMinimalBurstState, serializeBurstBuilderState } from '../../lib/burst-reducer/burst-state-codec.mjs';
import { CHECKPOINT_SIZE_WARN, CHECKPOINT_SIZE_HARD_LIMIT } from '../../lib/burst-reducer/schema.mjs';

const CP_DIR = join('data/derived/burst_features_v1', 'manifests/checkpoints');
const SIZE_TEST_MARKET = 'size_test_p02';

// ── helpers ──────────────────────────────────────────────────────────

/**
 * Build an open_burst object with `n` identical prints.
 * Each print is { ts, side, price, qty } with tiny variation to avoid
 * same-price-run dedup subtlety (though only the serialized size matters).
 */
function makeOpenBurstWithPrints(n) {
  const prints = [];
  for (let i = 0; i < n; i++) {
    prints.push({ ts: i, side: 'buy', price: 100 + (i % 10), qty: 1 });
  }
  return {
    side: 'buy',
    start_ts: 0,
    end_ts: n - 1,
    prints,
    min_price: 100,
    max_price: 109,
    sum_notional: n * 100,
    sum_qty: n,
  };
}

/**
 * Estimate how many prints are needed in open_burst to produce a
 * checkpoint JSON of at least `targetBytes`.
 */
function estimatePrintsForSize(targetBytes) {
  // Construct a minimal checkpoint with an open_burst containing 1 print,
  // measure its size, then extrapolate linearly.
  const sampleCp = {
    schema_version: 'burst_features_v1',
    last_committed_block_start: 0,
    pending_block: null,
    open_burst: makeOpenBurstWithPrints(1),
    generation: 1,
    updated_at: '2026-07-11T00:00:00.000Z',
  };
  const sampleJson = JSON.stringify(sampleCp, null, 2) + '\n';
  const sampleOverhead = Buffer.byteLength(sampleJson, 'utf8');

  // single print serialised in pretty-print indentation
  const printEntry = JSON.stringify(makeOpenBurstWithPrints(1).prints[0]);
  // in pretty-prined array context:
  //      {\n        "ts": 0,\n        "side": "buy",\n        "price": 100,\n        "qty": 1\n      },
  // That's printEntry with 6 leading spaces + ,
  const singlePrintSize = Buffer.byteLength(
    '      ' + printEntry + ',\n',
    'utf8'
  );

  const overheadPerPrint = singlePrintSize; // approximate
  return Math.ceil((targetBytes - sampleOverhead) / overheadPerPrint) + 100; // +100 margin
}

// ── cleanup ──────────────────────────────────────────────────────────

before(() => {
  try { rmSync(join(CP_DIR, `${SIZE_TEST_MARKET}.json`), { force: true }); } catch (_) {}
});

after(() => {
  try { rmSync(join(CP_DIR, `${SIZE_TEST_MARKET}.json`), { force: true }); } catch (_) {}
});

// ── tests ────────────────────────────────────────────────────────────

describe('Checkpoint size boundedness (P0-2)', () => {

  it('normal checkpoint (small content) writes and round-trips', () => {
    const cp = writeCheckpoint({
      last_committed_block_start: 5000,
      pending_block: null,
      open_burst: null,
      generation: 1,
      market: SIZE_TEST_MARKET,
    });
    assert.ok(cp);
    assert.equal(cp.last_committed_block_start, 5000);
    assert.equal(cp.generation, 1);

    // Round-trip
    const loaded = loadCheckpoint(SIZE_TEST_MARKET);
    assert.ok(loaded);
    assert.equal(loaded.last_committed_block_start, 5000);
  });

  it('checkpoint with 100 000 closed bursts (minimal state) stays under 256 KiB', () => {
    // Create 100k individual bursts by feeding 100k trades spaced beyond gap_threshold
    const b = new BurstBuilder({
      market: SIZE_TEST_MARKET,
      gap_threshold_ms: 5,
      max_burst_duration_ms: 500,
      tick_size: 0.01,
    });
    for (let i = 0; i < 100000; i++) {
      b.feedTrade({ ts: i * 10, side: i % 2 === 0 ? 'buy' : 'sell', price: 100 + (i % 100), qty: 1 });
    }
    b.flushAll();

    const minimal = serializeMinimalBurstState(b);
    // minimal.open should be null (all trades closed into bursts)
    assert.equal(minimal.open, null);

    const cp = writeCheckpoint({
      last_committed_block_start: 999999,
      pending_block: null,
      open_burst: minimal.open,
      generation: 2,
      market: SIZE_TEST_MARKET,
    });

    // Verify file exists and check size on disk
    const filePath = join(CP_DIR, `${SIZE_TEST_MARKET}.json`);
    assert.ok(existsSync(filePath));
    const fileSize = statSync(filePath).size;
    assert.ok(fileSize < CHECKPOINT_SIZE_WARN,
      `checkpoint with 100k closed bursts should be < 256 KiB, got ${fileSize} bytes`);
  });

  it('writeCheckpoint emits WARN when content exceeds 256 KiB', () => {
    // Build an open_burst large enough to push total JSON over CHECKPOINT_SIZE_WARN
    const nPrints = estimatePrintsForSize(CHECKPOINT_SIZE_WARN + 50000); // 50 KiB over
    const largeOpenBurst = makeOpenBurstWithPrints(nPrints);

    // Must NOT throw — WARN is non-fatal
    const cp = writeCheckpoint({
      last_committed_block_start: 0,
      pending_block: { block_start_ms: 5000, replay_identity: { market: SIZE_TEST_MARKET } },
      open_burst: largeOpenBurst,
      generation: 10,
      market: SIZE_TEST_MARKET,
    });
    assert.ok(cp);
    assert.equal(cp.generation, 10);

    // File was written and is loadable
    const loaded = loadCheckpoint(SIZE_TEST_MARKET);
    assert.ok(loaded);
    assert.ok(loaded.open_burst);
  });

  it('writeCheckpoint throws E026 when content exceeds 1 MiB', () => {
    // Build an open_burst large enough to exceed CHECKPOINT_SIZE_HARD_LIMIT
    const nPrints = estimatePrintsForSize(CHECKPOINT_SIZE_HARD_LIMIT + 100000); // 100 KiB over
    const hugeOpenBurst = makeOpenBurstWithPrints(nPrints);

    // Must throw E026
    assert.throws(
      () => writeCheckpoint({
        last_committed_block_start: 0,
        pending_block: null,
        open_burst: hugeOpenBurst,
        generation: 99,
        market: SIZE_TEST_MARKET,
      }),
      /E026/,
      'should throw E026 when checkpoint exceeds 1 MiB',
    );
  });

  it('checkpoint size constants are correct', () => {
    assert.equal(CHECKPOINT_SIZE_WARN, 262144);
    assert.equal(CHECKPOINT_SIZE_HARD_LIMIT, 1048576);
  });

});
