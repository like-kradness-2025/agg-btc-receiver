// test/burst-reducer/burst-detector.test.mjs — BurstDetector tests
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { BurstDetector } from '../../lib/burst-reducer/burst-detector.mjs';
import { BurstBuilder } from '../../lib/burst-builder.mjs';
import { GAP_THRESHOLD_MS, MAX_BURST_DURATION_MS } from '../../lib/burst-reducer/schema.mjs';


// Retention window constant: MAX_BURST_DURATION_MS + GAP_THRESHOLD_MS + 1000ms
const RETENTION_WINDOW_MS = MAX_BURST_DURATION_MS + GAP_THRESHOLD_MS + 1000;

describe('BurstDetector', () => {
  it('uses correct fixed parameters', () => {
    assert.equal(GAP_THRESHOLD_MS, 50);
    assert.equal(MAX_BURST_DURATION_MS, 5000);
  });

  it('forms burst from trades within gap threshold', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([
      { ts: 1000, side: 'buy', price: 100, qty: 1 },
      { ts: 1040, side: 'buy', price: 100, qty: 1 }, // gap=40 <= 50
    ]);
    bd.flushAll();
    const bursts = bd.getClosedBurstsOverlapping(1000);
    assert.equal(bursts.length, 1);
    assert.equal(bursts[0].burst_print_count, 2);
  });

  it('splits on gap > 50ms', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([
      { ts: 1000, side: 'buy', price: 100, qty: 1 },
      { ts: 1060, side: 'buy', price: 100, qty: 1 }, // gap=60 > 50
    ]);
    bd.flushAll();
    const bursts = bd.getClosedBurstsOverlapping(1000);
    assert.equal(bursts.length, 2);
  });

  it('returns open burst state for checkpoint via codec API', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([{ ts: 1000, side: 'buy', price: 100, qty: 1 }]);
    const state = bd.getOpenBurstState();
    assert.ok(state);
    // Codec state shape: { schemaVersion, open, closedBursts, nextId }
    assert.equal(state.schemaVersion, 1);
    assert.ok(Array.isArray(state.closedBursts));
    assert.equal(state.closedBursts.length, 0);
    assert.ok(typeof state.nextId === 'number' && state.nextId >= 1);
    assert.ok(state.open !== null);
    assert.equal(state.open.side, 'buy');
    assert.equal(state.open.prints.length, 1);
  });

  it('isFirstBlock is true when no checkpoint', () => {
    const bd = new BurstDetector('test');
    assert.equal(bd.isFirstBlock, true);
  });

  it('restores open burst from checkpoint', () => {
    const bd1 = new BurstDetector('test');
    bd1.feedTrades([{ ts: 1000, side: 'buy', price: 100, qty: 1 }]);
    const cp = bd1.getOpenBurstState();

    const bd2 = new BurstDetector('test', cp);
    assert.equal(bd2.isFirstBlock, false);
    // feed another trade continuing the burst
    bd2.feedTrades([{ ts: 1020, side: 'buy', price: 100, qty: 1 }]);
    bd2.flushAll();
    const bursts = bd2.getClosedBurstsOverlapping(1000);
    assert.equal(bursts.length, 1);
    assert.equal(bursts[0].burst_print_count, 2);
  });

  it('market getter returns correct value', () => {
    const bd = new BurstDetector('binance_spot');
    assert.equal(bd.market, 'binance_spot');
  });

  it('getAllClosedBursts uses codec API (deep copy)', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([{ ts: 1000, side: 'buy', price: 100, qty: 1 }]);
    bd.flushAll();
    const snapshot = bd.getAllClosedBursts();
    assert.equal(snapshot.length, 1);
    // Mutating snapshot does not affect detector
    snapshot[0].burst_notional = 999;
    const snapshot2 = bd.getAllClosedBursts();
    assert.equal(snapshot2[0].burst_notional, 100);
  });

  // ── P1-2: closed burst prune ──────────────────────────────────────────

  it('pruneClosedBurstsBefore removes old bursts, keeps recent ones', () => {
    const bb2 = new BurstBuilder({ market: 'test', gap_threshold_ms: 50, max_burst_duration_ms: 5000, tick_size: 0.01 });
    // burst A: end_ts=120
    bb2.feedTrade({ ts: 100, side: 'buy', price: 100, qty: 1 });
    bb2.feedTrade({ ts: 120, side: 'buy', price: 100, qty: 1 }); // within gap -> same burst A
    // gap: next trade at 5000+ > 50ms -> new burst B
    const burstBStart = 5000;
    bb2.feedTrade({ ts: burstBStart, side: 'buy', price: 100, qty: 1 });
    bb2.feedTrade({ ts: burstBStart + 20, side: 'buy', price: 100, qty: 1 }); // same burst B
    bb2.flushAll();

    // Both bursts exist — each in their own time bucket
    assert.equal(bb2.getClosedBurstsOverlapping(0).length, 1, 'burst A in bucket 0');
    assert.equal(bb2.getClosedBurstsOverlapping(burstBStart).length, 1, 'burst B at ts 5000');

    // Prune at blockStartMs = burstBStart + 10000 (way past burst A)
    // Retention window = 5000 + 50 + 1000 = 6050ms
    // cutoff = (burstBStart + 10000) - 6050 = burstBStart + 3950 = 8950
    // burst A end_ts=120 < 8950 -> pruned, burst B end_ts=5020 < 8950 -> also pruned? 
    // Wait, that's wrong. Let me recalculate.
    // cutoff = (5000 + 10000) - 6050 = 15000 - 6050 = 8950
    // burst A end_ts=120 < 8950 -> pruned
    // burst B end_ts=5020 < 8950 -> also pruned!
    // That means both are pruned. The test logic is wrong.
    // I need a blockStartMs that keeps burst B but prunes burst A.
    // cutoff must be between burst A end (120) and burst B end (5020)
    // blockStartMs = cutoff + 6050
    // BlockStartMs between (120+6050) and (5020+6050), i.e. 6170..11070
    // Let me use blockStartMs = 7000
    // cutoff = 7000 - 6050 = 950
    // burst A end_ts=120 < 950 -> pruned ✓
    // burst B end_ts=5020 >= 950 -> kept ✓
    const blockStartMs = 7000
    bb2.pruneClosedBurstsBefore(blockStartMs);
    assert.equal(bb2.getClosedBurstsOverlapping(0).length, 0, 'burst A pruned from bucket 0');
    assert.equal(bb2.getClosedBurstsOverlapping(burstBStart).length, 1, 'burst B remains at ts 5000');
  });

  it('pruneClosedBurstsBefore preserves boundary burst_end_ts === cutoff', () => {
    // Test the boundary condition: burst with end_ts exactly === cutoff is kept
    const bb = new BurstBuilder({ market: 'test', gap_threshold_ms: 50, max_burst_duration_ms: 5000, tick_size: 0.01 });
    // Create a burst at known position
    bb.feedTrade({ ts: 5000, side: 'buy', price: 100, qty: 1 });
    bb.feedTrade({ ts: 5050, side: 'buy', price: 100, qty: 1 }); // same burst, end_ts=5050
    bb.flushAll();

    const window = 5000 + 50 + 1000; // 6050
    // blockStartMs such that cutoff = 5050 exactly
    const blockStartMs = 5050 + window; // = 11100
    bb.pruneClosedBurstsBefore(blockStartMs);
    // cutoff = 11100 - 6050 = 5050, burst_end_ts=5050 >= 5050 -> kept
    const remaining = bb.getClosedBurstsOverlapping(5000);
    assert.equal(remaining.length, 1, 'burst at exact cutoff should be kept');
  });

  it('pruneClosedBurstsBefore is no-op with no closed bursts', () => {
    const bb = new BurstBuilder({ market: 'test', gap_threshold_ms: 50, max_burst_duration_ms: 5000, tick_size: 0.01 });
    // No trades fed -> no closed bursts
    assert.doesNotThrow(() => bb.pruneClosedBurstsBefore(100000));
  });

  it('BurstDetector exposes pruneClosedBurstsBefore wrapper', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([{ ts: 100, side: 'buy', price: 100, qty: 1 }]);
    bd.feedTrades([{ ts: 10000, side: 'buy', price: 100, qty: 1 }]);
    bd.flushAll();
    // Should not throw
    assert.doesNotThrow(() => bd.pruneClosedBurstsBefore(50000));
    // After prune with large blockStartMs, only bursts overlapping recent window remain
    // Both bursts end at 100 and 10000. With window=6050 and cutoff=50000-6050=43950
    // Both are < cutoff -> both should be pruned
    const remaining = bd.getClosedBurstsOverlapping(0);
    assert.equal(remaining.length, 0, 'all bursts pruned with large blockStartMs');
  });
});
