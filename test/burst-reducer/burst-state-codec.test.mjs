// test/burst-reducer/burst-state-codec.test.mjs — BurstStateCodec tests
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { BurstBuilder } from '../../lib/burst-builder.mjs';
import { serializeBurstBuilderState, restoreBurstBuilderState, serializeMinimalBurstState, getClosedBurstsSnapshot } from '../../lib/burst-reducer/burst-state-codec.mjs';

describe('BurstStateCodec', () => {
  it('round-trip: serialize → restore → serialize produces identical state', () => {
    const b1 = new BurstBuilder({ market: 'test', gap_threshold_ms: 50, max_burst_duration_ms: 5000, tick_size: 0.01 });
    b1.feedTrade({ ts: 100, side: 'buy', price: 100, qty: 1 });
    b1.feedTrade({ ts: 120, side: 'buy', price: 100, qty: 2 });
    b1.flushAll();

    const state1 = serializeBurstBuilderState(b1);
    assert.equal(state1.schemaVersion, 1);
    assert.equal(state1.closedBursts.length, 1);
    assert.equal(state1.open, null);
    assert.ok(state1.nextId > 1);

    const b2 = new BurstBuilder({ market: 'test', gap_threshold_ms: 50, max_burst_duration_ms: 5000, tick_size: 0.01 });
    restoreBurstBuilderState(b2, state1);

    const state2 = serializeBurstBuilderState(b2);
    assert.deepEqual(state2, state1);
  });

  it('restore then feed same next trade → byte-identical closed bursts', () => {
    const b1 = new BurstBuilder({ market: 'test', gap_threshold_ms: 50, max_burst_duration_ms: 5000, tick_size: 0.01 });
    b1.feedTrade({ ts: 100, side: 'buy', price: 100, qty: 1 });
    const cp = serializeBurstBuilderState(b1); // open burst

    const b2 = new BurstBuilder({ market: 'test', gap_threshold_ms: 50, max_burst_duration_ms: 5000, tick_size: 0.01 });
    restoreBurstBuilderState(b2, cp);
    b2.feedTrade({ ts: 120, side: 'buy', price: 100, qty: 2 });
    b2.flushAll();

    // Use getClosedBurstsSnapshot (codec API) not direct access
    const bursts2 = getClosedBurstsSnapshot(b2);
    assert.equal(bursts2.length, 1);
    assert.equal(bursts2[0].burst_print_count, 2);
    assert.equal(bursts2[0].burst_notional, 300);
    // _nextId restored correctly (burst_id = 'test-1')
    assert.equal(bursts2[0].burst_id, 'test-1');
  });

  it('malformed state throws E020', () => {
    const b = new BurstBuilder({ market: 'test' });
    assert.throws(() => restoreBurstBuilderState(b, null), /E020/);
    assert.throws(() => restoreBurstBuilderState(b, { schemaVersion: 99 }), /E020/);
    assert.throws(() => restoreBurstBuilderState(b, { schemaVersion: 1, closedBursts: 'not_array', nextId: 1 }), /E020/);
    assert.throws(() => restoreBurstBuilderState(b, { schemaVersion: 1, closedBursts: [], nextId: -1 }), /E020/);
  });

  it('serialize empty builder', () => {
    const b = new BurstBuilder({ market: 'test' });
    const state = serializeBurstBuilderState(b);
    assert.equal(state.nextId, 1);
    assert.equal(state.open, null);
    assert.deepEqual(state.closedBursts, []);
  });

  it('getClosedBurstsSnapshot returns deep copy', () => {
    const b = new BurstBuilder({ market: 'test', gap_threshold_ms: 50, max_burst_duration_ms: 5000, tick_size: 0.01 });
    b.feedTrade({ ts: 100, side: 'buy', price: 100, qty: 1 });
    b.flushAll();
    const snapshot = getClosedBurstsSnapshot(b);
    assert.equal(snapshot.length, 1);
    // Mutating snapshot should not affect builder
    snapshot[0].burst_notional = 999;
    const snapshot2 = getClosedBurstsSnapshot(b);
    assert.equal(snapshot2[0].burst_notional, 100); // unchanged
  });

  // P1-2: minimal state never contains closedBursts
  it('serializeMinimalBurstState output has no closedBursts field', () => {
    const b = new BurstBuilder({ market: 'test', gap_threshold_ms: 50, max_burst_duration_ms: 5000, tick_size: 0.01 });
    b.feedTrade({ ts: 100, side: 'buy', price: 100, qty: 1 });
    b.feedTrade({ ts: 120, side: 'buy', price: 100, qty: 1 });
    b.flushAll();

    const minimal = serializeMinimalBurstState(b);
    assert.ok(minimal);
    // Must NOT have closedBursts
    assert.ok(!('closedBursts' in minimal), 'minimal state should not contain closedBursts');
    // Must NOT have same_price_runs or prints at top level
    assert.ok(!('same_price_runs' in minimal), 'minimal state should not contain same_price_runs');
    assert.ok(!('prints' in minimal), 'minimal state should not contain prints');
    // Must have open and nextId
    assert.equal(minimal.open, null); // both trades closed into a burst, so open=null
    assert.ok(typeof minimal.nextId === 'number' && minimal.nextId >= 2);
  });

  it('serializeMinimalBurstState with open burst has prints but no closedBursts', () => {
    const b = new BurstBuilder({ market: 'test', gap_threshold_ms: 50, max_burst_duration_ms: 5000, tick_size: 0.01 });
    b.feedTrade({ ts: 100, side: 'buy', price: 100, qty: 1 });
    // Open burst still in progress — not flushed

    const minimal = serializeMinimalBurstState(b);
    assert.ok(minimal);
    assert.ok(!('closedBursts' in minimal), 'minimal state should not contain closedBursts');
    assert.ok(!('same_price_runs' in minimal));
    assert.ok(!('prints' in minimal));
    // open is NOT null
    assert.ok(minimal.open !== null);
    // open.prints IS present (required for continuation)
    assert.ok(Array.isArray(minimal.open.prints));
    assert.equal(minimal.open.prints.length, 1);
  });
});
