import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { computeBookFlowForSecond } from '../../lib/burst-reducer/book-flow-features.mjs';

describe('Book flow features', () => {
  it('computes add/cancel, Cont OFI and queue replenishment from a hand fixture', () => {
    const result = computeBookFlowForSecond({
      secondTs: 1000,
      stateBefore: {
        seeded: true,
        best_bid: 100, best_bid_qty: 2,
        best_ask: 101, best_ask_qty: 3,
        bids: [[100, 2]], asks: [[101, 3]],
      },
      events: [{
        type: 'update', event_ts_ms: 1500,
        bids: [['100', '5'], ['99', '4']],
        asks: [['101', '1'], ['102', '2']],
      }],
    });
    assert.equal(result.bid_add_qty_1s, 7);
    assert.equal(result.bid_cancel_qty_1s, 0);
    assert.equal(result.ask_add_qty_1s, 2);
    assert.equal(result.ask_cancel_qty_1s, 2);
    assert.equal(result.ofi_1s, 5);
    assert.equal(result.replenishment_qty_1s, 3);
    assert.equal(result.pulling_qty_1s, 2);
    assert.equal(result.depth_delta_1s, 98);
    assert.equal(result.depth_delta_30s, null);
  });

  it('fails closed before seed and excludes future events', () => {
    const empty = computeBookFlowForSecond({
      secondTs: 1000,
      stateBefore: { seeded: false },
      events: [{ type: 'update', event_ts_ms: 2000, bids: [['100', '1']], asks: [] }],
    });
    assert.equal(empty.ofi_1s, null);
    assert.equal(empty.bid_add_qty_1s, null);
  });
});
