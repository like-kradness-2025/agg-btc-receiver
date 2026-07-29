import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { computeTradeBookInteractionForSecond } from '../../lib/burst-reducer/trade-book-interaction.mjs';

describe('Trade-book interaction', () => {
  it('joins trades to the strict pre-trade book state', () => {
    const result = computeTradeBookInteractionForSecond({
      secondTs: 1000,
      trades: [
        { ts: 1100, side: 'buy', price: 102, qty: 2 },
        { ts: 1200, side: 'sell', price: 99, qty: 1 },
      ],
      stateAt: (ts) => ({ seeded: true, mid: 100.5, best_bid: 100, best_bid_qty: 4, best_ask: 101, best_ask_qty: 3, asks: [[101, 3], [102, 2]], bids: [[100, 4], [99, 2]] }),
    });
    assert.equal(result.trade_at_touch_qty_1s, 0);
    assert.equal(result.trade_through_touch_qty_1s, 3);
    assert.equal(result.trade_sweep_level_count_1s, 4);
    assert.equal(result.trade_sweep_notional_1s, 102 * 2 + 101 * 3 + 100 * 4 + 99 * 2);
    assert.ok(result.trade_slippage_bps_mean_1s > 0);
  });
});
