import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { toCanonicalBookEnvelope } from '../lib/book-updates-adapter.mjs';
import { materializeBookSnapshots } from '../lib/book-snapshot-materializer.mjs';
import { BookStateMachine } from '../lib/book-state-machine.mjs';
import { normalizeBookQuantity } from '../lib/market-registry.mjs';

const canonical = (raw) => {
  const result = toCanonicalBookEnvelope(raw);
  assert.equal(result.valid, true, result.errors?.join('; '));
  return result.envelope;
};

describe('canonical book boundary', () => {
  it('normalizes contract quantities to BTC', () => {
    assert.equal(normalizeBookQuantity('okx_perp', 50000, 10), 0.1);
    assert.equal(normalizeBookQuantity('bitmex_perp', 50000, 100), 0.002);
    const event = canonical({
      market: 'okx_perp', type: 'snapshot', ts: 1000, seq: 10,
      bids: [['50000', '10']], asks: [['50010', '20']],
    });
    assert.deepEqual(event.bids, [['50000', '0.1']]);
    assert.deepEqual(event.asks, [['50010', '0.2']]);
    assert.equal(event.qty_unit, 'BTC');
  });

  it('retains sequence range and previous sequence metadata', () => {
    const event = canonical({
      market: 'binance_spot', type: 'update', ts: 1000, seq: 12,
      prev_seq: 10, seq_start: 11, seq_end: 12,
      bids: [['50000', '1']], asks: [],
    });
    assert.equal(event.prev_seq, 10);
    assert.equal(event.seq_start, 11);
    assert.equal(event.seq_end, 12);
  });
});

describe('strict pre-second book snapshots', () => {
  it('excludes events at the anchor and emits 30 rows', () => {
    const start = Math.floor(1_700_000_000_000 / 30000) * 30000;
    const events = [
      canonical({ market: 'binance_spot', type: 'snapshot', ts: start - 1, seq: 100, bids: [['50000', '1']], asks: [['50010', '2']] }),
      canonical({ market: 'binance_spot', type: 'update', ts: start + 1000, seq: 101, prev_seq: 100, bids: [['50000', '3']], asks: [] }),
    ];
    const rows = materializeBookSnapshots(events, start);
    assert.equal(rows.length, 30);
    assert.equal(rows[0].bid_qtys[0], 1);
    assert.equal(rows[1].bid_qtys[0], 1);
    assert.equal(rows[2].bid_qtys[0], 3);
    assert.equal(rows[0].seeded, true);
  });

  it('keeps a gap quarantine across later snapshots', () => {
    const sm = new BookStateMachine();
    sm.apply({ type: 'snapshot', event_ts_ms: 1, seq: 10, bids: [[50000, 1]], asks: [[50010, 1]] });
    sm.apply({ type: 'update', event_ts_ms: 2, seq: 12, prev_seq: 10, bids: [[50000, 2]], asks: [] });
    assert.equal(sm.quarantined, true);
    sm.apply({ type: 'snapshot', event_ts_ms: 3, seq: 20, bids: [[50100, 1]], asks: [[50110, 1]] });
    assert.equal(sm.quarantined, true);
    assert.equal(sm.bestBid(), 50100);
    assert.equal(sm.last_seq, 20);
  });

  it('does not finalize a one-sided snapshot until both sides exist', () => {
    const start = Math.floor(1_700_000_000_000 / 30000) * 30000;
    const rows = materializeBookSnapshots([
      canonical({ market: 'bitstamp_spot', type: 'snapshot', ts: start - 1, seq: null, bids: [['50000', '1']], asks: [] }),
      canonical({ market: 'bitstamp_spot', type: 'update', ts: start + 1000, seq: null, bids: [], asks: [['50010', '2']] }),
    ], start);
    assert.equal(rows[0].seeded, false);
    assert.equal(rows[0].finalized, false);
    assert.equal(rows[0].book_status, 'unseeded');
    assert.equal(rows[2].seeded, true);
    assert.equal(rows[2].finalized, true);
    assert.equal(rows[2].mid, 50005);
  });
});
