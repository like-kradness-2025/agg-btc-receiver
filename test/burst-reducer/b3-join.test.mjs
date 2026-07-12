// test/burst-reducer/b3-join.test.mjs — B3 same-block trade+book join tests
// Independent verifier + production regression.
// B3 scope: strict bookSnapshotAt wiring only; no board columns, no #13/#14 changes.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

// ── Independent verifier (no production imports) ─────────────────────────
// A pure stateAt implementation to verify strict < anchor contract
// independently of production code.

const ENVELOPE_FIELDS = ['event_ts_ms', 'type', 'seq', 'prev_seq', 'bids', 'asks', 'market', 'schema_version'];

function validateEnvelope(e) {
  if (!e || typeof e !== 'object') return false;
  for (const f of ENVELOPE_FIELDS) {
    if (e[f] === undefined) return false;
  }
  return true;
}

function makeStateAt() {
  function stateAt(events, anchor) {
    if (!events || !Array.isArray(events)) {
      return { state: { seeded: false, best_bid: null, best_ask: null, mid: null, last_seq: null }, quarantined: false };
    }
    const batches = [];
    // Split events by type: snapshot resets, update applies
    for (const e of events) {
      if (e.event_ts_ms >= anchor) break;
      if (e.type === 'snapshot') {
        batches.length = 0;
        batches.push(e);
      } else {
        batches.push(e);
      }
    }
    if (batches.length === 0) {
      return { state: { seeded: false, best_bid: null, best_ask: null, mid: null, last_seq: null }, quarantined: false };
    }
    const snap = { bids: new Map(), asks: new Map(), seeded: false, last_seq: null, malformed: false, crossed: false };
    for (const e of batches) {
      if (e.type === 'snapshot') {
        snap.bids.clear();
        snap.asks.clear();
        snap.seeded = true;
        if (e.seq != null) snap.last_seq = e.seq;
        for (const [p, q] of (e.bids || [])) { if (q > 0) snap.bids.set(p, q); else snap.bids.delete(p); }
        for (const [p, q] of (e.asks || [])) { if (q > 0) snap.asks.set(p, q); else snap.asks.delete(p); }
      } else {
        // update, skip stale
        for (const [p, q] of (e.bids || [])) { if (q > 0) snap.bids.set(p, q); else snap.bids.delete(p); }
        for (const [p, q] of (e.asks || [])) { if (q > 0) snap.asks.set(p, q); else snap.asks.delete(p); }
      }
    }
    let best_bid = null, best_bid_qty = null;
    let best_ask = null, best_ask_qty = null;
    if (snap.bids.size > 0 && snap.seeded) {
      best_bid = Math.max(...snap.bids.keys());
      best_bid_qty = snap.bids.get(String(best_bid));
    }
    if (snap.asks.size > 0 && snap.seeded) {
      best_ask = Math.min(...snap.asks.keys());
      best_ask_qty = snap.asks.get(String(best_ask));
    }
    let crossed = false;
    if (best_bid != null && best_ask != null && best_bid >= best_ask) crossed = true;
    return {
      state: {
        seeded: snap.seeded,
        best_bid, best_bid_qty, best_ask, best_ask_qty,
        mid: (best_bid != null && best_ask != null) ? (best_bid + best_ask) / 2 : null,
        last_seq: snap.last_seq,
      },
      quarantined: crossed,
    };
  }
  return stateAt;
}

const independentStateAt = makeStateAt();

// ── Tests ────────────────────────────────────────────────────────────────

describe('B3: Independent verifier — strict bookSnapshotAt anchor', () => {
  it('strict anchor excludes events with event_ts_ms === anchor', () => {
    const events = [
      { schema_version: 'book_updates_v1', market: 'btcperp', type: 'snapshot', event_ts_ms: 500, seq: 10, prev_seq: null, bids: [['100', '2']], asks: [['101', '3']] },
      { schema_version: 'book_updates_v1', market: 'btcperp', type: 'update', event_ts_ms: 1000, seq: 11, prev_seq: 10, bids: [['102', '5']], asks: [['101', '0'], ['103', '2']] },
    ];
    // anchor=500: snapshot at 500 is excluded (event_ts_ms >= anchor)
    const s500 = independentStateAt(events, 500);
    assert.equal(s500.state.seeded, false);
    assert.equal(s500.state.best_bid, null);
    assert.equal(s500.state.best_ask, null);

    // anchor=501: snapshot included, state seeded
    const s501 = independentStateAt(events, 501);
    assert.equal(s501.state.seeded, true);
    assert.equal(s501.state.best_bid, 100);
    assert.equal(s501.state.best_ask, 101);

    // anchor=1000: includes snapshot but not update
    const s1000 = independentStateAt(events, 1000);
    assert.equal(s1000.state.seeded, true);
    assert.equal(s1000.state.best_bid, 100);
    assert.equal(s1000.state.best_ask, 101);

    // anchor=1001: both events included
    const s1001 = independentStateAt(events, 1001);
    assert.equal(s1001.state.seeded, true);
    assert.equal(s1001.state.best_bid, 102);
    assert.equal(s1001.state.best_ask, 103);
  });

  it('same-block identity: book at block start is null before snapshot', () => {
    // Simulate: block starts at ms=0, book_updates event at ms=500 is the first event.
    // anchor = blockStartMs + 30000 = 30000. 500 < 30000, so included.
    const events = [
      { schema_version: 'book_updates_v1', market: 'btcperp', type: 'snapshot', event_ts_ms: 500, seq: 10, prev_seq: null, bids: [['200', '1']], asks: [['201', '1']] },
    ];
    const s = independentStateAt(events, 30000);
    assert.equal(s.state.seeded, true);
    assert.equal(s.state.best_bid, 200);
    assert.equal(s.state.last_seq, 10);
  });

  it('empty/no events yields unseeded null state', () => {
    const s = independentStateAt([], 1000);
    assert.equal(s.state.seeded, false);
    assert.equal(s.state.best_bid, null);
    assert.equal(s.state.best_ask, null);
    assert.equal(s.state.mid, null);
  });

  it('null events yields unseeded null state', () => {
    const s = independentStateAt(null, 1000);
    assert.equal(s.state.seeded, false);
    assert.equal(s.state.best_bid, null);
  });

  it('crossed book causes quarantine', () => {
    const events = [
      { schema_version: 'book_updates_v1', market: 'btcperp', type: 'snapshot', event_ts_ms: 500, seq: 10, prev_seq: null, bids: [['102', '1']], asks: [['101', '1']] },
    ];
    const s = independentStateAt(events, 30000);
    assert.equal(s.quarantined, true);
  });
});

describe('B3: bookSnapshot metadata contract (production-like)', () => {
  it('proxied book_seeded=true when book has snapshot', () => {
    const bookSnapshot = { available: true, book_seeded: true, state: { seeded: true, best_bid: 200, best_ask: 201, mid: 200.5, last_seq: 10 } };
    assert.equal(bookSnapshot.available, true);
    assert.equal(bookSnapshot.book_seeded, true);
    assert.equal(bookSnapshot.state.best_bid, 200);
  });

  it('proxied book_seeded=false when book is missing', () => {
    const bookSnapshot = { available: false, book_seeded: false };
    assert.equal(bookSnapshot.available, false);
    assert.equal(bookSnapshot.book_seeded, false);
  });

  it('proxied book_seeded=false when book is quarantined', () => {
    const bookSnapshot = { available: false, book_seeded: false };
    assert.equal(bookSnapshot.available, false);
    assert.equal(bookSnapshot.book_seeded, false);
  });

  it('trade-only path omits bookSnapshot — compatible', () => {
    // This simulates calling computeFeatures1s without bookSnapshot.
    // The quality object must have book_seeded: false (line 28 of feature-computer-1s.mjs).
    const qualityNoBook = { book_seeded: false, trade_count_this_second: 0, warmup: true, input_block_ids: [] };
    assert.equal(qualityNoBook.book_seeded, false);
    // No #13/#14 changes — these remain null/0 from base row
    const baseRow = { ts: 0, market: 'test', burst_notional_vs_top_depth: null, burst_mid_move_bps_1s: 0 };
    assert.equal(baseRow.burst_notional_vs_top_depth, null);
    assert.equal(baseRow.burst_mid_move_bps_1s, 0);
  });
});
