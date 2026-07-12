// B2 focused tests for lib/book-state-machine.mjs
// Tests contract semantics from P0-0 spec sections 5-8 and 13.
// Imports the production module; does NOT import the fixture verifier.
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  BookStateMachine,
  ordered,
  processBlock,
  stateAt,
} from '../lib/book-state-machine.mjs';

// ─────────────────────────────────────────────
// Helpers for concise event construction
// ─────────────────────────────────────────────
const snap = (overrides = {}) => ({
  type: 'snapshot',
  event_ts_ms: 100,
  seq: 10,
  prev_seq: null,
  bids: [['100', '2']],
  asks: [['101', '3']],
  path: 'book_updates/test/1970-01-01/00-00-00.jsonl',
  line_no: 1,
  ...overrides,
});

const update = (overrides = {}) => ({
  type: 'update',
  event_ts_ms: 200,
  seq: 11,
  prev_seq: 10,
  bids: [],
  asks: [],
  path: 'book_updates/test/1970-01-01/00-00-00.jsonl',
  line_no: 2,
  ...overrides,
});

describe('BookStateMachine (B2 focused)', () => {

  // ── 1. Snapshot creates seeded state ──
  it('snapshot creates seeded state with correct best bid/ask/mid', () => {
    const sm = new BookStateMachine();
    sm.apply(snap());
    assert.equal(sm.seeded, true);
    assert.equal(sm.bestBid(), 100);
    assert.equal(sm.bestBidQty(), 2);
    assert.equal(sm.bestAsk(), 101);
    assert.equal(sm.bestAskQty(), 3);
    assert.equal(sm.mid(), 100.5);
    assert.equal(sm.last_seq, 10);
    assert.equal(sm.book_status, 'seeded');
    assert.equal(sm.sequence_status, 'ok');
  });

  // ── 2. Snapshot with empty sides ──
  it('snapshot with empty bids/asks creates seeded state with null bests', () => {
    const sm = new BookStateMachine();
    sm.apply(snap({ bids: [], asks: [] }));
    assert.equal(sm.seeded, true);
    assert.equal(sm.bestBid(), null);
    assert.equal(sm.bestAsk(), null);
    assert.equal(sm.mid(), null);
  });

  // ── 3. Update sets and deletes levels ──
  it('update sets qty>0 and deletes qty=0', () => {
    const sm = new BookStateMachine();
    sm.apply(snap());
    // Delete best bid (qty=0), add new best bid
    sm.apply(update({
      seq: 11, prev_seq: 10,
      bids: [['100', '0'], ['102', '5']],
      asks: [['101', '0'], ['103', '2']],
    }));
    assert.equal(sm.bestBid(), 102);
    assert.equal(sm.bestBidQty(), 5);
    assert.equal(sm.bestAsk(), 103);
    assert.equal(sm.bestAskQty(), 2);
    assert.equal(sm.mid(), 102.5);
    assert.equal(sm.last_seq, 11);
  });

  // ── 4. Pre-snapshot updates: unseeded state ──
  it('updates before snapshot leave state unseeded', () => {
    const sm = new BookStateMachine();
    sm.apply(update({
      seq: null, prev_seq: null,
      bids: [['100', '1']], asks: [['101', '1']],
    }));
    assert.equal(sm.seeded, false);
    assert.equal(sm.bestBid(), 100); // map is updated internally
    assert.equal(sm.bestAsk(), 101);
    // But best values are available since we don't hide the map
    // The spec says "before seed remains unseeded/not publicly available"
    // Actually the test fixture's BookStateMachine doesn't hide internal state
    // for bestBid/bestAsk, it only sets book_status='unseeded'.
    // The "not publicly available" is communicated by book_status, not hidden bests.
    assert.equal(sm.book_status, 'unseeded');
  });

  // ── 5. Null seq → unsequenced ──
  it('null seq leaves sequence_status unsequenced', () => {
    const sm = new BookStateMachine();
    sm.apply(snap({ seq: null }));
    assert.equal(sm.seeded, true);
    assert.equal(sm.sequence_status, 'unsequenced');
    assert.equal(sm.last_seq, null);
    assert.equal(sm.book_status, 'unsequenced');
  });

  // ── 6. Sequence gap: prev_seq mismatch ──
  it('detects gap when prev_seq does not match last_seq', () => {
    const sm = new BookStateMachine();
    sm.apply(snap());
    const result = sm.apply(update({
      seq: 11, prev_seq: 9, // prev_seq 9 !== last_seq 10
      bids: [['102', '1']],
    }));
    assert.equal(result.reason, 'SEQUENCE_GAP');
    assert.equal(sm.gap_detected, true);
    assert.equal(sm.sequence_status, 'gap');
    assert.equal(sm.error_code, 'SEQUENCE_GAP');
    assert.equal(sm.quarantined, true);
    assert.equal(sm.commit, false);
  });

  // ── 7. Sequence gap: seq jump without prev_seq ──
  it('detects gap when prev_seq is null and seq !== last_seq + 1', () => {
    const sm = new BookStateMachine();
    sm.apply(snap());
    const result = sm.apply(update({
      seq: 12, prev_seq: null, // seq 12 !== last_seq+1 (11)
      bids: [['102', '1']],
    }));
    assert.equal(result.reason, 'SEQUENCE_GAP');
    assert.equal(sm.gap_detected, true);
    assert.equal(sm.error_code, 'SEQUENCE_GAP');
  });

  // ── 8. Stale/duplicate does not mutate state ──
  it('stale/duplicate seq does not mutate state', () => {
    const sm = new BookStateMachine();
    sm.apply(snap());
    assert.equal(sm.bestBid(), 100);
    assert.equal(sm.last_seq, 10);

    sm.apply(update({
      seq: 11, prev_seq: 10,
      bids: [['100', '0'], ['102', '5']],
      asks: [['101', '0'], ['103', '2']],
    }));
    assert.equal(sm.bestBid(), 102);
    assert.equal(sm.last_seq, 11);

    // Stale event: seq=10 <= last_seq=11
    const result = sm.apply(update({
      seq: 10, prev_seq: null,
      bids: [['200', '999']],
    }));
    assert.equal(result.reason, 'stale_duplicate');
    assert.equal(sm.stale_detected, true);
    assert.equal(sm.sequence_status, 'stale_duplicate');
    // State unchanged
    assert.equal(sm.bestBid(), 102);
    assert.equal(sm.last_seq, 11);
    // Stale alone does NOT quarantine
    assert.equal(sm.quarantined, false);
  });

  // ── 9. Crossed book detection ──
  it('detects crossed book (best_bid >= best_ask) and quarantines', () => {
    const sm = new BookStateMachine();
    // Snapshot with crossed book: bid 102 >= ask 101
    sm.apply(snap({ bids: [['102', '1']], asks: [['101', '1']] }));
    assert.equal(sm.checkCrossed(), true);
    assert.equal(sm.error_code, 'CROSSED_BOOK');
    assert.equal(sm.quarantined, true);
    assert.equal(sm.commit, false);
    assert.equal(sm.cursor, 'retain');
    assert.equal(sm.book_status, 'quarantine');
  });

  // ── 10. Malformed level (negative qty) ──
  it('rejects malformed level (negative qty) with fail-closed', () => {
    const sm = new BookStateMachine();
    const result = sm.apply(snap({ bids: [['100', '-1']] }));
    assert.equal(result.reason, 'MALFORMED_LEVEL');
    assert.equal(sm.malformed_detected, true);
    assert.equal(sm.error_code, 'MALFORMED_LEVEL');
    assert.equal(sm.quarantined, true);
    assert.equal(sm.commit, false);
    assert.equal(sm.sequence_status, 'malformed');
  });

  // ── 11. Malformed level (non-positive price) ──
  it('rejects malformed level (price <= 0)', () => {
    const sm = new BookStateMachine();
    const result = sm.apply(snap({ bids: [['0', '1']] }));
    assert.equal(result.reason, 'MALFORMED_LEVEL');
  });

  // ── 12. Range event bridge ok ──
  it('accepts range event with correct bridge conditions', () => {
    const sm = new BookStateMachine();
    sm.apply(snap());
    const result = sm.apply({
      type: 'update', event_ts_ms: 200,
      seq: 15, prev_seq: 10,
      seq_start: 11, seq_end: 15,
      bids: [['102', '3']], asks: [],
      path: 'book_updates/test/1970-01-01/00-00-00.jsonl', line_no: 2,
    });
    assert.equal(result.applied, true);
    assert.equal(sm.last_seq, 15);
    assert.equal(sm.bestBid(), 102);
  });

  // ── 13. Range event bridge fail ──
  it('rejects range event with incorrect bridge (seq_start mismatch)', () => {
    const sm = new BookStateMachine();
    sm.apply(snap());
    const result = sm.apply({
      type: 'update', event_ts_ms: 200,
      seq: 15, prev_seq: 10,
      seq_start: 12, seq_end: 15, // seq_start 12 !== last_seq+1 (11)
      bids: [['102', '3']], asks: [],
      path: 'book_updates/test/1970-01-01/00-00-00.jsonl', line_no: 2,
    });
    assert.equal(result.reason, 'SEQUENCE_GAP');
    assert.equal(sm.gap_detected, true);
  });

  // ── 14. processBlock with null/undefined/{} ──
  it('processBlock returns blocked unknown-input for null/undefined/{}', () => {
    for (const bad of [null, undefined]) {
      const r = processBlock(bad);
      assert.equal(r.decisions.blocked_reason, 'unknown-input');
      assert.equal(r.decisions.commit, false);
      assert.equal(r.state, null);
    }
    const r = processBlock({});
    assert.equal(r.decisions.blocked_reason, 'unknown-input');
  });

  // ── 15. processBlock with empty events → valid-empty ──
  it('processBlock with empty events array returns valid-empty state', () => {
    const r = processBlock([]);
    assert.equal(r.quality.book_status, 'unseeded');
    assert.equal(r.quality.sequence_status, 'unsequenced');
    assert.equal(r.decisions.commit, true);
    assert.equal(r.decisions.cursor, 'advance');
    assert.deepEqual(r.state, {
      seeded: false, best_bid: null, best_bid_qty: null,
      best_ask: null, best_ask_qty: null, mid: null, last_seq: null,
    });
  });

  // ── 16. stateAt strict anchor boundary ──
  it('stateAt excludes events with event_ts_ms >= anchor', () => {
    const events = [
      snap({ event_ts_ms: 500, bids: [['100', '2']], asks: [['101', '3']] }),
      update({ event_ts_ms: 1000, seq: 11, prev_seq: 10, bids: [['102', '5']], asks: [['101', '0'], ['103', '2']] }),
    ];
    // anchor=500: excludes the snapshot itself (event_ts_ms=500 >= 500)
    const s500 = stateAt(events, 500);
    assert.equal(s500.seeded, false);
    assert.equal(s500.best_bid, null);

    // anchor=501: includes snapshot at 500, state seeded
    const s501 = stateAt(events, 501);
    assert.equal(s501.seeded, true);
    assert.equal(s501.best_bid, 100);

    // anchor=1000: includes snapshot but not update (update at 1000)
    const s1000 = stateAt(events, 1000);
    assert.equal(s1000.seeded, true);
    assert.equal(s1000.best_bid, 100);

    // anchor=1001: includes both
    const s1001 = stateAt(events, 1001);
    assert.equal(s1001.seeded, true);
    assert.equal(s1001.best_bid, 102);
  });

  // ── 17. Deterministic ordering ──
  it('ordered() sorts events deterministically per §13.2', () => {
    const events = [
      { type: 'update', event_ts_ms: 1000, seq: 12, prev_seq: 11, seq_start: 12, seq_end: 12, path: 'b.jsonl', line_no: 2 },
      { type: 'snapshot', event_ts_ms: 1000, seq: 11, prev_seq: null, path: 'a.jsonl', line_no: 1 },
      { type: 'update', event_ts_ms: 999, seq: 10, prev_seq: 9, path: 'a.jsonl', line_no: 4 },
    ];
    const sorted = ordered(events);
    assert.equal(sorted[0].event_ts_ms, 999);
    assert.equal(sorted[0].type, 'update');
    assert.equal(sorted[1].event_ts_ms, 1000);
    assert.equal(sorted[1].type, 'snapshot'); // snapshot before update
    assert.equal(sorted[2].type, 'update');
  });

  // ── 18. Gap after snapshot does not erase quarantine ──
  it('subsequent snapshot after gap does not clear quarantine', () => {
    const sm = new BookStateMachine();
    sm.apply(snap()); // seq=10, last_seq=10
    sm.apply(update({ seq: 12, prev_seq: 10, bids: [['102', '1']] })); // gap (12 !== 11)
    assert.equal(sm.gap_detected, true);
    assert.equal(sm.quarantined, true);

    // Another snapshot with new seed
    sm.apply(snap({
      event_ts_ms: 300, seq: 20, prev_seq: null,
      bids: [['200', '5']], asks: [['201', '5']],
    }));
    // gap_detected is still true
    assert.equal(sm.gap_detected, true);
    assert.equal(sm.quarantined, true);
    assert.equal(sm.book_status, 'quarantine');
  });

  // ── 19. stateAt with quarantined state machine ──
  it('stateAt returns quarantined flag when machine is in quarantine', () => {
    const events = [
      snap(),
      update({ seq: 12, prev_seq: 10 }), // gap
    ];
    const r = stateAt(events, 500);
    assert.equal(r.quarantined, true);
    assert.equal(r.state, null);
  });

  // ── 20. Full processBlock flow: valid snapshot+update ──
  it('processBlock processes valid snapshot+update and returns correct state', () => {
    const events = [
      snap(),
      update({ seq: 11, prev_seq: 10, bids: [['100', '0'], ['102', '5']], asks: [['101', '0'], ['103', '2']] }),
    ];
    const r = processBlock(events);
    assert.equal(r.decisions.commit, true);
    assert.equal(r.decisions.cursor, 'advance');
    assert.equal(r.decisions.quarantined, false);
    assert.equal(r.quality.book_status, 'seeded');
    assert.equal(r.quality.sequence_status, 'ok');
    assert.equal(r.state.best_bid, 102);
    assert.equal(r.state.best_ask, 103);
    assert.equal(r.state.mid, 102.5);
    assert.equal(r.state.last_seq, 11);
  });

  // ── 21. Crossed via processBlock ──
  it('processBlock returns null state and quarantine for crossed book', () => {
    const events = [snap({ bids: [['102', '1']], asks: [['101', '1']] })];
    const r = processBlock(events);
    assert.equal(r.state, null);
    assert.equal(r.decisions.quarantined, true);
    assert.equal(r.decisions.commit, false);
    assert.equal(r.decisions.error_code, 'CROSSED_BOOK');
  });

  // ── 22. Malformed via processBlock ──
  it('processBlock returns quarantine for malformed levels', () => {
    const events = [snap({ bids: [['100', '-1']] })];
    const r = processBlock(events);
    assert.equal(r.state, null);
    assert.equal(r.decisions.error_code, 'MALFORMED_LEVEL');
    assert.equal(r.decisions.quarantined, true);
    assert.equal(r.decisions.commit, false);
  });

  // ── 23. Stale via processBlock: commit=true, state unchanged ──
  it('processBlock with stale events: commit=true, state unchanged', () => {
    const events = [
      snap(),
      update({ seq: 11, prev_seq: 10, bids: [['102', '5']], asks: [['101', '0'], ['103', '2']] }),
      update({ seq: 10, prev_seq: null, bids: [['999', '9']] }), // stale
    ];
    const r = processBlock(events);
    assert.equal(r.decisions.commit, true);
    assert.equal(r.decisions.quarantined, false);
    assert.equal(r.quality.sequence_status, 'stale_duplicate');
    // State should reflect the valid events, not stale
    assert.equal(r.state.best_bid, 102);
    assert.equal(r.state.last_seq, 11);
  });

  // ── 24. snapshotState masks best fields when unseeded (P0-0 §7) ──
  it('snapshotState returns null bests when not seeded even if map has data', () => {
    const sm = new BookStateMachine();
    // Pre-snapshot update populates internal maps but seeded=false
    sm.apply(update({
      seq: null, prev_seq: null,
      bids: [['100', '1']], asks: [['101', '1']],
    }));
    assert.equal(sm.seeded, false);
    // Internal getters still return real values
    assert.equal(sm.bestBid(), 100);
    assert.equal(sm.bestAsk(), 101);
    // Public snapshot must mask
    const s = sm.snapshotState();
    assert.equal(s.best_bid, null);
    assert.equal(s.best_bid_qty, null);
    assert.equal(s.best_ask, null);
    assert.equal(s.best_ask_qty, null);
    assert.equal(s.mid, null);
    assert.equal(s.seeded, false);
  });

  // ── 25. processBlock pre-snapshot update returns null bests ──
  it('processBlock with pre-snapshot updates returns null bests in state', () => {
    const events = [
      update({ seq: null, prev_seq: null,
        bids: [['100', '1']], asks: [['101', '1']],
        event_ts_ms: 50, path: 'book_updates/test/1970-01-01/00-00-00.jsonl', line_no: 1,
      }),
    ];
    const r = processBlock(events);
    // Not quarantined but not seeded
    assert.equal(r.decisions.quarantined, false);
    assert.equal(r.decisions.commit, true);
    assert.equal(r.quality.book_status, 'unseeded');
    // State should have null bests
    assert.equal(r.state.seeded, false);
    assert.equal(r.state.best_bid, null);
    assert.equal(r.state.best_ask, null);
    assert.equal(r.state.mid, null);
  });

  // ── 26. Gap detected → stale must not overwrite sequence_status (§7.3) ──
  it('stale after gap preserves sequence_status=gap', () => {
    const sm = new BookStateMachine();
    sm.apply(snap()); // seq=10, last_seq=10
    // Gap: seq jumps from 10 to 12
    sm.apply(update({ seq: 12, prev_seq: 10, bids: [['102', '1']] }));
    assert.equal(sm.gap_detected, true);
    assert.equal(sm.sequence_status, 'gap');
    assert.equal(sm.last_seq, 10); // not updated
    // Stale event: seq=10 <= last_seq=10
    const r = sm.apply(update({ seq: 10, prev_seq: null, bids: [['999', '9']] }));
    assert.equal(r.reason, 'stale_duplicate');
    assert.equal(sm.stale_detected, true);
    // sequence_status must remain 'gap', not overwritten
    assert.equal(sm.sequence_status, 'gap');
    // gap_detected still true
    assert.equal(sm.gap_detected, true);
    // Quarantine decisions preserved
    assert.equal(sm.quarantined, true);
    assert.equal(sm.book_status, 'quarantine');
  });

  // ── 27. Gap detected via processBlock → stale preserves sequence_status ──
  it('processBlock: gap then stale preserves sequence_status=gap', () => {
    const events = [
      snap({ seq: 10, bids: [['100', '2']], asks: [['101', '3']] }),
      update({ seq: 12, prev_seq: 10, bids: [['102', '1']], event_ts_ms: 300, line_no: 2 }), // gap
      update({ seq: 10, prev_seq: null, bids: [['999', '9']], event_ts_ms: 400, line_no: 3 }), // stale after gap
    ];
    const r = processBlock(events);
    // Must be quarantined due to gap
    assert.equal(r.decisions.quarantined, true);
    assert.equal(r.decisions.commit, false);
    assert.equal(r.decisions.error_code, 'SEQUENCE_GAP');
    // sequence_status must be gap, not stale_duplicate
    assert.equal(r.quality.sequence_status, 'gap');
    assert.equal(r.state, null);
  });
});
