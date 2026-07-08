import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { replayBestBookState } from '../lib/replay-book-state.mjs';

describe('replayBestBookState', () => {
  it('returns null on empty events', () => {
    const bookAtTime = replayBestBookState([]);
    const state = bookAtTime(1000);
    assert.equal(state.bestBid, null);
    assert.equal(state.bestAsk, null);
    assert.equal(state.seeded, false);
  });

  it('returns state for a snapshot event and computes best bid/ask from side maps', () => {
    const events = [
      {
        effective_ts_ms: 500,
        subtype: 'book_update_snapshot',
        file_path: 'a.jsonl',
        line_no: 1,
        data: {
          type: 'snapshot',
          bids: [['50000', '1.5'], ['49900', '2.0']],
          asks: [['50010', '2.0'], ['50020', '1.0']],
        },
      },
    ];
    const bookAtTime = replayBestBookState(events);
    // ts > event.ts should see the state
    const state = bookAtTime(1000);
    assert.equal(state.bestBid, 50000);  // max bid price
    assert.equal(state.bestAsk, 50010);  // min ask price
    assert.equal(state.seeded, true);
    assert.equal(state.bestBidQty, 1.5);
    assert.equal(state.bestAskQty, 2.0);
  });

  it('does not include event at exact ts (half-open: < ts)', () => {
    const events = [
      {
        effective_ts_ms: 1000,
        subtype: 'book_update_snapshot',
        file_path: 'a.jsonl',
        line_no: 1,
        data: {
          type: 'snapshot',
          bids: [['50000', '1.0']],
          asks: [['50010', '1.0']],
        },
      },
    ];
    const bookAtTime = replayBestBookState(events);
    const state = bookAtTime(1000);
    // event.ts === 1000, should NOT be included
    assert.equal(state.bestBid, null);
    assert.equal(state.seeded, false);
    // ts=1001 should include it
    const state2 = bookAtTime(1001);
    assert.equal(state2.bestBid, 50000);
    assert.equal(state2.seeded, true);
  });

  it('forward-fills state between events', () => {
    const events = [
      {
        effective_ts_ms: 100,
        subtype: 'book_update_snapshot',
        file_path: 'a.jsonl',
        line_no: 1,
        data: {
          type: 'snapshot',
          bids: [['100', '1.0']],
          asks: [['101', '1.0']],
        },
      },
      {
        effective_ts_ms: 300,
        subtype: 'book_update_update',
        file_path: 'a.jsonl',
        line_no: 2,
        data: {
          type: 'update',
          bids: [['102', '1.0']],  // new best bid
          asks: [['103', '1.0']],  // new best ask
        },
      },
    ];
    const bookAtTime = replayBestBookState(events);
    // ts=200 → still sees first event
    assert.equal(bookAtTime(200).bestBid, 100);
    // ts=400 → sees second event (best bid updated to 102)
    assert.equal(bookAtTime(400).bestBid, 102);
  });

  it('incremental diff: qty>0 sets level, qty<=0 deletes level', () => {
    const events = [
      {
        effective_ts_ms: 100,
        subtype: 'book_update_snapshot',
        file_path: 'a.jsonl',
        line_no: 1,
        data: {
          type: 'snapshot',
          bids: [['50000', '1.0'], ['49900', '1.0']],
          asks: [['50100', '1.0'], ['50200', '1.0']],
        },
      },
      {
        effective_ts_ms: 200,
        subtype: 'book_update_update',
        file_path: 'a.jsonl',
        line_no: 2,
        data: {
          type: 'update',
          bids: [['50000', '0']],   // delete best bid
          asks: [['50100', '0']],   // delete best ask
        },
      },
    ];
    const bookAtTime = replayBestBookState(events);
    // ts=150 → sees snapshot
    assert.equal(bookAtTime(150).bestBid, 50000);
    assert.equal(bookAtTime(150).bestAsk, 50100);
    // ts=250 → best bid/ask deleted, next level becomes best
    assert.equal(bookAtTime(250).bestBid, 49900);
    assert.equal(bookAtTime(250).bestAsk, 50200);
  });

  it('not seeded: returns null when no snapshot has been applied', () => {
    const events = [
      {
        effective_ts_ms: 100,
        subtype: 'book_update_update',
        file_path: 'a.jsonl',
        line_no: 1,
        data: {
          type: 'update',
          bids: [['50000', '1.0']],
          asks: [['50100', '1.0']],
        },
      },
    ];
    const bookAtTime = replayBestBookState(events);
    const state = bookAtTime(150);
    assert.equal(state.bestBid, null);
    assert.equal(state.bestAsk, null);
    assert.equal(state.seeded, false);
  });
});
