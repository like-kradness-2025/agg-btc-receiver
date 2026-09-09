// test/active-book-sync.test.mjs — active L2 book sync invariants

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { BybitConnector } from '../lib/bybit-connector.mjs';
import { OkxConnector } from '../lib/okx-connector.mjs';
import { CoinbaseConnector } from '../lib/coinbase-connector.mjs';

function collectErrors(conn) {
  const errors = [];
  conn.on('error', (ev) => errors.push(ev));
  return errors;
}

function suppressReconnectTimer(conn) {
  conn._scheduleReconnect = () => { conn._setState('reconnecting'); };
}

describe('active book sync invariants', () => {
  it('Bybit buffers delta before snapshot even while connected', () => {
    const conn = new BybitConnector({});
    conn._setState('connected');
    conn._ringBuf = [];

    conn._handleDepth({
      type: 'delta',
      data: { seq: 11, ts: 1700000000011, b: [['65000', '1']], a: [] },
    });

    assert.strictEqual(conn._ringBuf.length, 1);
    assert.strictEqual(conn.book.bids.size, 0);
  });

  it('OKX buffers update before snapshot even while connected', () => {
    const conn = new OkxConnector({});
    conn._setState('connected');
    conn._ringBuf = [];

    conn._handleDepth({
      action: 'update',
      data: [{ seqId: 11, prevSeqId: 10, ts: '1700000000011', bids: [['65000', '1']], asks: [] }],
    });

    assert.strictEqual(conn._ringBuf.length, 1);
    assert.strictEqual(conn.book.bids.size, 0);
  });

  it('Coinbase buffers update before snapshot even while connected', () => {
    const conn = new CoinbaseConnector({});
    conn._setState('connected');
    conn._ringBuf = [];

    conn._handleDepth({
      channel: 'l2_data',
      sequence_num: 11,
      events: [{
        type: 'update',
        updates: [{ side: 'bid', price_level: '65000', new_quantity: '1' }],
      }],
    });

    assert.strictEqual(conn._ringBuf.length, 1);
    assert.strictEqual(conn.book.bids.size, 0);
  });

  it('Bybit rejects non-contiguous runtime delta instead of silently drifting', () => {
    const conn = new BybitConnector({});
    suppressReconnectTimer(conn);
    const errors = collectErrors(conn);
    conn._setState('running');
    conn._notifyWsSnapshotReceived(100);
    conn.book.applySnapshot([['64999', '1']], [['65001', '1']], 100);

    conn._handleDepth({
      type: 'delta',
      data: { u: 102, seq: 202, ts: 1700000000102, b: [['65000', '1']], a: [] },
    });

    assert.ok(errors.some((e) => String(e.message || '').includes('Bybit sequence gap')));
    assert.strictEqual(conn.book.bids.has('65000'), false);
  });

  it('OKX rejects update whose prevSeqId does not match local seq', () => {
    const conn = new OkxConnector({});
    suppressReconnectTimer(conn);
    const errors = collectErrors(conn);
    conn._setState('running');
    conn._notifyWsSnapshotReceived(100);
    conn.book.applySnapshot([['64999', '1']], [['65001', '1']], 100);

    conn._handleDepth({
      action: 'update',
      data: [{ seqId: 102, prevSeqId: 99, ts: '1700000000102', bids: [['65000', '1']], asks: [] }],
    });

    assert.ok(errors.some((e) => String(e.message || '').includes('OKX sequence gap')));
    assert.strictEqual(conn.book.bids.has('65000'), false);
  });

  it('Coinbase accepts a within-tolerance seq skip (server coalescing), then drops stale frames', () => {
    const conn = new CoinbaseConnector({});
    suppressReconnectTimer(conn);
    conn._setState('running');
    conn._notifyWsSnapshotReceived(100);
    conn.book.applySnapshot([['64999', '1']], [['65001', '1']], 100);

    // Issue #22: Coinbase coalesces book changes, so 102 after 100 (delta 2)
    // is NORMAL delivery and applies — no special bridge needed.
    conn._handleDepth({
      channel: 'l2_data',
      sequence_num: 102,
      events: [{
        type: 'update',
        updates: [{ side: 'bid', price_level: '65000', new_quantity: '1' }],
      }],
    });

    assert.strictEqual(conn.book.bids.get('65000'), '1');
    assert.strictEqual(conn.book._lastSeq, 102);

    // …and any later stale frame (seq <= localSeq) is still rejected.
    conn._handleDepth({
      channel: 'l2_data',
      sequence_num: 101,
      events: [{
        type: 'update',
        updates: [{ side: 'ask', price_level: '65002', new_quantity: '2' }],
      }],
    });

    assert.strictEqual(conn.book.asks.has('65002'), false);
    assert.strictEqual(conn.book._lastSeq, 102); // unchanged
    assert.strictEqual(conn._stats.l2DupDropCount, 1);
  });

  it('Coinbase rejects a steady-state seq gap beyond tolerance (fail-closed, book not polluted)', () => {
    const conn = new CoinbaseConnector({});
    suppressReconnectTimer(conn);
    const errors = collectErrors(conn);
    conn._setState('running');
    conn._notifyWsSnapshotReceived(100);
    conn.book.applySnapshot([['64999', '1']], [['65001', '1']], 100);

    // Within-tolerance skip (delta 2 = normal server coalescing) applies.
    conn._handleDepth({
      channel: 'l2_data',
      sequence_num: 102,
      events: [{ type: 'update', updates: [{ side: 'bid', price_level: '65000', new_quantity: '1' }] }],
    });
    assert.strictEqual(conn.book._lastSeq, 102);

    // Steady state: 200 after 102 skips 97 frames (> L2_SEQ_SKIP_TOLERANCE)
    // → real drop. The gapped frame must never be applied — book cleared,
    // resync forced.
    conn._handleDepth({
      channel: 'l2_data',
      sequence_num: 200,
      events: [{ type: 'update', updates: [{ side: 'ask', price_level: '65002', new_quantity: '2' }] }],
    });

    assert.ok(errors.some((e) => String(e.message || '').includes('coinbase l2 sequence gap: 102 -> 200')));
    assert.strictEqual(conn.book.asks.has('65002'), false); // gapped frame not applied
    assert.strictEqual(conn._stats.l2SeqGapCount, 1);
    assert.strictEqual(conn.getState(), 'reconnecting');
  });

  it('Coinbase rejects a seq jump beyond tolerance between ring-buffered replay frames (fail-closed)', () => {
    const conn = new CoinbaseConnector({});
    suppressReconnectTimer(conn);
    const errors = collectErrors(conn);
    conn._setState('syncing');
    conn._ringBuf = [];

    // Two update frames buffered before the snapshot; frames 102-199 are
    // missing between them (socket-level drop during sync) — a >TOL jump.
    conn._handleDepth({
      channel: 'l2_data',
      sequence_num: 101,
      events: [{ type: 'update', updates: [{ side: 'bid', price_level: '65000', new_quantity: '1' }] }],
    });
    conn._handleDepth({
      channel: 'l2_data',
      sequence_num: 200,
      events: [{ type: 'update', updates: [{ side: 'ask', price_level: '65002', new_quantity: '2' }] }],
    });
    assert.strictEqual(conn._ringBuf.length, 2);

    // Snapshot arrives → replay hits the 101→200 jump → fail-closed resync.
    conn._handleDepth({
      channel: 'l2_data',
      sequence_num: 100,
      events: [{
        type: 'snapshot',
        updates: [
          { side: 'bid', price_level: '64999', new_quantity: '1' },
          { side: 'ask', price_level: '65001', new_quantity: '1' },
        ],
      }],
    });

    assert.ok(errors.some((e) => String(e.message || '').includes('coinbase l2 replay sequence gap: 101 -> 200')));
    assert.strictEqual(conn.book.asks.has('65002'), false); // gapped frame never applied
    assert.strictEqual(conn._stats.l2SeqGapCount, 1);
    assert.strictEqual(conn.getState(), 'reconnecting');
  });

  it('Coinbase stays running across continuous within-tolerance skips (live incident shape, Qwen P2)', () => {
    const conn = new CoinbaseConnector({});
    suppressReconnectTimer(conn);
    const errors = collectErrors(conn);
    conn._setState('running');
    conn._notifyWsSnapshotReceived(100);
    conn.book.applySnapshot([['64999', '1']], [['65001', '1']], 100);

    // The live flapping shape: after the snapshot anchor, updates arrive with
    // coalesced skips (2, then 3, then 3) — all NORMAL delivery. Strict +1
    // resynced on every one of these; monotonic+tolerance must keep applying.
    conn._handleDepth({ channel: 'l2_data', sequence_num: 102, events: [{ type: 'update', updates: [{ side: 'bid', price_level: '65000', new_quantity: '1' }] }] });
    conn._handleDepth({ channel: 'l2_data', sequence_num: 105, events: [{ type: 'update', updates: [{ side: 'ask', price_level: '65002', new_quantity: '2' }] }] });
    conn._handleDepth({ channel: 'l2_data', sequence_num: 108, events: [{ type: 'update', updates: [{ side: 'bid', price_level: '65000', new_quantity: '0' }] }] });

    assert.strictEqual(errors.length, 0);
    assert.strictEqual(conn.getState(), 'running');
    assert.strictEqual(conn.book._lastSeq, 108);
    assert.strictEqual(conn.book.asks.get('65002'), '2'); // applied through skips
    assert.strictEqual(conn.book.bids.has('65000'), false); // last update removed it
    assert.strictEqual(conn._stats.l2SeqGapCount, 0);
    assert.strictEqual(conn._stats.l2TolSkipCount, 3);
    assert.strictEqual(conn._stats.l2MaxSeqSkipDelta, 3);
  });

  it('Coinbase replay applies within-tolerance skips between ring-buffered frames (Qwen P2)', () => {
    const conn = new CoinbaseConnector({});
    suppressReconnectTimer(conn);
    const errors = collectErrors(conn);
    conn._setState('syncing');
    conn._ringBuf = [];

    // Buffered 101 and 104 (delta 3 — normal coalesced delivery, same as live).
    conn._handleDepth({ channel: 'l2_data', sequence_num: 101, events: [{ type: 'update', updates: [{ side: 'bid', price_level: '65000', new_quantity: '1' }] }] });
    conn._handleDepth({ channel: 'l2_data', sequence_num: 104, events: [{ type: 'update', updates: [{ side: 'ask', price_level: '65002', new_quantity: '2' }] }] });
    assert.strictEqual(conn._ringBuf.length, 2);

    // Snapshot arrives → replay applies both (101→104 within tolerance).
    conn._handleDepth({
      channel: 'l2_data',
      sequence_num: 100,
      events: [{
        type: 'snapshot',
        updates: [
          { side: 'bid', price_level: '64999', new_quantity: '1' },
          { side: 'ask', price_level: '65001', new_quantity: '1' },
        ],
      }],
    });

    assert.strictEqual(errors.length, 0, 'within-tolerance replay must not resync');
    assert.strictEqual(conn.getState(), 'syncing', 'no fail-closed transition');
    assert.strictEqual(conn.book._lastSeq, 104);
    assert.strictEqual(conn.book.asks.get('65002'), '2'); // replayed through the skip
    assert.strictEqual(conn._stats.l2SeqGapCount, 0);
    assert.strictEqual(conn._stats.l2TolSkipCount, 1);
  });

  it('Coinbase awaiting _syncBook never finalizes to running when a replay gap beyond tolerance derails the sync (Astra audit #19 P1)', async () => {
    const conn = new CoinbaseConnector({});
    suppressReconnectTimer(conn);
    const errors = collectErrors(conn);
    conn._setState('connected');

    // _syncBook arms the WS-snapshot waiter (state → syncing). Updates 101
    // and 200 are fed AFTER it so they ride the ring buffer instead of being
    // cleared by _beginWsSnapshotSync. 101→200 is a >TOL drop (frames
    // 102-199 missing).
    const syncPromise = conn._syncBook();
    assert.strictEqual(conn.getState(), 'syncing');
    conn._handleDepth({
      channel: 'l2_data',
      sequence_num: 101,
      events: [{ type: 'update', updates: [{ side: 'bid', price_level: '65000', new_quantity: '1' }] }],
    });
    conn._handleDepth({
      channel: 'l2_data',
      sequence_num: 200,
      events: [{ type: 'update', updates: [{ side: 'ask', price_level: '65002', new_quantity: '2' }] }],
    });
    assert.strictEqual(conn._ringBuf.length, 2);

    // Snapshot 100 resolves the waiter; the synchronous replay then hits the
    // 101→200 jump and _handleSequenceGap clears the book, resets
    // _wsSnapshotReceived and moves to 'reconnecting' — all while _syncBook
    // is still awaiting the resolved waiter.
    conn._handleDepth({
      channel: 'l2_data',
      sequence_num: 100,
      events: [{
        type: 'snapshot',
        updates: [
          { side: 'bid', price_level: '64999', new_quantity: '1' },
          { side: 'ask', price_level: '65001', new_quantity: '1' },
        ],
      }],
    });

    await syncPromise;

    assert.ok(errors.some((e) => String(e.message || '').includes('coinbase l2 replay sequence gap: 101 -> 200')));
    assert.strictEqual(conn.getState(), 'reconnecting',
      'derailed sync must not flip the state back to running');
    assert.strictEqual(conn._wsSnapshotReceived, false);
    assert.strictEqual(conn.book.bids.size, 0, 'book stays empty until a real snapshot');
    assert.strictEqual(conn._stats.l2SeqGapCount, 1);
    assert.strictEqual(conn._stats.resyncCount, 0, 'derailed sync must not count as a resync');
  });

  it('Coinbase control: a clean snapshot sync still finalizes to running (Astra audit #19 P1)', async () => {
    const conn = new CoinbaseConnector({});
    suppressReconnectTimer(conn);
    conn._setState('connected');

    const syncPromise = conn._syncBook();
    assert.strictEqual(conn.getState(), 'syncing');
    // Buffered update rides the ring buffer; the snapshot then replays it.
    conn._handleDepth({
      channel: 'l2_data',
      sequence_num: 101,
      events: [{ type: 'update', updates: [{ side: 'bid', price_level: '65000', new_quantity: '1' }] }],
    });
    conn._handleDepth({
      channel: 'l2_data',
      sequence_num: 100,
      events: [{
        type: 'snapshot',
        updates: [
          { side: 'bid', price_level: '64999', new_quantity: '1' },
          { side: 'ask', price_level: '65001', new_quantity: '1' },
        ],
      }],
    });
    await syncPromise;

    assert.strictEqual(conn.getState(), 'running');
    assert.strictEqual(conn._wsSnapshotReceived, true);
    assert.strictEqual(conn._stats.resyncCount, 1);
    assert.strictEqual(conn.book.bids.get('65000'), '1'); // replayed diff applied
    assert.strictEqual(conn.book._lastSeq, 101);
  });
});
