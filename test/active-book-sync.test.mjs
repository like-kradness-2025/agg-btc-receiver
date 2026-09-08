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

  it('Coinbase accepts a seq jump on the FIRST post-snapshot update (bridge), then drops stale frames', () => {
    const conn = new CoinbaseConnector({});
    suppressReconnectTimer(conn);
    conn._setState('running');
    conn._notifyWsSnapshotReceived(100);
    conn.book.applySnapshot([['64999', '1']], [['65001', '1']], 100);

    // Issue #14: the snapshot→first-update bridge is the one frame whose seq
    // jump is unprovable (the update stream can already be ahead of the
    // snapshot frame's seq), so 102 after 100 is accepted once…
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
    assert.strictEqual(conn._l2BridgePending, false);

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

  it('Coinbase rejects a steady-state seq gap (fail-closed, book not polluted)', () => {
    const conn = new CoinbaseConnector({});
    suppressReconnectTimer(conn);
    const errors = collectErrors(conn);
    conn._setState('running');
    conn._notifyWsSnapshotReceived(100);
    conn.book.applySnapshot([['64999', '1']], [['65001', '1']], 100);

    // Bridge frame first (any strictly-newer seq accepted once).
    conn._handleDepth({
      channel: 'l2_data',
      sequence_num: 102,
      events: [{ type: 'update', updates: [{ side: 'bid', price_level: '65000', new_quantity: '1' }] }],
    });
    assert.strictEqual(conn.book._lastSeq, 102);

    // Steady state: 104 after 102 means frame 103 was dropped. The gapped
    // frame must never be applied — book is cleared and resync is forced.
    conn._handleDepth({
      channel: 'l2_data',
      sequence_num: 104,
      events: [{ type: 'update', updates: [{ side: 'ask', price_level: '65002', new_quantity: '2' }] }],
    });

    assert.ok(errors.some((e) => String(e.message || '').includes('coinbase l2 sequence gap: 102 -> 104')));
    assert.strictEqual(conn.book.asks.has('65002'), false); // gapped frame not applied
    assert.strictEqual(conn._stats.l2SeqGapCount, 1);
    assert.strictEqual(conn.getState(), 'reconnecting');
  });

  it('Coinbase rejects a seq jump between ring-buffered replay frames (fail-closed)', () => {
    const conn = new CoinbaseConnector({});
    suppressReconnectTimer(conn);
    const errors = collectErrors(conn);
    conn._setState('syncing');
    conn._ringBuf = [];

    // Two update frames buffered before the snapshot; frame 103 is missing
    // between them (socket-level drop during sync).
    conn._handleDepth({
      channel: 'l2_data',
      sequence_num: 101,
      events: [{ type: 'update', updates: [{ side: 'bid', price_level: '65000', new_quantity: '1' }] }],
    });
    conn._handleDepth({
      channel: 'l2_data',
      sequence_num: 103,
      events: [{ type: 'update', updates: [{ side: 'ask', price_level: '65002', new_quantity: '2' }] }],
    });
    assert.strictEqual(conn._ringBuf.length, 2);

    // Snapshot arrives → replay hits the 101→103 jump → fail-closed resync.
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

    assert.ok(errors.some((e) => String(e.message || '').includes('coinbase l2 replay sequence gap: 101 -> 103')));
    assert.strictEqual(conn.book.asks.has('65002'), false); // gapped frame never applied
    assert.strictEqual(conn._stats.l2SeqGapCount, 1);
    assert.strictEqual(conn.getState(), 'reconnecting');
  });
});
