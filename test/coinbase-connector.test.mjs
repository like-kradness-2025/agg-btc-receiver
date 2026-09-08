// test/coinbase-connector.test.mjs — Coinbase L2 sequence continuity and
// reconnect trade dedupe regression tests (Issue #14).
//
// Trade idempotency contract: market_trades events are 'snapshot' (recent
// trade window, re-delivered on every subscribe/reconnect) or 'update'
// (live). The connector's normalized trade stream must emit each
// (market, trade_id) at most once per process so reconnect snapshots never
// double-count downstream (CVD); raw/source frames are unaffected.

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { CoinbaseConnector } from '../lib/coinbase-connector.mjs';

function createConn() {
  const conn = new CoinbaseConnector({});
  conn._ws = { send: () => {} };
  conn._setState('running');
  return conn;
}

const tradeFrame = (eventType, trades) => ({
  channel: 'market_trades',
  events: [{ type: eventType, trades }],
});

const trade = (id, price = '65000', side = 'SELL') => ({
  trade_id: String(id),
  price,
  size: '0.1',
  side,
  time: '2026-06-05T00:00:00.000Z',
});

describe('CoinbaseConnector market_trades idempotency (Issue #14)', () => {
  it('propagates event.type (snapshot/update) onto emitted trades', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._handleTrade(tradeFrame('snapshot', [trade(100)]));
    conn._handleTrade(tradeFrame('update', [trade(101)]));

    assert.strictEqual(emitted.length, 2);
    assert.strictEqual(emitted[0].trade_event_type, 'snapshot');
    assert.strictEqual(emitted[1].trade_event_type, 'update');
    assert.strictEqual(emitted[0].tradeId, '100');
    assert.strictEqual(emitted[1].tradeId, '101');
  });

  it('suppresses a trade_id re-delivered by a reconnect snapshot (no CVD double count)', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    // Connection 1: live updates 1..3 emitted.
    for (const id of [1, 2, 3]) conn._handleTrade(tradeFrame('update', [trade(id)]));
    assert.strictEqual(emitted.length, 3);

    // Connection 2 (reconnect): snapshot re-sends the overlap window
    // (2,3) plus one trade that happened while disconnected (4).
    conn._handleTrade(tradeFrame('snapshot', [trade(2), trade(3), trade(4)]));
    assert.strictEqual(emitted.length, 4); // only 4 is new
    assert.deepStrictEqual(emitted.map((e) => e.tradeId), ['1', '2', '3', '4']);

    // Live update 5 after reconnect still flows.
    conn._handleTrade(tradeFrame('update', [trade(5)]));
    assert.strictEqual(emitted.length, 5);
    assert.strictEqual(conn._stats.dedupedTradeCount, 2);
  });

  it('dedupes order-agnostically (reconnect snapshot may list newest-first)', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    // First connection: snapshot lists trades newest-first 5,4,3 — all are
    // new, so all must be emitted despite the descending order.
    conn._handleTrade(tradeFrame('snapshot', [trade(5), trade(4), trade(3)]));
    assert.strictEqual(emitted.length, 3);

    // Reconnect snapshot re-lists 4,3 (descending overlap) → suppressed.
    conn._handleTrade(tradeFrame('snapshot', [trade(4), trade(3)]));
    assert.strictEqual(emitted.length, 3);
    assert.strictEqual(conn._stats.dedupedTradeCount, 2);
  });

  it('keeps emitted trade metadata intact (tradeId / ingress fields on both paths)', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._handleTrade(tradeFrame('snapshot', [trade(7)]));
    conn._handleTrade(tradeFrame('update', [trade(7), trade(8)])); // 7 dup, 8 new

    assert.strictEqual(emitted.length, 2);
    assert.strictEqual(emitted[0].tradeId, '7');
    assert.strictEqual(emitted[1].tradeId, '8');
    for (const ev of emitted) {
      assert.strictEqual(ev.market, 'coinbase_spot');
      assert.strictEqual(typeof ev.price, 'number');
      assert.ok(Number.isFinite(ev.ts)); // parsed exchange time, never Date.now() fallback
      assert.strictEqual(ev.source_event_time_known, true);
    }
  });
});
