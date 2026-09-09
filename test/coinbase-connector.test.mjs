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

  it('refreshes recency of a duplicate trade_id so window overflow cannot re-emit it (Astra audit #19 P1)', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    // Fill the recency window to its cap (IDs 1..8192).
    conn._handleTrade(tradeFrame('update', Array.from({ length: 8192 }, (_, i) => trade(i + 1))));
    assert.strictEqual(emitted.length, 8192);
    assert.strictEqual(conn._tradeIdWindow.size, 8192);

    // Reconnect overlap re-delivers the OLDEST id (1): it must be dropped AND
    // moved to the recency tail so the next overflow cannot evict it.
    conn._handleTrade(tradeFrame('snapshot', [trade(1)]));
    assert.strictEqual(emitted.length, 8192);
    assert.strictEqual(conn._stats.dedupedTradeCount, 1);

    // A new trade overflows the window: eviction must take the true oldest
    // (2), never the just-refreshed duplicate (1).
    conn._handleTrade(tradeFrame('update', [trade(8193)]));
    assert.strictEqual(emitted.length, 8193);
    assert.strictEqual(conn._tradeIdWindow.has('1'), true);

    // The reconnect overlap re-delivers id 1 once more → still deduped.
    // Pre-fix: the duplicate branch's early continue left id 1 at the head,
    // the 8193 overflow evicted it, and this re-delivery was emitted as new.
    conn._handleTrade(tradeFrame('snapshot', [trade(1)]));
    assert.strictEqual(emitted.length, 8193);
    assert.strictEqual(conn._stats.dedupedTradeCount, 2);

    const ids = emitted.map((e) => e.tradeId);
    assert.strictEqual(new Set(ids).size, ids.length, 'no trade_id may be emitted twice');
    assert.strictEqual(ids.filter((id) => id === '1').length, 1);
  });
});

describe('CoinbaseConnector _l2Continuity (Issue #14 / Astra audit #19 P2-4)', () => {
  it('anchors the first sequenced frame when no seq anchor exists (known limitation)', () => {
    const conn = createConn();
    // WS snapshots always carry sequence_num, so the WS path always leaves an
    // anchor; localSeq == null is only reachable after the REST-fallback
    // snapshot (seq deliberately null: REST/WS sequence domains differ), where
    // the first sequenced WS frame must become the anchor — continuity cannot
    // be proven against the REST domain, so it acts like the bridge
    // (documented known limitation, audit #19 P2-4).
    assert.strictEqual(conn._l2Continuity(42, null), 'ok');
    // A frame without sequence_num still fails closed regardless of anchor.
    assert.strictEqual(conn._l2Continuity(null, null), 'unverifiable');
    assert.strictEqual(conn._l2Continuity(null, 41), 'unverifiable');
  });

  it('is strict after an anchor exists: exact +1 only, dups dropped, jumps gap', () => {
    const conn = createConn();
    conn._l2BridgePending = false; // steady state
    assert.strictEqual(conn._l2Continuity(42, 41), 'ok');
    assert.strictEqual(conn._l2Continuity(41, 41), 'dup');
    assert.strictEqual(conn._l2Continuity(40, 41), 'dup');
    assert.strictEqual(conn._l2Continuity(44, 41), 'gap');
  });
});
