// test/bybit-topic-routing.test.mjs — Bybit depth topic routing
import { describe, it } from 'node:test';
import assert from 'node:assert';
import { BybitConnector } from '../lib/bybit-connector.mjs';
import { BybitSpotConnector } from '../lib/market-connectors.mjs';

describe('Bybit topic routing', () => {
  it('routes orderbook.200.BTCUSDT to _handleDepth for spot', () => {
    const conn = new BybitSpotConnector({});
    let depthCalled = false;
    conn._handleDepth = (data) => { depthCalled = true; };
    conn._handleTrade = (data) => { assert.fail('trade should not be called'); };
    conn._onMessage({ topic: 'orderbook.200.BTCUSDT', type: 'snapshot', data: { b: [], a: [] } });
    assert.ok(depthCalled, '_handleDepth should be called for orderbook.200');
  });

  it('routes orderbook.1000.BTCUSDT to _handleDepth for perp', () => {
    const conn = new BybitConnector({});
    let depthCalled = false;
    conn._handleDepth = (data) => { depthCalled = true; };
    conn._onMessage({ topic: 'orderbook.1000.BTCUSDT', type: 'snapshot', data: { b: [], a: [] } });
    assert.ok(depthCalled, '_handleDepth should be called for orderbook.1000');
  });

  it('routes publicTrade.BTCUSDT to _handleTrade (not depth)', () => {
    const conn = new BybitSpotConnector({});
    let depthCalled = false;
    let tradeCalled = false;
    conn._handleDepth = () => { depthCalled = true; };
    conn._handleTrade = () => { tradeCalled = true; };
    conn._onMessage({ topic: 'publicTrade.BTCUSDT', data: [] });
    assert.ok(!depthCalled, '_handleDepth should NOT be called for trade topic');
    assert.ok(tradeCalled, '_handleTrade should be called for trade topic');
  });
});
