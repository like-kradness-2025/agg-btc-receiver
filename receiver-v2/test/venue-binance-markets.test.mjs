import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  createBinanceSpotAdapter,
  createBinanceFuturesAdapter,
  createBinanceCoinMFuturesAdapter,
} from '../src/ingest/venues/binance-spot.mjs';
import { adapterFor, knownVenues } from '../src/entry/config.mjs';

const level = [['100', '1']];
const spotDepth = (symbol = 'BTCUSDC', stream = `${symbol.toLowerCase()}@depth@100ms`, extra = {}) => ({
  stream,
  data: { e: 'depthUpdate', E: 1, s: symbol, U: 11, u: 11, b: level, a: [], ...extra },
});
const futuresDepth = (extra = {}) => ({
  stream: 'btcusdt@depth@100ms',
  data: { e: 'depthUpdate', E: 1, T: 1, s: 'BTCUSDT', U: 11, u: 11, pu: 10, b: level, a: [], ...extra },
});

test('spot adapters are parameterized for BTCUSDT, BTCUSDC and BTCFDUSD with official URLs and limit', () => {
  for (const [market, symbol] of [['binance_spot', 'BTCUSDT'], ['binance_spot_usdc', 'BTCUSDC'], ['binance_spot_fdusd', 'BTCFDUSD']]) {
    const adapter = createBinanceSpotAdapter({ market, symbol });
    const lower = symbol.toLowerCase();
    assert.equal(adapter.symbol, symbol);
    assert.match(adapter.url, new RegExp(`streams=.*${lower}@trade.*${lower}@depth@100ms`));
    assert.equal(adapter.restUrl, `https://api.binance.com/api/v3/depth?symbol=${symbol}&limit=5000`);
    assert.deepEqual(adapter.parse(JSON.stringify({ stream: `${lower}@trade`, data: { e: 'trade', s: symbol, t: 1, p: '1', q: '1' } })), { kind: 'data', trade: true });
    assert.deepEqual(adapter.parse(JSON.stringify(spotDepth(symbol))), { kind: 'data', depth: true });
  }
});

test('spot adapters fail closed for unknown symbols and streams', () => {
  assert.throws(() => createBinanceSpotAdapter({ symbol: 'ETHUSDT' }), /unsupported|only supports/i);
  const adapter = createBinanceSpotAdapter({ symbol: 'BTCUSDC' });
  assert.equal(adapter.parse(JSON.stringify(spotDepth('BTCUSDT'))), null);
  assert.equal(adapter.parse(JSON.stringify({ stream: 'btcusdc@unknown', data: spotDepth('BTCUSDC').data })), null);
});

test('futures adapter uses USD-M public endpoint, @trade and pu continuity', () => {
  const adapter = createBinanceFuturesAdapter({ market: 'binance_perp', symbol: 'BTCUSDT' });
  assert.equal(adapter.url, 'wss://fstream.binance.com/stream?streams=btcusdt@trade/btcusdt@depth@100ms/btcusdt@forceOrder', 'one socket carries every stream, as v1 does');
  assert.equal(adapter.tradeUrl, undefined, 'no separate trade socket: the trade rides the one socket');
  assert.equal(adapter.restUrl, 'https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000');
  assert.deepEqual(adapter.parse(JSON.stringify({ stream: 'btcusdt@trade', data: { e: 'trade', s: 'BTCUSDT', t: 1, p: '1', q: '1', T: 1, m: true } })), { kind: 'data', trade: true });
  assert.deepEqual(adapter.parse(JSON.stringify(futuresDepth())), { kind: 'data', depth: true });
  adapter.syncSnapshot({ lastUpdateId: 10, bids: [], asks: [] });
  assert.equal(adapter.acceptDepthEvent(futuresDepth({ U: 9, u: 11 }).data).status, 'applied');
  assert.equal(adapter.acceptDepthEvent(futuresDepth({ U: 12, u: 12, pu: 10 }).data).status, 'resync');
  adapter.syncSnapshot({ lastUpdateId: 11, bids: [], asks: [] });
  assert.equal(adapter.acceptDepthEvent(futuresDepth({ U: 11, u: 1002, pu: 10 }).data).status, 'applied');
  assert.equal(adapter.acceptDepthEvent(futuresDepth({ U: 1000, u: 1002, pu: 1002 }).data).status, 'applied');
});

test('futures rejects spot-style depth and unknown stream fail-closed', () => {
  const adapter = createBinanceFuturesAdapter({ market: 'binance_perp', symbol: 'BTCUSDT' });
  assert.equal(adapter.parse(JSON.stringify({ stream: 'btcusdt@depth', data: { e: 'depthUpdate', s: 'BTCUSDT', U: 1, u: 1, b: [], a: [] } })), null);
  assert.equal(adapter.parse(JSON.stringify({ stream: 'ethusdt@depth', data: futuresDepth().data })), null);
});

test('USD-M BTCUSDC is isolated from BTCUSDT by symbol, streams and REST endpoint', () => {
  const adapter = createBinanceFuturesAdapter({ market: 'binance_perp_btcusdc', symbol: 'BTCUSDC' });
  assert.equal(adapter.symbol, 'BTCUSDC');
  assert.equal(adapter.url, 'wss://fstream.binance.com/stream?streams=btcusdc@trade/btcusdc@depth@100ms/btcusdc@forceOrder');
  assert.equal(adapter.tradeUrl, undefined);
  assert.equal(adapter.restUrl, 'https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDC&limit=1000');
  assert.deepEqual(adapter.parse(JSON.stringify({ stream: 'btcusdc@trade', data: { e: 'trade', s: 'BTCUSDC', t: 1, p: '1', q: '2', T: 1 } })), { kind: 'data', trade: true });
  assert.equal(adapter.parse(JSON.stringify({ stream: 'btcusdt@aggTrade', data: { e: 'aggTrade', s: 'BTCUSDT', a: 1, p: '1', q: '2', T: 1 } })), null);
});

test('COIN-M BTCUSD_PERP uses dstream/dapi and preserves contract quantities', () => {
  const adapter = createBinanceCoinMFuturesAdapter({ market: 'binance_coinm_perp' });
  assert.equal(adapter.symbol, 'BTCUSD_PERP');
  assert.equal(adapter.quantityUnit, 'contracts');
  assert.equal(adapter.url, 'wss://dstream.binance.com/stream?streams=btcusd_perp@aggTrade/btcusd_perp@depth@100ms/btcusd_perp@forceOrder');
  assert.equal(adapter.restUrl, 'https://dapi.binance.com/dapi/v1/depth?symbol=BTCUSD_PERP&limit=1000');
  const event = { stream: 'btcusd_perp@depth@100ms', data: { e: 'depthUpdate', s: 'BTCUSD_PERP', U: 9, u: 11, pu: 10, b: [['100', '7']], a: [] } };
  assert.deepEqual(adapter.parse(JSON.stringify(event)), { kind: 'data', depth: true });
  adapter.syncSnapshot({ lastUpdateId: 10, bids: [], asks: [] });
  assert.equal(adapter.acceptDepthEvent(event.data).status, 'applied');
  assert.deepEqual(adapter.changesFor({ raw: Buffer.from(JSON.stringify(event)) }), { replace: true, levels: [{ side: 'bid', price: 100, size: 7 }] });
});

test('COIN-M rejects USD-M payloads, wrong symbols and malformed force-order frames', () => {
  const adapter = createBinanceCoinMFuturesAdapter({ market: 'binance_coinm_perp' });
  assert.equal(adapter.parse(JSON.stringify({ stream: 'btcusd_perp@depth@100ms', data: { e: 'depthUpdate', s: 'BTCUSD_PERP', U: 1, u: 1, b: [], a: [] } })), null);
  assert.equal(adapter.parse(JSON.stringify({ stream: 'btcusdt@depth@100ms', data: { e: 'depthUpdate', s: 'BTCUSDT', U: 1, u: 1, pu: 0, b: [], a: [] } })), null);
  assert.equal(adapter.parse(JSON.stringify({ stream: 'btcusd_perp@forceOrder', data: { e: 'forceOrder', o: { s: 'ETHUSD_PERP' } } })), null);
});

test('configuration registers BTCUSDC USD-M while keeping COIN-M disabled', () => {
  assert.deepEqual(knownVenues().filter((v) => v.startsWith('binance')), ['binance_spot', 'binance_spot_usdc', 'binance_spot_fdusd', 'binance_perp', 'binance_perp_btcusdc']);
  for (const [venue, symbol] of [['binance_spot', 'BTCUSDT'], ['binance_spot_usdc', 'BTCUSDC'], ['binance_spot_fdusd', 'BTCFDUSD'], ['binance_perp', 'BTCUSDT'], ['binance_perp_btcusdc', 'BTCUSDC']]) {
    const adapter = adapterFor({ venue, market: venue, symbol, stream: 'trades' });
    assert.equal(adapter.market, venue);
  }
  assert.throws(() => adapterFor({ venue: 'binance_coinm_perp', market: 'binance_coinm_perp', symbol: 'BTCUSD_PERP', stream: 'trades' }), /not supported|disabled/i);
});
