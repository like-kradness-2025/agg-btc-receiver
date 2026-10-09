/**
 * Set 7b: the per-venue raw record shapes (`rawEventFor`) for trades and book frames.
 *
 * The running v1 store is the specification here: every payload asserted below was measured
 * 2026-10-10 from `~/Tool/agg-btc-receiver/data/sqlite/<market>.sqlite` (gunzip `raw_gzip`, parse one
 * envelope line) and traced back to the v1 connector's emit call. The tests fix the v1 SHAPE - the
 * exact key set (`market` included, no key v1 never wrote) and representative values - so a change
 * that silently diverges from the downstream's expectation fails here.
 *
 * `rawEventFor` may return one record, an array of records (a frame carrying several trades, or a
 * snapshot written to both `book_updates` and `snapshots`), or null (a frame that is not a raw
 * record).
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';

import { createBinanceSpotAdapter, createBinanceFuturesAdapter } from '../src/ingest/venues/binance-spot.mjs';
import { createBybitPerpAdapter, createBybitSpotAdapter } from '../src/ingest/venues/bybit.mjs';
import { createOkxPerpAdapter, createOkxSpotAdapter } from '../src/ingest/venues/okx.mjs';
import { createCoinbaseAdapter } from '../src/ingest/venues/coinbase.mjs';
import { createKrakenAdapter } from '../src/ingest/venues/kraken.mjs';
import { createHyperliquidAdapter } from '../src/ingest/venues/hyperliquid.mjs';
import { createBitfinexAdapter } from '../src/ingest/venues/bitfinex.mjs';
import { createBitstampAdapter } from '../src/ingest/venues/bitstamp.mjs';

const AT = 1_792_000_000_000;
const frame = (obj, atMs = AT) => ({ raw: JSON.stringify(obj), atMs });

const keysOf = (payload) => Object.keys(payload).sort();

const TRADE_KEYS = ['market', 'price', 'qty', 'side', 'tradeId', 'ts'];
const LIQ_KEYS = ['exchange', 'market', 'notional', 'price', 'qty', 'raw_type', 'side', 'source_ts', 'symbol', 'trade_id', 'ts'];

/** Every record of a `rawEventFor` result, as an array. */
const records = (result) => (result === null || result === undefined ? [] : Array.isArray(result) ? result : [result]);

// ---------------------------------------------------------------------------------------------------
// Binance Spot / USDⓈ-M Futures (measured: payload.market + price/qty/side/ts/tradeId)
// ---------------------------------------------------------------------------------------------------

test('binance_spot: a @trade frame is a v1 trades record', () => {
  const adapter = createBinanceSpotAdapter({ market: 'binance_spot' });
  const record = adapter.rawEventFor(frame({
    stream: 'btcusdt@trade',
    data: { e: 'trade', s: 'BTCUSDT', p: '100.5', q: '2', t: 5, T: 1_792_000_000_123, m: true },
  }));
  assert.equal(record.stream, 'trades');
  assert.equal(record.event_ts_ms, 1_792_000_000_123);
  assert.equal(record.source_event_ts_ms, 1_792_000_000_123);
  assert.equal(record.source_event_time_known, true);
  assert.equal(record.source_id, '5');
  assert.deepEqual(keysOf(record.payload), TRADE_KEYS);
  assert.deepEqual(record.payload, {
    market: 'binance_spot', price: 100.5, qty: 2, side: 'sell', ts: 1_792_000_000_123, tradeId: '5',
  });
});

test('binance_perp: an @aggTrade frame is a v1 trades record', () => {
  const adapter = createBinanceFuturesAdapter({ market: 'binance_perp' });
  const record = adapter.rawEventFor(frame({
    stream: 'btcusdt@aggTrade',
    data: { e: 'aggTrade', s: 'BTCUSDT', a: 7, p: '101', q: '3', T: 1_792_000_000_500, m: false },
  }));
  assert.equal(record.stream, 'trades');
  assert.deepEqual(keysOf(record.payload), TRADE_KEYS);
  assert.deepEqual(record.payload, {
    market: 'binance_perp', price: 101, qty: 3, side: 'buy', ts: 1_792_000_000_500, tradeId: '7',
  });
});

// ---------------------------------------------------------------------------------------------------
// Bybit (book: {prev_seq,type,seq} update / {snapshot_origin,type,seq} snapshot; trades; liquidations)
// ---------------------------------------------------------------------------------------------------

test('bybit_spot: trade, snapshot (double-written) and delta match the v1 shape', () => {
  const adapter = createBybitSpotAdapter({ market: 'bybit_spot' });

  const trade = adapter.rawEventFor(frame({
    topic: 'publicTrade.BTCUSDT',
    type: 'snapshot',
    data: [{ s: 'BTCUSDT', S: 'Buy', p: '100', v: '1.5', T: 1_792_000_000_111, i: 'trade-1' }],
  }));
  assert.deepEqual(keysOf(trade.payload), TRADE_KEYS);
  assert.deepEqual(trade.payload, { market: 'bybit_spot', price: 100, qty: 1.5, side: 'buy', ts: 1_792_000_000_111, tradeId: 'trade-1' });

  const snapshotResult = adapter.rawEventFor(frame({
    topic: 'orderbook.200.BTCUSDT',
    type: 'snapshot',
    ts: 1_792_000_000_200,
    data: { s: 'BTCUSDT', u: 5, seq: 5, b: [['100', '1']], a: [['101', '2']] },
  }));
  const snapshot = records(snapshotResult);
  assert.deepEqual(snapshot.map((r) => r.stream), ['book_updates', 'snapshots'], 'a snapshot is written to both streams');
  for (const record of snapshot) {
    assert.deepEqual(keysOf(record.payload), ['asks', 'bids', 'market', 'seq', 'snapshot_origin', 'ts', 'type']);
    assert.deepEqual(record.payload, {
      market: 'bybit_spot', snapshot_origin: 'ws_sync', type: 'snapshot', bids: [['100', '1']], asks: [['101', '2']], ts: 1_792_000_000_200, seq: 5,
    });
    assert.equal(record.source_event_ts_ms, 1_792_000_000_200);
    assert.equal(record.source_event_time_known, true);
  }

  // The delta carries prev_seq from the update id the last proven frame held.
  adapter.acceptDepthEvent(JSON.stringify({ topic: 'orderbook.200.BTCUSDT', type: 'snapshot', ts: 1_792_000_000_200, data: { s: 'BTCUSDT', u: 5, seq: 5, b: [['100', '1']], a: [['101', '2']] } }));
  const delta = adapter.rawEventFor(frame({
    topic: 'orderbook.200.BTCUSDT',
    type: 'delta',
    ts: 1_792_000_000_300,
    data: { s: 'BTCUSDT', u: 6, seq: 6, b: [['100', '0']], a: [['102', '1']] },
  }));
  assert.deepEqual(keysOf(delta.payload), ['asks', 'bids', 'market', 'prev_seq', 'seq', 'ts', 'type']);
  assert.deepEqual(delta.payload, {
    market: 'bybit_spot', prev_seq: 5, type: 'update', bids: [['100', '0']], asks: [['102', '1']], ts: 1_792_000_000_300, seq: 6,
  });
});

test('bybit_perp: an allLiquidation frame is a v1 liquidations record', () => {
  const adapter = createBybitPerpAdapter({ market: 'bybit_perp' });
  const recordsOut = records(adapter.rawEventFor(frame({
    topic: 'allLiquidation.BTCUSDT',
    type: 'snapshot',
    data: [{ s: 'BTCUSDT', S: 'Buy', p: '81958.4', v: '0.208', T: 1_791_575_496_697 }],
  })));
  assert.equal(recordsOut.length, 1);
  const record = recordsOut[0];
  assert.equal(record.stream, 'liquidations');
  assert.equal(record.source_event_ts_ms, 1_791_575_496_697);
  assert.equal(record.source_event_time_known, true);
  assert.deepEqual(keysOf(record.payload), LIQ_KEYS);
  assert.deepEqual(record.payload, {
    market: 'bybit_perp', exchange: 'bybit', symbol: 'BTCUSDT', side: 'buy', price: 81958.4, qty: 0.208,
    notional: 81958.4 * 0.208, raw_type: 'liquidation', trade_id: null, source_ts: 1_791_575_496_697, ts: AT,
  });
});

// ---------------------------------------------------------------------------------------------------
// OKX (book: {prev_seq,type,seq} update / {snapshot_origin,type,seq} snapshot; trades; liquidations)
// ---------------------------------------------------------------------------------------------------

test('okx_spot: trade and book update/snapshot match the v1 shape', () => {
  const adapter = createOkxSpotAdapter({ market: 'okx_spot' });

  const trade = adapter.rawEventFor(frame({
    arg: { channel: 'trades', instId: 'BTC-USDT' },
    data: [{ instId: 'BTC-USDT', side: 'sell', px: '82405.5', sz: '0.5', ts: '1792000000123', tradeId: '1068763411' }],
  }));
  assert.deepEqual(keysOf(trade.payload), TRADE_KEYS);
  assert.deepEqual(trade.payload, { market: 'okx_spot', price: 82405.5, qty: 0.5, side: 'sell', ts: 1_792_000_000_123, tradeId: '1068763411' });

  const snapshotResult = records(adapter.rawEventFor(frame({
    arg: { channel: 'books', instId: 'BTC-USDT' },
    action: 'snapshot',
    data: [{ seqId: 9, prevSeqId: -1, ts: '1792000000200', bids: [['82393.3', '0.88']], asks: [['82405.5', '1']] }],
  })));
  assert.deepEqual(snapshotResult.map((r) => r.stream), ['book_updates', 'snapshots']);
  assert.deepEqual(keysOf(snapshotResult[0].payload), ['asks', 'bids', 'market', 'seq', 'snapshot_origin', 'ts', 'type']);
  assert.deepEqual(snapshotResult[0].payload, {
    market: 'okx_spot', snapshot_origin: 'ws_sync', type: 'snapshot', bids: [['82393.3', '0.88']], asks: [['82405.5', '1']], ts: 1_792_000_000_200, seq: 9,
  });

  adapter.acceptDepthEvent(JSON.stringify({ arg: { channel: 'books', instId: 'BTC-USDT' }, action: 'snapshot', data: [{ seqId: 9, prevSeqId: -1, ts: '1', bids: [], asks: [] }] }));
  const update = adapter.rawEventFor(frame({
    arg: { channel: 'books', instId: 'BTC-USDT' },
    action: 'update',
    data: [{ seqId: 12, prevSeqId: 9, ts: '1792000000300', bids: [['82399.5', '0']], asks: [] }],
  }));
  assert.deepEqual(keysOf(update.payload), ['asks', 'bids', 'market', 'prev_seq', 'seq', 'ts', 'type']);
  assert.deepEqual(update.payload, {
    market: 'okx_spot', prev_seq: 9, type: 'update', bids: [['82399.5', '0']], asks: [], ts: 1_792_000_000_300, seq: 12,
  });
});

test('okx_perp: trade qty is in coin (sz * contract value) and a liquidation is recorded', () => {
  const adapter = createOkxPerpAdapter({ market: 'okx_perp' });

  const trade = adapter.rawEventFor(frame({
    arg: { channel: 'trades', instId: 'BTC-USDT-SWAP' },
    data: [{ instId: 'BTC-USDT-SWAP', side: 'buy', px: '82353.4', sz: '0.14', ts: '1792000000123', tradeId: '2985883826' }],
  }));
  assert.deepEqual(trade.payload, { market: 'okx_perp', price: 82353.4, qty: 0.0014000000000000002, side: 'buy', ts: 1_792_000_000_123, tradeId: '2985883826' });

  const liq = records(adapter.rawEventFor(frame({
    arg: { channel: 'liquidation-orders', instType: 'SWAP' },
    data: [{ instId: 'BTC-USDT-SWAP', details: [{ side: 'sell', sz: '2', bkPx: '80000', ts: '1792000000400' }] }],
  })));
  assert.equal(liq.length, 1);
  assert.equal(liq[0].stream, 'liquidations');
  assert.deepEqual(keysOf(liq[0].payload), LIQ_KEYS);
  assert.deepEqual(liq[0].payload, {
    market: 'okx_perp', exchange: 'okx', symbol: 'BTC-USDT-SWAP', side: 'sell', price: 80000, qty: 0.02,
    notional: 80000 * 0.02, raw_type: 'liquidation-orders', trade_id: null, source_ts: 1_792_000_000_400, ts: AT,
  });
});

// ---------------------------------------------------------------------------------------------------
// Coinbase (book: {event_time_source,prev_seq,type,seq} update; trades; trade_event_type)
// ---------------------------------------------------------------------------------------------------

test('coinbase_spot: trade carries trade_event_type and the book carries event_time_source', () => {
  const adapter = createCoinbaseAdapter({ market: 'coinbase_spot' });

  const trade = adapter.rawEventFor(frame({
    channel: 'market_trades',
    events: [{ type: 'update', trades: [{ product_id: 'BTC-USD', trade_id: '1104374878', price: '82306.83', size: '0.5', time: '2026-10-09T00:00:00.000Z', side: 'SELL' }] }],
  }));
  assert.deepEqual(keysOf(trade.payload), ['market', 'price', 'qty', 'side', 'tradeId', 'trade_event_type', 'ts']);
  assert.equal(trade.payload.trade_event_type, 'update');
  assert.equal(trade.payload.tradeId, '1104374878');
  assert.equal(trade.payload.ts, Date.parse('2026-10-09T00:00:00.000Z'));

  const snapshotResult = records(adapter.rawEventFor(frame({
    channel: 'l2_data',
    sequence_num: 100,
    events: [{ type: 'snapshot', product_id: 'BTC-USD', updates: [{ side: 'bid', price_level: '82300.56', new_quantity: '1', event_time: 'x' }] }],
  })));
  assert.deepEqual(snapshotResult.map((r) => r.stream), ['book_updates', 'snapshots']);
  assert.deepEqual(keysOf(snapshotResult[0].payload), ['asks', 'bids', 'event_time_source', 'market', 'seq', 'snapshot_origin', 'ts', 'type']);
  assert.deepEqual(snapshotResult[0].payload, {
    market: 'coinbase_spot', snapshot_origin: 'ws_sync', event_time_source: 'local', type: 'snapshot',
    bids: [['82300.56', '1']], asks: [], ts: AT, seq: 100,
  });
  assert.equal(snapshotResult[0].source_event_ts_ms, null);
  assert.equal(snapshotResult[0].source_event_time_known, false);

  adapter.acceptDepthEvent(JSON.stringify({ channel: 'l2_data', sequence_num: 100, events: [{ type: 'snapshot', product_id: 'BTC-USD', updates: [{ side: 'bid', price_level: '1', new_quantity: '1', event_time: 'x' }] }] }));
  const update = adapter.rawEventFor(frame({
    channel: 'l2_data',
    sequence_num: 101,
    events: [{ type: 'update', product_id: 'BTC-USD', updates: [{ side: 'offer', price_level: '82315.97', new_quantity: '0', event_time: 'x' }] }],
  }));
  assert.deepEqual(keysOf(update.payload), ['asks', 'bids', 'event_time_source', 'market', 'prev_seq', 'seq', 'ts', 'type']);
  assert.deepEqual(update.payload, {
    market: 'coinbase_spot', prev_seq: 100, event_time_source: 'local', type: 'update', bids: [], asks: [['82315.97', '0']], ts: AT, seq: 101,
  });
});

// ---------------------------------------------------------------------------------------------------
// Kraken (book: checksum/sequence_mode/event_time_source, no seq; trades)
// ---------------------------------------------------------------------------------------------------

test('kraken_spot: a book frame is v1 checksum shape (no seq) and a trade is a v1 record', () => {
  const adapter = createKrakenAdapter({ market: 'kraken_spot', symbol: 'BTC/USD' });

  const update = adapter.rawEventFor(frame({
    channel: 'book',
    type: 'update',
    data: [{ symbol: 'BTC/USD', bids: [], asks: [{ price: '82584.60000', qty: '0.00000000' }], checksum: 806804027 }],
  }));
  assert.deepEqual(keysOf(update.payload), ['asks', 'bids', 'checksum', 'event_time_source', 'market', 'sequence_mode', 'ts', 'type']);
  assert.deepEqual(update.payload, {
    market: 'kraken_spot', checksum: 806804027, sequence_mode: 'checksum', event_time_source: 'local', type: 'update',
    bids: [], asks: [['82584.60000', '0.00000000']], ts: AT,
  });
  assert.equal(update.source_event_ts_ms, null);
  assert.equal(update.source_event_time_known, false);

  const snapshotResult = records(adapter.rawEventFor(frame({
    channel: 'book',
    type: 'snapshot',
    data: [{ symbol: 'BTC/USD', bids: [{ price: '82326.6', qty: '0.9' }], asks: [{ price: '82330', qty: '1' }], checksum: 1996557688 }],
  })));
  assert.deepEqual(snapshotResult.map((r) => r.stream), ['book_updates', 'snapshots']);
  assert.equal(snapshotResult[0].payload.type, 'snapshot');
  assert.equal(snapshotResult[0].payload.checksum, 1996557688);

  const trade = adapter.rawEventFor(frame({
    channel: 'trade',
    type: 'update',
    data: [{ symbol: 'BTC/USD', price: '82313.4', qty: '0.03240875', side: 'buy', ord_type: 'market', trade_id: '1791578172.882000-82313.40000-0.03240875-m-', timestamp: '2026-10-09T00:00:00.000Z' }],
  }));
  assert.deepEqual(keysOf(trade.payload), TRADE_KEYS);
  assert.deepEqual(trade.payload, {
    market: 'kraken_spot', price: 82313.4, qty: 0.03240875, side: 'buy', ts: Date.parse('2026-10-09T00:00:00.000Z'),
    tradeId: '1791578172.882000-82313.40000-0.03240875-m-',
  });
});

// ---------------------------------------------------------------------------------------------------
// Hyperliquid (book: full-replace l2Book -> ws_sync snapshot once, then changed-level updates; trades)
// ---------------------------------------------------------------------------------------------------

test('hyperliquid_perp: the first l2Book is a ws_sync snapshot, the next is a changed-level update', () => {
  const adapter = createHyperliquidAdapter({ market: 'hyperliquid_perp' });

  const snapshotResult = records(adapter.rawEventFor(frame({
    channel: 'l2Book',
    data: { coin: 'BTC', time: 1_792_000_000_200, levels: [[{ px: '100.0', sz: '2' }], [{ px: '101.0', sz: '3' }]] },
  })));
  const snapshot = snapshotResult.filter((r) => r.payload.type === 'snapshot');
  assert.deepEqual(snapshot.map((r) => r.stream), ['book_updates', 'snapshots']);
  assert.deepEqual(keysOf(snapshot[0].payload), ['asks', 'bids', 'market', 'seq', 'snapshot_origin', 'ts', 'type']);
  assert.deepEqual(snapshot[0].payload, {
    market: 'hyperliquid_perp', snapshot_origin: 'ws_sync', type: 'snapshot', bids: [['100.0', '2']], asks: [['101.0', '3']], ts: 1_792_000_000_200, seq: null,
  });

  // The second l2Book: only the changed levels, v1's exact `type:'update'` with no seq and no meta.
  const update = adapter.rawEventFor(frame({
    channel: 'l2Book',
    data: { coin: 'BTC', time: 1_792_000_000_300, levels: [[{ px: '100.0', sz: '5' }], [{ px: '101.0', sz: '3' }]] },
  }));
  assert.deepEqual(keysOf(update.payload), ['asks', 'bids', 'market', 'ts', 'type']);
  assert.deepEqual(update.payload, { market: 'hyperliquid_perp', type: 'update', bids: [['100.0', '5']], asks: [], ts: 1_792_000_000_300 });

  const trade = adapter.rawEventFor(frame({
    channel: 'trades',
    data: [{ coin: 'BTC', side: 'B', px: '82350', sz: '0.00015', time: 1_792_000_000_400, tid: 729364064289350 }],
  }));
  assert.deepEqual(keysOf(trade.payload), TRADE_KEYS);
  assert.deepEqual(trade.payload, {
    market: 'hyperliquid_perp', price: 82350, qty: 0.00015, side: 'buy', ts: 1_792_000_000_400, tradeId: '729364064289350',
  });
});

// ---------------------------------------------------------------------------------------------------
// Bitfinex (book only: {event_time_source,seq,type}; trades are not subscribed in v2)
// ---------------------------------------------------------------------------------------------------

test('bitfinex_spot: a book snapshot/update is the v1 {event_time_source,seq,type} shape', () => {
  const adapter = createBitfinexAdapter({ market: 'bitfinex_spot' });
  adapter.parse(JSON.stringify({ event: 'subscribed', channel: 'book', chanId: 111, symbol: 'tBTCUSD' }));

  const snapshotResult = records(adapter.rawEventFor(frame([111, [['82417', '3', '0.00175969'], ['82430', '3', '-0.00022']]])));
  assert.deepEqual(snapshotResult.map((r) => r.stream), ['book_updates', 'snapshots']);
  assert.deepEqual(keysOf(snapshotResult[0].payload), ['asks', 'bids', 'event_time_source', 'market', 'seq', 'ts', 'type']);
  assert.deepEqual(snapshotResult[0].payload, {
    market: 'bitfinex_spot', event_time_source: 'local', type: 'snapshot', bids: [['82417', '0.00175969']], asks: [['82430', '0.00022']], ts: AT, seq: null,
  });

  const update = records(adapter.rawEventFor(frame([111, ['82417', '3', '0.5']])))[0];
  assert.deepEqual(keysOf(update.payload), ['asks', 'bids', 'event_time_source', 'market', 'seq', 'ts', 'type']);
  assert.deepEqual(update.payload, {
    market: 'bitfinex_spot', event_time_source: 'local', type: 'update', bids: [['82417', '0.5']], asks: [], ts: AT, seq: null,
  });
  assert.equal(update.source_event_ts_ms, null);
  assert.equal(update.source_event_time_known, false);
});

// ---------------------------------------------------------------------------------------------------
// Bitstamp (book diff: {seq,type}; trades; REST snapshot via the sink -> rest_sync)
// ---------------------------------------------------------------------------------------------------

test('bitstamp_spot: a diff frame is a v1 update and the REST snapshot reaches the sink', () => {
  const adapter = createBitstampAdapter({ market: 'bitstamp_spot' });

  const update = adapter.rawEventFor(frame({
    event: 'data',
    channel: 'diff_order_book_btcusd',
    data: { microtimestamp: '1792000000123000', bids: [['82310.09', '0.00607472']], asks: [['82318.43', '0.25000000']] },
  }));
  assert.deepEqual(keysOf(update.payload), ['asks', 'bids', 'market', 'seq', 'ts', 'type']);
  assert.deepEqual(update.payload, {
    market: 'bitstamp_spot', type: 'update', bids: [['82310.09', '0.00607472']], asks: [['82318.43', '0.25000000']], ts: 1_792_000_000_123, seq: null,
  });

  const trade = adapter.rawEventFor(frame({
    event: 'trade',
    channel: 'live_trades_btcusd',
    data: { price: '82321.12', amount: '0.00380371', microtimestamp: '1792000000123000', id: 652244811, type: 1 },
  }));
  assert.deepEqual(keysOf(trade.payload), TRADE_KEYS);
  assert.deepEqual(trade.payload, {
    market: 'bitstamp_spot', price: 82321.12, qty: 0.00380371, side: 'sell', ts: 1_792_000_000_123, tradeId: '652244811',
  });

  const seen = [];
  adapter.setRawSnapshotSink((snap) => seen.push(snap));
  adapter.syncSnapshot({ microtimestamp: '1792000000900000', bids: [['82300', '1']], asks: [['82400', '2']] });
  assert.equal(seen.length, 1);
  assert.deepEqual(keysOf(seen[0].payload), ['asks', 'bids', 'event_time_source', 'market', 'seq', 'snapshot_asof_ts_ms', 'snapshot_origin', 'ts', 'type']);
  assert.equal(seen[0].payload.snapshot_origin, 'rest_sync');
  assert.equal(seen[0].payload.event_time_source, 'rest_snapshot_source');
  assert.equal(seen[0].payload.snapshot_asof_ts_ms, 1_792_000_000_900);
  assert.equal(seen[0].source_event_ts_ms, 1_792_000_000_900);
  assert.equal(seen[0].source_event_time_known, true);
});

// ---------------------------------------------------------------------------------------------------
// The remaining Binance spot products share the same shape (the 14 markets list)
// ---------------------------------------------------------------------------------------------------

for (const [market, symbol, lower, upper] of [['binance_spot_usdc', 'BTCUSDC', 'btcusdc', 'BTCUSDC'], ['binance_spot_fdusd', 'BTCFDUSD', 'btcfdusd', 'BTCFDUSD']]) {
  test(`${market}: the trade shape is identical to binance_spot`, () => {
    const adapter = createBinanceSpotAdapter({ market, symbol });
    const record = adapter.rawEventFor(frame({
      stream: `${lower}@trade`,
      data: { e: 'trade', s: upper, p: '82', q: '1', t: 9, T: 1_792_000_000_900, m: false },
    }));
    assert.deepEqual(keysOf(record.payload), TRADE_KEYS);
    assert.equal(record.payload.market, market);
    assert.equal(record.payload.tradeId, '9');
  });
}

test('binance_perp_btcusdc: an aggTrade frame is a v1 trades record', () => {
  const adapter = createBinanceFuturesAdapter({ market: 'binance_perp_btcusdc', symbol: 'BTCUSDC' });
  const record = adapter.rawEventFor(frame({
    stream: 'btcusdc@aggTrade',
    data: { e: 'aggTrade', s: 'BTCUSDC', a: 11, p: '82296.9', q: '0.003', T: 1_792_000_000_700, m: false },
  }));
  assert.deepEqual(keysOf(record.payload), TRADE_KEYS);
  assert.deepEqual(record.payload, {
    market: 'binance_perp_btcusdc', price: 82296.9, qty: 0.003, side: 'buy', ts: 1_792_000_000_700, tradeId: '11',
  });
});
