/**
 * Set 8: the per-venue catalog additions (liquidation, tickers / OI / mark / funding, Bitfinex
 * trades) and the auxiliary open-interest poller.
 *
 * The open-interest payload asserted here is v1's measured `oi_v1` row, reproduced field for field by
 * `src/ingest/oi.mjs` (measured 2026-10-10 from `~/Tool/agg-btc-receiver/data/sqlite/<market>.sqlite`,
 * keys `{ts, source_ts, market, mark_price, funding_rate, open_interest, next_funding_time, source,
 * schema, instrument_type, status, as_of_ms, age_ms, native_unit, oi_native, oi_btc, oi_usd,
 * price_usd, error_code, error_message}`). The liquidation/trade shapes are the ones Set 7 fixed.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { gunzipSync } from 'node:zlib';
import { DatabaseSync } from 'node:sqlite';

import { createBinanceFuturesAdapter } from '../src/ingest/venues/binance-spot.mjs';
import { createBybitPerpAdapter } from '../src/ingest/venues/bybit.mjs';
import { createOkxPerpAdapter } from '../src/ingest/venues/okx.mjs';
import { createHyperliquidAdapter } from '../src/ingest/venues/hyperliquid.mjs';
import { createBitfinexAdapter } from '../src/ingest/venues/bitfinex.mjs';
import { createAuxCollector, REST_OI_SOURCES } from '../src/ingest/aux.mjs';
import { openInterestRecord } from '../src/ingest/oi.mjs';
import { openIngestProcess } from '../src/ingest/main.mjs';
import { startFakeOrganize } from '../test-support/fake-organize.mjs';

const AT = 1_792_000_000_000;
const frame = (obj, atMs = AT) => ({ raw: JSON.stringify(obj), atMs });
const keysOf = (payload) => Object.keys(payload).sort();
const records = (result) => (result === null || result === undefined ? [] : Array.isArray(result) ? result : [result]);

const LIQ_KEYS = ['exchange', 'market', 'notional', 'price', 'qty', 'raw_type', 'side', 'source_ts', 'symbol', 'trade_id', 'ts'];
const TRADE_KEYS = ['market', 'price', 'qty', 'side', 'tradeId', 'ts'];
const OI_KEYS = [
  'age_ms', 'as_of_ms', 'error_code', 'error_message', 'funding_rate', 'instrument_type', 'mark_price',
  'market', 'native_unit', 'next_funding_time', 'oi_btc', 'oi_native', 'oi_usd', 'open_interest',
  'price_usd', 'schema', 'source', 'source_ts', 'status', 'ts',
].sort();

// ---------------------------------------------------------------------------------------------------
// Binance USDⓈ-M `@forceOrder` (a liquidation; v1's store held zero rows because of its `o.f` gate)
// ---------------------------------------------------------------------------------------------------

test('binance_perp: an @forceOrder frame is a v1-shaped liquidation record', () => {
  const adapter = createBinanceFuturesAdapter({ market: 'binance_perp' });
  const f = {
    stream: 'btcusdt@forceOrder',
    data: {
      e: 'forceOrder',
      E: 1_792_000_000_100,
      o: { s: 'BTCUSDT', S: 'SELL', T: 1_792_000_000_500, p: '80000', q: '0.5', z: '0.3', X: 'FILLED', f: 'GTC' },
    },
  };
  // The v1 bug: its emit was gated on `o.f === 'LIQUIDATION'`, which the live `f` (a time-in-force)
  // never is. This receiver writes the official fields straight through - the record comes out even
  // though `f` is 'GTC'.
  assert.deepEqual(adapter.parse(JSON.stringify(f)), { kind: 'data', auxiliary: true, liquidation: true });
  const record = adapter.rawEventFor(frame(f));
  assert.equal(record.stream, 'liquidations');
  assert.equal(record.source_event_ts_ms, 1_792_000_000_500);
  assert.equal(record.source_event_time_known, true);
  assert.deepEqual(keysOf(record.payload), LIQ_KEYS);
  assert.deepEqual(record.payload, {
    market: 'binance_perp', exchange: 'binance', symbol: 'BTCUSDT', side: 'sell', price: 80000, qty: 0.3,
    notional: 24000, raw_type: 'forceOrder', trade_id: null, source_ts: 1_792_000_000_500, ts: AT,
  });
  // A BUY forceOrder is a liquidation of a short position; side follows `o.S`.
  const buy = adapter.rawEventFor(frame({ ...f, data: { ...f.data, o: { ...f.data.o, S: 'BUY' } } }));
  assert.equal(buy.payload.side, 'buy');
});

// ---------------------------------------------------------------------------------------------------
// Bybit `tickers` (OI + funding + mark in one frame)
// ---------------------------------------------------------------------------------------------------

test('bybit_perp: a tickers frame is a v1 open_interest record', () => {
  const adapter = createBybitPerpAdapter({ market: 'bybit_perp' });
  const TSB = 1_791_579_682_810;
  const f = {
    topic: 'tickers.BTCUSDT',
    type: 'snapshot',
    ts: TSB,
    data: {
      symbol: 'BTCUSDT', openInterest: '55891.704', markPrice: '82471.62',
      fundingRate: '-0.00000499', nextFundingTime: 1_791_590_400_000,
    },
  };
  assert.deepEqual(adapter.parse(JSON.stringify(f)), { kind: 'data', auxiliary: true });
  const record = adapter.rawEventFor(frame(f, TSB));
  assert.equal(record.stream, 'open_interest');
  assert.equal(record.source_id, 'bybit');
  assert.deepEqual(keysOf(record.payload), OI_KEYS);
  assert.deepEqual(record.payload, {
    ts: TSB, source_ts: TSB, market: 'bybit_perp', mark_price: 82471.62,
    funding_rate: -0.00000499, open_interest: 55891.704, next_funding_time: 1_791_590_400_000,
    source: 'bybit', schema: 'oi_v1', instrument_type: 'perp', status: 'fresh',
    as_of_ms: TSB, age_ms: 0, native_unit: 'BTC',
    oi_native: 55891.704, oi_btc: 55891.704, oi_usd: 55891.704 * 82471.62,
    price_usd: 82471.62, error_code: null, error_message: null,
  });
});

// ---------------------------------------------------------------------------------------------------
// OKX `open-interest` + `funding-rate` + `mark-price` (three channels, one row)
// ---------------------------------------------------------------------------------------------------

test('okx_perp: the OI/mark/funding channels coalesce into one v1 open_interest record', () => {
  const adapter = createOkxPerpAdapter({ market: 'okx_perp' });
  const inst = 'BTC-USDT-SWAP';
  // The mark and funding arrive on their own channels first; they are not records of their own.
  assert.equal(adapter.rawEventFor(frame({ arg: { channel: 'funding-rate', instId: inst }, data: [{ instId: inst, fundingRate: '0.0001', fundingTime: '1791590400000' }] })), null);
  assert.equal(adapter.rawEventFor(frame({ arg: { channel: 'mark-price', instId: inst }, data: [{ instId: inst, markPx: '82470', ts: '1792000000000' }] })), null);

  const f = {
    arg: { channel: 'open-interest', instId: inst },
    data: [{ instId: inst, oi: '2994564.84', oiCcy: '100', oiUsd: '8247000', ts: '1792000000000' }],
  };
  assert.deepEqual(adapter.parse(JSON.stringify(f)), { kind: 'data', auxiliary: true });
  const record = adapter.rawEventFor(frame(f));
  assert.equal(record.stream, 'open_interest');
  assert.deepEqual(keysOf(record.payload), OI_KEYS);
  assert.equal(record.payload.market, 'okx_perp');
  assert.equal(record.payload.native_unit, 'contract');
  assert.equal(record.payload.open_interest, 2994564.84);
  assert.equal(record.payload.oi_btc, 100);
  assert.equal(record.payload.oi_usd, 8247000);
  assert.equal(record.payload.mark_price, 82470);
  assert.equal(record.payload.funding_rate, 0.0001);
  assert.equal(record.payload.source, 'okx');
  assert.equal(record.payload.status, 'fresh');
});

// ---------------------------------------------------------------------------------------------------
// Hyperliquid `activeAssetCtx` (OI + funding + mark in one frame)
// ---------------------------------------------------------------------------------------------------

test('hyperliquid_perp: an activeAssetCtx frame is a v1 open_interest record', () => {
  const adapter = createHyperliquidAdapter({ market: 'hyperliquid_perp' });
  const f = { channel: 'activeAssetCtx', data: { coin: 'BTC', ctx: { funding: '0.0000045838', openInterest: '38455.70468', markPx: '82472', oraclePx: '82480' } } };
  assert.deepEqual(adapter.parse(JSON.stringify(f)), { kind: 'data', auxiliary: true });
  const record = adapter.rawEventFor(frame(f));
  assert.equal(record.stream, 'open_interest');
  assert.deepEqual(keysOf(record.payload), OI_KEYS);
  assert.equal(record.payload.market, 'hyperliquid_perp');
  assert.equal(record.payload.open_interest, 38455.70468);
  assert.equal(record.payload.oi_btc, 38455.70468);
  assert.equal(record.payload.mark_price, 82472);
  assert.equal(record.payload.funding_rate, 0.0000045838);
  assert.equal(record.payload.next_funding_time, null);
  assert.equal(record.payload.native_unit, 'BTC');
  assert.equal(record.payload.source, 'hyperliquid');
  assert.equal(record.payload.status, 'fresh');
});

// ---------------------------------------------------------------------------------------------------
// Bitfinex `trades` (v1 parity: keep `tu`, drop `te`)
// ---------------------------------------------------------------------------------------------------

test('bitfinex_spot: a confirmed trade is a v1 trades record, an immediate one is dropped', () => {
  const adapter = createBitfinexAdapter({ market: 'bitfinex_spot', symbol: 'tBTCUSD' });
  adapter.parse(JSON.stringify({ event: 'subscribed', channel: 'trades', symbol: 'tBTCUSD', chanId: 7 }));

  // A `te` is still a sequenced connection message, so it is classified as data; it just never
  // becomes a record.
  assert.deepEqual(adapter.parse(JSON.stringify([7, 'te', [1_991_319_423, 1_791_580_088_681, 0.00259902, 82615]])), { kind: 'data', trade: true }, 'the immediate, revisable trade is classified as data');
  assert.equal(records(adapter.rawEventFor(frame([7, 'te', [1_991_319_423, 1_791_580_088_681, 0.00259902, 82615]]))).length, 0, 'the immediate, revisable trade is not recorded');
  assert.deepEqual(adapter.parse(JSON.stringify([7, 'tu', [1_991_319_423, 1_791_580_088_681, 0.00259902, 82615]])), { kind: 'data', trade: true });

  const record = records(adapter.rawEventFor(frame([7, 'tu', [1_991_319_423, 1_791_580_088_681, 0.00259902, 82615]])))[0];
  assert.equal(record.stream, 'trades');
  assert.deepEqual(keysOf(record.payload), TRADE_KEYS);
  assert.deepEqual(record.payload, {
    market: 'bitfinex_spot', price: 82615, qty: 0.00259902, side: 'buy', ts: 1_791_580_088_681, tradeId: '1991319423',
  });

  // A negative amount is a sell, and its magnitude is the quantity.
  const sell = records(adapter.rawEventFor(frame([7, 'tu', [1, 1_791_580_082_534, -0.00018168, 82605]])))[0];
  assert.equal(sell.payload.side, 'sell');
  assert.equal(sell.payload.qty, 0.00018168);
});

// ---------------------------------------------------------------------------------------------------
// The REST poller: a 30 s clock, one v1 row per tick, and a loud stop on failure
// ---------------------------------------------------------------------------------------------------

test('the auxiliary poller ticks on the configured interval and writes one v1 open_interest row', async () => {
  const samples = {
    'premiumIndex?symbol=BTCUSDT': { markPrice: '82475.3', lastFundingRate: '0.00006609', nextFundingTime: 1_791_590_400_000, time: AT },
    'openInterest?symbol=BTCUSDT': { openInterest: '92446.667', time: AT },
  };
  const fetchImpl = async (url) => {
    const key = Object.keys(samples).find((k) => url.includes(k));
    if (!key) throw new Error(`unexpected url ${url}`);
    return { ok: true, status: 200, json: async () => samples[key] };
  };
  const timers = [];
  const setTimer = (cb, ms) => { timers.push({ cb, ms }); return { unref() {} }; };
  const appended = [];
  const collector = createAuxCollector({
    market: 'binance_perp',
    fetchImpl,
    nowMs: () => AT,
    setTimer,
    clearTimer: () => {},
    append: (record) => appended.push(record),
  });
  assert.equal(collector.start(), true);
  assert.equal(timers.length, 1);
  assert.equal(timers[0].ms, 30_000, 'v1 polled on a 30 second clock');

  await collector.tick();
  assert.equal(appended.length, 1);
  const record = appended[0];
  assert.equal(record.stream, 'open_interest');
  assert.deepEqual(keysOf(record.payload), OI_KEYS);
  assert.equal(record.payload.market, 'binance_perp');
  assert.equal(record.payload.open_interest, 92446.667);
  assert.equal(record.payload.mark_price, 82475.3);
  assert.equal(record.payload.funding_rate, 0.00006609);
  assert.equal(record.payload.native_unit, 'BTC');
  assert.equal(record.payload.source, 'binance');
  assert.equal(record.payload.status, 'fresh');
  collector.close();
});

test('the auxiliary poller reports a fetch failure loudly instead of skipping the tick', async () => {
  const errors = [];
  const collector = createAuxCollector({
    market: 'binance_perp',
    fetchImpl: async () => { throw new Error('network down'); },
    nowMs: () => AT,
    setTimer: () => ({ unref() {} }),
    clearTimer: () => {},
    append: () => { throw new Error('must not be reached'); },
    onError: (error) => errors.push(error),
  });
  await collector.tick();
  assert.equal(errors.length, 1);
  assert.match(errors[0].message, /network down/);
  collector.close();
});

test('every REST-backed market has a sample builder; a market without one is refused', () => {
  for (const market of ['binance_perp', 'binance_perp_btcusdc', 'bybit_perp', 'okx_perp', 'hyperliquid_perp']) {
    assert.equal(typeof REST_OI_SOURCES[market], 'function', `${market} has a REST source`);
  }
  assert.throws(() => createAuxCollector({ market: 'binance_spot', append: () => {} }), /no REST open-interest source/);
});

// ---------------------------------------------------------------------------------------------------
// The real path: an OI frame on the socket becomes an open_interest row in the canonical raw
// ---------------------------------------------------------------------------------------------------

function fakeSockets() {
  const sockets = [];
  const impl = function fakeSocket(url) {
    const socket = { url, sent: [], closed: false, onopen: null, onmessage: null, onclose: null, onerror: null, send(m) { socket.sent.push(m); }, close() { socket.closed = true; }, deliver(data) { socket.onmessage?.({ data }); } };
    sockets.push(socket);
    return socket;
  };
  return { sockets, impl };
}

async function until(predicate, { timeoutMs = 4000, stepMs = 5 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, stepMs));
  }
  throw new Error('timed out');
}

function readBatches(dir, market) {
  let db;
  try {
    db = new DatabaseSync(join(dir, 'raw', `${market}.sqlite`), { readOnly: true });
  } catch {
    return [];
  }
  try {
    return db.prepare('SELECT * FROM raw_batches ORDER BY batch_id').all();
  } finally {
    db.close();
  }
}

test('a Bybit tickers frame on the real receive path is recorded under open_interest', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'raw-aux-'));
  const organize = await startFakeOrganize(join(dir, 'organize.sock'), { batchFrames: 1 });
  const { sockets, impl } = fakeSockets();
  const process = await openIngestProcess({
    tailSaveMs: 0,
    market: 'bybit_perp',
    stream: 'trades',
    adapter: createBybitPerpAdapter({ market: 'bybit_perp' }),
    venue: 'bybit',
    runId: 'run-aux',
    webSocketImpl: impl,
    organizeSocketPath: organize.server.path,
    ingestStorePath: join(dir, 'ingest.sqlite'),
    spoolDir: join(dir, 'spool'),
    channelOptions: { batchFrames: 1 },
    rawDir: join(dir, 'raw'),
    oiPollIntervalMs: 0,
    rawBatchWindowMs: 3_600_000,
    onStop: () => {},
    onDiagnostic: () => {},
    onGap: () => {},
  });
  try {
    process.start();
    await until(() => sockets.length === 1);
    sockets[0].onopen();
    sockets[0].deliver(JSON.stringify({
      topic: 'tickers.BTCUSDT', type: 'snapshot', ts: 1_791_579_682_810,
      data: { symbol: 'BTCUSDT', openInterest: '55891.704', markPrice: '82471.62', fundingRate: '-0.00000499', nextFundingTime: 1_791_590_400_000 },
    }));
    process.stop();
    process.close();

    const rows = readBatches(dir, 'bybit_perp');
    assert.equal(rows.length, 1);
    assert.equal(rows[0].stream, 'open_interest');
    assert.equal(rows[0].market, 'bybit_perp');
    const line = gunzipSync(Buffer.from(rows[0].raw_gzip)).toString('utf8').replace(/\n$/, '');
    const envelope = JSON.parse(line);
    assert.equal(envelope.stream, 'open_interest');
    assert.equal(envelope.source_id, 'bybit');
    assert.equal(envelope.payload.market, 'bybit_perp');
    assert.equal(envelope.payload.open_interest, 55891.704);
    assert.equal(envelope.payload.schema, 'oi_v1');
  } finally {
    try { process.close(); } catch { /* already closed */ }
    await organize.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the REST poller writes an open_interest row through the ingest process', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'raw-aux-rest-'));
  const organize = await startFakeOrganize(join(dir, 'organize.sock'), { batchFrames: 1 });
  const { impl } = fakeSockets();
  const timers = [];
  const fetchImpl = async (url) => {
    if (url.includes('premiumIndex')) return { ok: true, status: 200, json: async () => ({ markPrice: '100', lastFundingRate: '0.001', nextFundingTime: 1, time: 1000 }) };
    return { ok: true, status: 200, json: async () => ({ openInterest: '50', time: 1000 }) };
  };
  const process = await openIngestProcess({
    tailSaveMs: 0,
    market: 'binance_perp',
    stream: 'trades',
    adapter: createBinanceFuturesAdapter({ market: 'binance_perp', symbol: 'BTCUSDT' }),
    venue: 'binance',
    runId: 'run-aux-rest',
    webSocketImpl: impl,
    organizeSocketPath: organize.server.path,
    ingestStorePath: join(dir, 'ingest.sqlite'),
    spoolDir: join(dir, 'spool'),
    channelOptions: { batchFrames: 1 },
    rawDir: join(dir, 'raw'),
    oiPollIntervalMs: 30_000,
    auxFetchImpl: fetchImpl,
    auxSetTimer: (cb, ms) => { timers.push({ cb, ms }); return { unref() {} }; },
    auxClearTimer: () => {},
    rawBatchWindowMs: 1,
    onStop: () => {},
    onDiagnostic: () => {},
    onGap: () => {},
  });
  try {
    process.start();
    assert.equal(timers.length, 1);
    assert.equal(timers[0].ms, 30_000);
    // Fire the poller's own interval callback once.
    await timers[0].cb();
    await process.stop();
    process.close();
    const rows = readBatches(dir, 'binance_perp');
    const oi = rows.filter((row) => row.stream === 'open_interest');
    assert.equal(oi.length, 1);
    const envelope = JSON.parse(gunzipSync(Buffer.from(oi[0].raw_gzip)).toString('utf8').replace(/\n$/, ''));
    assert.equal(envelope.payload.market, 'binance_perp');
    assert.equal(envelope.payload.open_interest, 50);
    assert.equal(envelope.payload.source, 'binance');
  } finally {
    try { process.close(); } catch { /* already closed */ }
    await organize.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

// A direct unit assertion on the record factory, independent of the adapter plumbing.
test('openInterestRecord is the v1 oi_v1 shape', () => {
  const record = openInterestRecord({
    market: 'hyperliquid_perp',
    sample: { open_interest: 38455.70468, mark_price: 82472, funding_rate: 0.0000045838, next_funding_time: null, source_ts: AT, ts: AT },
    ts: AT,
  });
  assert.deepEqual(keysOf(record.payload), OI_KEYS);
  assert.equal(record.payload.schema, 'oi_v1');
  assert.equal(record.event_ts_ms, AT);
});

// A failed auxiliary fetch is not a reception failure: v1 recorded a `status:'error'` placeholder row
// and carried on, and the live stages do not consume the open-interest stream at all. Stopping
// reception over a REST hiccup would trade a hole in an auxiliary stream for the market's whole
// reception going dark.
test('a failed auxiliary fetch is reported once per episode and never stops reception', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'raw-aux-fail-'));
  const organize = await startFakeOrganize(join(dir, 'organize.sock'), { batchFrames: 1 });
  const sockets = [];
  const impl = function fakeSocket(url) {
    const socket = { url, sent: [], closed: false, onopen: null, onmessage: null, onclose: null, onerror: null, send(m) { socket.sent.push(m); }, close() { socket.closed = true; }, deliver(data) { socket.onmessage?.({ data }); } };
    sockets.push(socket);
    return socket;
  };
  const timers = [];
  const diagnostics = [];
  const stops = [];
  let fail = true;
  const fetchImpl = async () => {
    if (fail) throw new Error('network down');
    return { ok: true, status: 200, json: async () => ({ result: { list: [{ openInterest: '50', markPrice: '100', fundingRate: '0.001', nextFundingTime: 1, updatedTime: 1000 }] } }) };
  };
  // Bybit's book arrives over the socket itself (no REST snapshot to synchronise first), so the
  // reception can be exercised without a depth fetch alongside the auxiliary one.
  const process = await openIngestProcess({
    tailSaveMs: 0,
    market: 'bybit_perp',
    stream: 'trades',
    adapter: createBybitPerpAdapter({ market: 'bybit_perp', symbol: 'BTCUSDT' }),
    venue: 'bybit',
    runId: 'run-aux-fail',
    webSocketImpl: impl,
    organizeSocketPath: organize.server.path,
    ingestStorePath: join(dir, 'ingest.sqlite'),
    spoolDir: join(dir, 'spool'),
    channelOptions: { batchFrames: 1 },
    rawDir: join(dir, 'raw'),
    oiPollIntervalMs: 30_000,
    auxFetchImpl: fetchImpl,
    auxSetTimer: (cb, ms) => { timers.push({ cb, ms }); return { unref() {} }; },
    auxClearTimer: () => {},
    rawBatchWindowMs: 1,
    onStop: (stop) => stops.push(stop),
    onDiagnostic: (d) => diagnostics.push(d),
    onGap: () => {},
  });
  const failed = () => diagnostics.filter((d) => /open-interest poller failed/.test(String(d.reason))).length;
  try {
    process.start();
    await timers[0].cb();
    await timers[0].cb();
    assert.equal(failed(), 1, 'one episode, one report');
    assert.equal(stops.length, 0, 'reception was not stopped by an auxiliary failure');

    // Reception still works while the auxiliary source is down.
    const until = async (predicate) => {
      const deadline = Date.now() + 4000;
      while (Date.now() < deadline) {
        if (predicate()) return;
        await new Promise((r) => setTimeout(r, 5));
      }
      throw new Error('timed out waiting for the condition');
    };
    await until(() => sockets.length === 1);
    sockets[0].onopen();
    // Bybit's book starts from a socket snapshot; the deltas only count once it has arrived.
    sockets[0].deliver(JSON.stringify({ topic: 'orderbook.1000.BTCUSDT', type: 'snapshot', ts: 1_792_000_000_200, data: { s: 'BTCUSDT', u: 5, seq: 5, b: [['100', '1']], a: [['101', '1']] } }));
    await until(() => process.receivedTails()[0]?.lastReceivedSeq >= 1);
    assert.ok(process.receivedTails()[0]?.lastReceivedSeq >= 1, 'the reception kept running');

    // A written sample ends the episode: the next failure is a new one and is reported again.
    fail = false;
    await timers[0].cb();
    fail = true;
    await timers[0].cb();
    assert.equal(failed(), 2, 'a failure after a healthy tick is a new episode');
    await process.stop();
  } finally {
    try { process.close(); } catch { /* already closed */ }
    await organize.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

// The tickers topic pushes a snapshot and then partial deltas; a delta carries only what changed.
// Dropping deltas lost every update after the first, and reading one without merging lost the fields
// it did not resend.
test('bybit_perp: a tickers delta merges over the snapshot instead of being dropped', () => {
  const adapter = createBybitPerpAdapter({ market: 'bybit_perp' });
  const TSB = 1_791_579_682_810;
  const snapshot = {
    topic: 'tickers.BTCUSDT', type: 'snapshot', ts: TSB,
    data: { symbol: 'BTCUSDT', openInterest: '55891.704', markPrice: '82471.62', fundingRate: '-0.00000499', nextFundingTime: 1_791_590_400_000 },
  };
  const delta = {
    topic: 'tickers.BTCUSDT', type: 'delta', ts: TSB + 1000,
    data: { symbol: 'BTCUSDT', openInterest: '55900.5', markPrice: '82500.1' },
  };
  assert.equal(adapter.rawEventFor(frame(snapshot, TSB)).payload.open_interest, 55891.704);
  const record = adapter.rawEventFor(frame(delta, TSB + 1000));
  assert.equal(record.stream, 'open_interest', 'the delta is not dropped');
  assert.equal(record.payload.open_interest, 55900.5);
  assert.equal(record.payload.mark_price, 82500.1);
  assert.equal(record.payload.funding_rate, -0.00000499, 'the fields the delta did not resend are kept');
  assert.equal(record.payload.next_funding_time, 1_791_590_400_000);
});

test('a sample that arrives after the collector stopped is not written', async () => {
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  const writes = [];
  const collector = createAuxCollector({
    market: 'binance_perp',
    fetchImpl: async () => { await gate; return { ok: true, status: 200, json: async () => ({ openInterest: '50', time: 1000 }) }; },
    nowMs: () => AT,
    setTimer: () => ({ unref() {} }),
    clearTimer: () => {},
    append: (record) => writes.push(record),
    onError: () => {},
  });
  const ticking = collector.tick();
  collector.stop();
  release();
  await ticking;
  assert.equal(writes.length, 0, 'nothing was written past the stop');
});
