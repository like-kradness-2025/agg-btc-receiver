/** Binance Spot and USDⓈ-M Futures market adapters. */

const SPOT_SYMBOLS = new Set(['BTCUSDT', 'BTCUSDC', 'BTCFDUSD']);
const DEFAULT_SPOT_WS = 'wss://stream.binance.com:9443/stream';
const DEFAULT_SPOT_REST = 'https://api.binance.com/api/v3/depth';
const DEFAULT_FUTURES_PUBLIC_WS = 'wss://fstream.binance.com/public/stream';
const DEFAULT_FUTURES_MARKET_WS = 'wss://fstream.binance.com/market/stream';
const DEFAULT_FUTURES_REST = 'https://fapi.binance.com/fapi/v1/depth';

function textOf(raw) { return typeof raw === 'string' ? raw : raw?.toString?.('utf8') ?? ''; }
function finiteNumber(value) {
  if (typeof value !== 'number' && typeof value !== 'string') return null;
  if (typeof value === 'string' && value.trim() === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
function validLevel(level) {
  if (!Array.isArray(level) || level.length !== 2) return false;
  const price = finiteNumber(level[0]);
  const size = finiteNumber(level[1]);
  return price !== null && price > 0 && size !== null && size >= 0;
}
function snapshotShape(snapshot) {
  if (!snapshot || typeof snapshot !== 'object' || !Number.isInteger(snapshot.lastUpdateId) || snapshot.lastUpdateId < 0) {
    throw new TypeError('Binance depth snapshot needs an integer lastUpdateId');
  }
  if (!Array.isArray(snapshot.bids) || !Array.isArray(snapshot.asks) || !snapshot.bids.every(validLevel) || !snapshot.asks.every(validLevel)) {
    throw new TypeError('Binance depth snapshot needs valid bids and asks arrays');
  }
  return snapshot;
}

function makeDepthSynchronizer({ eventOf, fetchSnapshot, continuity, waitForBufferMs = 0 }) {
  let lastUpdateId = null;
  let synced = false;
  let needsResync = true;
  let connectionId = null;
  let snapshot = null;
  let snapshotChangesPending = false;
  let previousEventId = null;

  function reset(nextConnectionId = null) {
    lastUpdateId = null; synced = false; needsResync = true; connectionId = nextConnectionId;
    snapshot = null; snapshotChangesPending = false; previousEventId = null;
  }
  function applySnapshot(nextSnapshot) {
    const normalized = snapshotShape(nextSnapshot);
    lastUpdateId = normalized.lastUpdateId; snapshot = normalized; snapshotChangesPending = true;
    synced = true; needsResync = false; previousEventId = null;
    return { status: 'snapshot', lastUpdateId };
  }
  function accept(value) {
    const event = eventOf(value);
    if (event === null) return { status: 'malformed', reason: 'malformed Binance depth event' };
    if (needsResync || !synced) return { status: 'resync', reason: 'depth stream is not synchronized' };
    const result = continuity(event, { lastUpdateId, previousEventId });
    if (result.status !== 'applied') {
      if (result.status === 'resync') { needsResync = true; synced = false; }
      return result;
    }
    lastUpdateId = event.u; previousEventId = event;
    return { status: 'applied', lastUpdateId };
  }
  async function sync(bufferedEvents = [], { isCurrent = () => true } = {}) {
    if (typeof fetchSnapshot !== 'function') throw new TypeError('Binance depth sync needs fetchSnapshot');
    for (;;) {
      const next = await fetchSnapshot();
      if (!isCurrent()) return { status: 'stale' };
      let buffered = (typeof bufferedEvents === 'function' ? bufferedEvents() : bufferedEvents).map(eventOf).filter(Boolean);
      const waitUntil = Date.now() + waitForBufferMs;
      while (buffered.length === 0 && waitForBufferMs > 0 && Date.now() < waitUntil && isCurrent()) {
        await new Promise((resolve) => setTimeout(resolve, 25));
        buffered = (typeof bufferedEvents === 'function' ? bufferedEvents() : bufferedEvents).map(eventOf).filter(Boolean);
      }
      applySnapshot(next);
      const first = buffered.find((event) => event.u >= lastUpdateId);
      if (first) {
        const verdict = continuity(first, { lastUpdateId, previousEventId: null });
        if (verdict.status === 'resync') continue;
      }
      return { status: 'synced', lastUpdateId };
    }
  }
  function replacementFor(event = null) {
    const levels = new Map();
    for (const [side, source] of [['bid', snapshot.bids], ['ask', snapshot.asks]]) {
      for (const level of source) levels.set(`${side}:${Number(level[0])}`, { side, price: Number(level[0]), size: Number(level[1]) });
    }
    if (event !== null) {
      for (const [side, source] of [['bid', event.b], ['ask', event.a]]) for (const level of source) {
        const change = { side, price: Number(level[0]), size: Number(level[1]) };
        const key = `${side}:${change.price}`;
        if (change.size === 0) levels.delete(key); else levels.set(key, change);
      }
    }
    return { replace: true, levels: [...levels.values()] };
  }
  return {
    reset, applySnapshot, accept, sync,
    get lastUpdateId() { return lastUpdateId; }, get synced() { return synced; },
    get needsResync() { return needsResync; }, get connectionId() { return connectionId; },
    takeSnapshotChanges(event = null) {
      if (!snapshotChangesPending || snapshot === null) return null;
      snapshotChangesPending = false; return replacementFor(event);
    },
  };
}

function parseWrapper(value) {
  let parsed = value;
  if (typeof value === 'string' || Buffer.isBuffer(value)) {
    try { parsed = JSON.parse(textOf(value)); } catch { return null; }
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed) || typeof parsed.stream !== 'string') return null;
  return parsed;
}
function makeUrls(base, streams, restBase, symbol, limit) {
  return {
    url: `${base}?streams=${streams.join('/')}`,
    restUrl: `${restBase}?symbol=${symbol}&limit=${limit}`,
  };
}
function makeAdapter({ market, symbol, url, tradeUrl, restUrl, streams, depthStream, tradeStream, depthOf, tradeOf, continuity, fetchImpl, waitForBufferMs = 0, forceOrderStream = null, forceOrderOf = null, quantityUnit = undefined }) {
  const sync = makeDepthSynchronizer({
    eventOf: depthOf,
    fetchSnapshot: async () => {
      if (typeof fetchImpl !== 'function') throw new TypeError('Binance depth sync needs fetch');
      const response = await fetchImpl(restUrl);
      if (!response?.ok) throw new Error(`Binance depth snapshot failed (${response?.status ?? 'unknown'})`);
      return response.json();
    },
    continuity, waitForBufferMs,
  });
  let activePreparation = 0;
  let preparationBuffer = [];
  return {
    market, symbol, url, ...(tradeUrl === undefined ? {} : { tradeUrl }), restUrl, ...(quantityUnit === undefined ? {} : { quantityUnit }), stream: 'trades', boundary: 'sequence', ackMode: 'first-data',
    subscribeMessages: () => [], heartbeatMessage: () => null, sync,
    get needsResync() { return sync.needsResync; }, get lastUpdateId() { return sync.lastUpdateId; },
    get connectionId() { return sync.connectionId; }, async syncDepth() { return sync.sync([]); },
    async onConnectionOpen({ connectionId } = {}) {
      const preparation = ++activePreparation; preparationBuffer = []; sync.reset(connectionId ?? null);
      return sync.sync(() => preparationBuffer, { isCurrent: () => preparation === activePreparation });
    },
    syncSnapshot(snapshot) { return sync.applySnapshot(snapshot); },
    acceptDepthEvent(value) { return sync.accept(value); },
    resetConnection(connectionId) { sync.reset(connectionId); },
    bufferDuringPreparation(raw) { const event = depthOf(raw); if (event !== null) preparationBuffer.push(event); },
    parse(raw) {
      const wrapper = parseWrapper(raw);
      if (wrapper === null || !streams.has(wrapper.stream)) return null;
      if (wrapper.stream === tradeStream && tradeOf(wrapper.data)) return { kind: 'data', trade: true };
      if (forceOrderStream !== null && wrapper.stream === forceOrderStream && forceOrderOf?.(wrapper.data)) return { kind: 'data', liquidation: true };
      if (wrapper.stream === depthStream && depthOf(wrapper)) return { kind: 'data', depth: true };
      return null;
    },
    changesFor(envelope) {
      const wrapper = parseWrapper(envelope?.raw);
      if (wrapper === null || !streams.has(wrapper.stream) || wrapper.stream === tradeStream) return { replace: false, changes: [] };
      const event = depthOf(wrapper);
      if (!event) return { replace: false, changes: [] };
      return sync.takeSnapshotChanges(event) ?? {
        replace: false,
        changes: [...event.b.map((x) => ({ side: 'bid', price: Number(x[0]), size: Number(x[1]) })), ...event.a.map((x) => ({ side: 'ask', price: Number(x[0]), size: Number(x[1]) }))],
      };
    },
    connects({ current } = {}) {
      const event = depthOf(current?.raw ?? current);
      if (event === null) return true;
      const result = sync.accept(event);
      return result.status !== 'resync' && result.status !== 'malformed';
    },
    venueSeqOf(raw) { const wrapper = parseWrapper(raw); const event = wrapper && depthOf(wrapper); return event?.u ?? null; },
  };
}

function spotDepthFactory(symbol, depthStream) {
  return (value) => {
    const wrapper = parseWrapper(value);
    const data = wrapper?.data ?? value;
    if (!data || data.e !== 'depthUpdate' || data.s !== symbol || wrapper && wrapper.stream !== depthStream || !Number.isInteger(data.U) || !Number.isInteger(data.u) || data.U < 0 || data.u < data.U || !Array.isArray(data.b) || !Array.isArray(data.a) || !data.b.every(validLevel) || !data.a.every(validLevel)) return null;
    return data;
  };
}
function spotTradeFactory(symbol) { return (data) => data && data.e === 'trade' && data.s === symbol && finiteNumber(data.p) !== null && Number(data.p) > 0 && finiteNumber(data.q) !== null && Number(data.q) > 0 && Number.isInteger(data.t) && data.t >= 0; }
function futuresDepthFactory(symbol, depthStream) {
  return (value) => {
    const wrapper = parseWrapper(value); const data = wrapper?.data ?? value;
    if (!data || data.e !== 'depthUpdate' || data.s !== symbol || wrapper && wrapper.stream !== depthStream || !Number.isInteger(data.U) || !Number.isInteger(data.u) || !Number.isInteger(data.pu) || data.U < 0 || data.u < data.U || data.pu < 0 || !Array.isArray(data.b) || !Array.isArray(data.a) || !data.b.every(validLevel) || !data.a.every(validLevel)) return null;
    return data;
  };
}
function futuresTradeFactory(symbol) { return (data) => data && data.e === 'aggTrade' && data.s === symbol && finiteNumber(data.p) !== null && Number(data.p) > 0 && finiteNumber(data.q) !== null && Number(data.q) > 0 && Number.isInteger(data.a) && data.a >= 0 && Number.isInteger(data.T) && data.T >= 0; }

export function createBinanceDepthSynchronizer({ fetchSnapshot } = {}) {
  const symbol = 'BTCUSDT';
  const depthStream = 'btcusdt@depth@100ms';
  return makeDepthSynchronizer({
    eventOf: spotDepthFactory(symbol, depthStream),
    fetchSnapshot,
    continuity: (event, { lastUpdateId: current }) => {
      if (event.u <= current) return { status: 'discarded', reason: 'depth update is at or before snapshot boundary' };
      const expected = current + 1;
      return event.U <= expected && expected <= event.u
        ? { status: 'applied' }
        : { status: 'resync', reason: 'depth update gap', expected, first: event.U, final: event.u };
    },
  });
}

export function createBinanceSpotAdapter({ market = 'binance_spot', symbol = 'BTCUSDT', url, restUrl, fetchImpl = globalThis.fetch } = {}) {
  const normalized = String(symbol).toUpperCase();
  if (!SPOT_SYMBOLS.has(normalized)) throw new TypeError(`unsupported Binance Spot symbol: ${symbol}`);
  const lower = normalized.toLowerCase(); const depthStream = `${lower}@depth@100ms`; const tradeStream = `${lower}@trade`;
  const defaults = makeUrls(DEFAULT_SPOT_WS, [tradeStream, depthStream], DEFAULT_SPOT_REST, normalized, 5000);
  const depthOf = spotDepthFactory(normalized, depthStream);
  return makeAdapter({ market, symbol: normalized, url: url ?? defaults.url, restUrl: restUrl ?? defaults.restUrl, streams: new Set([tradeStream, depthStream]), depthStream, tradeStream, depthOf, tradeOf: spotTradeFactory(normalized), fetchImpl, continuity: (event, { lastUpdateId: current }) => {
    if (event.u <= current) return { status: 'discarded', reason: 'depth update is at or before snapshot boundary' };
    const expected = current + 1;
    return event.U <= expected && expected <= event.u ? { status: 'applied' } : { status: 'resync', reason: 'depth update gap', expected, first: event.U, final: event.u };
  } });
}

export function createBinanceFuturesAdapter({ market = 'binance_perp', symbol = 'BTCUSDT', url, restUrl, fetchImpl = globalThis.fetch } = {}) {
  const normalized = String(symbol).toUpperCase();
  if (!new Set(['BTCUSDT', 'BTCUSDC']).has(normalized)) throw new TypeError(`unsupported Binance USDⓈ-M Futures symbol: ${symbol}`);
  const lower = normalized.toLowerCase(); const depthStream = `${lower}@depth`; const tradeStream = `${lower}@aggTrade`;
  const defaults = makeUrls(DEFAULT_FUTURES_PUBLIC_WS, [depthStream], DEFAULT_FUTURES_REST, normalized, 1000);
  const depthOf = futuresDepthFactory(normalized, depthStream);
  return makeAdapter({ market, symbol: normalized, url: url ?? defaults.url, tradeUrl: `${DEFAULT_FUTURES_MARKET_WS}?streams=${tradeStream}`, restUrl: restUrl ?? defaults.restUrl, streams: new Set([tradeStream, depthStream]), depthStream, tradeStream, depthOf, tradeOf: futuresTradeFactory(normalized), fetchImpl, waitForBufferMs: 5000, continuity: (event, state) => {
    if (event.u < state.lastUpdateId) return { status: 'discarded', reason: 'depth update is before snapshot boundary' };
    if (state.previousEventId === null) {
      return event.U <= state.lastUpdateId && state.lastUpdateId <= event.u ? { status: 'applied' } : { status: 'resync', reason: 'futures first depth update does not bridge snapshot', first: event.U, final: event.u };
    }
    if (event.pu !== state.lastUpdateId) return { status: 'resync', reason: 'futures previous update id mismatch', expected: state.lastUpdateId, previous: event.pu };
    return { status: 'applied' };
  } });
}

function coinMForceOrderFactory(symbol) {
  return (data) => data && data.e === 'forceOrder' && data.o && data.o.s === symbol;
}

export function createBinanceCoinMFuturesAdapter({ market = 'binance_coinm_perp', symbol = 'BTCUSD_PERP', url, restUrl, fetchImpl = globalThis.fetch } = {}) {
  const normalized = String(symbol).toUpperCase();
  if (normalized !== 'BTCUSD_PERP') throw new TypeError(`unsupported Binance COIN-M Futures symbol: ${symbol}`);
  const lower = normalized.toLowerCase();
  const depthStream = `${lower}@depth@100ms`;
  const tradeStream = `${lower}@aggTrade`;
  const forceOrderStream = `${lower}@forceOrder`;
  const defaults = makeUrls('wss://dstream.binance.com/stream', [tradeStream, depthStream, forceOrderStream], 'https://dapi.binance.com/dapi/v1/depth', normalized, 1000);
  const depthOf = futuresDepthFactory(normalized, depthStream);
  return makeAdapter({
    market, symbol: normalized, url: url ?? defaults.url, restUrl: restUrl ?? defaults.restUrl,
    streams: new Set([tradeStream, depthStream, forceOrderStream]), depthStream, tradeStream,
    forceOrderStream, forceOrderOf: coinMForceOrderFactory(normalized), depthOf,
    tradeOf: futuresTradeFactory(normalized), fetchImpl, quantityUnit: 'contracts', waitForBufferMs: 5000,
    continuity: (event, state) => {
      if (event.u < state.lastUpdateId) return { status: 'discarded', reason: 'depth update is before snapshot boundary' };
      if (state.previousEventId === null) return event.U <= state.lastUpdateId && state.lastUpdateId <= event.u
        ? { status: 'applied' }
        : { status: 'resync', reason: 'COIN-M first depth update does not bridge snapshot', first: event.U, final: event.u };
      if (event.pu !== state.lastUpdateId) return { status: 'resync', reason: 'COIN-M previous update id mismatch', expected: state.lastUpdateId, previous: event.pu };
      return { status: 'applied' };
    },
  });
}

export const DEFAULT_WS_URL = makeUrls(DEFAULT_SPOT_WS, ['btcusdt@trade', 'btcusdt@depth@100ms'], DEFAULT_SPOT_REST, 'BTCUSDT', 5000).url;
export const DEFAULT_REST_URL = makeUrls(DEFAULT_SPOT_WS, ['btcusdt@trade', 'btcusdt@depth@100ms'], DEFAULT_SPOT_REST, 'BTCUSDT', 5000).restUrl;
