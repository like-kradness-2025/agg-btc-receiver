// lib/kraken-connector.mjs — Kraken spot connector for btc-receiver v3.00

import { BaseConnector } from './base-connector.mjs';
import { FullBook } from './full-book.mjs';

const WS_SNAPSHOT_TIMEOUT_MS = 30000;

const toPairs = (rows = []) => rows.map(([p, q]) => [String(p), String(q)]);
/** Normalize price to WS precision (XBT/USD: 1 decimal, null-safe). */
const normPrice = (p) => { const n = parseFloat(p); return Number.isFinite(n) ? n.toFixed(1) : String(p); };
/** Normalize quantity to WS precision (XBT/USD: 8 decimals, null-safe). */
const normQty = (q) => { const n = parseFloat(q); return Number.isFinite(n) ? n.toFixed(8) : String(q); };
const bookFramePayload = (data) => {
  if (!Array.isArray(data)) return null;
  const channelIndex = data.findIndex((value, index) => index > 0 && typeof value === 'string' && value.startsWith('book-'));
  if (channelIndex < 0) return null;
  const payload = {};
  for (const part of data.slice(1, channelIndex)) {
    if (!part || typeof part !== 'object' || Array.isArray(part)) continue;
    if (Array.isArray(part.as)) payload.as = part.as;
    if (Array.isArray(part.bs)) payload.bs = part.bs;
    if (Array.isArray(part.a)) payload.a = [...(payload.a || []), ...part.a];
    if (Array.isArray(part.b)) payload.b = [...(payload.b || []), ...part.b];
    if (Object.prototype.hasOwnProperty.call(part, 'c')) payload.c = part.c;
  }
  return payload;
};
const isTradeArrayFrame = (data) => Array.isArray(data) && Array.isArray(data[1]) && data[2] === 'trade';
const restBookKey = (result, preferred) => {
  if (!result || typeof result !== 'object') return null;
  if (preferred && result[preferred]) return preferred;
  return Object.keys(result).find((k) => k !== 'last') || null;
};
// Kraken v1 checksum tokens remove the decimal point and then leading zeros.
// Keep one zero for the degenerate zero value.
const checksumToken = (value) => String(value).replace('.', '').replace(/^0+/, '') || '0';
const parseChecksum = (data) => {
  if (data?.c == null) return { present: false, value: null };
  const value = Number(data.c);
  return {
    present: true,
    value: Number.isSafeInteger(value) && value >= 0 && value <= 0xFFFFFFFF ? value : null,
  };
};
const crc32 = (str) => {
  let crc = ~0;
  for (let i = 0; i < str.length; i++) {
    crc ^= str.charCodeAt(i);
    for (let j = 0; j < 8; j++) {
      crc = (crc >>> 1) ^ (0xEDB88320 & -(crc & 1));
    }
  }
  return (~crc) >>> 0;
};

export function calculateKrakenChecksum(book) {
  const { bids, asks } = book.getTop(10);
  let payload = '';
  for (const [price, qty] of asks) payload += checksumToken(price) + checksumToken(qty);
  for (const [price, qty] of bids) payload += checksumToken(price) + checksumToken(qty);
  return crc32(payload);
}

export class KrakenSpotConnector extends BaseConnector {
  constructor(config) {
    super(config, {
      market: 'kraken_spot',
      wsUrl: config.wsUrl || 'wss://ws.kraken.com',
      restUrl: config.restUrl || 'https://api.kraken.com/0/public/Depth?pair=XBTUSD&count=1000',
    });
    this.symbol = config.symbol || 'XBT/USD';
    this.restPair = config.restPair || 'XBTUSD';
    // Kraken v1 sends a fixed-depth book.  A delete pulls the next level into
    // scope and an insert pushes the outermost level out; retaining levels
    // beyond the subscription depth eventually makes that implicit shift
    // diverge from Kraken's checksum state.
    const depth = Number(config.depthLimit);
    this.book = new FullBook('kraken_spot', {
      maxLevels: Number.isInteger(depth) && depth > 0 ? depth : 1000,
    });
    this._checksumWarm = false;
    this._checksumDisabled = false;
  }

  subscribe() {
    this._ws.send(JSON.stringify({
      event: 'subscribe',
      pair: [this.symbol],
      subscription: { name: 'book', depth: 1000 },
    }));
    this._ws.send(JSON.stringify({
      event: 'subscribe',
      pair: [this.symbol],
      subscription: { name: 'trade' },
    }));
  }

  _onMessage(data) {
    if (data.event === 'subscriptionStatus' || data.event === 'systemStatus' || data.event === 'heartbeat') return;
    if (data.event === 'pong' || data.event === 'ping') return;
    if (data.event === 'error') {
      this.emit('error', { market: this.market, message: `subscription error: ${data.errorMessage || JSON.stringify(data)}` });
      return;
    }
    if (Array.isArray(data)) {
      const bookPayload = bookFramePayload(data);
      if (bookPayload) {
        if (bookPayload.as || bookPayload.bs) return this._handleBookSnapshot(bookPayload);
        if (bookPayload.a || bookPayload.b) return this._handleBookUpdate(bookPayload);
      }
      if (isTradeArrayFrame(data)) return this._handleTrades(data);
      return;
    }
    if (data.as || data.bs) return this._handleBookSnapshot(data);
    if (data.a || data.b) return this._handleBookUpdate(data);
  }

  _handleBookSnapshot(data) {
    const bids = toPairs(data.bs || []);
    const asks = toPairs(data.as || []);
    const ts = Date.now();
    this.book.applySnapshot(bids, asks);
    const checksumState = parseChecksum(data);
    const checksum = checksumState.value;
    const levelCount = this.book.getLevelCount();
    const checksumMismatch = checksumState.present && checksum === null
      || checksum !== null && levelCount.bids >= 10 && levelCount.asks >= 10
      && calculateKrakenChecksum(this.book) !== checksum;
    const crossed = this._isCrossedBook();
    if (crossed || checksumMismatch) {
      this.book.clear();
      this._handleSequenceGap(crossed ? 'Kraken crossed snapshot book' : 'Kraken snapshot checksum mismatch', data);
      return;
    }
    this._emitDepth('snapshot', bids, asks, ts, undefined, {
      checksum,
      sequence_mode: 'checksum',
    });
    this._notifyWsSnapshotReceived(null);
    this._checksumWarm = false;
    this._checksumDisabled = false;
    this._replayRingBufAfterSnapshot();
    this._ringBuf = [];
  }

  _replayRingBufAfterSnapshot() {
    const pending = this._ringBuf;
    this._ringBuf = [];
    this._checksumDisabled = true; // suppress checksum validation during replay
    for (const msg of pending) {
      if (msg?.a || msg?.b) this._handleBookUpdate(msg);
    }
    this._checksumWarm = true;
    this._checksumDisabled = false; // re-enable for live updates
  }

  _handleBookUpdate(data) {
    if (!this._wsSnapshotReceived) {
      this._bufferMsg(data);
      return;
    }
    if (this._state === 'reconnecting') return;
    const bids = toPairs(data.b || []);
    const asks = toPairs(data.a || []);
    const ts = Date.now();
    const prevSnapshot = this.book.toSnapshot(ts);
    const checksumState = parseChecksum(data);
    const checksum = checksumState.value ?? undefined;
    // Apply each side as one exchange message. Kraken's delete/insert rows
    // are ordered as a batch; trimming after every row can pull the wrong
    // border level into the subscribed depth before the message is complete.
    this.book.applyDiffs('ask', asks);
    this.book.applyDiffs('bid', bids);
    if (this._isCrossedBook()) {
      this.book.applySnapshot(prevSnapshot.bids, prevSnapshot.asks, prevSnapshot.seq);
      this._handleSequenceGap('Kraken crossed book', data);
      return;
    }
    if (checksumState.present && checksum === undefined) {
      this.book.applySnapshot(prevSnapshot.bids, prevSnapshot.asks, prevSnapshot.seq);
      this._handleSequenceGap('Kraken invalid checksum value', data);
      return;
    }
    if (checksum !== undefined && this._checksumWarm && !this._checksumDisabled) {
      const levelCount = this.book.getLevelCount();
      if (levelCount.bids >= 10 && levelCount.asks >= 10) {
        const localChecksum = calculateKrakenChecksum(this.book);
        if (localChecksum !== checksum) {
          this.book.applySnapshot(prevSnapshot.bids, prevSnapshot.asks, prevSnapshot.seq);
          this._handleSequenceGap(`Kraken checksum mismatch: local=${localChecksum}, remote=${checksum}`, data);
          return;
        }
      }
    }
    this._checksumWarm = true;
    this._emitDepth('update', bids, asks, ts, undefined, {
      checksum: checksum ?? null,
      sequence_mode: 'checksum',
    });
  }

  _isCrossedBook() {
    const bid = this.book.getBestBid();
    const ask = this.book.getBestAsk();
    return bid !== null && ask !== null && Number(bid) >= Number(ask);
  }

  _handleTrades(msg) {
    const trades = msg[1] || [];
    for (const t of trades) {
      const [price, qty, time, side, orderType, misc] = t;
      this._emitTrade(parseFloat(price), parseFloat(qty), side === 's' ? 'sell' : 'buy', Math.floor(Number(time) * 1000) || Date.now(), `${time}-${price}-${qty}-${orderType}-${misc}`);
    }
  }

  async _syncBook() {
    this._setState('syncing');
    if (this._wsSnapshotReceived) {
      this._finalizeWsSnapshotSync();
      return;
    }
    this._beginWsSnapshotSync();
    try {
      await this._waitForWsSnapshot(WS_SNAPSHOT_TIMEOUT_MS, 'ws snapshot timeout');
    } catch (err) {
      if (err?.code === 'WS_SNAPSHOT_ABORTED') return;
      let restored = false;
      try {
        const snapshot = await this._fetchSnapshot();
        const key = restBookKey(snapshot.result, this.restPair);
        const rawBids = snapshot.result?.[key]?.bids;
        const rawAsks = snapshot.result?.[key]?.asks;
        if (!this._validateSnapshotLevels({ bids: rawBids, asks: rawAsks })) {
          throw new Error('invalid REST depth snapshot');
        }
        const bids = toPairs(rawBids).map(([p, q]) => [normPrice(p), normQty(q)]);
        const asks = toPairs(rawAsks).map(([p, q]) => [normPrice(p), normQty(q)]);
        const ts = Date.now();
        this.book.applySnapshot(bids, asks);
        if (this._isCrossedBook()) {
          this.book.clear();
          this._handleSequenceGap('Kraken crossed REST fallback snapshot', snapshot);
          throw new Error('crossed REST fallback snapshot');
        }
        this._emitDepth('snapshot', bids, asks, ts, undefined, {
          checksum: null,
          sequence_mode: 'checksum',
          snapshot_origin: 'rest_fallback',
        });
        this._notifyWsSnapshotReceived(null);
        this._checksumWarm = false;
        this._checksumDisabled = false;
        this._replayRingBufAfterSnapshot();
        restored = true;
      } catch (restErr) {
        this.emit('error', { market: this.market, message: `sync REST fallback failed: ${restErr.message}` });
      }
      if (!restored) {
        this._failWsSnapshotSync('init sync failed');
        throw new Error(`init sync failed for ${this.market}`);
      }
    }
    if (this._state !== 'error') this._finalizeWsSnapshotSync();
  }

  async _fetchSnapshot() {
    const res = await fetch(this.restUrl, { signal: AbortSignal.timeout(10000) });
    if (!res.ok) throw new Error(`REST snapshot ${res.status}`);
    return res.json();
  }
}
