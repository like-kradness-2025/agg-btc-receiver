// lib/kraken-connector.mjs — Kraken spot connector for btc-receiver v3.00

import { BaseConnector } from './base-connector.mjs';
import { FullBook } from './full-book.mjs';

const WS_SNAPSHOT_TIMEOUT_MS = 30000;

const toPairs = (rows = []) => rows.map(([p, q]) => [String(p), String(q)]);
/** Normalize price to WS precision (XBT/USD: 1 decimal, null-safe). */
const normPrice = (p) => { const n = parseFloat(p); return Number.isFinite(n) ? n.toFixed(1) : String(p); };
/** Normalize quantity to WS precision (XBT/USD: 8 decimals, null-safe). */
const normQty = (q) => { const n = parseFloat(q); return Number.isFinite(n) ? n.toFixed(8) : String(q); };
const isBookArrayFrame = (data) => Array.isArray(data) && data[1] && typeof data[1] === 'object' && !Array.isArray(data[1]) && typeof data[2] === 'string' && data[2].startsWith('book-');
const isTradeArrayFrame = (data) => Array.isArray(data) && Array.isArray(data[1]) && data[2] === 'trade';
const restBookKey = (result, preferred) => {
  if (!result || typeof result !== 'object') return null;
  if (preferred && result[preferred]) return preferred;
  return Object.keys(result).find((k) => k !== 'last') || null;
};
const checksumToken = (value) => String(value).replace('.', '');
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
    this.book = new FullBook('kraken_spot', { maxLevels: config.depthLimit ?? 0 });
    this._checksumWarm = false;
    this._lastChecksumMismatchAt = 0;
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
      if (isBookArrayFrame(data)) {
        if (data[1].as || data[1].bs) return this._handleBookSnapshot(data[1]);
        if (data[1].a || data[1].b) return this._handleBookUpdate(data[1]);
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
    this._emitDepth('snapshot', bids, asks, ts);
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
    // Guard: data.c can be null → Number(null)=0 which would produce false mismatches
    const seq = (data.c != null && Number.isFinite(Number(data.c))) ? Number(data.c) : undefined;
    for (const [p, q] of bids) this.book.applyDiff('bid', p, q, seq);
    for (const [p, q] of asks) this.book.applyDiff('ask', p, q, seq);
    // Always compute checksum when warm for match tracking (re-enables _checksumDisabled)
    if (seq !== undefined && this._checksumWarm) {
      const levelCount = this.book.getLevelCount();
      if (levelCount.bids >= 10 && levelCount.asks >= 10) {
        const localChecksum = calculateKrakenChecksum(this.book);
        if (localChecksum === seq) {
          // Checksum matched — re-enable validation, allow future mismatch reconnects
          this._checksumDisabled = false;
        } else if (!this._checksumDisabled) {
          // Mismatch and not currently suppressed
          const now = Date.now();
          if (now - this._lastChecksumMismatchAt >= 300000) {
            this._lastChecksumMismatchAt = now;
            this._handleSequenceGap(`Kraken checksum mismatch: local=${localChecksum}, remote=${seq}`, data);
            return;
          }
          // Cooldown branch: suppress storm, disable further checksum validation
          this._checksumDisabled = true;
          this.book.applySnapshot(prevSnapshot.bids, prevSnapshot.asks, prevSnapshot.seq);
          return;
        }
        // else: mismatch but disabled — let the update through without adding noise
      }
    }
    this._checksumWarm = true;
    this._emitDepth('update', bids, asks, ts, seq);
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
      const snapshot = await this._fetchSnapshot();
      const key = restBookKey(snapshot.result, this.restPair);
      const bids = toPairs(snapshot.result?.[key]?.bids || []).map(([p, q]) => [normPrice(p), normQty(q)]);
      const asks = toPairs(snapshot.result?.[key]?.asks || []).map(([p, q]) => [normPrice(p), normQty(q)]);
      const ts = Date.now();
      this.book.applySnapshot(bids, asks);
      this._emitDepth('snapshot', bids, asks, ts);
      this._notifyWsSnapshotReceived(null);
      this._checksumWarm = false;
      this._checksumDisabled = false;
      this._replayRingBufAfterSnapshot();
    }
    if (this._state !== 'error') this._finalizeWsSnapshotSync();
  }

  async _fetchSnapshot() {
    const res = await fetch(this.restUrl, { signal: AbortSignal.timeout(10000) });
    if (!res.ok) throw new Error(`REST snapshot ${res.status}`);
    return res.json();
  }
}
