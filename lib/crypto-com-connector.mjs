// lib/crypto-com-connector.mjs — Crypto.com BTC/USD spot connector

import { TradeOnlyConnector } from './trade-only-connector.mjs';

const WS_URL = 'wss://stream.crypto.com/exchange/v1/market';
const CHANNEL = 'trade.BTC_USD';
const BOOK_CHANNEL = 'book.BTC_USD.10';

const toNumber = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const normalizeSide = (trade) => {
  if (trade == null || typeof trade !== 'object') return null;
  const raw = trade.s ?? trade.side ?? trade.direction ?? trade.takerSide;
  if (raw == null) return null;
  const v = String(raw).toUpperCase();
  if (v === 'BUY' || v === 'B' || v === '1') return 'buy';
  if (v === 'SELL' || v === 'S' || v === '0') return 'sell';
  return null;
};

const normalizeTs = (value) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return Date.now();
  if (n > 1e15) return Math.floor(n / 1000);
  if (n > 1e12) return Math.floor(n);
  if (n > 1e9) return Math.floor(n * 1000);
  return Math.floor(n);
};

const collectTrades = (data) => {
  if (!data || typeof data !== 'object') return [];
  // Crypto.com trade push: {channel, data: [...]}
  if (Array.isArray(data.data)) return data.data;
  // Crypto.com book push: {id, method, result: {channel, data: [...]}}
  if (data.result?.data && Array.isArray(data.result.data)) return data.result.data;
  if (Array.isArray(data.result)) return data.result;
  if (data.data && typeof data.data === 'object') return [data.data];
  if (data.channel === CHANNEL || String(data.channel || '').startsWith('trade') || data.method?.includes('trade')) return [data];
  return [];
};

export class CryptoComConnector extends TradeOnlyConnector {
  constructor(config) {
    super(config, {
      market: 'crypto_com_spot',
      wsUrl: config.wsUrl || WS_URL,
      restUrl: config.restUrl || '',
    });
  }

  subscribe() {
    this._ws.send(JSON.stringify({
      id: 1,
      method: 'subscribe',
      params: { channels: [CHANNEL, BOOK_CHANNEL] },
    }));
  }

  _onMessage(data) {
    if (!data || typeof data !== 'object') return;

    if (data.method === 'public/heartbeat') {
      this._ws.send(JSON.stringify({ id: data.id, method: 'public/respond-heartbeat' }));
      return;
    }

    // Handle book depth data (full snapshot every message)
    if (data.method === 'subscribe' && data.result?.channel?.startsWith('book')) {
      const bookData = data.result.data?.[0];
      if (bookData && bookData.bids && bookData.asks) {
        const bids = bookData.bids.map(([p, a]) => [String(p), String(a)]);
        const asks = bookData.asks.map(([p, a]) => [String(p), String(a)]);
        const ts = normalizeTs(bookData.t);
        this._handleBookSnapshot(bids, asks, ts);
      }
      return;
    }

    // Handle book push updates (id=-1, channel starts with 'book')
    // Crypto.com sends a new full snapshot on every book push.
    if (data.channel?.startsWith('book') && data.id === -1 && Array.isArray(data.data)) {
      const bookData = data.data[0];
      if (bookData && bookData.bids && bookData.asks) {
        const bids = bookData.bids.map(([p, a]) => [String(p), String(a)]);
        const asks = bookData.asks.map(([p, a]) => [String(p), String(a)]);
        const ts = normalizeTs(bookData.t);
        this._handleBookSnapshot(bids, asks, ts);
      }
      return;
    }

    // Only skip subscribe ACKs (id is positive), not push messages (id=-1)
    if (data.id != null && data.id !== -1 && data.code === 0 && (data.method === 'subscribe' || data.method === 'public/subscribe')) return;

    const trades = collectTrades(data);
    for (const t of trades) {
      const price = toNumber(t.p ?? t.price ?? (Array.isArray(t) ? t[0] : null));
      const qty = toNumber(t.q ?? t.amount ?? t.size ?? (Array.isArray(t) ? t[1] : null));
      const side = normalizeSide(t);
      const ts = normalizeTs(t.t ?? t.timestamp ?? t.time ?? t.E ?? (Array.isArray(t) ? t[2] : null));
      const tradeId = String(t.tid ?? t.trade_id ?? t.tradeId ?? t.d ?? t.id ?? `${ts}-${price}-${qty}`);
      if (price == null || qty == null || !side) continue;
      this._emitTrade(price, qty, side, ts, tradeId);
    }
  }

  /** Handle a full book snapshot by computing and emitting an incremental diff
   *  vs the previous book state. Crypto.com only sends full snapshots, so we
   *  compute the changes ourselves to produce a depth diff stream. */
  _handleBookSnapshot(bids, asks, ts) {
    if (!this._isValidTimestamp(ts)) {
      this._stats.droppedDepthCount++;
      return;
    }
    if (!this._wsSnapshotReceived && this._state === 'syncing') {
      if (!this._emitDepth('snapshot', bids, asks, ts, null, { snapshot_origin: 'ws_sync' })) return;
      this.book.applySnapshot(bids, asks);
      this._notifyWsSnapshotReceived(null);
      return;
    }

    const oldBids = new Map(this.book.bids);
    const oldAsks = new Map(this.book.asks);

    this.book.applySnapshot(bids, asks);

    const changedBids = [];
    const changedAsks = [];

    const allBidPrices = new Set([...oldBids.keys(), ...bids.map(p => p[0])]);
    for (const price of allBidPrices) {
      const oldQty = oldBids.get(price) ?? '0';
      const newQty = this.book.bids.get(price) ?? '0';
      if (oldQty !== newQty) changedBids.push([String(price), newQty]);
    }
    const allAskPrices = new Set([...oldAsks.keys(), ...asks.map(p => p[0])]);
    for (const price of allAskPrices) {
      const oldQty = oldAsks.get(price) ?? '0';
      const newQty = this.book.asks.get(price) ?? '0';
      if (oldQty !== newQty) changedAsks.push([String(price), newQty]);
    }

    // Note: toPairs and toNumber are used above, so prices here are already
    // stringified. Emit even if empty (frequencies are low for top-10 book).
    this._emitDepth('update', changedBids, changedAsks, ts, null);
  }

  async _syncBook() {
    this._setState('syncing');
    if (this._wsSnapshotReceived) {
      this._finalizeWsSnapshotSync();
      return;
    }
    this._beginWsSnapshotSync();
    try {
      await this._waitForWsSnapshot(15000, 'Crypto.com book snapshot timeout');
    } catch (error) {
      if (error?.code === 'WS_SNAPSHOT_ABORTED') return;
      this._failWsSnapshotSync(error.message);
      throw error;
    }
    this._finalizeWsSnapshotSync();
  }
}
