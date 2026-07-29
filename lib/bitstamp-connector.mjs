// lib/bitstamp-connector.mjs — Bitstamp BTC/USD spot connector

import { TradeOnlyConnector } from './trade-only-connector.mjs';

const WS_URL = 'wss://ws.bitstamp.net';
const CHANNEL = 'live_trades_btcusd';

const toNumber = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const normalizeSide = (value) => {
  if (value == null) return null;
  if (typeof value === 'string') {
    const v = value.toLowerCase();
    if (v === 'buy' || v === 'b' || v === '0' || v === 'bid') return 'buy';
    if (v === 'sell' || v === 's' || v === '1' || v === 'ask') return 'sell';
  }
  if (value === 0) return 'buy';
  if (value === 1) return 'sell';
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

const normalizeLevels = (levels) => {
  if (!Array.isArray(levels)) return null;
  const normalized = levels.map((level) => {
    if (!Array.isArray(level) || level.length < 2) return null;
    const [price, qty] = level;
    if (String(price).trim() === '' || String(qty).trim() === '') return null;
    if (!Number.isFinite(Number(price)) || Number(price) <= 0
      || !Number.isFinite(Number(qty)) || Number(qty) < 0) return null;
    return [String(price), String(qty)];
  });
  return normalized.some((level) => level === null) ? null : normalized.filter(([, qty]) => Number(qty) > 0);
};

export class BitstampConnector extends TradeOnlyConnector {
  constructor(config) {
    super(config, {
      market: 'bitstamp_spot',
      wsUrl: config.wsUrl || WS_URL,
      restUrl: config.restUrl || '',
    });
    this._depthInitialized = false;
    this._depthSyncing = true;
    this._pendingDepth = [];
  }

  subscribe() {
    for (const ch of ['live_trades_btcusd', 'diff_order_book_btcusd']) {
      this._ws.send(JSON.stringify({
        event: 'bts:subscribe',
        data: { channel: ch },
      }));
    }
  }

  _onMessage(data) {
    if (!data || typeof data !== 'object') return;

    const event = data.event || data.event_type || data.type;
    const channel = data.channel || data.channel_name || '';

    if (event === 'bts:subscription_succeeded' || event === 'bts:unsubscription_succeeded' || event === 'bts:request_reconnect' || event === 'bts:heartbeat') {
      return;
    }

    // Depth channel
    if (event === 'data' && channel === 'diff_order_book_btcusd') {
      const payload = data.data;
      if (payload) {
        const bids = payload.bids || [];
        const asks = payload.asks || [];
        const ts = Number(payload.microtimestamp) / 1000;
        if (this._depthSyncing) {
          this._pendingDepth.push({ bids, asks, ts });
        } else {
          this._handleDepth('update', bids, asks, ts, null);
        }
      }
      return;
    }

    if (event !== 'trade' && channel !== CHANNEL) return;

    const payload = data.data ?? data;
    const trades = Array.isArray(payload)
      ? payload
      : (payload && Array.isArray(payload.data) ? payload.data : [payload]);

    for (const t of trades) {
      if (!t) continue;
      const price = toNumber(t.price ?? t.price_str ?? t.p ?? (Array.isArray(t) ? t[1] : null));
      const qty = toNumber(t.amount ?? t.amount_str ?? t.qty ?? t.q ?? (Array.isArray(t) ? t[2] : null));
      const side = normalizeSide(t.side ?? t.type ?? t.order_type ?? (Array.isArray(t) ? t[3] : null));
      const ts = normalizeTs(t.microtimestamp ?? t.timestamp ?? t.time ?? t.E ?? (Array.isArray(t) ? t[4] : null));
      const tradeId = String(t.id ?? t.trade_id ?? t.microtimestamp ?? `${ts}-${price}-${qty}`);
      if (price == null || qty == null || !side) continue;
      this._emitTrade(price, qty, side, ts, tradeId);
    }
  }

  async _syncBook() {
    if (this._state === 'reconnecting' || this._state === 'error') return;
    if (!this.restUrl) throw new Error(`${this.market}: REST orderbook URL is required for initial sync`);

    this._setState('syncing');
    this._depthSyncing = true;
    this._depthInitialized = false;
    this._pendingDepth = [];

    try {
      const response = await fetch(this.restUrl, { signal: AbortSignal.timeout(10000) });
      if (response.ok === false) throw new Error(`REST orderbook HTTP ${response.status}`);
      const payload = await response.json();
      const bids = normalizeLevels(payload?.bids);
      const asks = normalizeLevels(payload?.asks);
      if (!bids || !asks || !bids.length || !asks.length) {
        throw new Error(`REST orderbook is incomplete: bids=${bids?.length ?? 0} asks=${asks?.length ?? 0}`);
      }

      this.book.applySnapshot(bids, asks, null);
      this._depthSyncing = false;
      this._depthInitialized = true;
      this._stats.resyncCount++;
      this._emitDepth('snapshot', bids, asks, Date.now(), null, { snapshot_origin: 'rest_sync' });

      const pending = this._pendingDepth;
      this._pendingDepth = [];
      for (const event of pending) {
        this._handleDepth('update', event.bids, event.asks, event.ts, null);
      }
      this._setState('running');
    } catch (error) {
      this._depthSyncing = false;
      this._pendingDepth = [];
      this._setState('error');
      this.emit('error', { market: this.market, message: `REST orderbook sync failed: ${error.message}` });
      throw error;
    }
  }

  /**
   * Handle depth data from diff_order_book_btcusd channel.
   * @param {'snapshot'|'update'} type
   * @param {Array<[string, string]>} bids - [price, amount] pairs
   * @param {Array<[string, string]>} asks - [price, amount] pairs
   * @param {number} ts - timestamp in seconds
   * @param {number|null} seq - sequence number (null for Bitstamp)
   */
  _handleDepth(type, bids, asks, ts, seq) {
    if (!this._isValidTimestamp(ts)) {
      this._stats.droppedDepthCount++;
      return false;
    }
    if (type === 'snapshot') {
      this.book.applySnapshot(bids, asks, seq);
    } else {
      for (const [price, qty] of bids) {
        this.book.applyDiff('bid', price, qty, seq);
      }
      for (const [price, qty] of asks) {
        this.book.applyDiff('ask', price, qty, seq);
      }
    }
    this._emitDepth(type, bids, asks, ts, seq);
    return true;
  }

  _resetBook() {
    this._depthInitialized = false;
    this._depthSyncing = true;
    this._pendingDepth = [];
    super._resetBook();
  }
}
