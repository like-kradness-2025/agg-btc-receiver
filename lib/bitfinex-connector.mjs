// lib/bitfinex-connector.mjs — Bitfinex BTC/USD spot connector

import { TradeOnlyConnector } from './trade-only-connector.mjs';

const WS_URL = 'wss://api-pub.bitfinex.com/ws/2';
const SYMBOL = 'tBTCUSD';

const toNumber = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const normalizeSide = (amount) => {
  const n = Number(amount);
  if (!Number.isFinite(n)) return null;
  return n < 0 ? 'sell' : 'buy';
};

const normalizeTs = (value) => {
  // Astra P2-3: type-check BEFORE numeric coercion — Number(null)===0,
  // Number('')===0 and Number(false)===0 would otherwise fabricate a known
  // ts=0 (source_event_ts_ms=0 / source_event_time_known=true) for missing
  // timestamps. Invalid/missing stays null; callers fail-closed on null.
  if (value === null || value === undefined) return null;
  if (typeof value !== 'number' && typeof value !== 'string') return null;
  if (typeof value === 'string' && value.trim() === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return null;
  if (n > 1e15) return Math.floor(n / 1000);
  return Math.floor(n);
};

// Bitfinex book frames carry no exchange event timestamp: ts is the local
// processing time. Issue #12 marks that explicitly so source_event_ts_ms
// never inherits a locally synthesized wall clock.
const LOCAL_DEPTH_META = Object.freeze({
  source_event_ts_ms: null,
  source_event_time_known: false,
  event_time_source: 'local',
});

const emitTrade = (conn, trade) => {
  if (!trade) return;
  const arr = Array.isArray(trade);
  const id = arr ? trade[0] : (trade.id ?? trade.trade_id ?? trade.tradeId);
  const ts = normalizeTs(arr ? trade[1] : (trade.mts ?? trade.timestamp ?? trade.time));
  const amount = toNumber(arr ? trade[2] : (trade.amount ?? trade.qty ?? trade.size));
  const price = toNumber(arr ? trade[3] : (trade.price ?? trade.p));
  if (price == null || amount == null) return;
  const side = normalizeSide(amount);
  if (!side) return;
  const qty = Math.abs(amount);
  conn._emitTrade(price, qty, side, ts, String(id ?? `${ts}-${price}-${qty}`));
};

export class BitfinexConnector extends TradeOnlyConnector {
  constructor(config) {
    super(config, {
      market: 'bitfinex_spot',
      wsUrl: config.wsUrl || WS_URL,
      restUrl: config.restUrl || '',
    });
    this._bookChanId = null;
    this._bookSnapshotReceived = false;
  }

  subscribe() {
    this._ws.send(JSON.stringify({
      event: 'subscribe',
      channel: 'trades',
      symbol: SYMBOL,
    }));
    this._ws.send(JSON.stringify({
      event: 'subscribe',
      channel: 'book',
      symbol: SYMBOL,
      prec: 'P0',
      freq: 'F0',
      len: '25',
    }));
  }

  /** 保守中 (20060〜20061) はこちらの判断での復旧を保留する。時間切れで自動解除 (M3)。 */
  _recoveryPaused() {
    const until = Number(this._maintenanceUntilMs) || 0;
    if (until === 0) return false;
    if (Date.now() > until) {
      this._maintenanceUntilMs = 0;
      return false;
    }
    return true;
  }

  _onMessage(data) {
    if (!data) return;
    if (!Array.isArray(data)) {
      if (data.event === 'subscribed') {
        if (data.channel === 'book') {
          this._bookChanId = data.chanId;
        }
        return;
      }
      if (data.event === 'info' || data.event === 'error') {
        // Bitfinex の info コード (公式 ws-general の Info Codes):
        //   20051 Stop/Restart (please reconnect) → 自分から張り直す
        //   20060 保守開始 (活動を止めろ / 20061 まで最大120秒) → こちらの復旧を保留 (受信は継続)
        //   20061 保守終了 → 保留解除 (全ch再購読は M2 実装後)
        const code = Number(data.code);
        if (code === 20051) {
          this._proactiveReconnect('bitfinex 20051 stop/restart');
        } else if (code === 20060) {
          this._maintenanceUntilMs = Date.now() + 300000; // 120秒 ± 余裕。20061 が来なければ自動解除
          console.error(`[${this.market}] bitfinex maintenance start (20060): 復旧を保留`);
        } else if (code === 20061) {
          this._maintenanceUntilMs = 0;
          console.error(`[${this.market}] bitfinex maintenance end (20061): 保留解除 (全ch再購読は未実装)`);
        }
        return;
      }
      if (data.type === 'trade') {
        emitTrade(this, data.data ?? data.trade ?? data);
      }
      return;
    }

    // Array-based messages: [chanId, ...]
    const chanId = data[0];

    // Book channel
    if (this._bookChanId !== null && chanId === this._bookChanId) {
      this._handleBook(data);
      return;
    }

    // Trade channel
    // [chanId, 'te'|'tu', [id, mts, amount, price]]
    // Note: 'te' is immediate but amount may be revised; 'tu' is confirmed final.
    // Only emit 'tu' to avoid double-counting the same trade.
    if (typeof data[1] === 'string') {
      const kind = data[1];
      if (kind !== 'tu') return;
      emitTrade(this, data[2]);
      return;
    }

    // Snapshot: [chanId, [[id, mts, amount, price], ...]]
    if (Array.isArray(data[1])) {
      for (const trade of data[1]) emitTrade(this, trade);
    }
  }

  _handleBook(data) {
    // Heartbeat: [chanId, 'hb']
    if (data[1] === 'hb') return;

    const payload = data[1];
    if (!Array.isArray(payload)) return;

    // Snapshot: [chanId, [[PRICE, COUNT, AMOUNT], ...]]
    if (Array.isArray(payload[0])) {
      const bids = [];
      const asks = [];

      for (const entry of payload) {
        const [price, count, amount] = entry;
        const p = toNumber(price);
        if (p == null) continue;
        const qty = Math.abs(toNumber(amount) || 0);
        const side = Number(amount) > 0 ? 'bid' : 'ask';
        if (side === 'bid') {
          bids.push([String(p), String(qty)]);
        } else {
          asks.push([String(p), String(qty)]);
        }
      }

      this.book.applySnapshot(bids, asks);
      this._bookSnapshotReceived = true;
      this._emitDepth('snapshot', bids, asks, Date.now(), null, LOCAL_DEPTH_META);
      return;
    }

    // Single update: [chanId, [PRICE, COUNT, AMOUNT]]
    if (typeof payload[0] === 'number') {
      const [price, count, amount] = payload;
      const p = toNumber(price);
      if (p == null) return;
      const qty = count === 0 ? '' : String(Math.abs(toNumber(amount) || 0));
      const side = Number(amount) > 0 ? 'bid' : 'ask';
      this.book.applyDiff(side, String(p), qty, null);
      const bids = side === 'bid' ? [[String(p), String(qty)]] : [];
      const asks = side === 'ask' ? [[String(p), String(qty)]] : [];

      const type = this._bookSnapshotReceived ? 'update' : 'snapshot';
      this._emitDepth(type, bids, asks, Date.now(), null, LOCAL_DEPTH_META);
      return;
    }
  }
}
