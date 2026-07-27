// lib/coinbase-international-connector.mjs — Coinbase International BTC-PERP connector

import { TradeOnlyConnector } from './trade-only-connector.mjs';

const WS_URL = 'wss://ws-md.international.coinbase.com';
const PRODUCT_ID = 'BTC-PERP';
const CHANNEL = 'MATCH';

const toNumber = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const normalizeTs = (value) => {
  if (value == null) return Date.now();
  const n = Number(value);
  if (Number.isFinite(n)) {
    if (n > 1e15) return Math.floor(n / 1000);
    if (n > 1e12) return Math.floor(n);
    if (n > 1e9) return Math.floor(n * 1000);
    return Math.floor(n);
  }
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : Date.now();
};

const normalizeSide = (trade) => {
  if (!trade || typeof trade !== 'object') return null;
  const side = String(trade.side ?? trade.aggressor_side ?? trade.aggressorSide ?? '').toUpperCase();
  if (side === 'BUY') return 'buy';
  if (side === 'SELL') return 'sell';

  const makerSide = String(trade.maker_side ?? trade.makerSide ?? '').toUpperCase();
  if (makerSide === 'BUY') return 'sell';
  if (makerSide === 'SELL') return 'buy';

  return null;
};

const collectTrades = (data) => {
  if (!data || typeof data !== 'object') return [];
  if (Array.isArray(data.events)) {
    const out = [];
    for (const event of data.events) {
      if (!event || typeof event !== 'object') continue;
      if (Array.isArray(event.trades)) out.push(...event.trades);
      else if (event.price != null || event.size != null || event.qty != null) out.push(event);
    }
    if (out.length) return out;
  }
  if (Array.isArray(data.trades)) return data.trades;
  if (Array.isArray(data.data)) return data.data;
  if (data.price != null || data.size != null || data.qty != null) return [data];
  return [];
};

export class CoinbaseInternationalConnector extends TradeOnlyConnector {
  constructor(config) {
    super(config, {
      market: 'coinbase_international_perp',
      wsUrl: config.wsUrl || WS_URL,
      restUrl: config.restUrl || '',
    });
    this.supportsDepth = false;
  }

  subscribe() {
    const auth = this.config?.auth || {};
    const { key, passphrase, signature, time } = auth;
    if (!key || !passphrase || !signature || !time) {
      this.emit('error', {
        market: this.market,
        message: 'Coinbase International auth missing; set config.markets.coinbase_international_perp.auth.{key,passphrase,signature,time}',
      });
      return;
    }

    this._ws.send(JSON.stringify({
      type: 'SUBSCRIBE',
      product_ids: [PRODUCT_ID],
      channels: [CHANNEL],
      time: String(time),
      key,
      passphrase,
      signature,
    }));
  }

  _onMessage(data) {
    if (!data || typeof data !== 'object') return;
    const channel = String(data.channel || '').toUpperCase();
    const type = String(data.type || '').toUpperCase();
    if (type === 'SUBSCRIBED' || type === 'SUBSCRIBE' || channel === 'SUBSCRIPTIONS' || type === 'HEARTBEAT') return;
    if (channel !== CHANNEL && type !== CHANNEL && data.product_id !== PRODUCT_ID) return;

    const trades = collectTrades(data);
    for (const t of trades) {
      const price = toNumber(t.price ?? t.px ?? t.match_price ?? t.fill_price);
      const qty = toNumber(t.size ?? t.qty ?? t.q ?? t.fill_size);
      let side = normalizeSide(t);
      if (!side && t.side != null) {
        const raw = String(t.side).toUpperCase();
        if (raw === 'BUY') side = 'buy';
        if (raw === 'SELL') side = 'sell';
      }
      const ts = normalizeTs(t.time ?? t.ts ?? t.timestamp ?? data.time ?? data.ts);
      const tradeId = String(t.trade_id ?? t.tradeId ?? t.id ?? t.match_id ?? `${ts}-${price}-${qty}`);
      if (price == null || qty == null || !side) continue;
      this._emitTrade(price, qty, side, ts, tradeId);
    }
  }
}
