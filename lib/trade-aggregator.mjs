// lib/trade-aggregator.mjs — 1-second trade aggregation for btc-receiver v3.00

export { classifyTradeNotional } from './trade-size-buckets.mjs';
import { classifyTradeNotional } from './trade-size-buckets.mjs';

/**
 * @typedef {Object} TradeAggregatedRow
 * @property {number} ts
 * @property {string} market
 * @property {number} open
 * @property {number} high
 * @property {number} low
 * @property {number} close
 * @property {number} volume
 * @property {number} buy_volume
 * @property {number} sell_volume
 * @property {number} trade_count
 * @property {number} buy_count
 * @property {number} sell_count
 * @property {number} vwap
 * @property {number} small_volume
 * @property {number} medium_volume
 * @property {number} large_volume
 * @property {number} small_count
 * @property {number} medium_count
 * @property {number} large_count
 */

export class TradeAggregator {
  /** @type {string} */ market;
  /** @type {number} */ _windowMs;
  /** @type {number} */ _windowStartTs;
  /** @type {import('./events.mjs').TradeEvent[]} */ _buffer;

  constructor(market, windowMs = 1000) {
    this.market = market;
    this._windowMs = windowMs;
    this._buffer = [];
    this._windowStartTs = 0;
  }

  /**
   * Add a trade event to the buffer.
   * @param {import('./events.mjs').TradeEvent} trade
   */
  addTrade(trade) {
    if (this._buffer.length === 0) {
      this._windowStartTs = trade.ts;
    }
    this._buffer.push(trade);
  }

  /**
   * Flush aggregated result if window has elapsed.
   * @param {number} now - current time in ms
   * @returns {TradeAggregatedRow|null}
   */
  flushIfDue(now) {
    if (this._buffer.length === 0) return null;
    if (now - this._windowStartTs < this._windowMs) return null;

    const result = this._aggregate();
    this._buffer = [];
    this._windowStartTs = 0;
    return result;
  }

  /** Reset buffer (e.g. on reconnect). */
  reset() {
    this._buffer = [];
    this._windowStartTs = 0;
  }

  /** @returns {number} */
  getPendingCount() {
    return this._buffer.length;
  }

  /**
   * Immediately flush and return current window aggregate (even if window not elapsed).
   * Used during shutdown to avoid losing pending trades.
   * @returns {TradeAggregatedRow|null}
   */
  flushNow() {
    if (this._buffer.length === 0) return null;
    const result = this._aggregate();
    this._buffer = [];
    this._windowStartTs = 0;
    return result;
  }

  // ====== Internal ======

  _aggregate() {
    const first = this._buffer[0];
    const last = this._buffer[this._buffer.length - 1];
    let open = 0, high = -Infinity, low = Infinity, close = 0;
    let volume = 0, buyVolume = 0, sellVolume = 0;
    let tradeCount = 0, buyCount = 0, sellCount = 0;
    let vwapNum = 0, vwapDen = 0;
    const sizeVol = { small: 0, medium: 0, large: 0 };
    const sizeCnt = { small: 0, medium: 0, large: 0 };

    for (const t of this._buffer) {
      const price = t.price;
      const qty = t.qty;
      const side = t.side;

      if (tradeCount === 0) open = price;
      if (price > high) high = price;
      if (price < low) low = price;
      close = price;

      volume += qty;
      if (side === 'buy') { buyVolume += qty; buyCount++; }
      else { sellVolume += qty; sellCount++; }

      vwapNum += price * qty;
      vwapDen += qty;
      tradeCount++;

      const klass = classifyTradeNotional(price, qty);
      sizeVol[klass] += qty;
      sizeCnt[klass]++;
    }

    return {
      ts: this._windowStartTs,
      market: this.market,
      open,
      high: high === -Infinity ? 0 : high,
      low: low === Infinity ? 0 : low,
      close,
      volume,
      buy_volume: buyVolume,
      sell_volume: sellVolume,
      trade_count: tradeCount,
      buy_count: buyCount,
      sell_count: sellCount,
      vwap: vwapDen > 0 ? Math.round((vwapNum / vwapDen) * 100) / 100 : 0,
      small_volume: sizeVol.small,
      medium_volume: sizeVol.medium,
      large_volume: sizeVol.large,
      small_count: sizeCnt.small,
      medium_count: sizeCnt.medium,
      large_count: sizeCnt.large,
    };
  }
}
