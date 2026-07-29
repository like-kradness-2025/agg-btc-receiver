// lib/full-book.mjs — Orderbook (FullBook) data structure for btc-receiver v3.00

/**
 * @typedef {Object} FullBookOptions
 * @property {number} [maxLevels=0] 0 = unlimited
 */

/**
 * @typedef {Object} BookSnapshot
 * @property {string} market
 * @property {number} ts
 * @property {number} [seq]
 * @property {Array<[string, string]>} bids
 * @property {Array<[string, string]>} asks
 * @property {number} bidLevelCount
 * @property {number} askLevelCount
 */

export class FullBook {
  /** @type {string} */ market;
  /** @type {Map<string, string>} */ bids;
  /** @type {Map<string, string>} */ asks;

  /** @type {string|null} */ _bestBidStr;
  /** @type {string|null} */ _bestAskStr;
  /** @type {number} */ _maxLevels;
  /** @type {number|null} */ _lastSeq;
  /** @type {number} */ _ts;

  /**
   * @param {string} market
   * @param {FullBookOptions} [options]
   */
  constructor(market, options = {}) {
    this.market = market;
    this.bids = new Map();
    this.asks = new Map();
    this._bestBidStr = null;
    this._bestAskStr = null;
    this._maxLevels = options.maxLevels ?? 0;
    this._lastSeq = null;
    this._ts = 0;
  }

  // ====== Update methods ======

  /**
   * Full snapshot replace.
   * @param {Array<[string, string]>} bids sorted desc
   * @param {Array<[string, string]>} asks sorted asc
   * @param {number} [seq]
   */
  applySnapshot(bids, asks, seq) {
    this.bids.clear();
    this.asks.clear();

    for (const [price, qty] of bids) {
      if (this._maxLevels > 0 && this.bids.size >= this._maxLevels) break;
      if (qty === '' || Number(qty) === 0) continue;
      this.bids.set(price, qty);
    }
    for (const [price, qty] of asks) {
      if (this._maxLevels > 0 && this.asks.size >= this._maxLevels) break;
      if (qty === '' || Number(qty) === 0) continue;
      this.asks.set(price, qty);
    }

    this._recalcBestBid();
    this._recalcBestAsk();
    this._lastSeq = seq ?? null;
    this._ts = Date.now();
  }

  /**
   * Single-level update (Binance style). qty="" or numeric zero means delete.
   * @param {'bid'|'ask'} side
   * @param {string} price
   * @param {string} qty
   * @param {number} [seq]
   */
  applyDiff(side, price, qty, seq) {
    this.applyDiffs(side, [[price, qty]], seq);
  }

  /**
   * Apply one exchange update message as a batch, then trim the subscribed
   * depth.  Kraken delete/insert messages rely on this ordering: trimming
   * between rows can discard a level before the complete message is applied.
   * @param {'bid'|'ask'} side
   * @param {Array<[string, string]>} levels
   * @param {number} [seq]
   */
  applyDiffs(side, levels, seq) {
    const map = side === 'bid' ? this.bids : this.asks;
    let bestWasDeleted = false;
    let nextBest = side === 'bid' ? this._bestBidStr : this._bestAskStr;
    for (const [price, qty] of levels ?? []) {
      if (qty === '' || Number(qty) === 0) {
        if (price === nextBest) bestWasDeleted = true;
        map.delete(price);
      } else {
        map.set(price, qty);
        if (nextBest === null || this._comparePrice(price, nextBest, side) > 0) nextBest = price;
      }
    }
    if (bestWasDeleted) {
      if (side === 'bid') this._recalcBestBid();
      else this._recalcBestAsk();
    } else if (side === 'bid') this._bestBidStr = nextBest;
    else this._bestAskStr = nextBest;
    this._trimToMaxLevels(side);
    if (seq != null) this._lastSeq = seq;
    this._ts = Date.now();
  }

  // ====== Query methods ======

  /** @returns {string|null} */
  getBestBid() { return this._bestBidStr; }

  /** @returns {string|null} */
  getBestAsk() { return this._bestAskStr; }

  /** @returns {number|null} */
  getMid() {
    if (this._bestBidStr === null || this._bestAskStr === null) return null;
    return (parseFloat(this._bestBidStr) + parseFloat(this._bestAskStr)) / 2;
  }

  /** @returns {number|null} */
  getSpread() {
    if (this._bestBidStr === null || this._bestAskStr === null) return null;
    return parseFloat(this._bestAskStr) - parseFloat(this._bestBidStr);
  }

  /** @returns {number|null} */
  getLastSeq() {
    return this._lastSeq;
  }

  /**
   * @param {number} seq
   */
  setLastSeq(seq) {
    this._lastSeq = seq;
  }

  /**
   * Get top N levels.
   * @param {number} [levels=10]
   * @returns {{ bids: Array<[string, string]>, asks: Array<[string, string]> }}
   */
  getTop(levels = 10) {
    return {
      bids: this._getSortedSlice(this.bids, 'bid', levels),
      asks: this._getSortedSlice(this.asks, 'ask', levels),
    };
  }

  /**
   * Calculate imbalance within a percentage from best price.
   * @param {number} [percent=1.0]
   * @returns {number|null} -1 to +1
   */
  getImbalance(percent = 1.0) {
    const { bidQty, askQty } = this._getDepthAtPercent(percent);
    if (bidQty === 0 && askQty === 0) return null;
    return (bidQty - askQty) / (bidQty + askQty);
  }

  /** @returns {{ bids: number, asks: number }} */
  getLevelCount() {
    return { bids: this.bids.size, asks: this.asks.size };
  }

  /** @returns {boolean} true if both bid and ask sides are empty. */
  isEmpty() {
    return this.bids.size === 0 && this.asks.size === 0;
  }

  /** Clear all data. */
  clear() {
    this.bids.clear();
    this.asks.clear();
    this._bestBidStr = null;
    this._bestAskStr = null;
    this._lastSeq = null;
    this._ts = 0;
  }

  /**
   * Full snapshot output.
   * @param {number} [ts]
   * @returns {BookSnapshot}
   */
  toSnapshot(ts) {
    const bidsArr = [];
    for (const [price, qty] of this._getSortedEntries(this.bids, 'bid')) {
      bidsArr.push([price, qty]);
    }
    const asksArr = [];
    for (const [price, qty] of this._getSortedEntries(this.asks, 'ask')) {
      asksArr.push([price, qty]);
    }
    return {
      market: this.market,
      ts: ts ?? this._ts,
      seq: this._lastSeq ?? undefined,
      bids: bidsArr,
      asks: asksArr,
      bidLevelCount: this.bids.size,
      askLevelCount: this.asks.size,
    };
  }

  // ====== Internal helpers ======

  _recalcBestBid() {
    let best = null;
    for (const price of this.bids.keys()) {
      if (best === null || this._comparePrice(price, best, 'bid') > 0) {
        best = price;
      }
    }
    this._bestBidStr = best;
  }

  _recalcBestAsk() {
    let best = null;
    for (const price of this.asks.keys()) {
      if (best === null || this._comparePrice(price, best, 'ask') > 0) {
        best = price;
      }
    }
    this._bestAskStr = best;
  }

  /**
   * Compare two price strings.
   * @param {string} a
   * @param {string} b
   * @param {'bid'|'ask'} side
   * @returns {number} positive if a is better than b for given side
   */
  _comparePrice(a, b, side) {
    const aNum = parseFloat(a);
    const bNum = parseFloat(b);
    if (side === 'bid') return aNum - bNum;
    return bNum - aNum;
  }

  /**
   * Trim levels on one side if exceeding maxLevels.
   * @param {'bid'|'ask'} side
   */
  _trimToMaxLevels(side) {
    if (this._maxLevels <= 0) return;
    const map = side === 'bid' ? this.bids : this.asks;
    if (map.size <= this._maxLevels) return;
    const sorted = this._getSortedEntries(map, side);
    while (map.size > this._maxLevels) {
      const worst = sorted.pop();
      if (worst) map.delete(worst[0]);
    }
  }

  /**
   * Get Map entries sorted by best-first.
   * @param {Map<string, string>} map
   * @param {'bid'|'ask'} side
   * @returns {Array<[string, string]>}
   */
  _getSortedEntries(map, side) {
    const entries = [...map.entries()];
    if (side === 'bid') {
      entries.sort((a, b) => parseFloat(b[0]) - parseFloat(a[0])); // desc
    } else {
      entries.sort((a, b) => parseFloat(a[0]) - parseFloat(b[0])); // asc
    }
    return entries;
  }

  /**
   * Get top N sorted entries.
   * @param {Map<string, string>} map
   * @param {'bid'|'ask'} side
   * @param {number} n
   * @returns {Array<[string, string]>}
   */
  _getSortedSlice(map, side, n) {
    return this._getSortedEntries(map, side).slice(0, n);
  }

  /**
   * Calculate total bid/ask qty within a percentage from best.
   * @param {number} percent
   * @returns {{ bidQty: number, askQty: number }}
   */
  _getDepthAtPercent(percent) {
    const bestBid = this._bestBidStr ? parseFloat(this._bestBidStr) : null;
    const bestAsk = this._bestAskStr ? parseFloat(this._bestAskStr) : null;
    let bidQty = 0, askQty = 0;

    if (bestBid !== null) {
      const threshold = bestBid * (1 - percent / 100);
      for (const [priceStr, qtyStr] of this.bids) {
        if (parseFloat(priceStr) >= threshold) {
          bidQty += parseFloat(qtyStr);
        }
      }
    }
    if (bestAsk !== null) {
      const threshold = bestAsk * (1 + percent / 100);
      for (const [priceStr, qtyStr] of this.asks) {
        if (parseFloat(priceStr) <= threshold) {
          askQty += parseFloat(qtyStr);
        }
      }
    }
    return { bidQty, askQty };
  }
}
