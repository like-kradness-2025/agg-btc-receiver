// lib/feature-accumulator.mjs — 1-second feature accumulator + JSONL append
//
// Replaces raw depth/snapshot/fairprice JSONL storage with
// pre-computed 1-second aggregated features written to JSONL (data/1s_features/{date}/{market}.jsonl).
//
// Usage:
//   const acc = new FeatureAccumulator('data/1s_features');
//   acc.feedTrade(market, tradeEvent);
//   acc.feedDepth(market, depthEvent);  // bid/ask diffs
//   acc.feedBook(market, book);         // full book state at second boundary
//   await acc.flush();                  // write buffered features to JSONL

import fs from 'node:fs';
import path from 'node:path';
import { BufferedWriter } from './buffered-writer.mjs';
import { classifyTradeNotional } from './trade-size-buckets.mjs';

// ── Helpers ────────────────────────────────────────────────────────────

/** Get price level qty from a Map, parsed as float. */
function levelQty(map, priceKey) {
  const qty = map.get(priceKey) ?? map.get(String(priceKey));
  return qty ? parseFloat(qty) : 0;
}

/** Get bps from a mid price and a given price. */
function priceToBps(mid, price) {
  if (!mid || !price || mid <= 0) return Infinity;
  return Math.abs((price - mid) / mid) * 10000;
}

/** Compute ring-bucketed depth from book state.
 *  Inner rings (0-1, 1-2, 2-5) use best bid/ask as reference.
 *  Outer rings (5-25, 25-100) use mid as reference.
 */
function computeRingDepth(book, mid, bestBid, bestAsk) {
  if (!book || !mid) return null;
  const result = {};
  const ringDefs = [
    { name: '0_1bps',  lo: 0,   hi: 1 },
    { name: '1_2bps',  lo: 1,   hi: 2 },
    { name: '2_5bps',  lo: 2,   hi: 5 },
    { name: '5_25bps',  lo: 5,   hi: 25 },
    { name: '25_100bps', lo: 25, hi: 100 },
  ];

  for (const { name, lo, hi } of ringDefs) {
    let bidVol = 0, askVol = 0;
    const isInner = hi <= 5;

    // Bids
    for (const [priceStr, qtyStr] of book.bids) {
      const qty = parseFloat(qtyStr);
      if (qty <= 0) continue;
      const price = parseFloat(priceStr);
      const ref = isInner && bestBid ? bestBid : mid;
      const bps = ref > 0 ? (ref - price) / ref * 10000 : Infinity;
      if (bps > lo && bps <= hi) bidVol += qty;
    }
    // Asks
    for (const [priceStr, qtyStr] of book.asks) {
      const qty = parseFloat(qtyStr);
      if (qty <= 0) continue;
      const price = parseFloat(priceStr);
      const ref = isInner && bestAsk ? bestAsk : mid;
      const bps = ref > 0 ? (price - ref) / ref * 10000 : Infinity;
      if (bps > lo && bps <= hi) askVol += qty;
    }
    result[`bid_${name}`] = bidVol;
    result[`ask_${name}`] = askVol;
  }
  return result;
}

/** UTC date string YYYY-MM-DD */
function utcDateStr(d) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

// ── RingBuffer ──────────────────────────────────────────────────────────

/** Fixed-size ring buffer with stats methods. */
class RingBuffer {
  constructor(capacity) {
    this.capacity = capacity;
    this.buffer = [];
  }

  push(value) {
    this.buffer.push(value);
    if (this.buffer.length > this.capacity) {
      this.buffer.shift();
    }
  }

  get length() { return this.buffer.length; }
  isFull() { return this.buffer.length >= this.capacity; }
  sum() { return this.buffer.reduce((a, b) => a + b, 0); }

  /** Sample standard deviation (divides by N, not N-1). Returns null if < 2 entries. */
  std() {
    if (this.buffer.length < 2) return null;
    const mean = this.sum() / this.buffer.length;
    const variance = this.buffer.reduce((s, v) => s + (v - mean) ** 2, 0) / this.buffer.length;
    return Math.sqrt(variance);
  }

  values() { return [...this.buffer]; }
}

// ── FeatureAccumulator ─────────────────────────────────────────────────

function normalizeBurstSide(side) {
  if (side === 'buy' || side === 'BUY' || side === true || side === 'true' || side === '1') return 'buy';
  return 'sell';
}

function burstOverlapsSecond(burst, second) {
  const bucketEnd = second + 1000;
  return burst.startTs < bucketEnd && burst.endTs >= second;
}

const BOOK_COVERAGE_TIERS = {
  coinbase_spot: 'tier_a_full_book_like',
  bitmex_perp: 'tier_a_full_book_like',
  binance_spot: 'tier_a_full_book_like',
  binance_spot_usdc: 'tier_a_full_book_like',
  kraken_spot: 'tier_a_full_book_like',
  binance_perp: 'tier_b_snapshot_limited_mid_depth',
  binance_perp_btcusdc: 'tier_b_snapshot_limited_mid_depth',
  bybit_perp: 'tier_b_snapshot_limited_mid_depth',
  okx_perp: 'tier_c_bounded_depth_near_book',
  okx_spot: 'tier_c_bounded_depth_near_book',
  bybit_spot: 'tier_c_bounded_depth_near_book',
  bitstamp_spot: 'tier_c_bounded_depth_near_book',
  bitfinex_spot: 'tier_c_bounded_depth_near_book',
  crypto_com_spot: 'tier_c_bounded_depth_near_book',
  hyperliquid_perp: 'tier_c_bounded_depth_near_book',
};

function bookCoverageTier(market) {
  return BOOK_COVERAGE_TIERS[market] ?? 'tier_unknown';
}

export class FeatureAccumulator {
  /**
   * @param {string} outputBase - base dir for Parquet output (e.g. 'data/1s_features')
   * @param {object} [options]
   * @param {number} [options.flushIntervalMs=60000] - flush buffered rows to Parquet every N ms
   * @param {number[]} [options.bpsLevels=[1,2,5,25,100]] - bps buckets for depth state
   */
  constructor(outputBase, options = {}) {
    this._outputBase = outputBase;
    this._flushIntervalMs = options.flushIntervalMs ?? 60000;

    /** Per-market 1s feature rows, keyed by market. Map<ts, row> */
    this._buffers = new Map();

    /** Per-market depth flow accumulators. Map<market, FlowState> */
    this._flow = new Map();

    /** Per-market trade accumulators for current partial second. Map<market, TradeState> */
    this._tradeAccums = new Map();

    /** Per-market last-second L1 state for boundary tracking */
    this._lastL1 = new Map();

    /** Per-market event counters for current second */
    this._eventCounts = new Map();

    /** Per-market ring buffers for rolling window features */
    this._ringBufs = new Map();  // Map<market, { midReturns: RingBuffer(10), cvd10: RingBuffer(10), cvd30: RingBuffer(30), adverseQueue }>

    /** Per-market burst state for trade-only burst features */
    this._burstStates = new Map();
    this._burstGapThresholdMs = options.burstGapThresholdMs ?? 50;
    this._burstMaxDurationMs = options.burstMaxDurationMs ?? 1000;
    this._burstTickSizeByMarket = options.burstTickSizeByMarket ?? {};
    this._burstTickSizeByVenue = options.burstTickSizeByVenue ?? {};

    this._lastFlush = 0;
    this._closed = false;

    // Writer cache for JSONL append (keyed by "YYYY-MM-DD/market")
    this._writers = new Map();
    this._currentDate = null;

    // 30s bo...[truncated]
    this._bookWriters = new Map();
    this._last30sBookFlush = new Map(); // Map<market, last 30s bucket ts>
  }

  // ── JSONL writer management ──────────────────────────────────────────

  /** Get or create a BufferedWriter for a market+date partition. */
  _getWriter(market, dateStr) {
    const key = `${dateStr}/${market}`;
    let w = this._writers.get(key);
    if (!w) {
      const dir = path.join(this._outputBase, dateStr);
      fs.mkdirSync(dir, { recursive: true });
      w = new BufferedWriter(path.join(dir, `${market}.jsonl`), {
        flushIntervalMs: 1000,
        idleCloseMs: 120000, // >60s flush interval avoids per-flush reopen
      });
      this._writers.set(key, w);
    }
    return w;
  }

  /** Get or create a BufferedWriter for 30s book-bin snapshots. */
  _getBookWriter(market, dateStr) {
    const key = `${dateStr}/${market}`;
    let w = this._bookWriters.get(key);
    if (!w) {
      const dir = path.join(this._outputBase, '..', '30s_book', dateStr);
      fs.mkdirSync(dir, { recursive: true });
      w = new BufferedWriter(path.join(dir, `${market}.jsonl`), {
        flushIntervalMs: 1000,
        idleCloseMs: 120000,
      });
      this._bookWriters.set(key, w);
    }
    return w;
  }

  /**
   * Aggregate a FullBook into $1 price bins.
   * Bids use floor(price), asks use ceil(price) to avoid touch-bin ambiguity.
   * @param {object} book - FullBook instance
   * @param {number} ts - snapshot timestamp ms
   * @param {string} market
   * @returns {object} { ts, market, best_bid, best_ask, bids, asks }
   */
  _buildBookSnapshot(book, ts, market) {
    const bestBidStr = book.getBestBid();
    const bestAskStr = book.getBestAsk();
    const best_bid = bestBidStr ? parseFloat(bestBidStr) : null;
    const best_ask = bestAskStr ? parseFloat(bestAskStr) : null;
    const best_bid_qty = bestBidStr ? levelQty(book.bids, bestBidStr) : 0;
    const best_ask_qty = bestAskStr ? levelQty(book.asks, bestAskStr) : 0;
    const mid = book.getMid();
    const spread = (best_bid !== null && best_ask !== null) ? best_ask - best_bid : null;
    const spread_bps = spread !== null && mid ? (spread / mid) * 10000 : null;

    // Aggregate bids into $1 bins (floor), descending
    const bidBins = new Map();
    let sum_bid_qty = 0;
    for (const [priceStr, qtyStr] of book.bids) {
      const qty = parseFloat(qtyStr);
      if (qty <= 0) continue;
      const bin = Math.floor(parseFloat(priceStr));
      bidBins.set(bin, (bidBins.get(bin) || 0) + qty);
      sum_bid_qty += qty;
    }
    const bids = Array.from(bidBins.entries())
      .sort((a, b) => b[0] - a[0])
      .map(([price, qty]) => [price, qty]);

    // Aggregate asks into $1 bins (ceil), ascending
    const askBins = new Map();
    let sum_ask_qty = 0;
    for (const [priceStr, qtyStr] of book.asks) {
      const qty = parseFloat(qtyStr);
      if (qty <= 0) continue;
      const bin = Math.ceil(parseFloat(priceStr));
      askBins.set(bin, (askBins.get(bin) || 0) + qty);
      sum_ask_qty += qty;
    }
    const asks = Array.from(askBins.entries())
      .sort((a, b) => a[0] - b[0])
      .map(([price, qty]) => [price, qty]);

    const has_bid = best_bid !== null;
    const has_ask = best_ask !== null;

    return {
      schema_version: 1,
      stream: 'book_bins_30s',
      ts,
      bucket_start_ts: ts,
      bucket_end_ts: ts + 30000,
      snapshot_ts: Date.now(),
      market,
      coverage_tier: bookCoverageTier(market),
      bin_mode: 'abs_usd',
      bin_size: 1,
      bid_bin_rule: 'floor',
      ask_bin_rule: 'ceil',
      qty_unit: 'base',
      best_bid,
      best_ask,
      best_bid_qty,
      best_ask_qty,
      mid,
      spread,
      spread_bps,
      has_bid,
      has_ask,
      book_valid: has_bid && has_ask,
      bid_level_count_raw: book.bids.size,
      ask_level_count_raw: book.asks.size,
      bid_bin_count: bids.length,
      ask_bin_count: asks.length,
      sum_bid_qty,
      sum_ask_qty,
      bids,
      asks,
    };
  }

  /** Rotate date partition: close writers from old date, start new date. */
  async _ensureDate(dateStr) {
    if (dateStr === this._currentDate) return;
    if (this._currentDate !== null) {
      const oldDate = this._currentDate;
      this._currentDate = dateStr;
      const promises = [];
      for (const [key, w] of this._writers) {
        if (key.startsWith(oldDate + '/')) promises.push(w.close());
      }
      await Promise.allSettled(promises);
      for (const key of this._writers.keys()) {
        if (key.startsWith(oldDate + '/')) this._writers.delete(key);
      }
    } else {
      this._currentDate = dateStr;
    }
  }

  // ── Ring buffer accessor ──────────────────────────────────────────────

  /** Get or create ring buffers for a market. */
  _getRingBufs(market) {
    let rb = this._ringBufs.get(market);
    if (!rb) {
      rb = {
        midReturns: new RingBuffer(10),
        cvd10: new RingBuffer(10),
        cvd30: new RingBuffer(30),
        adverseQueue: [], // { ts, mid, side }
      };
      this._ringBufs.set(market, rb);
    }
    return rb;
  }

  _getBurstState(market) {
    let state = this._burstStates.get(market);
    if (!state) {
      state = { openBurst: null, closedBursts: [] };
      this._burstStates.set(market, state);
    }
    return state;
  }

  _getTickSize(market) {
    if (this._burstTickSizeByMarket[market] != null) return this._burstTickSizeByMarket[market];
    const venue = market.includes('_') ? market.split('_')[0] : market;
    if (this._burstTickSizeByVenue[venue] != null) return this._burstTickSizeByVenue[venue];
    return 0.01;
  }

  _newSamePriceRun(price, notional) {
    return {
      canonicalPrice: price,
      startTs: null,
      endTs: null,
      printCount: 0,
      notional: 0,
      _seedNotional: notional,
    };
  }

  _startBurst(market, tradeNorm) {
    const run = this._newSamePriceRun(tradeNorm.price, tradeNorm.notional);
    run.startTs = tradeNorm.ts;
    run.endTs = tradeNorm.ts;
    run.printCount = 1;
    run.notional = tradeNorm.notional;
    delete run._seedNotional;
    return {
      market,
      side: tradeNorm.side,
      startTs: tradeNorm.ts,
      endTs: tradeNorm.ts,
      printCount: 1,
      qty: tradeNorm.qty,
      notional: tradeNorm.notional,
      minPrice: tradeNorm.price,
      maxPrice: tradeNorm.price,
      lastPrice: tradeNorm.price,
      samePriceRuns: [run],
      distinctPrices: new Set([tradeNorm.price]),
    };
  }

  _appendTradeToBurst(burst, tradeNorm) {
    burst.endTs = tradeNorm.ts;
    burst.printCount += 1;
    burst.qty += tradeNorm.qty;
    burst.notional += tradeNorm.notional;
    burst.minPrice = Math.min(burst.minPrice, tradeNorm.price);
    burst.maxPrice = Math.max(burst.maxPrice, tradeNorm.price);
    burst.distinctPrices.add(tradeNorm.price);

    const currentRun = burst.samePriceRuns[burst.samePriceRuns.length - 1];
    if (currentRun && currentRun.canonicalPrice === tradeNorm.price) {
      currentRun.endTs = tradeNorm.ts;
      currentRun.printCount += 1;
      currentRun.notional += tradeNorm.notional;
    } else {
      const run = this._newSamePriceRun(tradeNorm.price, tradeNorm.notional);
      run.startTs = tradeNorm.ts;
      run.endTs = tradeNorm.ts;
      run.printCount = 1;
      run.notional = tradeNorm.notional;
      delete run._seedNotional;
      burst.samePriceRuns.push(run);
    }
    burst.lastPrice = tradeNorm.price;
  }

  _finalizeBurst(market, burst) {
    const tickSize = this._getTickSize(market);
    const distinctPriceCount = burst.distinctPrices.size;
    const durationMs = burst.endTs - burst.startTs;
    const isMultilevel = distinctPriceCount >= 2;
    const spanTicks = isMultilevel
      ? Math.round((burst.maxPrice - burst.minPrice) / tickSize)
      : 0;
    return {
      market,
      side: burst.side,
      startTs: burst.startTs,
      endTs: burst.endTs,
      printCount: burst.printCount,
      qty: burst.qty,
      notional: burst.notional,
      minPrice: burst.minPrice,
      maxPrice: burst.maxPrice,
      durationMs,
      distinctPriceCount,
      isMultilevel,
      spanTicks,
      samePriceRuns: burst.samePriceRuns.map((run) => ({
        canonicalPrice: run.canonicalPrice,
        startTs: run.startTs,
        endTs: run.endTs,
        printCount: run.printCount,
        notional: run.notional,
      })),
    };
  }

  _closeBurst(market) {
    const state = this._getBurstState(market);
    if (!state.openBurst) return null;
    const finalized = this._finalizeBurst(market, state.openBurst);
    state.closedBursts.push(finalized);
    state.openBurst = null;
    return finalized;
  }

  _ingestBurstTrade(market, tradeNorm) {
    const state = this._getBurstState(market);
    const gapThresholdMs = this._burstGapThresholdMs;
    const maxBurstDurationMs = this._burstMaxDurationMs;
    const open = state.openBurst;
    if (!open) {
      state.openBurst = this._startBurst(market, tradeNorm);
      return;
    }
    const gapMs = tradeNorm.ts - open.endTs;
    const nextDurationMs = tradeNorm.ts - open.startTs;
    const shouldSplit = open.side !== tradeNorm.side
      || gapMs > gapThresholdMs
      || nextDurationMs > maxBurstDurationMs;
    if (shouldSplit) {
      this._closeBurst(market);
      state.openBurst = this._startBurst(market, tradeNorm);
      return;
    }
    this._appendTradeToBurst(open, tradeNorm);
  }

  _computeBurstSlice1Features(market, second) {
    const state = this._getBurstState(market);
    const allBursts = [...state.closedBursts];
    if (state.openBurst) allBursts.push(this._finalizeBurst(market, state.openBurst));
    const overlapping = allBursts.filter((burst) => burstOverlapsSecond(burst, second));
    const samePriceRuns = overlapping.flatMap((burst) => burst.samePriceRuns.filter((run) => burstOverlapsSecond(run, second)));
    const multilevelBursts = overlapping.filter((burst) => burst.isMultilevel);
    const buyBurstNotional = overlapping.filter((burst) => burst.side === 'buy').reduce((sum, burst) => sum + burst.notional, 0);
    const sellBurstNotional = overlapping.filter((burst) => burst.side === 'sell').reduce((sum, burst) => sum + burst.notional, 0);
    const totalBurstNotional = overlapping.reduce((sum, burst) => sum + burst.notional, 0);
    const maxBurstNotional = overlapping.reduce((max, burst) => Math.max(max, burst.notional), 0);
    return {
      burst_count_1s: overlapping.length,
      max_burst_notional_1s: maxBurstNotional,
      max_burst_prints_1s: overlapping.reduce((max, burst) => Math.max(max, burst.printCount), 0),
      max_burst_duration_ms_1s: overlapping.reduce((max, burst) => Math.max(max, burst.durationMs), 0),
      same_price_burst_count_1s: samePriceRuns.length,
      same_price_burst_max_len_1s: samePriceRuns.reduce((max, run) => Math.max(max, run.printCount), 0),
      same_price_burst_notional_1s: samePriceRuns.reduce((sum, run) => sum + run.notional, 0),
      multilevel_burst_count_1s: multilevelBursts.length,
      multilevel_burst_max_span_ticks_1s: multilevelBursts.reduce((max, burst) => Math.max(max, burst.spanTicks), 0),
      multilevel_burst_notional_1s: multilevelBursts.reduce((sum, burst) => sum + burst.notional, 0),
      buy_burst_notional_1s: buyBurstNotional,
      sell_burst_notional_1s: sellBurstNotional,
      burst_delta_notional_1s: buyBurstNotional - sellBurstNotional,
      largest_burst_share_notional_1s: totalBurstNotional > 0 ? maxBurstNotional / totalBurstNotional : 0,
    };
  }

  _percentileLinear(sortedValues, p) {
    if (sortedValues.length === 0) return null;
    if (sortedValues.length === 1) return sortedValues[0];
    const pos = (sortedValues.length - 1) * p;
    const lower = Math.floor(pos);
    const upper = Math.ceil(pos);
    if (lower === upper) return sortedValues[lower];
    const weight = pos - lower;
    return sortedValues[lower] + (sortedValues[upper] - sortedValues[lower]) * weight;
  }

  _computePrintStructureFeatures(ta) {
    const prints = ta?.prints ?? [];
    if (prints.length === 0) {
      return {
        max_same_side_run_prints_1s: 0,
        side_flip_count_1s: 0,
        same_side_gap_ms_min_1s: null,
        same_side_gap_ms_p25_1s: null,
      };
    }

    let maxRun = 1;
    let currentRun = 1;
    let sideFlipCount = 0;
    const sameSideGaps = [];

    for (let i = 1; i < prints.length; i++) {
      const prev = prints[i - 1];
      const curr = prints[i];
      if (prev.side === curr.side) {
        currentRun += 1;
        sameSideGaps.push(curr.ts - prev.ts);
      } else {
        sideFlipCount += 1;
        currentRun = 1;
      }
      if (currentRun > maxRun) maxRun = currentRun;
    }

    const sortedGaps = [...sameSideGaps].sort((a, b) => a - b);
    return {
      max_same_side_run_prints_1s: maxRun,
      side_flip_count_1s: sideFlipCount,
      same_side_gap_ms_min_1s: sortedGaps.length > 0 ? sortedGaps[0] : null,
      same_side_gap_ms_p25_1s: this._percentileLinear(sortedGaps, 0.25),
    };
  }

  _computeBurstBookValidationFeatures(flow, ta) {
    const classified = ta?.burstBookClassifiedNotional ?? 0;
    const atTouch = ta?.burstBookAtTouchNotional ?? 0;
    const through = ta?.burstBookThroughNotional ?? 0;
    const hasAtTouchTrade = ta?.burstHasAtTouchTrade ?? false;
    const burst_at_touch_ratio_1s = classified > 0 ? atTouch / classified : null;
    const burst_through_ratio_1s = classified > 0 ? through / classified : null;

    if (!flow) {
      return {
        burst_at_touch_ratio_1s,
        burst_through_ratio_1s,
        burst_depletion_count_1s: null,
        burst_replenish_after_touch_count_1s: null,
      };
    }

    return {
      burst_at_touch_ratio_1s,
      burst_through_ratio_1s,
      burst_depletion_count_1s: hasAtTouchTrade ? ((flow._depleteBid ?? 0) + (flow._depleteAsk ?? 0)) : 0,
      burst_replenish_after_touch_count_1s: hasAtTouchTrade ? (flow.bestReplenish ?? 0) : 0,
    };
  }

  _cleanupBurstState(market, flushedSeconds) {
    const state = this._burstStates.get(market);
    if (!state || flushedSeconds.length === 0) return;
    const maxFlushedSecond = Math.max(...flushedSeconds);
    state.closedBursts = state.closedBursts.filter((burst) => burst.endTs >= maxFlushedSecond);
  }

  // ── Feed methods ─────────────────────────────────────────────────────

  /**
   * Feed a trade event.
   * @param {string} market
   * @param {object} trade - { ts, price, qty, side }
   */
  feedTrade(market, trade) {
    const now = trade.ts || Date.now();
    const second = now - (now % 1000);
    const key = second;

    // Get or create trade accumulator for this market+second
    let tsMap = this._tradeAccums.get(market);
    if (!tsMap) {
      tsMap = new Map();
      this._tradeAccums.set(market, tsMap);
    }
    let ta = tsMap.get(key);
    if (!ta) {
      ta = {
        open: null, high: -Infinity, low: Infinity, close: null,
        vwapNum: 0, vwapDen: 0,
        count: 0,
        buyQty: 0, sellQty: 0, buyNotional: 0, sellNotional: 0,
        buySmall: 0, buyMed: 0, buyLarge: 0,
        sellSmall: 0, sellMed: 0, sellLarge: 0,
        buySmallCount: 0, buyMedCount: 0, buyLargeCount: 0,
        sellSmallCount: 0, sellMedCount: 0, sellLargeCount: 0,
        prints: [],
        burstBookClassifiedNotional: 0,
        burstBookUnclassifiedNotional: 0,
        burstBookAtTouchNotional: 0,
        burstBookThroughNotional: 0,
        burstHasAtTouchTrade: false,
      };
      tsMap.set(key, ta);
    }

    const price = parseFloat(trade.price);
    const qty = parseFloat(trade.qty);
    const notional = price * qty;
    const normalizedSide = normalizeBurstSide(trade.side);

    this._ingestBurstTrade(market, {
      ts: now,
      price,
      qty,
      notional,
      side: normalizedSide,
    });

    if (ta.open === null) ta.open = price;
    ta.high = Math.max(ta.high, price);
    ta.low = Math.min(ta.low, price);
    ta.close = price;
    ta.vwapNum += notional;
    ta.vwapDen += qty;
    ta.count++;
    ta.prints.push({ ts: now, side: normalizedSide, price, qty, notional });

    const tier = classifyTradeNotional(price, qty);

    if (trade.side === 'buy' || trade.side === 'BUY' || trade.side === true || trade.side === 'true' || trade.side === '1') {
      ta.buyQty += qty;
      ta.buyNotional += notional;
      if (tier === 'small') { ta.buySmall += qty; ta.buySmallCount++; }
      else if (tier === 'medium') { ta.buyMed += qty; ta.buyMedCount++; }
      else { ta.buyLarge += qty; ta.buyLargeCount++; }
    } else {
      ta.sellQty += qty;
      ta.sellNotional += notional;
      if (tier === 'small') { ta.sellSmall += qty; ta.sellSmallCount++; }
      else if (tier === 'medium') { ta.sellMed += qty; ta.sellMedCount++; }
      else { ta.sellLarge += qty; ta.sellLargeCount++; }
    }

    // Track event counts
    this._incrementCount(market, 'trade_events', second);

    // Track atouch trades: if trade price equals current best bid/ask
    const flow = this._flow.get(market);
    if (flow && price > 0) {
      // B4: Track exchange-to-recv lag (recv_time - exchange_event_time)
      const recvTime = Date.now();
      const eventTime = trade.ts || recvTime;
      flow.exchange_lag_sum_ms += recvTime - eventTime;
      flow.exchange_lag_count++;

      // At-touch: side-aware exact price match against opposite touch.
      const isBuy = normalizedSide === 'buy';
      const isSell = normalizedSide === 'sell';
      const hasRelevantBestState = isBuy ? flow._bestAskPrice !== null : flow._bestBidPrice !== null;
      const atBid = isSell && flow._bestBidPrice !== null && price === flow._bestBidPrice;
      const atAsk = isBuy && flow._bestAskPrice !== null && price === flow._bestAskPrice;

      if (hasRelevantBestState) {
        ta.burstBookClassifiedNotional += notional;
      } else {
        ta.burstBookUnclassifiedNotional += notional;
      }

      if (atBid) {
        flow.bestBidAtouchTradeQty += qty;
        flow.trade_at_touch_qty += qty;
        ta.burstBookAtTouchNotional += notional;
        ta.burstHasAtTouchTrade = true;
      }
      if (atAsk) {
        flow.bestAskAtouchTradeQty += qty;
        flow.trade_at_touch_qty += qty;
        ta.burstBookAtTouchNotional += notional;
        ta.burstHasAtTouchTrade = true;
      }

      // B2: Track adverse selection — aggressive trade at best level
      if (atBid || atAsk) {
        const l1 = this._lastL1.get(market);
        if (l1 && l1.mid) {
          const rb = this._getRingBufs(market);
          rb.adverseQueue.push({ ts: now, mid: l1.mid, side: atBid ? 'sell' : 'buy' });
        }
      }

      // B3: Through trades — price BEYOND the touch level
      // For burst book-validation ratios, follow the burst-book-validation contract:
      // buy through => price > best ask, sell through => price < best bid.
      // Existing quantity counters continue to use the same classification branch here.
      if (!atBid && !atAsk) {
        let isThrough = false;

        if (isBuy && flow._bestAskPrice !== null && price > flow._bestAskPrice) {
          flow.trade_through_qty += qty;
          isThrough = true;
        }
        if (isSell && flow._bestBidPrice !== null && price < flow._bestBidPrice) {
          flow.trade_through_qty += qty;
          isThrough = true;
        }
        if (isThrough) {
          ta.burstBookThroughNotional += notional;
        }
      }
    }
  }

  /**
   * Feed a depth diff event. Accumulates flow (adds/cancels).
   * @param {string} market
   * @param {object} depthEvent - { ts, bids: [[price, qty], ...], asks: [[price, qty], ...] }
   * @param {number} mid - current mid price for bps classification
   */
  feedDepth(market, depthEvent, mid) {
    const now = depthEvent.ts || Date.now();
    const second = now - (now % 1000);
    this._incrementCount(market, 'depth_updates', second);

    // B4: Track exchange-to-recv lag for depth events
    let flow = this._flow.get(market);
    if (flow) {
      const recvTime = Date.now();
      const eventTime = depthEvent.ts || recvTime;
      flow.exchange_lag_sum_ms += recvTime - eventTime;
      flow.exchange_lag_count++;
    }

    // Track flow: compare with previous state
    if (!flow) {
      flow = {
        bidAddNear: 0, bidCancelNear: 0, askAddNear: 0, askCancelNear: 0,
        bidAddDeep: 0, bidCancelDeep: 0, askAddDeep: 0, askCancelDeep: 0,
        bidAddCntNear: 0, bidCancelCntNear: 0, askAddCntNear: 0, askCancelCntNear: 0,
        bidAddCntDeep: 0, bidCancelCntDeep: 0, askAddCntDeep: 0, askCancelCntDeep: 0,
        // Best queue dynamics
        bestBidAtouchAddQty: 0, bestBidAtouchCancelQty: 0, bestBidAtouchTradeQty: 0,
        bestAskAtouchAddQty: 0, bestAskAtouchCancelQty: 0, bestAskAtouchTradeQty: 0,
        bestBidPriceMoveOut: 0, bestAskPriceMoveOut: 0,
        bestReplenish: 0,
        // Trade at-touch / through (B3)
        trade_at_touch_qty: 0, trade_through_qty: 0,
        // Exchange-to-recv lag (B4)
        exchange_lag_sum_ms: 0, exchange_lag_count: 0,
        // Track current book levels for diff comparison
        _bids: new Map(), _asks: new Map(),
        _depleteBid: 0, _depleteAsk: 0, _spreadWiden: 0,
        _lastSpread: null,
        _bestBidPrice: null, _bestAskPrice: null,
        _bestBidDepleted: false, _bestAskDepleted: false,
        _prevBestBidQty: 0, _prevBestAskQty: 0,
      };
      this._flow.set(market, flow);
    }

    // Track depletion
    const prevBestBid = flow._bids.size > 0 ? Math.max(...flow._bids.keys()) : null;
    const prevBestAsk = flow._asks.size > 0 ? Math.min(...flow._asks.keys()) : null;
    const prevBestBidQty = prevBestBid !== null ? levelQty(flow._bids, prevBestBid) : 0;
    const prevBestAskQty = prevBestAsk !== null ? levelQty(flow._asks, prevBestAsk) : 0;

    // Process bid diffs
    if (depthEvent.bids) {
      for (const [priceStr, qtyStr] of depthEvent.bids) {
        const price = parseFloat(priceStr);
        const qty = parseFloat(qtyStr);
        const bps = mid ? priceToBps(mid, price) : Infinity;
        const isNear = bps <= 5;

        const prevQty = levelQty(flow._bids, priceStr);

        if (prevQty === 0 && qty > 0) {
          // Add
          if (isNear) { flow.bidAddNear += qty; flow.bidAddCntNear++; }
          else { flow.bidAddDeep += qty; flow.bidAddCntDeep++; }
          // Atouch tracking
          if (prevBestBid !== null && price === prevBestBid) {
            flow.bestBidAtouchAddQty += qty;
          }
        } else if (prevQty > 0 && qty === 0) {
          // Cancel
          if (isNear) { flow.bidCancelNear += prevQty; flow.bidCancelCntNear++; }
          else { flow.bidCancelDeep += prevQty; flow.bidCancelCntDeep++; }
          // Atouch tracking
          if (prevBestBid !== null && price === prevBestBid) {
            flow.bestBidAtouchCancelQty += prevQty;
          }
        } else if (prevQty > 0 && qty > 0 && qty !== prevQty) {
          // Modify (treat as cancel+add for flow tracking)
          const diff = qty - prevQty;
          if (diff > 0) {
            if (isNear) { flow.bidAddNear += diff; flow.bidAddCntNear++; }
            else { flow.bidAddDeep += diff; flow.bidAddCntDeep++; }
            if (prevBestBid !== null && price === prevBestBid) {
              flow.bestBidAtouchAddQty += diff;
            }
          } else {
            if (isNear) { flow.bidCancelNear += -diff; flow.bidCancelCntNear++; }
            else { flow.bidCancelDeep += -diff; flow.bidCancelCntDeep++; }
            if (prevBestBid !== null && price === prevBestBid) {
              flow.bestBidAtouchCancelQty += -diff;
            }
          }
        }

        // Update tracked state
        if (qty > 0) flow._bids.set(priceStr, qty);
        else flow._bids.delete(priceStr);
      }
    }

    // Process ask diffs
    if (depthEvent.asks) {
      for (const [priceStr, qtyStr] of depthEvent.asks) {
        const price = parseFloat(priceStr);
        const qty = parseFloat(qtyStr);
        const bps = mid ? priceToBps(mid, price) : Infinity;
        const isNear = bps <= 5;

        const prevQty = levelQty(flow._asks, priceStr);

        if (prevQty === 0 && qty > 0) {
          if (isNear) { flow.askAddNear += qty; flow.askAddCntNear++; }
          else { flow.askAddDeep += qty; flow.askAddCntDeep++; }
          // Atouch tracking
          if (prevBestAsk !== null && price === prevBestAsk) {
            flow.bestAskAtouchAddQty += qty;
          }
        } else if (prevQty > 0 && qty === 0) {
          if (isNear) { flow.askCancelNear += prevQty; flow.askCancelCntNear++; }
          else { flow.askCancelDeep += prevQty; flow.askCancelCntDeep++; }
          // Atouch tracking
          if (prevBestAsk !== null && price === prevBestAsk) {
            flow.bestAskAtouchCancelQty += prevQty;
          }
        } else if (prevQty > 0 && qty > 0 && qty !== prevQty) {
          const diff = qty - prevQty;
          if (diff > 0) {
            if (isNear) { flow.askAddNear += diff; flow.askAddCntNear++; }
            else { flow.askAddDeep += diff; flow.askAddCntDeep++; }
            if (prevBestAsk !== null && price === prevBestAsk) {
              flow.bestAskAtouchAddQty += diff;
            }
          } else {
            if (isNear) { flow.askCancelNear += -diff; flow.askCancelCntNear++; }
            else { flow.askCancelDeep += -diff; flow.askCancelCntDeep++; }
            if (prevBestAsk !== null && price === prevBestAsk) {
              flow.bestAskAtouchCancelQty += -diff;
            }
          }
        }

        if (qty > 0) flow._asks.set(priceStr, qty);
        else flow._asks.delete(priceStr);
      }
    }

    // Detect best-level depletion and price moves
    // Compute new best bid/ask
    const newBestBid = flow._bids.size > 0 ? Math.max(...flow._bids.keys()) : null;
    const newBestAsk = flow._asks.size > 0 ? Math.min(...flow._asks.keys()) : null;

    // Depletion: best level moved to a worse price
    if (prevBestBid !== null && newBestBid !== null && newBestBid < prevBestBid) flow._depleteBid++;
    if (prevBestAsk !== null && newBestAsk !== null && newBestAsk > prevBestAsk) flow._depleteAsk++;

    // Price move out: best price changed due to new level becoming best (or old best removed)
    if (prevBestBid !== null && newBestBid !== null && newBestBid !== prevBestBid) {
      flow.bestBidPriceMoveOut++;
    }
    if (prevBestAsk !== null && newBestAsk !== null && newBestAsk !== prevBestAsk) {
      flow.bestAskPriceMoveOut++;
    }

    // Replenish: best level recovers after being depleted
    if (flow._bestBidDepleted && newBestBid !== null) {
      // Check if best bid recovered to the depleted level or better
      if (prevBestBid !== null && newBestBid >= prevBestBid) {
        flow.bestReplenish++;
        flow._bestBidDepleted = false;
      } else if (newBestBid !== null) {
        flow.bestReplenish++;
        flow._bestBidDepleted = false;
      }
    }
    if (flow._bestAskDepleted && newBestAsk !== null) {
      if (prevBestAsk !== null && newBestAsk <= prevBestAsk) {
        flow.bestReplenish++;
        flow._bestAskDepleted = false;
      } else if (newBestAsk !== null) {
        flow.bestReplenish++;
        flow._bestAskDepleted = false;
      }
    }

    // Track depletion state: if best level depleted (qty went to zero at best)
    if (prevBestBidQty > 0) {
      const newBestBidQty = newBestBid !== null ? levelQty(flow._bids, newBestBid) : 0;
      // Depleted if best bid qty went to 0 and price didn't improve, or best is now null
      if (newBestBid === null || (newBestBidQty === 0 && newBestBid <= (prevBestBid ?? 0))) {
        flow._bestBidDepleted = true;
      }
    }
    if (prevBestAskQty > 0) {
      const newBestAskQty = newBestAsk !== null ? levelQty(flow._asks, newBestAsk) : 0;
      if (newBestAsk === null || (newBestAskQty === 0 && newBestAsk >= (prevBestAsk ?? Infinity))) {
        flow._bestAskDepleted = true;
      }
    }

    // Store current best prices for atouch trade detection in feedTrade
    flow._bestBidPrice = newBestBid !== null ? parseFloat(newBestBid) : null;
    flow._bestAskPrice = newBestAsk !== null ? parseFloat(newBestAsk) : null;

    // Track spread widenings
    if (mid) {
      const spread = flow._asks.size > 0 && flow._bids.size > 0
        ? (Math.min(...flow._asks.keys()) - Math.max(...flow._bids.keys())) / mid * 10000
        : null;
      if (flow._lastSpread !== null && spread !== null && spread > flow._lastSpread * 1.5) {
        flow._spreadWiden++;
      }
      flow._lastSpread = spread;
    }
  }

  /**
   * Feed end-of-second book state. Produces a 1s feature row.
   * @param {string} market
   * @param {number} ts - second boundary timestamp (epoch ms)
   * @param {object} book - FullBook instance
   * @param {number|null} [refMid] - reference market mid (binance_spot) for premium/basis
   */
  feedSecond(market, ts, book, refMid = null) {
    const second = ts - (ts % 1000);
    const mid = book.getMid();

    // ── 30s book-bin snapshot ────────────────────────────────────────────
    const bucket30s = second - (second % 30000);
    const last30s = this._last30sBookFlush.get(market) ?? -1;
    if (bucket30s > last30s && !book.isEmpty()) {
      this._last30sBookFlush.set(market, bucket30s);
      const snap = this._buildBookSnapshot(book, bucket30s, market);
      const dateStr = utcDateStr(new Date(bucket30s));
      this._getBookWriter(market, dateStr).write(snap);
    }

    const bestBid = book.getBestBid();
    const bestAsk = book.getBestAsk();
    const bidPrice = bestBid ? parseFloat(bestBid) : null;
    const askPrice = bestAsk ? parseFloat(bestAsk) : null;
    const spread = (bidPrice !== null && askPrice !== null) ? askPrice - bidPrice : null;
    const spreadBps = spread !== null && mid ? (spread / mid) * 10000 : null;

    // Microprice: weighted by best-level sizes
    const bidSize = bestBid ? levelQty(book.bids, bestBid) : 0;
    const askSize = bestAsk ? levelQty(book.asks, bestAsk) : 0;
    const microprice = (bidSize > 0 && askSize > 0 && bidPrice !== null && askPrice !== null)
      ? (bidSize * askPrice + askSize * bidPrice) / (bidSize + askSize)
      : null;

    // Get previous second L1 for `open` values
    const prevL1 = this._lastL1.get(market) || {};

    // Resolve trade accumulator for this second
    const tsMap = this._tradeAccums.get(market);
    const ta = tsMap ? tsMap.get(second) : null;

    // Compute bps depth
    const bpsDepth = computeRingDepth(book, mid, bidPrice, askPrice);

    // Compute imbalance for each ring
    const imbalance = {};
    for (const ring of ['0_1bps', '1_2bps', '2_5bps', '5_25bps', '25_100bps']) {
      const bid = bpsDepth?.[`bid_${ring}`] ?? 0;
      const ask = bpsDepth?.[`ask_${ring}`] ?? 0;
      const sum = bid + ask;
      imbalance[`imbalance_${ring}`] = sum > 0 ? (bid - ask) / sum : 0;
    }

    // Compute absorption features (simplified)
    const flow = this._flow.get(market);
    const ec = this._eventCounts.get(market)?.get(second) || {};

    // CRR: cancel-to-remove ratio
    const totalCancelVol = flow
      ? flow.bidCancelNear + flow.bidCancelDeep + flow.askCancelNear + flow.askCancelDeep
      : 0;
    const totalTradeVol = ta ? ta.buyQty + ta.sellQty : 0;
    const crr = totalTradeVol > 0 ? totalCancelVol / totalTradeVol : 0;

    // TMR: trade-to-market ratio
    const totalAddVol = flow
      ? flow.bidAddNear + flow.bidAddDeep + flow.askAddNear + flow.askAddDeep
      : 0;
    const tmr = totalAddVol > 0 ? totalTradeVol / (totalTradeVol + totalAddVol) : 0;

    // RVZ: realized variance z-score (placeholder — needs rolling window)
    // For now, just store the absolute mid return
    const rvz = prevL1.mid !== null && mid !== null ? Math.abs(mid - prevL1.mid) / (prevL1.mid || 1) : 0;

    // Quality: stale_ms
    const bookAge = book._ts ? (Date.now() - book._ts) : 0;
    const missingFlag = (ta ? 0 : 1) | (flow ? 0 : 2) | (book.isEmpty() ? 4 : 0);

    // Cross-venue premium / basis to reference market (binance_spot)
    const isPerp = market.includes('_perp');
    const premiumToRefBps = (refMid && mid && refMid > 0 && mid > 0)
      ? (mid / refMid - 1) * 10000
      : null;
    const basisToRefBps = isPerp ? premiumToRefBps : null;

    // ── Phase B2: Rolling window features (ring buffers) ──────────────
    const rb = this._getRingBufs(market);

    // Mid return (for realized vol)
    const midReturn = prevL1.mid && mid ? (mid - prevL1.mid) / prevL1.mid : 0;
    rb.midReturns.push(midReturn);

    // CVD (cumulative volume delta) per second
    const deltaNotional = (ta?.buyNotional ?? 0) - (ta?.sellNotional ?? 0);
    rb.cvd10.push(deltaNotional);
    rb.cvd30.push(deltaNotional);

    // Rolling window outputs (NULL until full)
    const realizedVol10s = rb.midReturns.isFull() ? rb.midReturns.std() : null;
    const cvd10s = rb.cvd10.isFull() ? rb.cvd10.sum() : null;
    const cvd30s = rb.cvd30.isFull() ? rb.cvd30.sum() : null;

    // Adverse selection: mature events aged 1-2 seconds
    let adverseSelectionBps = null;
    const matured = [];
    const pending = [];
    for (const evt of rb.adverseQueue) {
      const age = (ts - evt.ts) / 1000; // seconds
      if (age >= 1 && age <= 2) {
        matured.push(evt);
      } else if (age < 1) {
        pending.push(evt);
      }
      // Events > 2s are silently dropped
    }
    rb.adverseQueue = pending;

    if (matured.length > 0 && mid) {
      let totalBps = 0;
      for (const evt of matured) {
        totalBps += Math.abs((mid - evt.mid) / evt.mid) * 10000;
      }
      adverseSelectionBps = totalBps / matured.length;
    }

    const burstFeatures = this._computeBurstSlice1Features(market, second);
    const printStructureFeatures = this._computePrintStructureFeatures(ta);
    const burstBookValidationFeatures = this._computeBurstBookValidationFeatures(flow, ta);

    // Build feature row
    const row = {
      ts: second,
      market,

      // Trade agg
      open: ta?.open ?? null,
      high: ta?.high ?? null,
      low: ta?.low ?? null,
      close: ta?.close ?? null,
      vwap: ta && ta.vwapDen > 0 ? ta.vwapNum / ta.vwapDen : null,
      trade_count: ta?.count ?? 0,
      buy_qty: ta?.buyQty ?? 0,
      sell_qty: ta?.sellQty ?? 0,
      buy_notional: ta?.buyNotional ?? 0,
      sell_notional: ta?.sellNotional ?? 0,
      delta_notional: (ta?.buyNotional ?? 0) - (ta?.sellNotional ?? 0),
      buy_small_qty: ta?.buySmall ?? 0,
      buy_medium_qty: ta?.buyMed ?? 0,
      buy_large_qty: ta?.buyLarge ?? 0,
      buy_small_count: ta?.buySmallCount ?? 0,
      buy_medium_count: ta?.buyMedCount ?? 0,
      buy_large_count: ta?.buyLargeCount ?? 0,
      sell_small_qty: ta?.sellSmall ?? 0,
      sell_medium_qty: ta?.sellMed ?? 0,
      sell_large_qty: ta?.sellLarge ?? 0,
      sell_small_count: ta?.sellSmallCount ?? 0,
      sell_medium_count: ta?.sellMedCount ?? 0,
      sell_large_count: ta?.sellLargeCount ?? 0,

      // Burst slice 1 (overlap-based trade-only burst summaries)
      ...burstFeatures,

      // Print-structure slice 2 (bucket-local only)
      ...printStructureFeatures,

      // Burst book-validation slice 3 (bucket-local classified burst prints + event co-occurrence)
      ...burstBookValidationFeatures,

      // L1 boundary
      mid_open: prevL1.mid,
      mid_close: mid,
      spread_bps_open: prevL1.spreadBps,
      spread_bps_close: spreadBps,
      best_bid_open: prevL1.bidPrice,
      best_ask_open: prevL1.askPrice,
      best_bid_close: bidPrice,
      best_ask_close: askPrice,
      best_bid_size_open_qty: prevL1.bidSize ?? null,
      best_bid_size_close_qty: bestBid ? levelQty(book.bids, bestBid) : null,
      best_ask_size_open_qty: prevL1.askSize ?? null,
      best_ask_size_close_qty: bestAsk ? levelQty(book.asks, bestAsk) : null,
      microprice_close: microprice,

      // Cross-venue premium/basis
      premium_to_ref_bps: premiumToRefBps,
      basis_to_ref_bps: basisToRefBps,

      // Best queue dynamics (atouch)
      best_bid_atouch_add_qty: flow?.bestBidAtouchAddQty ?? 0,
      best_bid_atouch_cancel_qty: flow?.bestBidAtouchCancelQty ?? 0,
      best_bid_atouch_trade_qty: flow?.bestBidAtouchTradeQty ?? 0,
      best_ask_atouch_add_qty: flow?.bestAskAtouchAddQty ?? 0,
      best_ask_atouch_cancel_qty: flow?.bestAskAtouchCancelQty ?? 0,
      best_ask_atouch_trade_qty: flow?.bestAskAtouchTradeQty ?? 0,
      best_bid_price_move_out_count: flow?.bestBidPriceMoveOut ?? 0,
      best_ask_price_move_out_count: flow?.bestAskPriceMoveOut ?? 0,
      best_replenish_count: flow?.bestReplenish ?? 0,

      // Trade at-touch / through (B3)
      // Note: 1s resolution limits accuracy — at-touch vs through classification
      // depends on book snapshot alignment with trade timestamp. A trade that appears
      // "through" may reflect stale best bid/ask rather than true price improvement.
      trade_at_touch_qty: flow?.trade_at_touch_qty ?? 0,
      trade_through_qty: flow?.trade_through_qty ?? 0,

      // Depth state (bps buckets)
      ...(bpsDepth || {}),

      // Imbalance
      ...(imbalance || {}),

      // Depth flow
      bid_add_qty_near: flow?.bidAddNear ?? 0,
      bid_cancel_qty_near: flow?.bidCancelNear ?? 0,
      ask_add_qty_near: flow?.askAddNear ?? 0,
      ask_cancel_qty_near: flow?.askCancelNear ?? 0,
      bid_add_qty_deep: flow?.bidAddDeep ?? 0,
      bid_cancel_qty_deep: flow?.bidCancelDeep ?? 0,
      ask_add_qty_deep: flow?.askAddDeep ?? 0,
      ask_cancel_qty_deep: flow?.askCancelDeep ?? 0,
      bid_add_cnt_near: flow?.bidAddCntNear ?? 0,
      bid_cancel_cnt_near: flow?.bidCancelCntNear ?? 0,
      ask_add_cnt_near: flow?.askAddCntNear ?? 0,
      ask_cancel_cnt_near: flow?.askCancelCntNear ?? 0,
      bid_add_cnt_deep: flow?.bidAddCntDeep ?? 0,
      bid_cancel_cnt_deep: flow?.bidCancelCntDeep ?? 0,
      ask_add_cnt_deep: flow?.askAddCntDeep ?? 0,
      ask_cancel_cnt_deep: flow?.askCancelCntDeep ?? 0,

      // Absorption features
      rvz,
      crr,
      tmr,

      // Phase B2: Rolling window features
      realized_vol_10s: realizedVol10s,
      cvd_10s: cvd10s,
      cvd_30s: cvd30s,
      adverse_selection_bps: adverseSelectionBps,

      // Depletion / sweep
      replenish_lag_ms: null, // placeholder — computed per-event, not per-second
      best_deplete_count: (flow?._depleteBid ?? 0) + (flow?._depleteAsk ?? 0),
      spread_widen_count: flow?._spreadWiden ?? 0,

      // Quality
      depth_update_count: ec.depth_updates ?? 0,
      stale_ms: bookAge,
      missing_flag: missingFlag,

      // Exchange-to-recv latency (B4)
      exchange_to_recv_lag_ms_avg: flow && flow.exchange_lag_count > 0
        ? flow.exchange_lag_sum_ms / flow.exchange_lag_count
        : null,
    };

    // Store the row in buffer
    let marketBuf = this._buffers.get(market);
    if (!marketBuf) {
      marketBuf = new Map();
      this._buffers.set(market, marketBuf);
    }
    marketBuf.set(second, row);

    // Save current L1 as previous for next tick
    this._lastL1.set(market, {
      mid,
      spreadBps,
      bidPrice: bidPrice !== null ? bidPrice : null,
      askPrice: askPrice !== null ? askPrice : null,
      bidSize: bestBid ? levelQty(book.bids, bestBid) : null,
      askSize: bestAsk ? levelQty(book.asks, bestAsk) : null,
    });

    // Clean up flow accumulator for this market (reset for next second)
    // Keep the price level maps for diff tracking, but reset counters
    if (flow) {
      flow.bidAddNear = 0; flow.bidCancelNear = 0;
      flow.askAddNear = 0; flow.askCancelNear = 0;
      flow.bidAddDeep = 0; flow.bidCancelDeep = 0;
      flow.askAddDeep = 0; flow.askCancelDeep = 0;
      flow.bidAddCntNear = 0; flow.bidCancelCntNear = 0;
      flow.askAddCntNear = 0; flow.askCancelCntNear = 0;
      flow.bidAddCntDeep = 0; flow.bidCancelCntDeep = 0;
      flow.askAddCntDeep = 0; flow.askCancelCntDeep = 0;
      flow.bestBidAtouchAddQty = 0; flow.bestBidAtouchCancelQty = 0; flow.bestBidAtouchTradeQty = 0;
      flow.bestAskAtouchAddQty = 0; flow.bestAskAtouchCancelQty = 0; flow.bestAskAtouchTradeQty = 0;
      flow.bestBidPriceMoveOut = 0; flow.bestAskPriceMoveOut = 0;
      flow.bestReplenish = 0;
      flow.trade_at_touch_qty = 0; flow.trade_through_qty = 0;
      flow.exchange_lag_sum_ms = 0; flow.exchange_lag_count = 0;
      flow._depleteBid = 0; flow._depleteAsk = 0;
      flow._spreadWiden = 0;
    }
  }

  // ── Flush to JSONL ───────────────────────────────────────────────────

  /**
   * Flush buffered feature rows to JSONL via BufferedWriter.
   * @returns {Promise<number>} number of rows flushed
   */
  async flush() {
    if (this._buffers.size === 0) return 0;
    if (this._closed) return 0;

    let totalRows = 0;
    // Group rows by date partition (derived from each row's ts)
    const byDate = new Map(); // Map<dateStr, Map<market, rows[]>>
    for (const [market, rows] of this._buffers) {
      if (rows.size === 0) continue;
      for (const [second, row] of rows) {
        const dateStr = utcDateStr(new Date(second));
        if (!byDate.has(dateStr)) byDate.set(dateStr, new Map());
        if (!byDate.get(dateStr).has(market)) byDate.get(dateStr).set(market, []);
        byDate.get(dateStr).get(market).push(row);
        totalRows++;
      }
      // Clean up per-second event/trade accumulators for flushed seconds
      const ecMap = this._eventCounts.get(market);
      const taMap = this._tradeAccums.get(market);
      if (ecMap || taMap) {
        for (const second of rows.keys()) {
          ecMap?.delete(second);
          taMap?.delete(second);
        }
      }
      this._cleanupBurstState(market, Array.from(rows.keys()));
      // Stale cleanup: remove entries older than 300s (handles empty-book periods)
      const staleCutoff = Date.now() - 300000;
      if (ecMap) { for (const k of ecMap.keys()) { if (k < staleCutoff) ecMap.delete(k); } }
      if (taMap) { for (const k of taMap.keys()) { if (k < staleCutoff) taMap.delete(k); } }
    }

    const sortedDates = Array.from(byDate.entries()).sort(([a], [b]) => a.localeCompare(b));
    for (const [dateStr, markets] of sortedDates) {
      await this._ensureDate(dateStr);
      for (const [market, rows] of markets) {
        const writer = this._getWriter(market, dateStr);
        for (const row of rows) {
          writer.write(row); // BufferedWriter handles internal batching
        }
      }
    }

    this._buffers.clear();
    this._lastFlush = Date.now();
    return totalRows;
  }

  /** Close the accumulator, flushing remaining data and closing writers. */
  async close() {
    const flushed = await this.flush();
    this._closed = true;
    const promises = [];
    for (const w of this._writers.values()) {
      promises.push(w.close());
    }
    for (const w of this._bookWriters.values()) {
      promises.push(w.close());
    }
    await Promise.allSettled(promises);
    this._writers.clear();
    this._bookWriters.clear();
    return flushed;
  }

  // ── Helpers ──────────────────────────────────────────────────────────

  _incrementCount(market, field, second) {
    let ecMap = this._eventCounts.get(market);
    if (!ecMap) {
      ecMap = new Map();
      this._eventCounts.set(market, ecMap);
    }
    let ec = ecMap.get(second);
    if (!ec) {
      ec = { trade_events: 0, depth_updates: 0, snapshot_resets: 0 };
      ecMap.set(second, ec);
    }
    ec[field] = (ec[field] || 0) + 1;
  }
}
