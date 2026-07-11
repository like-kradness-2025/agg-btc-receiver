// lib/burst-builder.mjs — Phase 1 burst formation from ordered trade stream
//
// Implements burst-formation-contract.md:
//   A burst is a maximal contiguous run of same-side trades where:
//   - adjacent print-to-print gap <= gap_threshold_ms
//   - total burst duration <= max_burst_duration_ms
//
// Same-price sub-runs (same-price-burst-contract.md) and multilevel
// classification (multilevel-burst-contract.md) are computed at burst
// close time as downstream characterizations.

/**
 * @typedef {Object} TradePrint
 * @property {number} ts   - millisecond timestamp
 * @property {'buy'|'sell'} side
 * @property {number} price
 * @property {number} qty
 */

/**
 * @typedef {Object} SamePriceRun
 * @property {number} same_price_key
 * @property {number} same_price_start_ts
 * @property {number} same_price_end_ts
 * @property {number} same_price_print_count
 * @property {number} same_price_notional
 */

/**
 * @typedef {Object} Burst
 * @property {string} burst_id
 * @property {string} market
 * @property {'buy'|'sell'} side
 * @property {number} burst_notional
 * @property {number} burst_print_count
 * @property {number} burst_duration_ms
 * @property {number} burst_start_ts
 * @property {number} burst_end_ts
 * @property {number} min_price
 * @property {number} max_price
 * @property {number} distinct_price_count
 * @property {number} span_ticks
 * @property {SamePriceRun[]} same_price_runs
 * @property {TradePrint[]} prints
 */

const DEFAULT_GAP_THRESHOLD_MS = 5;
const DEFAULT_MAX_BURST_DURATION_MS = 500;
const DEFAULT_TICK_SIZE = 0.01;

export class BurstBuilder {
  /**
   * @param {Object} [opts]
   * @param {number} [opts.gap_threshold_ms=5]
   * @param {number} [opts.max_burst_duration_ms=500]
   * @param {number} [opts.tick_size=0.01]
   * @param {string} [opts.market='unknown']
   */
  constructor({
    gap_threshold_ms = DEFAULT_GAP_THRESHOLD_MS,
    max_burst_duration_ms = DEFAULT_MAX_BURST_DURATION_MS,
    tick_size = DEFAULT_TICK_SIZE,
    market = 'unknown',
  } = {}) {
    this._gapThreshold = gap_threshold_ms;
    this._maxDuration = max_burst_duration_ms;
    this._tickSize = tick_size;
    this._market = market;
    /** @type {Object|null} */
    this._open = null;
    /** @type {Burst[]} */
    this._closedBursts = [];
    this._nextId = 1;
  }

  /**
   * Feed one trade into the burst formation stream.
   * Trades must arrive in ascending ts order (equal-ts trades in stable
   * original order).  Non-monotone input produces undefined behaviour.
   * @param {TradePrint} trade
   */
  feedTrade(trade) {
    const { ts, side, price, qty } = trade;
    if (this._open !== null) {
      const open = this._open;
      const gap = ts - open.end_ts;
      const candidateDuration = ts - open.start_ts;
      if (open.side !== side || gap > this._gapThreshold || candidateDuration > this._maxDuration) {
        this._closeOpenBurst();
      }
    }

    // Preserve all trade fields (including _idx, tradeId) in prints for
    // lossless burst membership identification downstream.
    const print = { ts, price, qty, side, ...trade };

    if (this._open === null) {
      this._open = {
        side,
        start_ts: ts,
        end_ts: ts,
        prints: [print],
        min_price: price,
        max_price: price,
        sum_notional: price * qty,
        sum_qty: qty,
      };
    } else {
      const open = this._open;
      open.end_ts = ts;
      open.prints.push(print);
      if (price < open.min_price) open.min_price = price;
      if (price > open.max_price) open.max_price = price;
      open.sum_notional += price * qty;
      open.sum_qty += qty;
    }
  }

  // ── private ──────────────────────────────────────────────────────────

  _closeOpenBurst() {
    if (this._open === null) return;
    const b = this._open;
    const prints = b.prints;

    const burst_notional = b.sum_notional;
    const burst_print_count = prints.length;
    const burst_duration_ms = b.end_ts - b.start_ts;

    // distinct price keys (canonical numeric price)
    const priceSet = new Set();
    for (const p of prints) priceSet.add(p.price);
    const distinct_price_count = priceSet.size;
    const span_ticks = distinct_price_count >= 2
      ? Math.round((b.max_price - b.min_price) / this._tickSize)
      : 0;

    // same-price sub-runs — maximal contiguous equal-price sequences
    const same_price_runs = [];
    let runStart = 0;
    for (let i = 1; i <= prints.length; i++) {
      if (i === prints.length || prints[i].price !== prints[runStart].price) {
        const runPrints = prints.slice(runStart, i);
        let runNotional = 0;
        for (const p of runPrints) runNotional += p.price * p.qty;
        same_price_runs.push({
          same_price_key: prints[runStart].price,
          same_price_start_ts: runPrints[0].ts,
          same_price_end_ts: runPrints[runPrints.length - 1].ts,
          same_price_print_count: runPrints.length,
          same_price_notional: runNotional,
        });
        runStart = i;
      }
    }

    this._closedBursts.push({
      burst_id: `${this._market}-${this._nextId++}`,
      market: this._market,
      side: b.side,
      burst_notional,
      burst_print_count,
      burst_duration_ms,
      burst_start_ts: b.start_ts,
      burst_end_ts: b.end_ts,
      min_price: b.min_price,
      max_price: b.max_price,
      distinct_price_count,
      span_ticks,
      same_price_runs,
      prints,
    });

    this._open = null;
  }

  // ── query ────────────────────────────────────────────────────────────

  /**
   * Return closed bursts whose interval overlaps the 1-second bucket
   * starting at `secondTs`.
   *
   * Overlap rule (burst-formation-contract §10.2):
   *   burst_start_ts < bucket_end_ts  AND  burst_end_ts >= bucket_start_ts
   *
   * @param {number} secondTs - bucket start (floored to second)
   * @returns {Burst[]}
   */
  getClosedBurstsOverlapping(secondTs) {
    const bucketStart = secondTs;
    const bucketEnd = secondTs + 1000;
    return this._closedBursts.filter(b =>
      b.burst_start_ts < bucketEnd && b.burst_end_ts >= bucketStart,
    );
  }

  /**
   * Return currently open burst(s) for diagnostics.
   * @returns {Object[]}

   */
  getOpenBursts() {
    if (this._open === null) return [];
    const b = this._open;
    return [{
      burst_id: `open-${this._market}`,
      market: this._market,
      side: b.side,
      burst_notional: b.sum_notional,
      burst_print_count: b.prints.length,
      burst_duration_ms: b.end_ts - b.start_ts,
      burst_start_ts: b.start_ts,
      burst_end_ts: b.end_ts,
      min_price: b.min_price,
      max_price: b.max_price,
      prints: b.prints,
    }];
  }

  /**
   * Force-close any open burst.  Call at end-of-stream before queries.
   */
  flushAll() {
    this._closeOpenBurst();
  }

  /**
   * Prune closed bursts whose end timestamp is strictly before the
   * retention cutoff computed from `blockStartMs`.
   *
   * Retention window = max_burst_duration_ms + gap_threshold_ms + 1000ms.
   * Boundary condition: burst with burst_end_ts === cutoff is kept.
   *
   * @param {number} blockStartMs - reference block start timestamp (ms)
   */
  pruneClosedBurstsBefore(blockStartMs) {
    const retentionWindow = this._maxDuration + this._gapThreshold + 1000;
    const cutoff = blockStartMs - retentionWindow;
    this._closedBursts = this._closedBursts.filter(b => b.burst_end_ts >= cutoff);
  }
}
