// lib/burst-reducer/feature-computer-1s.mjs — 1s feature computation
// Follows plan Task 6

import { createBaseRow, generateSeconds } from './schema.mjs';

const EPS = 1e-10;

/**
 * Compute 30 feature rows for a single 30s block.
 * @param {Object} params
 * @param {import('./burst-detector.mjs').BurstDetector} params.detector
 * @param {number} params.blockStartMs
 * @param {number[]} params.tradeTsList - all trade timestamps (for trade_count_this_second)
 * @param {boolean} params.warmup
 * @param {string[]} params.inputBlockIds
 * @param {Map<number,number>} [params.lookupTradedNotional30s] - secondTs → traded_notional_30s
 * @param {Object} [params.bookSnapshot] - B3: optional book snapshot with {book_seeded}
 * @returns {Object[]} 30 rows (1s × 30)
 */
export function computeFeatures1s({ detector, blockStartMs, tradeTsList, warmup, inputBlockIds, lookupTradedNotional30s, bookSnapshot }) {
  const rows = [];

  for (const secondTs of generateSeconds(blockStartMs)) {
    const overlapping = detector.getClosedBurstsOverlapping(secondTs);
    const tradeCount = tradeTsList.filter(ts => ts >= secondTs && ts < secondTs + 1000).length;

    const quality = {
      book_seeded: bookSnapshot ? bookSnapshot.book_seeded : false,
      trade_count_this_second: tradeCount,
      warmup,
      input_block_ids: inputBlockIds,
    };

    // ── #12 fail-closed: complete 30-key lookup is a block-level validity prerequisite ──
    if (!lookupTradedNotional30s || !lookupTradedNotional30s.has(secondTs)) {
      throw new Error(`E007: lookupTradedNotional30s is missing for secondTs=${secondTs}. Caller must provide complete raw trade coverage.`);
    }

    if (overlapping.length === 0) {
      rows.push(createBaseRow(secondTs, detector.market, quality));
      continue;
    }

    // ── trade-only 11 features ──
    const burstCount = overlapping.length;
    let totalNotional = 0;
    let maxNotional = 0;
    let maxPrints = 0;
    let maxDuration = 0;
    let buyNotional = 0;
    let sellNotional = 0;
    let samePriceCount = 0;
    let multilevelCount = 0;

    for (const b of overlapping) {
      totalNotional += b.burst_notional;
      if (b.burst_notional > maxNotional) maxNotional = b.burst_notional;
      if (b.burst_print_count > maxPrints) maxPrints = b.burst_print_count;
      if (b.burst_duration_ms > maxDuration) maxDuration = b.burst_duration_ms;

      if (b.side === 'buy') buyNotional += b.burst_notional;
      else sellNotional += b.burst_notional;

      if (b.distinct_price_count === 1) samePriceCount++;
      else multilevelCount++;
    }

    const imbalanceRatio = (buyNotional - sellNotional) / (buyNotional + sellNotional + EPS);
    const largestShare = totalNotional > 0 ? maxNotional / totalNotional : 0;

    // ── #12: burst_notional_vs_30s_traded_notional ──
    const denom = lookupTradedNotional30s.get(secondTs);
    const vs30s = (denom > 0) ? totalNotional / denom : 0;

    const row = createBaseRow(secondTs, detector.market, quality);
    row.burst_count_1s = burstCount;
    row.total_burst_notional_1s = totalNotional;
    row.max_burst_notional_1s = maxNotional;
    row.max_burst_prints_1s = maxPrints;
    row.max_burst_duration_ms_1s = maxDuration;
    row.buy_burst_notional_1s = buyNotional;
    row.sell_burst_notional_1s = sellNotional;
    row.burst_imbalance_ratio_1s = imbalanceRatio;
    row.largest_burst_share_notional_1s = largestShare;
    row.same_price_burst_count_1s = samePriceCount;
    row.multilevel_burst_count_1s = multilevelCount;
    row.burst_notional_vs_30s_traded_notional = vs30s;

    rows.push(row);
  }

  return rows;
}
