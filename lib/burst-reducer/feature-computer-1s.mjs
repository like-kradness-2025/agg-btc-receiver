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
    // P4: #15-#22 accumulators
    let samePriceMaxLen = 0;
    let samePriceNotional = 0;
    let multiLevelMaxSpanTicks = 0;
    let multiLevelMaxSpanBps = 0;
    let multiLevelNotional = 0;

    for (const b of overlapping) {
      totalNotional += b.burst_notional;
      if (b.burst_notional > maxNotional) maxNotional = b.burst_notional;
      if (b.burst_print_count > maxPrints) maxPrints = b.burst_print_count;
      if (b.burst_duration_ms > maxDuration) maxDuration = b.burst_duration_ms;

      if (b.side === 'buy') buyNotional += b.burst_notional;
      else sellNotional += b.burst_notional;

      if (b.distinct_price_count === 1) {
        samePriceCount++;
        samePriceMaxLen = Math.max(samePriceMaxLen, b.burst_print_count);
        samePriceNotional += b.burst_notional;
      } else {
        multilevelCount++;
        multiLevelMaxSpanTicks = Math.max(multiLevelMaxSpanTicks, b.span_ticks || 0);
        multiLevelNotional += b.burst_notional;
        // Compute bps_span from price range and mid price
        if (b.max_price > 0 && b.min_price > 0) {
          const midPrice = (b.max_price + b.min_price) / 2;
          const bpsSpan = ((b.max_price - b.min_price) / midPrice) * 10000;
          multiLevelMaxSpanBps = Math.max(multiLevelMaxSpanBps, bpsSpan);
        }
      }
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

    // ── P4: #13-#22 activation ──
    const deltaNotional = buyNotional - sellNotional;
    const meanNotional = burstCount > 0 ? totalNotional / burstCount : 0;
    const outlierFlag = burstCount > 0 && overlapping.some(b => b.burst_notional > 5 * meanNotional) ? 1 : 0;

    row.same_price_burst_max_len_1s = samePriceMaxLen;           // #15
    row.same_price_burst_notional_1s = samePriceNotional;         // #16
    row.multilevel_burst_max_span_ticks_1s = multiLevelMaxSpanTicks;  // #17
    row.multilevel_burst_max_span_bps_1s = multiLevelMaxSpanBps; // #18
    row.multilevel_burst_notional_1s = multiLevelNotional;        // #19
    row.same_price_absorption_ratio_1s = totalNotional > 0 ? samePriceNotional / totalNotional : 0;  // #20
    row.burst_delta_notional_1s = deltaNotional;                  // #21
    row.outlier_trade_flag_1s = outlierFlag;                      // #22

    // ── Board candidate columns (B4, spec §10) ──
    // Compute from bookSnapshot.state when book is available and seeded
    if (bookSnapshot?.available && bookSnapshot?.state?.seeded && bookSnapshot.state.best_bid != null && bookSnapshot.state.best_ask != null) {
      const { best_bid, best_bid_qty, best_ask, best_ask_qty } = bookSnapshot.state;
      const topDepth = best_bid * best_bid_qty + best_ask * best_ask_qty;

      row.board_top_depth_ratio = topDepth > 0 ? totalNotional / topDepth : null;
      row.board_mid_move_bps_1s = null; // B4 scope: null at row level (no cross-block prior state)
      row.board_vs_30s = denom > 0 ? totalNotional / denom : null;
      row.board_vs_depth = row.board_top_depth_ratio; // alias per §10.4

      // P4: #13 / #14 activation — set from book state when available
      row.burst_notional_vs_top_depth = row.board_top_depth_ratio;   // #13
      row.burst_mid_move_bps_1s = 0;                                  // #14: no cross-block mid state at row level
    }
    // When book is unavailable/unseeded, board columns remain null (from createBaseRow base)
    // #13 remains null, #14 remains 0 (from createBaseRow)

    rows.push(row);
  }

  return rows;
}
