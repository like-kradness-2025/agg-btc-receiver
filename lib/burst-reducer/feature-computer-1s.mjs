// lib/burst-reducer/feature-computer-1s.mjs — 1s feature computation
// Follows plan Task 6

import { createBaseRow, generateSeconds, FEATURE_SCHEMA_VERSION } from './schema.mjs';
import { computeTradeFlowForSecond } from './trade-flow-features.mjs';
import { computeBookFlowForSecond } from './book-flow-features.mjs';
import { computeTradeBookInteractionForSecond } from './trade-book-interaction.mjs';

const EPS = 1e-10;

function depthWithinWindow(levels, mid, dollars) {
  if (!Array.isArray(levels) || !Number.isFinite(mid)) return null;
  let total = 0;
  for (const level of levels) {
    if (!Array.isArray(level) || level.length < 2) continue;
    const price = Number(level[0]);
    const qty = Number(level[1]);
    if (Number.isFinite(price) && Number.isFinite(qty) && qty >= 0 && Math.abs(price - mid) <= dollars) {
      total += price * qty;
    }
  }
  return Number.isFinite(total) ? total : null;
}

function applyBookFeatures(row, bookSnapshot, totalNotional, denom) {
  if (!bookSnapshot?.available || !bookSnapshot?.state?.seeded
      || bookSnapshot.state.best_bid == null || bookSnapshot.state.best_ask == null) return;

  const { best_bid, best_bid_qty, best_ask, best_ask_qty } = bookSnapshot.state;
  const topDepth = best_bid * best_bid_qty + best_ask * best_ask_qty;
  row.board_top_depth_ratio = topDepth > 0 ? totalNotional / topDepth : null;
  row.board_mid_move_bps_1s = null;
  row.board_vs_30s = denom > 0 ? totalNotional / denom : null;
  row.board_vs_depth = row.board_top_depth_ratio;
  // #13 is a burst-conditioned feature; empty seconds remain null.
  row.burst_notional_vs_top_depth = totalNotional > 0 ? row.board_top_depth_ratio : null;
  row.burst_mid_move_bps_1s = 0;

  const mid = (best_bid + best_ask) / 2;
  row.book_mid_price = mid;
  row.book_spread_bps = mid > 0 ? ((best_ask - best_bid) / mid) * 10000 : null;

  const bids = Array.isArray(bookSnapshot.state.bids) ? bookSnapshot.state.bids : [[best_bid, best_bid_qty]];
  const asks = Array.isArray(bookSnapshot.state.asks) ? bookSnapshot.state.asks : [[best_ask, best_ask_qty]];
  row.book_bid_depth_100 = depthWithinWindow(bids, mid, 100);
  row.book_ask_depth_100 = depthWithinWindow(asks, mid, 100);
  row.book_bid_depth_1000 = depthWithinWindow(bids, mid, 1000);
  row.book_ask_depth_1000 = depthWithinWindow(asks, mid, 1000);

  const totalDepth100 = row.book_bid_depth_100 + row.book_ask_depth_100;
  row.book_imbalance_100 = totalDepth100 > 0
    ? (row.book_bid_depth_100 - row.book_ask_depth_100) / totalDepth100 : null;
  const totalDepth1000 = row.book_bid_depth_1000 + row.book_ask_depth_1000;
  row.book_imbalance_1000 = totalDepth1000 > 0
    ? (row.book_bid_depth_1000 - row.book_ask_depth_1000) / totalDepth1000 : null;
  const totalQty = best_bid_qty + best_ask_qty;
  row.book_microprice = totalQty > 0
    ? (best_ask * best_bid_qty + best_bid * best_ask_qty) / totalQty : null;
}

function bookSnapshotForSecond(bookSnapshot, secondTs) {
  if (!bookSnapshot?.statesBySecond) return bookSnapshot;
  const state = bookSnapshot.statesBySecond.get(secondTs) ?? null;
  return state
    ? { ...bookSnapshot, state, book_seeded: state.seeded === true, available: true }
    : { available: false, book_seeded: false };
}

/**
 * Compute 30 feature rows for a single 30s block.
 * @param {Object} params
 * @param {import('./burst-detector.mjs').BurstDetector} params.detector
 * @param {number} params.blockStartMs
 * @param {number[]} params.tradeTsList - all trade timestamps (for trade_count_this_second)
 * @param {Object[]} [params.tradeRecords] - validated raw trade records for Phase 0 features
 * @param {Object[]} [params.tradeHistory] - prior + current validated records for rolling features
 * @param {boolean} params.warmup
 * @param {string[]} params.inputBlockIds
 * @param {Map<number,number>} [params.lookupTradedNotional30s] - secondTs → traded_notional_30s
 * @param {Object} [params.bookSnapshot] - B3: optional book snapshot with {book_seeded}
 * @param {Object[]} [params.bookEvents] - canonical book events for P1 flow
 * @returns {Object[]} 30 rows (1s × 30)
 */
export function computeFeatures1s({
  detector,
  blockStartMs,
  tradeTsList,
  tradeRecords = null,
  tradeHistory = null,
  warmup,
  inputBlockIds,
  lookupTradedNotional30s,
  bookSnapshot,
  bookEvents = null,
  bookStateAt = null,
  largeTradeNotionalThreshold,
  largeTradeThresholdVersion,
}) {
  const rows = [];
  const records = Array.isArray(tradeRecords) ? tradeRecords : [];
  const history = Array.isArray(tradeHistory) ? tradeHistory : records;
  const tradeFeatureOptions = {
    trades: records,
    historyTrades: history,
    ...(largeTradeNotionalThreshold === undefined ? {} : { largeTradeNotionalThreshold }),
    ...(largeTradeThresholdVersion === undefined ? {} : { largeTradeThresholdVersion }),
  };

  for (const secondTs of generateSeconds(blockStartMs)) {
    const secondBookSnapshot = bookSnapshotForSecond(bookSnapshot, secondTs);
    const bookFlow = computeBookFlowForSecond({
      secondTs,
      events: bookEvents,
      stateBefore: secondBookSnapshot?.state,
    });
    const interaction = computeTradeBookInteractionForSecond({ secondTs, trades: records, stateAt: bookStateAt });
    const overlapping = detector.getClosedBurstsOverlapping(secondTs);
    const tradeCount = tradeTsList.filter(ts => ts >= secondTs && ts < secondTs + 1000).length;
    const tradeFeatures = computeTradeFlowForSecond({ secondTs, ...tradeFeatureOptions });

    const quality = {
      book_seeded: secondBookSnapshot ? secondBookSnapshot.book_seeded : false,
      trade_count_this_second: tradeCount,
      feature_schema_version: FEATURE_SCHEMA_VERSION,
      trade_feature_source: Array.isArray(tradeRecords) ? 'raw_trades' : 'not_provided',
      large_trade_threshold: tradeFeatures._trade_feature_quality.large_trade_threshold,
      large_trade_threshold_version: tradeFeatures._trade_feature_quality.large_trade_threshold_version,
      warmup,
      input_block_ids: inputBlockIds,
    };

    // ── #12 fail-closed: complete 30-key lookup is a block-level validity prerequisite ──
    if (!lookupTradedNotional30s || !lookupTradedNotional30s.has(secondTs)) {
      throw new Error(`E007: lookupTradedNotional30s is missing for secondTs=${secondTs}. Caller must provide complete raw trade coverage.`);
    }

    if (overlapping.length === 0) {
      const row = createBaseRow(secondTs, detector.market, quality);
      Object.assign(row, tradeFeatures);
      Object.assign(row, bookFlow);
      Object.assign(row, interaction);
      delete row._trade_feature_quality;
      applyBookFeatures(row, secondBookSnapshot, 0, lookupTradedNotional30s.get(secondTs));
      rows.push(row);
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
    Object.assign(row, tradeFeatures);
    Object.assign(row, bookFlow);
    Object.assign(row, interaction);
    delete row._trade_feature_quality;
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

    applyBookFeatures(row, secondBookSnapshot, totalNotional, denom);
    // When book is unavailable/unseeded, board columns remain null (from createBaseRow base)
    // #13 remains null, #14 remains 0 (from createBaseRow)
    // Book features B1-B9 also remain null (from createBaseRow base)

    rows.push(row);
  }

  return rows;
}
