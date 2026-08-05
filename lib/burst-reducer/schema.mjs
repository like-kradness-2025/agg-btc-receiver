// lib/burst-reducer/schema.mjs — Burst Reducer v1 contract definitions
// Follows plan Task 2 and design §2, §3

export const SCHEMA_VERSION = 'burst_features_v1';
// Feature payload version is separate from the reducer/checkpoint namespace.
// This allows the additive OrderFlow payload to be identified without
// invalidating the existing burst checkpoint protocol.
export const FEATURE_SCHEMA_VERSION = 'orderflow_features_1s_v2';
export const P1_BOOK_FLOW_FIELDS = [
  'ofi_1s', 'spread_delta_1s', 'depth_delta_1s', 'depth_delta_30s',
  'imbalance_delta_1s', 'bid_add_qty_1s', 'bid_cancel_qty_1s',
  'ask_add_qty_1s', 'ask_cancel_qty_1s', 'replenishment_qty_1s',
  'pulling_qty_1s',
];
export const P2_TRADE_BOOK_FIELDS = [
  'trade_at_touch_qty_1s', 'trade_at_touch_notional_1s',
  'trade_through_touch_qty_1s', 'trade_through_touch_notional_1s',
  'trade_slippage_bps_mean_1s', 'trade_sweep_level_count_1s',
  'trade_sweep_notional_1s', 'aggressive_qty_over_top_depth_1s',
];

// Burst detection fixed parameters (design §2.1)
export const GAP_THRESHOLD_MS = 50;
export const MAX_BURST_DURATION_MS = 5000;

// Output directory structure (design §3.2)
export const DERIVED_DIR = 'data/derived/burst_features_v1';
export const FEATURES_1S_DIR = 'features_1s';
export const FEATURES_30S_DIR = 'features_30s';
export const FEATURES_5MIN_DIR = 'features_5min';
export const MANIFESTS_DIR = 'manifests';
export const CHECKPOINTS_DIR = 'manifests/checkpoints';

// Market tick size map (design §2.1)
export const MARKET_TICK_SIZE = new Map([
  ['binance_spot', 0.01],
  ['bybit_spot', 0.01],
  ['okx_spot', 0.01],
  ['coinbase_spot', 0.01],
  ['kraken_spot', 0.01],
  ['bitget_spot', 0.01],
  ['mexc_spot', 0.01],
  ['htX_spot', 0.01],
  ['bitfinex_spot', 0.01],
  ['binance_perp', 1.0],
  ['bybit_perp', 0.1],
  ['okx_perp', 0.1],
  ['dYdX_perp', 0.1],
  ['hyperliquid_perp', 0.1],
  ['kraken_perp', 0.1],
  ['bitfinex_perp', 0.1],
]);

/** Get tick size for a market. Returns null if undefined. */
export function getTickSize(market) {
  return MARKET_TICK_SIZE.get(market) ?? null;
}

// Physical row envelope keys (not features, present in JSON row)
export const ROW_ENVELOPE_FIELDS = ['ts', 'market', '_quality'];

// 1s feature field names in fixed output order.
export const FEATURE_1S_FIELDS = [
  'burst_count_1s', 'total_burst_notional_1s', 'max_burst_notional_1s',
  'max_burst_prints_1s', 'max_burst_duration_ms_1s',
  'buy_burst_notional_1s', 'sell_burst_notional_1s',
  'burst_imbalance_ratio_1s', 'largest_burst_share_notional_1s',
  'same_price_burst_count_1s', 'multilevel_burst_count_1s',
  'burst_notional_vs_30s_traded_notional',
  'burst_notional_vs_top_depth', 'burst_mid_move_bps_1s',
  'same_price_burst_max_len_1s', 'same_price_burst_notional_1s',
  'multilevel_burst_max_span_ticks_1s', 'multilevel_burst_max_span_bps_1s',
  'multilevel_burst_notional_1s', 'same_price_absorption_ratio_1s',
  'burst_delta_notional_1s', 'outlier_trade_flag_1s',
  // Book features B1-B9 (nullable)
  'book_mid_price', 'book_spread_bps',
  'book_bid_depth_100', 'book_ask_depth_100',
  'book_bid_depth_1000', 'book_ask_depth_1000',
  'book_imbalance_100', 'book_imbalance_1000',
  'book_microprice',
  'board_top_depth_ratio', 'board_mid_move_bps_1s', 'board_vs_30s', 'board_vs_depth',
];

// Phase 1 trade-only fields (#1-#12)
export const PHASE1_FIELDS = new Set([
  'burst_count_1s', 'total_burst_notional_1s', 'max_burst_notional_1s',
  'max_burst_prints_1s', 'max_burst_duration_ms_1s',
  'buy_burst_notional_1s', 'sell_burst_notional_1s',
  'burst_imbalance_ratio_1s', 'largest_burst_share_notional_1s',
  'same_price_burst_count_1s', 'multilevel_burst_count_1s',
  'burst_notional_vs_30s_traded_notional',
]);

// Book-dependent fields (#13, #14)
export const BOOK_DEPENDENT_FIELDS = new Set([
  'burst_notional_vs_top_depth', 'burst_mid_move_bps_1s',
]);

// Research fields (#15-#21; P1 always 0)
export const RESEARCH_FIELDS = new Set([
  'same_price_burst_max_len_1s', 'same_price_burst_notional_1s',
  'multilevel_burst_max_span_ticks_1s', 'multilevel_burst_max_span_bps_1s',
  'multilevel_burst_notional_1s', 'same_price_absorption_ratio_1s',
  'burst_delta_notional_1s',
]);

// Monitoring field (#22; P1 always 0)
export const MONITORING_FIELDS = new Set([
  'outlier_trade_flag_1s',
]);

// Book feature fields (B1-B9; nullable: null when book unseeded/crossed)
export const BOOK_FEATURE_FIELDS = [
  'book_mid_price', 'book_spread_bps',
  'book_bid_depth_100', 'book_ask_depth_100',
  'book_bid_depth_1000', 'book_ask_depth_1000',
  'book_imbalance_100', 'book_imbalance_1000',
  'book_microprice',
];
export const BOOK_FEATURE_FIELDS_SET = new Set(BOOK_FEATURE_FIELDS);

// Board candidate fields (B4, spec §10)
export const BOARD_FIELDS = [
  'board_top_depth_ratio', 'board_mid_move_bps_1s', 'board_vs_30s', 'board_vs_depth',
];
export const BOARD_CANDIDATE_FIELDS = new Set(BOARD_FIELDS);

// Phase 0: raw-trade OrderFlow fields. These are independent of book seed/replay.
export const P0_TRADE_FLOW_FIELDS = [
  'trade_open_1s', 'trade_high_1s', 'trade_low_1s', 'trade_close_1s',
  'trade_count_1s', 'buy_trade_count_1s', 'sell_trade_count_1s',
  'traded_qty_1s', 'traded_notional_1s',
  'buy_qty_1s', 'sell_qty_1s', 'buy_notional_1s', 'sell_notional_1s',
  'signed_volume_1s', 'signed_notional_1s',
  'trade_imbalance_qty_1s', 'trade_imbalance_notional_1s',
  'mean_trade_notional_1s', 'median_trade_notional_1s', 'max_trade_notional_1s',
  'large_trade_count_1s', 'large_trade_notional_1s', 'large_trade_notional_share_1s',
  'mean_interarrival_ms_1s', 'median_interarrival_ms_1s', 'p95_interarrival_ms_1s',
  'side_flip_count_1s', 'realized_vol_10s', 'realized_vol_60s',
];
export const P0_TRADE_FLOW_FIELDS_SET = new Set(P0_TRADE_FLOW_FIELDS);

// The threshold is configuration, not a universal market truth. It is the
// explicit fallback used until per-market thresholds are supplied.
export const DEFAULT_LARGE_TRADE_NOTIONAL = 100_000;
export const DEFAULT_LARGE_TRADE_THRESHOLD_VERSION = 'default-quote-100k-v1';

FEATURE_1S_FIELDS.push(...P0_TRADE_FLOW_FIELDS);
FEATURE_1S_FIELDS.push(...P1_BOOK_FLOW_FIELDS);
FEATURE_1S_FIELDS.push(...P2_TRADE_BOOK_FIELDS);

/**
 * Create a zero-filled base row conforming to P1 contract.
 * #1-#12: 0, #13: null, #14: 0, #15-#22: 0.
 * P1 0 = "no observation", not data missing.
 * Returns the physical top-level row envelope plus all burst/book/OrderFlow fields.
 */
export function createBaseRow(ts, market, quality) {
  return {
    ts,
    market,
    burst_count_1s: 0,
    total_burst_notional_1s: 0,
    max_burst_notional_1s: 0,
    max_burst_prints_1s: 0,
    max_burst_duration_ms_1s: 0,
    buy_burst_notional_1s: 0,
    sell_burst_notional_1s: 0,
    burst_imbalance_ratio_1s: 0,
    largest_burst_share_notional_1s: 0,
    same_price_burst_count_1s: 0,
    multilevel_burst_count_1s: 0,
    burst_notional_vs_30s_traded_notional: 0,
    burst_notional_vs_top_depth: null,     // #13: P1 null (explicit no-book)
    burst_mid_move_bps_1s: 0,              // #14: P1 0 (not null)
    same_price_burst_max_len_1s: 0,        // #15: P1 0
    same_price_burst_notional_1s: 0,       // #16: P1 0
    multilevel_burst_max_span_ticks_1s: 0, // #17: P1 0
    multilevel_burst_max_span_bps_1s: 0,   // #18: P1 0
    multilevel_burst_notional_1s: 0,       // #19: P1 0
    same_price_absorption_ratio_1s: 0,     // #20: P1 0
    burst_delta_notional_1s: 0,            // #21: P1 0
    outlier_trade_flag_1s: 0,              // #22: P1 0
    // ── Book features B1-B9 (nullable: null when book unseeded/crossed) ──
    book_mid_price: null,
    book_spread_bps: null,
    book_bid_depth_100: null,
    book_ask_depth_100: null,
    book_bid_depth_1000: null,
    book_ask_depth_1000: null,
    book_imbalance_100: null,
    book_imbalance_1000: null,
    book_microprice: null,
    board_top_depth_ratio: null,           // B4: board candidate (spec §10)
    board_mid_move_bps_1s: null,           // B4: board candidate (spec §10)
    board_vs_30s: null,                    // B4: board candidate (spec §10)
    board_vs_depth: null,                  // B4: board candidate (spec §10)
    // ── Phase 0 raw-trade OrderFlow ──
    trade_open_1s: null,
    trade_high_1s: null,
    trade_low_1s: null,
    trade_close_1s: null,
    trade_count_1s: 0,
    buy_trade_count_1s: 0,
    sell_trade_count_1s: 0,
    traded_qty_1s: 0,
    traded_notional_1s: 0,
    buy_qty_1s: 0,
    sell_qty_1s: 0,
    buy_notional_1s: 0,
    sell_notional_1s: 0,
    signed_volume_1s: 0,
    signed_notional_1s: 0,
    trade_imbalance_qty_1s: 0,
    trade_imbalance_notional_1s: 0,
    mean_trade_notional_1s: null,
    median_trade_notional_1s: null,
    max_trade_notional_1s: null,
    large_trade_count_1s: 0,
    large_trade_notional_1s: 0,
    large_trade_notional_share_1s: 0,
    mean_interarrival_ms_1s: null,
    median_interarrival_ms_1s: null,
    p95_interarrival_ms_1s: null,
    side_flip_count_1s: 0,
    realized_vol_10s: null,
    realized_vol_60s: null,
    // ── Phase 1 book event flow (null until strict seeded state exists) ──
    ofi_1s: null,
    spread_delta_1s: null,
    depth_delta_1s: null,
    depth_delta_30s: null,
    imbalance_delta_1s: null,
    bid_add_qty_1s: null,
    bid_cancel_qty_1s: null,
    ask_add_qty_1s: null,
    ask_cancel_qty_1s: null,
    replenishment_qty_1s: null,
    pulling_qty_1s: null,
    trade_at_touch_qty_1s: null,
    trade_at_touch_notional_1s: null,
    trade_through_touch_qty_1s: null,
    trade_through_touch_notional_1s: null,
    trade_slippage_bps_mean_1s: null,
    trade_sweep_level_count_1s: null,
    trade_sweep_notional_1s: null,
    aggressive_qty_over_top_depth_1s: null,
    _quality: quality,
  };
}

// _quality contract: spec §9.2a. input_block_ids contains only raw trade block IDs.
// Agg hashes live only in manifest auxiliary_input_hashes.

/** Floor ts to second boundary (ts - (ts % 1000)) */
export function floorToSecond(ts) { return ts - (ts % 1000); }

/**
 * Generate the 30 second-start timestamps covering [blockStartMs, blockStartMs+30000).
 */
export function* generateSeconds(blockStartMs) {
  for (let s = blockStartMs; s < blockStartMs + 30000; s += 1000) {
    yield s;
  }
}

// ── P0-1: Input kind constants ──────────────────────────────────────
export const INPUT_KIND = {
  TRADES: 'trades',
  BOOK_UPDATES: 'book_updates',
};
export const VALID_INPUT_KINDS = new Set([INPUT_KIND.TRADES, INPUT_KIND.BOOK_UPDATES]);

// 30s block duration constant (consolidated from pipeline.mjs, tfp.mjs)
export const BLOCK_DURATION_MS = 30000;

// P0-2: Checkpoint size boundedness (design §3.4)
export const CHECKPOINT_SIZE_WARN = 262144;      // 256 KiB — emit WARN if exceeded
export const CHECKPOINT_SIZE_HARD_LIMIT = 1048576; // 1 MiB — throw E026 if exceeded
