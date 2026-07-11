// lib/burst-reducer/schema.mjs — Burst Reducer v1 contract definitions
// Follows plan Task 2 and design §2, §3

export const SCHEMA_VERSION = 'burst_features_v1';

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

// 1s feature field names only (#1-#22, fixed order, 22 columns)
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

/**
 * Create a zero-filled base row conforming to P1 contract.
 * #1-#12: 0, #13: null, #14: 0, #15-#22: 0.
 * P1 0 = "no observation", not data missing.
 * Returns 25 physical top-level keys: ts, market, 22 features, _quality.
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
