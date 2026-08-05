// Phase 0 raw-trade OrderFlow feature calculations.

import {
  DEFAULT_LARGE_TRADE_NOTIONAL,
  DEFAULT_LARGE_TRADE_THRESHOLD_VERSION,
} from './schema.mjs';

const EPS = 1e-12;

function finiteTrades(trades) {
  return (Array.isArray(trades) ? trades : [])
    .filter((trade) => trade && Number.isFinite(trade.ts)
      && Number.isFinite(trade.price) && trade.price > 0
      && Number.isFinite(trade.qty) && trade.qty > 0
      && (trade.side === 'buy' || trade.side === 'sell'))
    .sort((a, b) => a.ts - b.ts || (a._idx ?? 0) - (b._idx ?? 0));
}

function percentile(values, p) {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const rank = Math.max(0, Math.ceil(sorted.length * p) - 1);
  return sorted[rank];
}

function populationStd(values) {
  if (values.length === 0) return null;
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  const variance = values.reduce((sum, value) => sum + ((value - mean) ** 2), 0) / values.length;
  return Math.sqrt(variance);
}

function realizedVol(trades, secondTs, windowMs) {
  // Strict-past window: the current second is intentionally excluded.
  const prices = trades
    .filter((trade) => trade.ts >= secondTs - windowMs && trade.ts < secondTs)
    .map((trade) => trade.price);
  // Two returns are the minimum warmup; a single return is not a stable RV.
  if (prices.length < 3) return null;
  const returns = [];
  for (let i = 1; i < prices.length; i += 1) {
    returns.push(Math.log(prices[i] / prices[i - 1]));
  }
  return populationStd(returns);
}

/**
 * Compute raw-trade features for one 1s bucket.
 *
 * `trades` must contain the target block. `historyTrades` should also contain
 * the preceding 60s so strict-past RV and boundary interarrival metrics work.
 */
export function computeTradeFlowForSecond({
  secondTs,
  trades = [],
  historyTrades = trades,
  largeTradeNotionalThreshold = DEFAULT_LARGE_TRADE_NOTIONAL,
  largeTradeThresholdVersion = DEFAULT_LARGE_TRADE_THRESHOLD_VERSION,
}) {
  const current = finiteTrades(trades).filter((trade) => trade.ts >= secondTs && trade.ts < secondTs + 1000);
  const history = finiteTrades(historyTrades);
  const prices = current.map((trade) => trade.price);

  let buyQty = 0;
  let sellQty = 0;
  let buyNotional = 0;
  let sellNotional = 0;
  const notionals = [];
  let largeTradeCount = 0;
  let largeTradeNotional = 0;

  for (const trade of current) {
    const notional = trade.price * trade.qty;
    notionals.push(notional);
    if (trade.side === 'buy') {
      buyQty += trade.qty;
      buyNotional += notional;
    } else {
      sellQty += trade.qty;
      sellNotional += notional;
    }
    if (notional >= largeTradeNotionalThreshold) {
      largeTradeCount += 1;
      largeTradeNotional += notional;
    }
  }

  const tradedQty = buyQty + sellQty;
  const tradedNotional = buyNotional + sellNotional;
  const signedVolume = buyQty - sellQty;
  const signedNotional = buyNotional - sellNotional;

  const intervals = [];
  let sideFlipCount = 0;
  for (let i = 1; i < history.length; i += 1) {
    const previous = history[i - 1];
    const currentTrade = history[i];
    if (currentTrade.ts < secondTs || currentTrade.ts >= secondTs + 1000) continue;
    intervals.push(Math.max(0, currentTrade.ts - previous.ts));
    if (currentTrade.side !== previous.side) sideFlipCount += 1;
  }

  return {
    trade_open_1s: prices.length > 0 ? prices[0] : null,
    trade_high_1s: prices.length > 0 ? Math.max(...prices) : null,
    trade_low_1s: prices.length > 0 ? Math.min(...prices) : null,
    trade_close_1s: prices.length > 0 ? prices[prices.length - 1] : null,
    trade_count_1s: current.length,
    buy_trade_count_1s: current.filter((trade) => trade.side === 'buy').length,
    sell_trade_count_1s: current.filter((trade) => trade.side === 'sell').length,
    traded_qty_1s: tradedQty,
    traded_notional_1s: tradedNotional,
    buy_qty_1s: buyQty,
    sell_qty_1s: sellQty,
    buy_notional_1s: buyNotional,
    sell_notional_1s: sellNotional,
    signed_volume_1s: signedVolume,
    signed_notional_1s: signedNotional,
    trade_imbalance_qty_1s: tradedQty > EPS ? signedVolume / tradedQty : 0,
    trade_imbalance_notional_1s: tradedNotional > EPS ? signedNotional / tradedNotional : 0,
    mean_trade_notional_1s: notionals.length > 0
      ? tradedNotional / notionals.length : null,
    median_trade_notional_1s: percentile(notionals, 0.5),
    max_trade_notional_1s: notionals.length > 0 ? Math.max(...notionals) : null,
    large_trade_count_1s: largeTradeCount,
    large_trade_notional_1s: largeTradeNotional,
    large_trade_notional_share_1s: tradedNotional > EPS ? largeTradeNotional / tradedNotional : 0,
    mean_interarrival_ms_1s: percentile(intervals, 0.5) === null
      ? null : intervals.reduce((sum, value) => sum + value, 0) / intervals.length,
    median_interarrival_ms_1s: percentile(intervals, 0.5),
    p95_interarrival_ms_1s: percentile(intervals, 0.95),
    side_flip_count_1s: sideFlipCount,
    realized_vol_10s: realizedVol(history, secondTs, 10_000),
    realized_vol_60s: realizedVol(history, secondTs, 60_000),
    _trade_feature_quality: {
      source: 'raw_trades',
      large_trade_threshold: largeTradeNotionalThreshold,
      large_trade_threshold_version: largeTradeThresholdVersion,
    },
  };
}
