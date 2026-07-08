// lib/trade-size-buckets.mjs — shared USD-notional trade size buckets
//
// v1 CVD bucket contract:
//   small:  < $1k
//   medium: $1k <= notional < $10k
//   large:  >= $10k
//
// Important: "large" here means high-notional flow for CVD density, not whale.
// Future v2 may add a separate whale tier at >= $100k.

export const TRADE_SIZE_THRESHOLDS_USD = Object.freeze({
  mediumMin: 1_000,
  largeMin: 10_000,
});

export const TRADE_SIZE_BUCKETS = Object.freeze(['small', 'medium', 'large']);

/**
 * Classify a trade by USD notional size.
 * @param {number} price - Trade price in USD.
 * @param {number} qty - Trade quantity normalized to BTC-equivalent.
 * @returns {'small'|'medium'|'large'}
 */
export function classifyTradeNotional(price, qty) {
  const notional = price * qty;
  if (notional >= TRADE_SIZE_THRESHOLDS_USD.largeMin) return 'large';
  if (notional >= TRADE_SIZE_THRESHOLDS_USD.mediumMin) return 'medium';
  return 'small';
}
