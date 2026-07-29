const FIELDS = [
  'trade_at_touch_qty_1s', 'trade_at_touch_notional_1s',
  'trade_through_touch_qty_1s', 'trade_through_touch_notional_1s',
  'trade_slippage_bps_mean_1s', 'trade_sweep_level_count_1s',
  'trade_sweep_notional_1s', 'aggressive_qty_over_top_depth_1s',
];
const empty = () => Object.fromEntries(FIELDS.map((field) => [field, null]));

export function computeTradeBookInteractionForSecond({ secondTs, trades, stateAt }) {
  if (!Array.isArray(trades) || typeof stateAt !== 'function') return empty();
  const bucket = trades.filter((trade) => trade.ts >= secondTs && trade.ts < secondTs + 1000);
  if (bucket.length === 0) return Object.fromEntries(FIELDS.map((field) => [field, 0]));
  const result = Object.fromEntries(FIELDS.map((field) => [field, 0]));
  let slippageCount = 0;
  for (const trade of bucket) {
    const state = stateAt(trade.ts);
    if (!state?.seeded || state.best_bid == null || state.best_ask == null) return empty();
    const price = Number(trade.price); const qty = Number(trade.qty);
    const notional = price * qty; const buy = trade.side === 'buy';
    const touch = buy ? state.best_ask : state.best_bid;
    const through = buy ? price > touch : price < touch;
    const at = buy ? price === touch : price === touch;
    if (at) { result.trade_at_touch_qty_1s += qty; result.trade_at_touch_notional_1s += notional; }
    if (through) { result.trade_through_touch_qty_1s += qty; result.trade_through_touch_notional_1s += notional; }
    const mid = state.mid;
    if (Number.isFinite(mid) && mid > 0) {
      result.trade_slippage_bps_mean_1s += (buy ? price - touch : touch - price) / mid * 10000;
      slippageCount++;
    }
    const levels = buy ? (state.asks || []) : (state.bids || []);
    for (const [levelPrice, levelQty] of levels) {
      const p = Number(levelPrice); const q = Number(levelQty);
      if ((buy && p <= price) || (!buy && p >= price)) {
        result.trade_sweep_level_count_1s++;
        result.trade_sweep_notional_1s += p * q;
      }
    }
    const topQty = buy ? state.best_ask_qty : state.best_bid_qty;
    if (Number.isFinite(topQty) && topQty > 0) result.aggressive_qty_over_top_depth_1s += qty / topQty;
  }
  result.trade_slippage_bps_mean_1s = slippageCount ? result.trade_slippage_bps_mean_1s / slippageCount : null;
  return result;
}
export { FIELDS };
