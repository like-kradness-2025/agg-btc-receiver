// Pure 1s book event-flow features for canonical book_updates_v1 events.
const FLOW_FIELDS = [
  'ofi_1s', 'spread_delta_1s', 'depth_delta_1s', 'depth_delta_30s',
  'imbalance_delta_1s', 'bid_add_qty_1s', 'bid_cancel_qty_1s',
  'ask_add_qty_1s', 'ask_cancel_qty_1s', 'replenishment_qty_1s',
  'pulling_qty_1s',
];
const empty = () => Object.fromEntries(FLOW_FIELDS.map((field) => [field, null]));
const mapFromLevels = (levels) => new Map((levels || []).map(([p, q]) => [Number(p), Number(q)]));
function best(side, bid) {
  let result = null;
  for (const [price, qty] of side) {
    if (!Number.isFinite(price) || !Number.isFinite(qty) || qty <= 0) continue;
    if (result === null || (bid ? price > result : price < result)) result = price;
  }
  return result;
}
const bestQty = (side, price) => price === null ? 0 : (side.get(price) ?? 0);

function applyLevels(side, levels, stats, bid) {
  for (const [rawPrice, rawQty] of (levels || [])) {
    const price = Number(rawPrice); const next = Number(rawQty);
    if (!Number.isFinite(price) || !Number.isFinite(next) || next < 0) continue;
    const old = side.get(price) ?? 0;
    const delta = next - old;
    if (delta > 0) stats[bid ? 'bid_add_qty_1s' : 'ask_add_qty_1s'] += delta;
    if (delta < 0) stats[bid ? 'bid_cancel_qty_1s' : 'ask_cancel_qty_1s'] += -delta;
    if (next === 0) side.delete(price); else side.set(price, next);
  }
}

function topDepth(state) {
  return state?.seeded && state.best_bid != null && state.best_ask != null
    ? state.best_bid * state.best_bid_qty + state.best_ask * state.best_ask_qty : null;
}
function imbalance(state) {
  if (!state?.seeded || state.best_bid_qty == null || state.best_ask_qty == null) return null;
  const total = state.best_bid_qty + state.best_ask_qty;
  return total > 0 ? (state.best_bid_qty - state.best_ask_qty) / total : null;
}

/** Compute event flow for [secondTs, secondTs+1000), without future events. */
export function computeBookFlowForSecond({ secondTs, events, stateBefore }) {
  if (!stateBefore?.seeded || !Array.isArray(events)) return empty();
  const bids = mapFromLevels(stateBefore.bids); const asks = mapFromLevels(stateBefore.asks);
  const stats = Object.fromEntries(FLOW_FIELDS.map((field) => [field, 0]));
  const beforeDepth = topDepth(stateBefore); const beforeImbalance = imbalance(stateBefore);

  for (const event of events) {
    if (event.event_ts_ms < secondTs || event.event_ts_ms >= secondTs + 1000) continue;
    if (event.type === 'snapshot') {
      bids.clear(); asks.clear();
      for (const [p, q] of event.bids || []) if (Number(q) > 0) bids.set(Number(p), Number(q));
      for (const [p, q] of event.asks || []) if (Number(q) > 0) asks.set(Number(p), Number(q));
      continue;
    }
    const oldBid = best(bids, true); const oldAsk = best(asks, false);
    const oldBidQty = bestQty(bids, oldBid); const oldAskQty = bestQty(asks, oldAsk);
    applyLevels(bids, event.bids, stats, true); applyLevels(asks, event.asks, stats, false);
    const newBid = best(bids, true); const newAsk = best(asks, false);
    const newBidQty = bestQty(bids, newBid); const newAskQty = bestQty(asks, newAsk);
    if (newBid !== null && oldBid !== null && newBid >= oldBid) stats.ofi_1s += newBidQty - oldBidQty;
    if (newAsk !== null && oldAsk !== null && newAsk <= oldAsk) stats.ofi_1s -= newAskQty - oldAskQty;
    stats.replenishment_qty_1s += Math.max(0, newBidQty - oldBidQty) + Math.max(0, newAskQty - oldAskQty);
    stats.pulling_qty_1s += Math.max(0, oldBidQty - newBidQty) + Math.max(0, oldAskQty - newAskQty);
  }

  const end = { seeded: true, best_bid: best(bids, true), best_bid_qty: bestQty(bids, best(bids, true)), best_ask: best(asks, false), best_ask_qty: bestQty(asks, best(asks, false)) };
  const afterDepth = topDepth(end); const afterImbalance = imbalance(end);
  const beforeSpread = stateBefore.best_bid && stateBefore.best_ask ? (stateBefore.best_ask - stateBefore.best_bid) / ((stateBefore.best_ask + stateBefore.best_bid) / 2) * 10000 : null;
  const afterSpread = end.best_bid && end.best_ask ? (end.best_ask - end.best_bid) / ((end.best_ask + end.best_bid) / 2) * 10000 : null;
  stats.spread_delta_1s = beforeSpread !== null && afterSpread !== null ? afterSpread - beforeSpread : null;
  stats.depth_delta_1s = beforeDepth !== null && afterDepth !== null ? afterDepth - beforeDepth : null;
  stats.imbalance_delta_1s = beforeImbalance !== null && afterImbalance !== null ? afterImbalance - beforeImbalance : null;
  stats.depth_delta_30s = null;
  return stats;
}

export { FLOW_FIELDS };
