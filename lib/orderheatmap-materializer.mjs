// ⚠️  LEGACY — Builds orderheatmap from the old JSONL book snapshot path
//    (data/derived/burst_features_v1/). The live production pipeline uses:
//      Receiver SQLite → agg-btc-downstream-live → agg-btc-orderheatmap-publisher
//    See docs/current/canonical-pipeline.md for the canonical architecture.
//
// Build strict market-level OrderHeatmap rows from Book Snapshot v2 JSONL.
// The product is deliberately list-based: one row per second, one parallel
// price/qty list per side. Missing or quarantined seconds stay explicit gaps.

export const ORDERHEATMAP_SCHEMA_VERSION = 'market_orderheatmap_1s_v2';
export const ORDERHEATMAP_DEPTH_LIMIT_USD = 10_000;

function addLevels(target, prices = [], quantities = [], mid = null) {
  for (let i = 0; i < Math.min(prices.length, quantities.length); i++) {
    const price = Number(prices[i]);
    const qty = Number(quantities[i]);
    if (!Number.isFinite(price) || !Number.isFinite(qty) || price <= 0 || qty < 0) continue;
    if (Number.isFinite(mid) && Math.abs(price - mid) > ORDERHEATMAP_DEPTH_LIMIT_USD) continue;
    const bucket = Math.floor(price);
    // The displayed price is the bucket, not the raw venue level. Apply the
    // cap again after bucketing so floor() cannot push a bid just outside the
    // promised +/- $10k display window.
    if (Number.isFinite(mid) && Math.abs(bucket - mid) > ORDERHEATMAP_DEPTH_LIMIT_USD) continue;
    target.set(bucket, Number(((target.get(bucket) || 0) + qty).toPrecision(15)));
  }
}

function levelsToArrays(levels) {
  return [...levels.entries()].sort((a, b) => a[0] - b[0]);
}

/** Aggregate one finalized market snapshot row to $1 absolute price buckets. */
export function materializeOrderHeatmapRow(snapshot) {
  const bid = new Map();
  const ask = new Map();
  const usable = snapshot?.finalized === true && snapshot.seeded === true
    && snapshot.gap !== true && snapshot.crossed !== true && snapshot.stale !== true
    && ['seeded', 'unsequenced'].includes(snapshot.book_status);
  if (usable) {
    const mid = Number(snapshot.mid);
    addLevels(bid, snapshot.bid_prices, snapshot.bid_qtys, mid);
    addLevels(ask, snapshot.ask_prices, snapshot.ask_qtys, mid);
  }
  const bids = levelsToArrays(bid);
  const asks = levelsToArrays(ask);
  return {
    schema_version: ORDERHEATMAP_SCHEMA_VERSION,
    product: snapshot.market,
    market: snapshot.market,
    ts: snapshot.ts,
    finalized: usable,
    seeded: snapshot.seeded === true,
    gap: snapshot.gap === true || snapshot.book_status === 'quarantine',
    crossed: snapshot.crossed === true,
    stale: snapshot.stale === true,
    book_status: snapshot.book_status || 'unseeded',
    best_bid: usable ? (snapshot.best_bid ?? null) : null,
    best_ask: usable ? (snapshot.best_ask ?? null) : null,
    mid: usable ? (snapshot.mid ?? null) : null,
    bid_prices: bids.map(([price]) => price),
    bid_qtys_btc: bids.map(([, qty]) => qty),
    ask_prices: asks.map(([price]) => price),
    ask_qtys_btc: asks.map(([, qty]) => qty),
    contributor_count: usable ? 1 : 0,
    eligible_market_count: 1,
    coverage_ratio: usable ? 1 : 0,
    carried_market_count: 0,
    depth_limit_usd: ORDERHEATMAP_DEPTH_LIMIT_USD,
  };
}

export function materializeOrderHeatmapBlock(snapshots) {
  return (snapshots || []).map(materializeOrderHeatmapRow);
}
