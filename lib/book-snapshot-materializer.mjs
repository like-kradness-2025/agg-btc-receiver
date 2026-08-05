// ⚠️  LEGACY — This materializer was used for the old JSONL-based book snapshot
//    path (data/derived/burst_features_v1/). The live production pipeline is:
//      Receiver SQLite → agg-btc-downstream-live → agg-btc-orderheatmap-publisher
//    See docs/current/canonical-pipeline.md for the canonical architecture.
//
// Strict pre-second Book Snapshot materializer.
// Input: canonical book_updates_v1 envelopes for one market/block.
// Output rows are as-of the beginning of each second; current-second events
// are deliberately excluded (event_ts_ms < ts).

import { stateAtDetailed, ordered } from './book-state-machine.mjs';

export const BOOK_SNAPSHOT_SCHEMA_VERSION = 'book_snapshot_1s_v2';
export const SECOND_MS = 1000;
export const BLOCK_MS = 30000;

function levelPrices(levels) { return (levels || []).map(([price]) => Number(price)); }
function levelQtys(levels) { return (levels || []).map(([, qty]) => Number(qty)); }

export function materializeBookSnapshots(events, blockStartMs) {
  if (!Number.isSafeInteger(blockStartMs) || blockStartMs % BLOCK_MS !== 0) {
    throw new Error(`blockStartMs must be a 30s-aligned integer: ${blockStartMs}`);
  }
  const rows = [];
  const sorted = ordered(events || []);
  const market = sorted[0]?.market || null;
  for (let ts = blockStartMs; ts < blockStartMs + BLOCK_MS; ts += SECOND_MS) {
    const result = stateAtDetailed(sorted, ts, { includeLevels: true });
    const state = result.state;
    const bidPrices = levelPrices(state?.bids);
    const askPrices = levelPrices(state?.asks);
    // A one-sided exchange snapshot is not a usable book seed. Keep the
    // internal state for later updates, but do not expose it as finalized
    // data until both sides are present.
    const usableSeeded = Boolean(state?.seeded && bidPrices.length > 0 && askPrices.length > 0);
    const crossed = bidPrices.length > 0 && askPrices.length > 0 && Math.max(...bidPrices) >= Math.min(...askPrices);
    const gap = result.decisions?.gap_detected === true || result.quality?.sequence_status === 'gap';
    rows.push({
      schema_version: BOOK_SNAPSHOT_SCHEMA_VERSION,
      market,
      ts,
      finalized: Boolean(usableSeeded && !result.quarantined && !crossed),
      seeded: usableSeeded,
      gap,
      crossed,
      stale: false,
      book_status: result.quarantined ? 'quarantine' : (usableSeeded ? (result.quality?.book_status || 'unseeded') : 'unseeded'),
      sequence_status: result.quality?.sequence_status || 'unsequenced',
      error_code: result.decisions?.error_code || null,
      source_event_count: result.quality?.book_event_count_applied || 0,
      last_seq: state?.last_seq ?? null,
      best_bid: state?.best_bid ?? null,
      best_ask: state?.best_ask ?? null,
      mid: state?.mid ?? null,
      bid_prices: bidPrices,
      bid_qtys: levelQtys(state?.bids),
      ask_prices: askPrices,
      ask_qtys: levelQtys(state?.asks),
      base_price_bin_usd: 1,
    });
  }
  return rows;
}
