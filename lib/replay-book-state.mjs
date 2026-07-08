#!/usr/bin/env node
/**
 * replay-book-state.mjs — Book state reconstruction from raw events.
 *
 * Maintains a price→qty side map for bids and asks, applies incremental
 * diffs (qty>0 = set, qty<=0 = delete), and derives best bid/ask from
 * the maintained maps.  Respects `seeded` flag — snapshot 適用前は
 * bestBid/bestAsk を null にする。
 */

/**
 * @typedef {Object} BookEvent
 * @property {number} effective_ts_ms
 * @property {'snapshot_file'|'book_update_snapshot'|'book_update_update'} subtype
 * @property {string} file_path
 * @property {number} line_no
 * @property {object} data
 * @property {'snapshot'|'update'} data.type
 * @property {Array<[string,string]>} [data.bids] — [[price, qty], ...]
 * @property {Array<[string,string]>} [data.asks] — [[price, qty], ...]
 */

/**
 * Replay a sorted array of book events and return a lookup function.
 *
 * @param {BookEvent[]} bookEvents — sorted by (effective_ts_ms, subtype_priority, file_path, line_no)
 * @returns {(ts: number) => { bestBid: number|null, bestAsk: number|null, seeded: boolean, bestBidQty: number|null, bestAskQty: number|null }}
 */
export function replayBestBookState(bookEvents) {
  /** @type {Map<string, number>} */
  const bidsMap = new Map();
  /** @type {Map<string, number>} */
  const asksMap = new Map();
  let seeded = false;

  /** @type {{ bestBid: number|null, bestAsk: number|null, seeded: boolean, bestBidQty: number|null, bestAskQty: number|null, _ts: number }[]} */
  const checkpoints = [];
  let lastTs = -1;

  /** Compute current best bid/ask from the side maps */
  function snapshotState() {
    let bestBid = null, bestBidQty = null;
    let bestAsk = null, bestAskQty = null;
    if (seeded) {
      for (const [price, qty] of bidsMap) {
        if (qty > 0) {
          const p = parseFloat(price);
          if (bestBid === null || p > bestBid) {
            bestBid = p;
            bestBidQty = qty;
          }
        }
      }
      for (const [price, qty] of asksMap) {
        if (qty > 0) {
          const p = parseFloat(price);
          if (bestAsk === null || p < bestAsk) {
            bestAsk = p;
            bestAskQty = qty;
          }
        }
      }
    }
    return { bestBid, bestAsk, seeded, bestBidQty, bestAskQty };
  }

  for (const ev of bookEvents) {
    const ts = ev.effective_ts_ms;
    if (ts !== lastTs) {
      // snapshot state at the boundary of this ts
      checkpoints.push({ ...snapshotState(), _ts: lastTs });
      lastTs = ts;
    }

    // Apply event
    const { type, bids, asks } = ev.data;

    if (type === 'snapshot') {
      seeded = true;
      bidsMap.clear();
      asksMap.clear();
      if (bids) {
        for (const [p, q] of bids) {
          const qty = parseFloat(q);
          if (qty > 0) bidsMap.set(String(p), qty);
        }
      }
      if (asks) {
        for (const [p, q] of asks) {
          const qty = parseFloat(q);
          if (qty > 0) asksMap.set(String(p), qty);
        }
      }
    } else {
      // incremental update: qty > 0 → set, qty <= 0 → delete
      if (bids) {
        for (const [p, q] of bids) {
          const qty = parseFloat(q);
          if (qty <= 0) bidsMap.delete(String(p));
          else bidsMap.set(String(p), qty);
        }
      }
      if (asks) {
        for (const [p, q] of asks) {
          const qty = parseFloat(q);
          if (qty <= 0) asksMap.delete(String(p));
          else asksMap.set(String(p), qty);
        }
      }
    }
  }
  // final checkpoint
  checkpoints.push({ ...snapshotState(), _ts: lastTs });

  /**
   * Lookup book state at an arbitrary timestamp.
   * Uses event._ts < t (strictly before), not <=.
   * Returns null bestBid/bestAsk when not yet seeded.
   *
   * @param {number} ts
   * @returns {{ bestBid: number|null, bestAsk: number|null, seeded: boolean, bestBidQty: number|null, bestAskQty: number|null }}
   */
  function bookAtTime(ts) {
    // binary search for the last checkpoint with _ts < ts
    let lo = 0, hi = checkpoints.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (checkpoints[mid]._ts < ts) lo = mid + 1;
      else hi = mid;
    }
    const cp = checkpoints[lo - 1];
    if (!cp) return { bestBid: null, bestAsk: null, seeded: false, bestBidQty: null, bestAskQty: null };
    return { bestBid: cp.bestBid, bestAsk: cp.bestAsk, seeded: cp.seeded, bestBidQty: cp.bestBidQty, bestAskQty: cp.bestAskQty };
  }

  return bookAtTime;
}
