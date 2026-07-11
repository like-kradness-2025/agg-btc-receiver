// lib/burst-reducer/burst-detector.mjs — Thin BurstBuilder wrapper with codec API access
// Follows plan Task 5

import { BurstBuilder } from '../burst-builder.mjs';
import { GAP_THRESHOLD_MS, MAX_BURST_DURATION_MS, getTickSize } from './schema.mjs';
import { serializeBurstBuilderState, serializeMinimalBurstState, restoreBurstBuilderState, restoreMinimalBurstState, getClosedBurstsSnapshot } from './burst-state-codec.mjs';

export class BurstDetector {
  /**
   * @param {string} market
   * @param {Object|null} [savedState=null] - serializeBurstBuilderState output format.
   *   null on first start. Pass checkpoint open_burst on restart.
   */
  constructor(market, savedState = null) {
    this._market = market;
    const tickSize = getTickSize(market);
    this._builder = new BurstBuilder({
      market,
      gap_threshold_ms: GAP_THRESHOLD_MS,
      max_burst_duration_ms: MAX_BURST_DURATION_MS,
      tick_size: tickSize ?? 0.01, // fallback for unknown markets
    });
    this._isFirstBlock = (savedState === null);

    // Restore BurstBuilder internal state via codec API only.
    // BurstBuilder constructor must NOT receive checkpoint options.
    if (savedState) {
      // P1-1: Detect minimal state (no closedBursts) vs full state
      if (savedState.closedBursts !== undefined) {
        restoreBurstBuilderState(this._builder, savedState);
      } else {
        restoreMinimalBurstState(this._builder, savedState);
      }
    }
  }

  get isFirstBlock() { return this._isFirstBlock; }

  /** Public getter for market (avoid accessing _market in feature computer) */
  get market() { return this._market; }

  feedTrades(trades) {
    for (const t of trades) {
      this._builder.feedTrade(t);
    }
  }

  getClosedBurstsOverlapping(secondTs) {
    return this._builder.getClosedBurstsOverlapping(secondTs);
  }

  /**
   * Deep-clone all closed bursts via codec API.
   * Never access _closedBursts directly (encapsulation).
   */
  getAllClosedBursts() {
    return getClosedBurstsSnapshot(this._builder);
  }

  /** Get serialized burst builder state for checkpoint (full — for in-memory snapshots). */
  getOpenBurstState() {
    return serializeBurstBuilderState(this._builder);
  }

  /** Get MINIMAL serialized burst builder state for checkpoint persistence (P1-1). */
  getMinimalBurstState() {
    return serializeMinimalBurstState(this._builder);
  }

  /** Restore from MINIMAL checkpoint state — closedBursts starts empty, caller must re-feed. */
  restoreFromMinimalState(state) {
    restoreMinimalBurstState(this._builder, state);
  }

  /**
   * Called at block end. Does NOT close open burst.
   * Open burst kept for next block continuation.
   */
  finalizeBlock() {
    // No-op. Open burst persists.
    // flushAll called only when all blocks complete or intermediate aggregation is needed.
  }

  /**
   * Force-close all open bursts. Call at end of all blocks.
   */
  flushAll() {
    this._builder.flushAll();
  }

  /**
   * P1-2: Prune closed bursts beyond retention window.
   * @param {number} blockStartMs - reference block start timestamp
   */
  pruneClosedBurstsBefore(blockStartMs) {
    this._builder.pruneClosedBurstsBefore(blockStartMs);
  }
}
