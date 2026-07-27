#!/usr/bin/env node
/**
 * book-state-machine.mjs — Pure book state machine (Phase B2).
 *
 * A deterministic state machine for book_updates_v1 envelopes.
 * No file I/O, no pipeline wiring, no checkpoint/quarantine I/O.
 *
 * Exports:
 *   BookStateMachine  — class with apply/reset/snapshot
 *   ordered(events)   — deterministic sort per §13.2
 *   processBlock(events) — one-shot processing
 *   stateAt(events, anchor) — strict-anchor lookup
 */

// ─────────────────────────────────────────────
// Deterministic ordering (§13.2)
// ─────────────────────────────────────────────
const typePriority = type => type === 'snapshot' ? 0 : 1;
const sequenceOrRangeStart = event => {
  if (event.seq_start != null) return event.seq_start;
  if (event.seq != null) return event.seq;
  return Number.POSITIVE_INFINITY;
};
const eventKey = event => [event.event_ts_ms, typePriority(event.type), sequenceOrRangeStart(event), event.path, event.line_no];
const compareKeys = (a, b) => {
  const ka = eventKey(a);
  const kb = eventKey(b);
  for (let i = 0; i < ka.length; i++) {
    if (ka[i] < kb[i]) return -1;
    if (ka[i] > kb[i]) return 1;
  }
  return 0;
};

/**
 * Deterministic stable sort of book events.
 * @param {object[]} events
 * @returns {object[]}
 */
export function ordered(events) {
  if (!events) return [];
  return events.map((event, index) => ({ event, index }))
    .sort((a, b) => compareKeys(a.event, b.event) || a.index - b.index)
    .map(({ event }) => event);
}

// ─────────────────────────────────────────────
// Pure book state machine (§5, §13.4)
// ─────────────────────────────────────────────

export class BookStateMachine {
  constructor() {
    this.reset();
  }

  reset() {
    /** @type {Map<number, number>} */
    this.bids = new Map();
    /** @type {Map<number, number>} */
    this.asks = new Map();
    this.seeded = false;
    this.last_seq = null;
    this.last_event_ts_ms = null;

    this.book_status = 'unseeded';
    this.sequence_status = 'unsequenced';
    this.events_applied = 0;
    this.events_ignored = 0;
    this.gap_detected = false;
    this.stale_detected = false;
    this.malformed_detected = false;
    this.error_code = null;

    this.quarantined = false;
    this.commit = true;
    this.cursor = 'advance';
  }

  /**
   * Apply a single book update event.
   * @param {object} event - Canonical book envelope
   * @returns {{applied: boolean, rolledBack: boolean, reason?: string}}
   */
  apply(event) {
    this.last_event_ts_ms = event.event_ts_ms;

    // ── 1. Malformed level check ──
    for (const [price, qty] of [...(event.bids || []), ...(event.asks || [])]) {
      const p = Number(price);
      const q = Number(qty);
      if (!Number.isFinite(p) || p <= 0 || !Number.isFinite(q) || q < 0) {
        this.malformed_detected = true;
        this.sequence_status = 'malformed';
        this.book_status = 'quarantine';
        this.quarantined = true;
        this.commit = false;
        this.cursor = 'retain';
        this.error_code = 'MALFORMED_LEVEL';
        return { applied: false, rolledBack: false, reason: 'MALFORMED_LEVEL' };
      }
    }

    // ── 2. Sequence continuity check ──
    const hasSeq = event.seq != null;

    if (hasSeq) {
      if (this.sequence_status === 'unsequenced') {
        this.sequence_status = 'ok';
      }

      if (event.type === 'snapshot') {
        this.last_seq = event.seq;
      } else {
        const market = String(event.market || '');
        const isBinance = market.startsWith('binance_');
        const isOkx = market.startsWith('okx_');
        const canEstablishCoinbaseSequence = market === 'coinbase_spot' && this.last_seq == null;
        const canBridgeCoinbaseSequence = market === 'coinbase_spot';
        // Stale/duplicate must be checked BEFORE gap
        if (event.seq <= this.last_seq && this.last_seq != null) {
          this.stale_detected = true;
          this.events_ignored++;
          // Preserve gap diagnostics — stale after gap must not overwrite sequence_status (§7.3)
          if (!this.gap_detected) {
            this.sequence_status = 'stale_duplicate';
          }
          return { applied: false, rolledBack: false, reason: 'stale_duplicate' };
        }

        // Gap detection
        let gap = false;
        const isRange = event.seq_start != null && event.seq_end != null;

        if (isRange) {
          // The normal contract is an exact range bridge. Binance differs:
          // its U..u range may overlap a REST snapshot, and later ranges are
          // bridged by pu (prev_seq) even when U skips numeric IDs.
          const strictRange = event.prev_seq === this.last_seq
            && event.seq_start === this.last_seq + 1
            && event.seq_end >= event.seq_start;
          const binanceRange = isBinance
            && event.seq_end > this.last_seq
            && (
              event.prev_seq === this.last_seq
              || (event.prev_seq < this.last_seq
                && event.seq_start <= this.last_seq + 1
                && this.last_seq + 1 <= event.seq_end)
            );
          if ((!strictRange && !binanceRange) || event.seq_end < event.seq_start) {
            gap = true;
          }
        } else {
          // Single seq bridge
          if (canEstablishCoinbaseSequence) {
            // Coinbase REST fallback can seed an unsequenced book; the first
            // WS sequence establishes the cursor.
          } else if (event.prev_seq != null) {
            const monotonicBridge = (isOkx || canBridgeCoinbaseSequence)
              && event.prev_seq === this.last_seq
              && event.seq > this.last_seq;
            if (event.prev_seq !== this.last_seq || (!monotonicBridge && event.seq !== this.last_seq + 1)) {
              gap = true;
            }
          } else {
            if (event.seq !== this.last_seq + 1) {
              gap = true;
            }
          }
        }

        if (gap) {
          this.gap_detected = true;
          this.sequence_status = 'gap';
          this.book_status = 'quarantine';
          this.quarantined = true;
          this.commit = false;
          this.cursor = 'retain';
          this.error_code = 'SEQUENCE_GAP';
          return { applied: false, rolledBack: false, reason: 'SEQUENCE_GAP' };
        }

        this.last_seq = event.seq;
      }

      if (this.sequence_status !== 'gap' && this.sequence_status !== 'malformed' && this.sequence_status !== 'stale_duplicate') {
        this.sequence_status = 'ok';
      }
    }

    // ── 3. Apply state changes ──
    if (event.type === 'snapshot') {
      this.bids.clear();
      this.asks.clear();
      this.seeded = true;
      if (!hasSeq) {
        this.book_status = 'unsequenced';
      } else if (this.gap_detected) {
        this.book_status = 'quarantine';
      } else {
        this.book_status = 'seeded';
      }
    }

    for (const [price, qty] of (event.bids || [])) {
      const p = Number(price);
      const q = Number(qty);
      if (q === 0) { this.bids.delete(p); } else { this.bids.set(p, q); }
    }
    for (const [price, qty] of (event.asks || [])) {
      const p = Number(price);
      const q = Number(qty);
      if (q === 0) { this.asks.delete(p); } else { this.asks.set(p, q); }
    }

    if (!this.gap_detected && !this.malformed_detected) {
      this.events_applied++;
    }

    // ── 4. Post-apply: crossed book check ──
    this.checkCrossed();

    return { applied: true, rolledBack: false };
  }

  /** Check if book is crossed (best_bid >= best_ask). */
  checkCrossed() {
    const bb = this.bestBid();
    const ba = this.bestAsk();
    if (bb !== null && ba !== null && bb >= ba) {
      this.book_status = 'quarantine';
      this.quarantined = true;
      this.commit = false;
      this.cursor = 'retain';
      this.error_code = 'CROSSED_BOOK';
      return true;
    }
    return false;
  }

  /** @returns {number|null} */
  bestBid() {
    if (this.bids.size === 0) return null;
    let best = -Infinity;
    for (const p of this.bids.keys()) { if (p > best) best = p; }
    return best;
  }

  /** @returns {number|null} */
  bestBidQty() {
    const p = this.bestBid();
    return p !== null ? this.bids.get(p) : null;
  }

  /** @returns {number|null} */
  bestAsk() {
    if (this.asks.size === 0) return null;
    let best = Infinity;
    for (const p of this.asks.keys()) { if (p < best) best = p; }
    return best;
  }

  /** @returns {number|null} */
  bestAskQty() {
    const p = this.bestAsk();
    return p !== null ? this.asks.get(p) : null;
  }

  /** @returns {number|null} */
  mid() {
    const bid = this.bestBid();
    const ask = this.bestAsk();
    if (bid !== null && ask !== null) return (bid + ask) / 2;
    return null;
  }

  /** Finalize state decisions after all events are applied. */
  finalize() {
    if (this.malformed_detected || this.gap_detected) {
      this.book_status = 'quarantine';
      this.quarantined = true;
      this.commit = false;
      this.cursor = 'retain';
    }
  }

  /**
   * @param {{includeLevels?: boolean}} [options]
   * @returns {object} Current public book state (null bests if not seeded).
   */
  snapshotState({ includeLevels = false } = {}) {
    const seeded = this.seeded;
    const state = {
      seeded,
      best_bid: seeded ? this.bestBid() : null,
      best_bid_qty: seeded ? this.bestBidQty() : null,
      best_ask: seeded ? this.bestAsk() : null,
      best_ask_qty: seeded ? this.bestAskQty() : null,
      mid: seeded ? this.mid() : null,
      last_seq: this.last_seq,
    };
    if (includeLevels && seeded) {
      state.bids = [...this.bids.entries()].sort((a, b) => b[0] - a[0]);
      state.asks = [...this.asks.entries()].sort((a, b) => a[0] - b[0]);
    }
    return state;
  }

  /** @returns {object} Quality metadata. */
  snapshotQuality() {
    return {
      book_status: this.book_status,
      sequence_status: this.sequence_status,
      book_event_count_applied: this.events_applied,
      book_event_count_ignored: this.events_ignored,
    };
  }

  /** @returns {object} Commit/cursor/quarantine decisions. */
  snapshotDecisions() {
    return {
      commit: this.commit,
      cursor: this.cursor,
      quarantined: this.quarantined,
      error_code: this.error_code,
      gap_detected: this.gap_detected,
    };
  }
}

// ─────────────────────────────────────────────
// Convenience functions
// ─────────────────────────────────────────────

/**
 * Process a block of events through the state machine.
 * @param {object[]|null} events - array of canonical envelopes
 * @returns {{state: object|null, quality: object, decisions: object}}
 */
export function processBlock(events) {
  if (events === null || events === undefined) {
    return {
      state: null,
      quality: { book_status: 'unavailable', sequence_status: 'unsequenced' },
      decisions: { commit: false, cursor: 'retain', quarantined: false, blocked_reason: 'unknown-input' },
    };
  }
  if (!Array.isArray(events)) {
    return {
      state: null,
      quality: { book_status: 'unavailable', sequence_status: 'unsequenced' },
      decisions: { commit: false, cursor: 'retain', quarantined: false, blocked_reason: 'unknown-input' },
    };
  }
  const sm = new BookStateMachine();
  const sorted = ordered(events);
  for (const event of sorted) {
    sm.apply(event);
  }
  sm.finalize();
  return {
    state: sm.quarantined ? null : sm.snapshotState(),
    quality: sm.snapshotQuality(),
    decisions: sm.snapshotDecisions(),
  };
}

/**
 * Compute state at a strict anchor.
 * Only events with event_ts_ms < anchor are applied.
 * Returns {state, quarantined} — if quarantined, state is null.
 *
 * @param {object[]} events
 * @param {number} anchor
 * @returns {{state: object|null, quarantined?: boolean}}
 */
export function stateAt(events, anchor, options = {}) {
  const result = stateAtDetailed(events, anchor, options);
  if (result.quarantined) return { state: null, quarantined: true };
  return result.state;
}

/**
 * Strict-anchor replay with quality and commit diagnostics.
 * The state itself only includes events with event_ts_ms < anchor.
 */
export function stateAtDetailed(events, anchor, options = {}) {
  const sm = new BookStateMachine();
  if (!events) {
    return { state: sm.snapshotState(options), quality: sm.snapshotQuality(), decisions: sm.snapshotDecisions(), quarantined: false };
  }
  const sorted = ordered(events);
  for (const event of sorted) {
    if (event.event_ts_ms >= anchor) break;
    sm.apply(event);
  }
  sm.finalize();
  return {
    state: sm.quarantined ? null : sm.snapshotState(options),
    quality: sm.snapshotQuality(),
    decisions: sm.snapshotDecisions(),
    quarantined: sm.quarantined,
  };
}
