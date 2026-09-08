// lib/market-status.mjs — Issue #16: process/worker readiness vs per-market
// data completeness tracking.
//
// A worker being "ready" (process up, all connector attempts finished) is a
// DIFFERENT concept from a market's data being complete (required market is
// streaming with a synced book). This tracker is the shared state model used
// by:
//   - the worker (lib/orderflow-worker.mjs) to compute its own completeness
//     at ready time and whenever a market degrades/reovers,
//   - the main thread (orderflow_monitor.mjs) to aggregate the per-market
//     view across workers and publish expected/running/degraded/completeness
//     in health output and IPC logs.
//
// Semantics (docs/current/data-contract.md § ready vs data_complete):
//   process_ready  : worker process started, connectors prepared, all initial
//                    connect attempts done (even when some markets degraded)
//   market_running : a connector reached 'running' (streaming + book synced)
//   market_degraded: market isolated from its feed (initial sync failure /
//                    watchdog restart), retried in the background
//   data_complete  : every REQUIRED market is running and none is degraded;
//                    optional markets never block completeness
//
// Complete and running are transient, re-observable facts: a market that
// drops into 'reconnecting' is not running at that instant, so completeness
// goes false until it streams again. Downstream must treat false as "data
// missing/degraded — do not present as a normal aggregate" (fail-visible).

/** Max recorded state transitions kept per market (memory bound). */
const MAX_TRANSITIONS_PER_MARKET = 20;

export class MarketStatusTracker {
  /**
   * @param {Object} [options]
   * @param {string[]} [options.expectedMarkets] markets this scope must cover
   * @param {string[]} [options.optionalMarkets] subset that never blocks data_complete
   */
  constructor({ expectedMarkets = [], optionalMarkets = [] } = {}) {
    this._expected = new Set(expectedMarkets);
    this._optional = new Set(optionalMarkets);
    /** @type {Map<string, {state: string, degradedReason: string|null, updatedAtMs: number, transitions: Array<{from: string, to: string, tsMs: number}>}>} */
    this._markets = new Map();
    for (const market of expectedMarkets) {
      this._markets.set(market, {
        state: 'unknown',
        degradedReason: null,
        updatedAtMs: 0,
        transitions: [],
      });
    }
  }

  _entry(market) {
    let entry = this._markets.get(market);
    if (!entry) {
      // A market not in the expected set is still tracked (defensive), but it
      // can never make data_complete false when it is not required.
      entry = { state: 'unknown', degradedReason: null, updatedAtMs: 0, transitions: [] };
      this._markets.set(market, entry);
    }
    return entry;
  }

  isRequired(market) {
    return this._expected.has(market) && !this._optional.has(market);
  }

  _recordTransition(market, entry, to) {
    const from = entry.state;
    if (from === to) return false;
    entry.transitions.push({ market, from, to, tsMs: Date.now() });
    if (entry.transitions.length > MAX_TRANSITIONS_PER_MARKET) {
      entry.transitions.splice(0, entry.transitions.length - MAX_TRANSITIONS_PER_MARKET);
    }
    return true;
  }

  /**
   * Apply one market's reported state.
   * @returns {{dataComplete: boolean, changed: boolean, recovered: boolean, transition: {market: string, from: string, to: string}|null}}
   */
  observeState(market, state) {
    const entry = this._entry(market);
    const wasDegraded = entry.degradedReason !== null;
    const before = this.snapshot().data_complete;
    const normalized = state || 'unknown';
    const recorded = this._recordTransition(market, entry, normalized);
    entry.state = normalized;
    entry.updatedAtMs = Date.now();
    let recovered = false;
    if (normalized === 'running') {
      if (wasDegraded) recovered = true;
      entry.degradedReason = null;
    }
    const after = this.snapshot().data_complete;
    return {
      dataComplete: after,
      changed: before !== after,
      recovered,
      transition: recorded
        ? { market, from: entry.transitions[entry.transitions.length - 1].from, to: normalized }
        : null,
    };
  }

  /**
   * Mark a market degraded (isolated, background retry) or clear it.
   * @returns {object} same shape as observeState
   */
  markDegraded(market, reason) {
    const entry = this._entry(market);
    const before = this.snapshot().data_complete;
    if (reason === null || reason === undefined || reason === '') {
      if (entry.degradedReason === null) {
        const after = this.snapshot().data_complete;
        return { dataComplete: after, changed: before !== after, recovered: false, transition: null };
      }
      entry.degradedReason = null;
      entry.state = entry.state === 'degraded' ? 'unknown' : entry.state;
      const after = this.snapshot().data_complete;
      return { dataComplete: after, changed: before !== after, recovered: false, transition: null };
    }
    const wasDegraded = entry.degradedReason !== null;
    const from = entry.state;
    const recorded = this._recordTransition(market, entry, 'degraded');
    entry.degradedReason = reason;
    entry.state = 'degraded';
    entry.updatedAtMs = Date.now();
    const after = this.snapshot().data_complete;
    return {
      dataComplete: after,
      changed: before !== after,
      recovered: false,
      transition: recorded ? { market, from, to: 'degraded' } : null,
    };
  }

  /** Apply the worker's authoritative startup report (from the 'ready' IPC). */
  applyReady(workerId, report = {}) {
    for (const item of report.markets ?? []) {
      if (!item?.market) continue;
      const reason = item.degradedReason ?? null;
      if (reason) this.markDegraded(item.market, reason);
      else this.observeState(item.market, item.state || 'running');
    }
    return this.snapshot();
  }

  /** Recompute the aggregate view. Pure read — never mutates. */
  snapshot() {
    const degradedMarkets = {};
    const markets = {};
    for (const [market, entry] of this._markets) {
      if (entry.degradedReason !== null) degradedMarkets[market] = entry.degradedReason;
      markets[market] = {
        state: entry.state,
        degraded_reason: entry.degradedReason,
        required: this.isRequired(market),
        updated_at_ms: entry.updatedAtMs,
        last_transition: entry.transitions.length
          ? entry.transitions[entry.transitions.length - 1]
          : null,
      };
    }
    const required = [...this._expected].filter((m) => this.isRequired(m));
    const running = [...this._expected].filter((m) => {
      const e = this._markets.get(m);
      return e && e.state === 'running' && e.degradedReason === null;
    });
    const dataComplete = required.length > 0
      && required.every((m) => {
        const e = this._markets.get(m);
        return e && e.state === 'running' && e.degradedReason === null;
      });
    const transitions = [];
    for (const [market, entry] of this._markets) {
      for (const t of entry.transitions) transitions.push(t);
    }
    transitions.sort((a, b) => a.tsMs - b.tsMs);
    return {
      ts_ms: Date.now(),
      expected_markets: [...this._expected],
      optional_markets: [...this._optional],
      running_markets: running,
      degraded_markets: degradedMarkets,
      data_complete: dataComplete,
      markets,
      transitions: transitions.slice(-50),
    };
  }
}
