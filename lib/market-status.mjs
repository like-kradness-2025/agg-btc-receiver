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

import fsp from 'node:fs/promises';
import path from 'node:path';

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

  /** Boolean aggregate: every REQUIRED market running and none degraded. */
  _computeDataComplete() {
    const required = [...this._expected].filter((m) => this.isRequired(m));
    // No required markets (all-optional scope) is vacuously complete —
    // nothing may block data_complete by definition of "required".
    return required.length === 0
      || required.every((m) => {
        const e = this._markets.get(m);
        return e && e.state === 'running' && e.degradedReason === null;
      });
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
    const before = this._computeDataComplete();
    const normalized = state || 'unknown';
    const recorded = this._recordTransition(market, entry, normalized);
    entry.state = normalized;
    entry.updatedAtMs = Date.now();
    let recovered = false;
    if (normalized === 'running') {
      if (wasDegraded) recovered = true;
      entry.degradedReason = null;
    }
    const after = this._computeDataComplete();
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
   * Apply a periodic stats-tick observation (the worker's 2s health push).
   * A stats snapshot may DOWNGRADE a market (running → reconnecting/error,
   * so a mid-run outage is reflected without waiting for a stateChange
   * frame) but it must never clear an active degradation: recovery is only
   * declared by explicit events (stateChange → running, marketRestarted,
   * ready report). Otherwise an isolated market whose connector keeps
   * ticking a stale 'running' stats payload would silently flip
   * data_complete back to true — a false recovery.
   */
  observeStatsState(market, state) {
    const entry = this._entry(market);
    if (entry.degradedReason !== null && state === 'running') {
      return { dataComplete: this._computeDataComplete(), changed: false, recovered: false, transition: null };
    }
    return this.observeState(market, state);
  }

  /**
   * Mark a market degraded (isolated, background retry) or clear it.
   * @returns {object} same shape as observeState
   */
  markDegraded(market, reason) {
    const entry = this._entry(market);
    const before = this._computeDataComplete();
    if (reason === null || reason === undefined || reason === '') {
      if (entry.degradedReason === null) {
        const after = this._computeDataComplete();
        return { dataComplete: after, changed: before !== after, recovered: false, transition: null };
      }
      entry.degradedReason = null;
      let transition = null;
      if (entry.state === 'degraded') {
        // Clear-without-recovery ends the degraded state in 'unknown' —
        // record it so the transition history stays complete (degraded →
        // unknown is observable, not a silent jump).
        const recorded = this._recordTransition(market, entry, 'unknown');
        entry.state = 'unknown';
        if (recorded) transition = { market, from: 'degraded', to: 'unknown' };
      }
      entry.updatedAtMs = Date.now();
      const after = this._computeDataComplete();
      return { dataComplete: after, changed: before !== after, recovered: false, transition };
    }
    const from = entry.state;
    const recorded = this._recordTransition(market, entry, 'degraded');
    let transition = null;
    if (!recorded && entry.state === 'degraded' && entry.degradedReason !== reason) {
      // PR #20 re-audit: already degraded with a DIFFERENT reason — the reason was
      // replaced (e.g. a module restart failure supersedes the original
      // isolation reason). Record it so the transition history shows why the
      // degradation persists/changed instead of an invisible reason swap.
      entry.transitions.push({ market, from: 'degraded', to: 'degraded', reason, tsMs: Date.now() });
      if (entry.transitions.length > MAX_TRANSITIONS_PER_MARKET) {
        entry.transitions.splice(0, entry.transitions.length - MAX_TRANSITIONS_PER_MARKET);
      }
      transition = { market, from: 'degraded', to: 'degraded', reason };
    }
    entry.degradedReason = reason;
    entry.state = 'degraded';
    entry.updatedAtMs = Date.now();
    const after = this._computeDataComplete();
    return {
      dataComplete: after,
      changed: before !== after,
      recovered: false,
      transition: recorded ? { market, from, to: 'degraded' } : transition,
    };
  }

  /** Apply the worker's authoritative startup report (from the 'ready' IPC). */
  applyReady(workerId, report = {}) {
    for (const item of report.markets ?? []) {
      if (!item?.market) continue;
      const reason = item.degradedReason ?? null;
      if (reason) this.markDegraded(item.market, reason);
      // A missing state is NOT running: it is an unknown market that must not
      // be presented as complete until an explicit running report arrives
      // (fail-visible default — never optimistically assume streaming).
      else this.observeState(item.market, item.state || 'unknown');
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
    const running = [...this._expected].filter((m) => {
      const e = this._markets.get(m);
      return e && e.state === 'running' && e.degradedReason === null;
    });
    const dataComplete = this._computeDataComplete();
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

// ====== receiver-market-status/v1 file projection (Issue #9 Done条件#8) ======
// The main thread derives the downstream market-status.json document from the
// SAME tracker snapshot that feeds health.jsonl (one state model, two
// projections). The file lives at
//   <dirname(receiver --database-dir)>/market-status.json
// (shared derivation with the downstream reader, override via --status-file)
// and is refreshed by the main thread on every state change plus a ~2s
// periodic flush. Downstream polls this file and treats a
// missing/unparsable/stale (>15s) file as unknown — NEVER as complete.

/** Default status file location shared with the downstream reader. */
export function defaultStatusFilePath(databaseDir) {
  return path.join(path.dirname(path.resolve(databaseDir)), 'market-status.json');
}

/**
 * Shape a MarketStatusTracker snapshot into the receiver-market-status/v1
 * document. Pure — no I/O.
 *
 * Contract keys (docs/current/data-contract.md § receiver market-status):
 *   schema / ts_ms / process_ready / expected_markets / optional_markets /
 *   running_markets / degraded_markets / data_complete /
 *   markets{state, degraded_reason, required, updated_at_ms}
 *
 * Semantics follow the tracker (§16 vocabulary): expected_markets lists every
 * enabled market (the optional subset is repeated in optional_markets);
 * data_complete is the tracker verdict over REQUIRED markets only (expected
 * minus optional). degraded_markets MAY include optional markets — they are
 * reported (fail-visible) but never flip data_complete. process_ready is a
 * separate main-thread fact (all workers accepted) and does not gate the
 * tracker's data_complete verdict.
 *
 * @param {Object} snapshot a MarketStatusTracker.snapshot()
 * @param {Object} [options]
 * @param {boolean} [options.processReady=true] main thread accepted all workers
 * @returns {Object} receiver-market-status/v1 document
 */
export function formatMarketStatusV1(snapshot, { processReady = true } = {}) {
  const markets = {};
  for (const [market, entry] of Object.entries(snapshot.markets ?? {})) {
    const degradedReason = entry.degraded_reason ?? null;
    markets[market] = {
      // A degraded market is NOT streaming even if its last connector state
      // said running/reconnecting moments before the isolation — degraded is
      // authoritative over the raw state (mirrors health-monitor.mjs).
      state: degradedReason ? 'degraded' : (entry.state ?? 'unknown'),
      degraded_reason: degradedReason,
      required: entry.required === true,
      updated_at_ms: entry.updated_at_ms ?? null,
    };
  }
  return {
    schema: 'receiver-market-status/v1',
    ts_ms: snapshot.ts_ms ?? Date.now(),
    process_ready: processReady === true,
    expected_markets: [...(snapshot.expected_markets ?? [])],
    optional_markets: [...(snapshot.optional_markets ?? [])],
    running_markets: [...(snapshot.running_markets ?? [])],
    degraded_markets: { ...(snapshot.degraded_markets ?? {}) },
    data_complete: snapshot.data_complete === true,
    markets,
  };
}

/**
 * Atomically persist the status document (tmp file + rename within the same
 * directory). No fsync is issued: a crash between writeFile and rename may
 * leave a `*.tmp-<pid>` residue and the previous file intact — readers MUST
 * ignore non-exact filenames and stale/old content (see data-contract.md).
 */
export async function writeMarketStatusFile(filePath, doc) {
  const destination = path.resolve(filePath);
  await fsp.mkdir(path.dirname(destination), { recursive: true });
  const temporary = `${destination}.tmp-${process.pid}`;
  await fsp.writeFile(temporary, `${JSON.stringify(doc)}\n`, 'utf8');
  await fsp.rename(temporary, destination);
}
