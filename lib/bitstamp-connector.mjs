// lib/bitstamp-connector.mjs — Bitstamp BTC/USD spot connector

import { TradeOnlyConnector } from './trade-only-connector.mjs';

const WS_URL = 'wss://ws.bitstamp.net';
const CHANNEL = 'live_trades_btcusd';

const toNumber = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const normalizeSide = (value) => {
  if (value == null) return null;
  if (typeof value === 'string') {
    const v = value.toLowerCase();
    if (v === 'buy' || v === 'b' || v === '0' || v === 'bid') return 'buy';
    if (v === 'sell' || v === 's' || v === '1' || v === 'ask') return 'sell';
  }
  if (value === 0) return 'buy';
  if (value === 1) return 'sell';
  return null;
};

const normalizeTs = (value) => {
  const n = Number(value);
  // Issue #12: unparseable source timestamp is unknown (null), never the
  // local receive time. Callers fail-closed on null via _emitTrade.
  if (!Number.isFinite(n)) return null;
  if (n > 1e15) return Math.floor(n / 1000);
  if (n > 1e12) return Math.floor(n);
  if (n > 1e9) return Math.floor(n * 1000);
  return Math.floor(n);
};

const normalizeLevels = (levels) => {
  if (!Array.isArray(levels)) return null;
  const normalized = levels.map((level) => {
    if (!Array.isArray(level) || level.length < 2) return null;
    const [price, qty] = level;
    if (String(price).trim() === '' || String(qty).trim() === '') return null;
    if (!Number.isFinite(Number(price)) || Number(price) <= 0
      || !Number.isFinite(Number(qty)) || Number(qty) < 0) return null;
    return [String(price), String(qty)];
  });
  return normalized.some((level) => level === null) ? null : normalized.filter(([, qty]) => Number(qty) > 0);
};

// Maximum REST sync attempts. A retry only ever moves the snapshot boundary
// forward in source time, so buffered diffs that could not be proven against
// an earlier snapshot are provably covered by the next one.
const MAX_SYNC_ATTEMPTS = 3;
const SYNC_RETRY_DELAY_MS = 500;

/**
 * Parse a Bitstamp source timestamp (`microtimestamp` preferred, seconds
 * `timestamp` fallback) into epoch milliseconds. Bitstamp publishes
 * `microtimestamp` in microseconds (~1.7e15); millisecond/second-shaped
 * values (fixtures, older payloads) are tolerated.
 * Issue #13: this is the ONLY timestamp allowed to decide the snapshot/diff
 * boundary — both the REST order_book response and WS diff events carry the
 * same matching-engine clock, so the comparison is a source-time proof, not
 * a wall-clock guess.
 * @param {Object|null} payload
 * @returns {number|null} ms since epoch, or null when absent/unparseable
 */
const parseSourceMs = (payload) => {
  if (!payload) return null;
  const n = Number(payload.microtimestamp ?? payload.timestamp);
  if (!Number.isFinite(n) || n <= 0) return null;
  if (n > 1e14) return Math.floor(n / 1000); // microseconds
  if (n > 1e12) return Math.floor(n);        // milliseconds
  return Math.floor(n * 1000);               // seconds
};

export class BitstampConnector extends TradeOnlyConnector {
  constructor(config) {
    super(config, {
      market: 'bitstamp_spot',
      wsUrl: config.wsUrl || WS_URL,
      restUrl: config.restUrl || '',
    });
    this._depthInitialized = false;
    this._depthSyncing = true;
    this._pendingDepth = [];
    this._pingTimer = null;
    // Issue #13: source microtimestamp (ms) of the last diff actually applied
    // to the book. Steady-state diffs must never move backwards from this —
    // a regression would re-apply an older diff over newer book state.
    this._lastAppliedDiffTsMs = null;
    // Stats semantics (Astra audit #19 P2-3): counters are only advanced when
    // the decision they describe is FINAL —
    //   snapshotIncludedDiffCount: diffs proven included in a snapshot that
    //     was actually APPLIED. Counted during partition, committed to the
    //     stat only after applySnapshot succeeds, so an aborted attempt can
    //     never pre-count diffs that the retry snapshot re-examines (no
    //     double counting across retries).
    //   boundaryResyncCount: boundary-provability aborts, one per aborted
    //     attempt. A single _syncBook may abort several attempts before it
    //     succeeds or errors, so the counter is "abort events", not "syncs".
    this._stats.snapshotIncludedDiffCount = 0;
    this._stats.boundaryResyncCount = 0;
  }

  _onOpen() {
    super._onOpen();
    // Bitstamp does NOT send server-side heartbeats (verified 2026-08-16:
    // zero bts:heartbeat over 75s), so during quiet order-book periods no
    // messages arrive and the 30s stale detector false-triggers a reconnect
    // loop. Keep the connection alive with client pings and treat every
    // pong as liveness so _lastMsgAt stays fresh.
    if (this._ws) {
      this._ws.on('pong', () => {
        this._lastMsgAt = Date.now();
      });
      this._startPingTimer();
    }
  }

  _startPingTimer() {
    this._stopPingTimer();
    this._pingTimer = setInterval(() => {
      if (this._ws && this._ws.readyState === 1) {
        try { this._ws.ping(); } catch { /* ignore */ }
      }
    }, 15000);
  }

  _stopPingTimer() {
    if (this._pingTimer) {
      clearInterval(this._pingTimer);
      this._pingTimer = null;
    }
  }

  _clearTimers() {
    this._stopPingTimer();
    super._clearTimers();
  }

  subscribe() {
    for (const ch of ['live_trades_btcusd', 'diff_order_book_btcusd']) {
      this._ws.send(JSON.stringify({
        event: 'bts:subscribe',
        data: { channel: ch },
      }));
    }
  }

  _onMessage(data) {
    if (!data || typeof data !== 'object') return;

    const event = data.event || data.event_type || data.type;
    const channel = data.channel || data.channel_name || '';

    if (event === 'bts:subscription_succeeded' || event === 'bts:unsubscription_succeeded' || event === 'bts:request_reconnect' || event === 'bts:heartbeat') {
      return;
    }

    // Depth channel
    if (event === 'data' && channel === 'diff_order_book_btcusd') {
      const payload = data.data;
      if (payload) {
        const bids = payload.bids || [];
        const asks = payload.asks || [];
        const ts = parseSourceMs(payload);
        if (this._depthSyncing) {
          // Issue #12: snapshot the ingress onto the pending entry so replay
          // after REST sync keeps the ORIGINAL receive time. Issue #13: the
          // source microtimestamp rides on the entry so the replay partition
          // can PROVE which buffered diffs the REST snapshot already covers.
          this._pendingDepth.push({
            bids,
            asks,
            ts,
            ingress: this._ingress ? { ...this._ingress } : null,
          });
        } else {
          // Issue #13 steady state: the WS diff stream must never move
          // backwards in source time. Re-applying an older diff over newer
          // book state would silently roll the book back, so any regression
          // is fail-closed: drop the frame and force a full re-sync through
          // the same provable-boundary path as reconnect.
          // Asymmetry vs the sync path (Astra audit #19 P2-1): a diff with
          // NO source timestamp (ts == null) skips this guard (ts != null is
          // required to compare) and is dropped below by _handleDepth's
          // _isValidTimestamp fail-closed check (droppedDepthCount), leaving
          // the connector 'running'. That is coherent because the book is
          // already fully applied: an unlocatable frame cannot roll it back,
          // the anchor (_lastAppliedDiffTsMs) is deliberately NOT advanced,
          // and the frame is treated like any invalid frame. The SYNC path
          // must be stricter: there a null-ts frame is buffered precisely
          // because the REST snapshot may NOT include it — silently dropping
          // it would lose an unaccounted change and reach 'running' with an
          // unprovable book, so _syncBook errors out instead (see partition).
          if (this._lastAppliedDiffTsMs != null && ts != null && ts < this._lastAppliedDiffTsMs) {
            this._stats.droppedDepthCount++;
            this._handleSequenceGap(
              `diff microtimestamp regression: ${ts} < last applied ${this._lastAppliedDiffTsMs}`,
              null);
            return;
          }
          // Issue #12: no explicit ingress arg here — this call site runs
          // inside the socket message callback, so _handleDepth→_emitDepth
          // resolves the CURRENT frame's ingress via _resolveIngress's
          // this._ingress fallback (same metadata as the buffered path).
          this._handleDepth('update', bids, asks, ts, null);
          if (ts != null) this._lastAppliedDiffTsMs = ts;
        }
      }
      return;
    }

    if (event !== 'trade' && channel !== CHANNEL) return;

    const payload = data.data ?? data;
    const trades = Array.isArray(payload)
      ? payload
      : (payload && Array.isArray(payload.data) ? payload.data : [payload]);

    for (const t of trades) {
      if (!t) continue;
      const price = toNumber(t.price ?? t.price_str ?? t.p ?? (Array.isArray(t) ? t[1] : null));
      const qty = toNumber(t.amount ?? t.amount_str ?? t.qty ?? t.q ?? (Array.isArray(t) ? t[2] : null));
      const side = normalizeSide(t.side ?? t.type ?? t.order_type ?? (Array.isArray(t) ? t[3] : null));
      const ts = normalizeTs(t.microtimestamp ?? t.timestamp ?? t.time ?? t.E ?? (Array.isArray(t) ? t[4] : null));
      const tradeId = String(t.id ?? t.trade_id ?? t.microtimestamp ?? `${ts}-${price}-${qty}`);
      if (price == null || qty == null || !side) continue;
      this._emitTrade(price, qty, side, ts, tradeId);
    }
  }

  /**
   * Issue #13 — provable snapshot/diff boundary.
   *
   * Both the REST order_book response and the WS `diff_order_book_btcusd`
   * events carry the matching-engine `microtimestamp`, so the boundary is
   * decided on SOURCE time, never wall clock:
   *
   *   boundary B = REST snapshot's source microtimestamp (ms)
   *   buffered diff with ts <  B  → provably already included in the
   *                                  snapshot (it was applied server-side
   *                                  before the snapshot was assembled) → DROP
   *   buffered diff with ts >  B  → provably newer than the snapshot → REPLAY
   *   buffered diff with ts == B  → inclusion is ambiguous (server internals
   *                                  decide whether the diff landed before the
   *                                  dump) → boundary NOT provable → re-sync
   *                                  with a newer snapshot (bounded retries),
   *                                  never a guessed apply/drop
   *   REST response without a usable microtimestamp → boundary unprovable →
   *                                  fail-closed: never reach 'running'
   *
   * The retry invariant: each new snapshot is fetched later, so its boundary
   * is strictly newer and provably covers every diff buffered during earlier
   * attempts — no buffered diff is ever lost or double-applied across retries.
   */
  async _syncBook() {
    if (this._state === 'reconnecting' || this._state === 'error') return;
    if (!this.restUrl) throw new Error(`${this.market}: REST orderbook URL is required for initial sync`);

    // Issue #19 (Astra P1): identity of THIS sync attempt. Every connect()
    // and every sequence-gap teardown bumps _wsGeneration; a retry loop that
    // outlives its generation must abandon silently instead of applying a
    // stale REST snapshot over a newer connection's already-recovered book
    // (which would roll the book back while the machine keeps reporting
    // 'running'). Checks run at every async boundary (fetch / json / retry
    // delay).
    const syncGeneration = this._wsGeneration;
    const superseded = () =>
      this._wsGeneration !== syncGeneration
      || this._state === 'reconnecting'
      || this._state === 'error';

    this._setState('syncing');
    this._depthSyncing = true;
    this._depthInitialized = false;
    this._lastAppliedDiffTsMs = null;

    let lastError = null;
    for (let attempt = 0; attempt < MAX_SYNC_ATTEMPTS; attempt++) {
      try {
        const response = await fetch(this.restUrl, { signal: AbortSignal.timeout(10000) });
        if (superseded()) return; // a newer connection owns the connector now
        if (response.ok === false) throw new Error(`REST orderbook HTTP ${response.status}`);
        const payload = await response.json();
        if (superseded()) return;
        const bids = normalizeLevels(payload?.bids);
        const asks = normalizeLevels(payload?.asks);
        if (!bids || !asks || !bids.length || !asks.length) {
          throw new Error(`REST orderbook is incomplete: bids=${bids?.length ?? 0} asks=${asks?.length ?? 0}`);
        }

        // Boundary MUST come from the exchange clock carried by the snapshot
        // itself. No microtimestamp → the snapshot/diff inclusion relation is
        // unprovable → fail-closed (retry, then error; never 'running').
        const boundaryMs = parseSourceMs(payload);
        if (boundaryMs == null) {
          throw new Error('REST snapshot has no source microtimestamp; snapshot/diff boundary unprovable (fail-closed)');
        }

        // Partition buffered diffs against the boundary BEFORE anything is
        // applied, so a stale diff can never roll the book back after the
        // snapshot and a newer diff can never be lost. Any event that cannot
        // be PROVEN covered-or-newer against the boundary aborts this attempt
        // (boundaryResyncCount) and re-syncs with a strictly newer snapshot,
        // which provably covers it; if the condition persists past
        // MAX_SYNC_ATTEMPTS the sync errors instead of reaching 'running'.
        const pending = this._pendingDepth;
        const replay = [];
        let boundaryUnprovable = false;
        let boundaryReason = null;
        // Issue #19 (Astra P2): a null-ts buffered diff is PERMANENTLY
        // unresolvable — no newer snapshot boundary can ever locate it, so
        // retrying only wastes the remaining attempts on identical failures.
        let permanentBoundaryFailure = false;
        let lastReplayTs = boundaryMs;
        // Diffs proven inside THIS attempt's boundary; committed to the stat
        // only when the snapshot below is actually applied (P2-3: an aborted
        // attempt must not pre-count them for the retry to re-count).
        let includedSinceBoundary = 0;
        for (const event of pending) {
          if (event.ts == null) {
            // Issue #19 (Astra P1): a diff with NO source timestamp cannot be
            // located against the boundary — it may be NEWER than the
            // snapshot, so dropping it and declaring the boundary proven
            // would lose an unaccounted change. Fail-closed: treat it like an
            // unprovable boundary; it can never be resolved by a retry, so
            // fail immediately instead of burning MAX_SYNC_ATTEMPTS retries.
            boundaryUnprovable = true;
            permanentBoundaryFailure = true;
            boundaryReason = 'buffered diff has no source timestamp (boundary unprovable, fail-closed)';
            break;
          }
          if (event.ts < boundaryMs) {
            // Provably inside the REST snapshot: re-applying it would be the
            // exact stale-replay rollback this issue forbids.
            includedSinceBoundary++;
            continue;
          }
          if (event.ts === boundaryMs) {
            // Ambiguous inclusion. Do NOT guess: a newer snapshot provably
            // covers this diff, so restart the sync with a fresh boundary.
            boundaryUnprovable = true;
            boundaryReason = `buffered diff at ts=${event.ts} equals snapshot boundary; re-sync with newer snapshot (fail-closed)`;
            break;
          }
          if (event.ts < lastReplayTs) {
            // Issue #19 (Astra P1): the diff stream must never move backwards
            // in source time. Replaying an OLDER diff after a newer one would
            // roll the book back — the same regression the steady-state
            // guard in _onMessage forbids, but replay called _handleDepth
            // directly and skipped it. Detected here, BEFORE any book
            // mutation: the next, strictly newer snapshot provably covers
            // both diffs.
            boundaryUnprovable = true;
            boundaryReason = `buffered diff at ts=${event.ts} regresses over earlier diff ts=${lastReplayTs}; re-sync with newer snapshot (fail-closed)`;
            break;
          }
          replay.push(event);
          lastReplayTs = event.ts;
        }
        if (boundaryUnprovable) {
          this._stats.boundaryResyncCount++;
          const err = new Error(boundaryReason);
          if (permanentBoundaryFailure) err.permanentBoundaryFailure = true;
          throw err;
        }

        this.book.applySnapshot(bids, asks, null);
        this._depthSyncing = false;
        this._depthInitialized = true;
        this._stats.resyncCount++;
        // Commit only now: the snapshot that proves these diffs included was
        // actually applied, so an earlier aborted attempt's pre-counts can
        // never be double-counted by this attempt (P2-3).
        this._stats.snapshotIncludedDiffCount += includedSinceBoundary;
        // Snapshot ts = its own source microtimestamp (ms), matching the
        // source-time ts of replayed diffs so downstream ordering never mixes
        // the receive wall clock with exchange event time.
        this._emitDepth('snapshot', bids, asks, boundaryMs, null, {
          snapshot_origin: 'rest_sync',
          snapshot_asof_ts_ms: boundaryMs,
          source_event_ts_ms: boundaryMs,
          source_event_time_known: true,
          event_time_source: 'rest_snapshot_source',
        }, this._localCapture());

        for (const event of replay) {
          this._handleDepth('update', event.bids, event.asks, event.ts, null, event.ingress ?? null);
        }
        // Partition already proved the replay is source-time monotonic, so the
        // last replayed ts (or the boundary when the replay is empty) is a
        // valid steady-state anchor.
        this._lastAppliedDiffTsMs = lastReplayTs;
        this._pendingDepth = [];
        this._setState('running');
        return;
      } catch (error) {
        if (superseded()) return;
        lastError = error;
        // Fail fast on a PERMANENTLY unresolvable boundary (buffered diff
        // with no source timestamp): a strictly newer snapshot cannot locate
        // it either, so each retry would re-fetch and fail identically —
        // skip straight to 'error' (P2-2). Transient conditions (ts == B,
        // regression) keep the bounded retry: a newer boundary provably
        // covers them.
        if (error?.permanentBoundaryFailure) break;
        if (attempt < MAX_SYNC_ATTEMPTS - 1) {
          // Buffered diffs are intentionally KEPT across attempts: the next
          // snapshot's boundary is strictly newer and provably covers them.
          await new Promise((resolve) => setTimeout(resolve, SYNC_RETRY_DELAY_MS));
          if (superseded()) return; // a reconnect took over during the delay
        }
      }
    }

    if (superseded()) return; // exhausted, but no longer our machine to fail
    this._depthSyncing = false;
    this._depthInitialized = false;
    this._pendingDepth = [];
    this._setState('error');
    this.emit('error', { market: this.market, message: `REST orderbook sync failed: ${lastError?.message ?? 'unknown error'}` });
    throw lastError ?? new Error(`REST orderbook sync failed for ${this.market}`);
  }

  /**
   * Handle depth data from diff_order_book_btcusd channel.
   * @param {'snapshot'|'update'} type
   * @param {Array<[string, string]>} bids - [price, amount] pairs
   * @param {Array<[string, string]>} asks - [price, amount] pairs
   * @param {number} ts - timestamp in milliseconds
   * @param {number|null} seq - sequence number (null for Bitstamp)
   * @param {Object|null} [ingress] - ingress metadata snapshot (replay)
   */
  _handleDepth(type, bids, asks, ts, seq, ingress = null) {
    if (!this._isValidTimestamp(ts)) {
      this._stats.droppedDepthCount++;
      return false;
    }
    if (type === 'snapshot') {
      this.book.applySnapshot(bids, asks, seq);
    } else {
      for (const [price, qty] of bids) {
        this.book.applyDiff('bid', price, qty, seq);
      }
      for (const [price, qty] of asks) {
        this.book.applyDiff('ask', price, qty, seq);
      }
    }
    this._emitDepth(type, bids, asks, ts, seq, {}, ingress ?? null);
    return true;
  }

  _resetBook() {
    this._depthInitialized = false;
    this._depthSyncing = true;
    this._pendingDepth = [];
    this._lastAppliedDiffTsMs = null;
    super._resetBook();
  }
}
