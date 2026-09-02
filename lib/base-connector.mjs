// lib/base-connector.mjs — Base connector class for btc-receiver v3.00
import { EventEmitter } from 'node:events';

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30000;
const MAX_RECONNECT_ATTEMPTS = 30;
// HTTP 429 (rate limited) deserves its own, much longer backoff curve. Reusing
// the generic 30s-capped backoff hammers the exchange and extends the ban.
const RATE_LIMIT_BASE_MS = 60000;
const RATE_LIMIT_MAX_MS = 600000;
// Circuit breaker for repeated sequence gaps (checksum mismatch, crossed
// books, ...): N gaps inside the window add one extra cooldown to the next
// reconnect instead of dropping the socket every few seconds.
const SEQ_GAP_WINDOW_MS = 600000;
const SEQ_GAP_THRESHOLD = 5;
const SEQ_GAP_EXTRA_DELAY_MS = 120000;
const STALE_MSG_THRESHOLD_MS = 30000;
const RING_BUF_MAX = 65536;
let connectionSequence = 0;

/** @typedef {import('./events.mjs').ConnectorState} ConnectorState */

export class BaseConnector extends EventEmitter {
  /**
   * @param {Object} config
   * @param {Object} options
   * @param {string} options.market
   * @param {string} options.wsUrl
   * @param {string} options.restUrl
   */
  constructor(config, { market, wsUrl, restUrl }) {
    super();
    this.config = config;
    this.market = market;
    this.wsUrl = wsUrl;
    this.restUrl = restUrl;

    /** @type {ConnectorState} */
    this._state = 'init';
    this._ws = null;
    this._wsGeneration = 0;
    this._reconnectAttempt = 0;
    this._reconnectTimer = null;
    this._errorRecoveryTimer = null;
    this._rateLimitAttempt = 0;
    this._lastWsError = null;
    this._seqGapTimestamps = [];
    this._staleTimer = null;
    this._periodicSnapshotTimer = null;
    this._isShuttingDown = false;
    this._firstRunningDiff = false;

    this._ringBuf = [];
    this._ringBufPos = 0;
    this._lastMsgAt = 0;

    this._wsSnapshotReceived = false;
    this._wsSnapshotSeq = null;
    this._wsSnapshotWaiter = null;
    this._connectionId = null;

    this._stats = {
      state: 'init',
      connectedAt: 0,
      lastDepthMsgAt: 0,
      lastTradeMsgAt: 0,
      depthMsgCount: 0,
      tradeMsgCount: 0,
      droppedTradeCount: 0,
      droppedLiquidationCount: 0,
      reconnectCount: 0,
      resyncCount: 0,
      lastSeq: 0,
      droppedDepthCount: 0,
    };
  }

  // ====== Public API ======

  /**
   * Connect to WebSocket.
   * @returns {Promise<void>}
   */
  connect() {
    this._setState('connecting');
    // Socket generation guard: every connect call gets a fresh generation id.
    // Callbacks from stale sockets (delayed close/error/events) must never
    // mutate state, schedule a reconnect, or emit data after a newer socket
    // has taken over. This prevents concurrent socket duplicates feeding the
    // same market twice (observed as 2.0x raw trade duplication).
    const generation = ++this._wsGeneration;
    // Ensure any previous socket is fully closed before opening a new one.
    this._closeCurrentSocket();
    return new Promise((resolve, reject) => {
      (async () => {
        try {
          const WebSocket = await this._getWsImpl();
          if (!WebSocket) throw new Error('No WebSocket implementation available (install ws or use global WebSocket)');
          // Another connect() may have started while awaiting _getWsImpl();
          // if so, this socket is already obsolete — abandon it.
          if (generation !== this._wsGeneration) {
            throw new Error('connect superseded by newer generation');
          }
          this._connectionId = `${this.market}:${process.pid}:${++connectionSequence}`;
          this._ws = new WebSocket(this.wsUrl);

          /** Tracks whether the connect promise has settled (resolve/reject once). */
          let settled = false;
          const safeResolve = () => { if (!settled) { settled = true; resolve(); } };
          const safeReject = (err) => { if (!settled) { settled = true; reject(err); } };

          this._ws.on('open', () => {
            if (generation !== this._wsGeneration) return; // stale socket
            this._reconnectAttempt = 0;
            this._rateLimitAttempt = 0;
            this._lastWsError = null;
            this._stats.connectedAt = Date.now();
            this._lastMsgAt = Date.now();
            this._setState('connected');
            this._startStaleTimer();
            this._onOpen();
            safeResolve();
          });

          this._ws.on('message', (raw) => {
            if (generation !== this._wsGeneration) return; // stale socket
            this._lastMsgAt = Date.now();
            if (this._preprocessRaw(raw)) return;
            try {
              const data = JSON.parse(raw.toString());
              this._onMessage(data);
            } catch (err) {
              this.emit('error', { market: this.market, message: `parse error: ${err.message}`, raw: raw.toString() });
            }
          });

          this._ws.on('close', (code, reason) => {
            console.error(`[${this.market}] WS CLOSE code=${code} reason=${reason ? reason.toString() : '(empty)'} settled=${settled} state=${this._state} gen=${generation}/${this._wsGeneration}`);
            // Stale socket close (superseded by a newer generation): ignore.
            // The newer socket's own close/error path owns reconnect semantics.
            if (generation !== this._wsGeneration) return;
            if (settled) {
              // Runtime close after connection established — original reconnect logic
              if (!this._isShuttingDown && this._state !== 'reconnecting' && this._state !== 'error') {
                this._setState('reconnecting');
                this._scheduleReconnect();
              }
            } else {
              // Close before open — reject the connect promise so startup can proceed
              safeReject(new Error(`${this.market}: WebSocket closed before open`));
              if (!this._isShuttingDown) {
                this._setState('reconnecting');
                this._scheduleReconnect();
              }
            }
          });

          this._ws.on('error', (err) => {
            if (generation !== this._wsGeneration) return; // stale socket
            if (err?.message) this._lastWsError = String(err.message);
            this.emit('error', { market: this.market, message: err.message });
            if (settled) {
              // Runtime error after connection established — original reconnect logic
              if (!this._isShuttingDown && this._ws && (this._ws.readyState !== 1 && this._ws.readyState !== 0)) {
                this._setState('reconnecting');
                this._scheduleReconnect();
              }
            } else {
              // Error before open — reject the connect promise so startup can proceed
              safeReject(new Error(`${this.market}: WebSocket error before open: ${err.message}`));
              if (!this._isShuttingDown) {
                this._setState('reconnecting');
                this._scheduleReconnect();
              }
            }
          });
        } catch (err) {
          reject(err);
        }
      })();
    });
  }

  /**
   * Close the currently tracked socket (if any). Generation bumping is the
   * caller's job — this only ensures no previous socket remains open when a
   * new connect() begins, so stale sockets cannot keep feeding trades.
   */
  _closeCurrentSocket() {
    const current = this._ws;
    this._ws = null;
    if (current) {
      try {
        current.removeAllListeners?.('message');
        current.removeAllListeners?.('open');
        current.removeAllListeners?.('error');
        current.removeAllListeners?.('close');
        // hard close after removing listeners so no stale callbacks fire
        current.terminate?.();
        current.close?.(1000, 'superseded');
      } catch { /* ignore close errors of a stale socket */ }
    }
  }

  /** Graceful disconnect. */
  disconnect() {
    this._isShuttingDown = true;
    this._wsGeneration++; // invalidate any pending callbacks
    this._clearTimers();
    this._clearWsSnapshotWaiter();
    if (this._ws) {
      try { this._ws.close(1000, 'shutdown'); } catch {}
      this._ws = null;
    }
    this._setState('init');
  }

  /**
   * Subscribe to depth and trade channels. Subclasses MUST override.
   */
  subscribe() {
    throw new Error(`${this.market}: subscribe() must be overridden`);
  }

  /** @returns {ConnectorState} */
  getState() { return this._state; }

  /** @returns {Object} */
  getStats() {
    return { ...this._stats, state: this._state };
  }

  // ====== Subclass hooks ======

  _onOpen() { this.subscribe(); }

  /**
   * @param {Object} data
   */
  _onMessage(data) {
    throw new Error(`${this.market}: _onMessage() must be overridden`);
  }

  /** Init sync protocol. */
  async _syncBook() {
    this._setState('syncing');
    this._ringBuf = [];
    this._ringBufPos = 0;

    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const snapshot = await this._fetchSnapshot();
        if (typeof this._validateSnapshotLevels === 'function'
          && !this._validateSnapshotLevels(snapshot)) {
          throw new Error(`${this.market}: invalid REST depth snapshot`);
        }
        const valid = this._validateSync(snapshot);
        if (valid) {
          this._applyRingBuf(snapshot);
          this._stats.resyncCount++;
          this._stats.lastSeq = this.book._lastSeq || snapshot.lastUpdateId || 0;
          this._firstRunningDiff = true;
          this._setState('running');
          this._ringBuf = [];
          return;
        }
      } catch (err) {
        this.emit('error', { market: this.market, message: `sync attempt ${attempt} failed: ${err.message}` });
      }
      // Delay between retries to avoid rate limiting / allow buffer accumulation
      if (attempt < 2) await new Promise(resolve => setTimeout(resolve, 1000));
    }
    this._setState('error');
    this.emit('error', { market: this.market, message: 'init sync failed after 3 retries' });
    throw new Error(`init sync failed for ${this.market} after 3 retries`);
  }

  async _fetchSnapshot() {
    throw new Error(`${this.market}: _fetchSnapshot() must be overridden`);
  }

  /** @param {Object} snapshot @returns {boolean} */
  _validateSync(snapshot) {
    throw new Error(`${this.market}: _validateSync() must be overridden`);
  }

  /** Apply ring-buffered messages after snapshot. */
  _applyRingBuf(snapshot) {
    for (const msg of this._ringBuf) {
      this._applyDiff(msg);
    }
  }

  /** @param {Object} msg */
  _applyDiff(msg) {
    throw new Error(`${this.market}: _applyDiff() must be overridden`);
  }

  /** Buffer incoming message during sync. */
  _bufferMsg(msg) {
    this._lastMsgAt = Date.now();
    if (this._ringBuf.length < RING_BUF_MAX) {
      this._ringBuf.push(msg);
    } else {
      this._ringBuf[this._ringBufPos % RING_BUF_MAX] = msg;
      this._ringBufPos++;
    }
  }

  /**
   * Reset buffered snapshot sync state.
   * Subclasses call this before waiting for the first snapshot on connect/reconnect.
   * @param {Object} [options]
   * @param {boolean} [options.clearRingBuf=true]
   */
  _beginWsSnapshotSync({ clearRingBuf = true } = {}) {
    if (clearRingBuf) {
      this._ringBuf = [];
      this._ringBufPos = 0;
    }
    this._wsSnapshotReceived = false;
    this._wsSnapshotSeq = null;
    this._clearWsSnapshotWaiter();
  }

  /**
   * Wait for a WebSocket snapshot to arrive.
   * @param {number} timeoutMs
   * @param {string} timeoutMessage
   * @returns {Promise<void>}
   */
  _waitForWsSnapshot(timeoutMs = 15000, timeoutMessage = 'ws snapshot timeout') {
    if (this._wsSnapshotReceived) return Promise.resolve();

    return new Promise((resolve, reject) => {
      const waiter = {
        settled: false,
        timer: null,
        resolve: null,
        reject: null,
      };

      const settle = (fn, value) => {
        if (waiter.settled) return;
        waiter.settled = true;
        if (waiter.timer) clearTimeout(waiter.timer);
        if (this._wsSnapshotWaiter === waiter) this._wsSnapshotWaiter = null;
        fn(value);
      };

      waiter.resolve = () => settle(resolve);
      waiter.reject = (err) => settle(reject, err);
      waiter.timer = setTimeout(() => {
        const err = new Error(timeoutMessage);
        err.code = 'WS_SNAPSHOT_TIMEOUT';
        settle(reject, err);
      }, timeoutMs);

      this._wsSnapshotWaiter = waiter;
    });
  }

  /** Mark snapshot received and unblock any waiter. */
  _notifyWsSnapshotReceived(seq = null) {
    this._wsSnapshotReceived = true;
    this._wsSnapshotSeq = seq;

    const waiter = this._wsSnapshotWaiter;
    if (!waiter) return;
    waiter.resolve();
  }

  /** Abort any pending WS snapshot wait. */
  _clearWsSnapshotWaiter() {
    const waiter = this._wsSnapshotWaiter;
    if (!waiter) return;
    this._wsSnapshotWaiter = null;
    if (waiter.timer) clearTimeout(waiter.timer);
    waiter.reject(Object.assign(new Error('ws snapshot wait aborted'), { code: 'WS_SNAPSHOT_ABORTED' }));
  }

  /** Finalize a successful WS/REST snapshot sync. */
  _finalizeWsSnapshotSync() {
    this._stats.resyncCount++;
    this._setState('running');
  }

  /** Emit standardized DepthEvent. */
  _isValidTimestamp(ts) {
    return typeof ts === 'number' && Number.isFinite(ts) && ts >= 0;
  }

  _validateSnapshotLevels(snapshot) {
    const validLevels = (levels) => Array.isArray(levels) && levels.every((level) => {
      if (!Array.isArray(level) || level.length < 2) return false;
      const [price, quantity] = level;
      if ((typeof price !== 'number' && typeof price !== 'string')
        || (typeof quantity !== 'number' && typeof quantity !== 'string')) return false;
      if (String(price).trim() === '' || String(quantity).trim() === '') return false;
      const numericPrice = Number(price);
      const numericQuantity = Number(quantity);
      return Number.isFinite(numericPrice) && numericPrice > 0
        && Number.isFinite(numericQuantity) && numericQuantity >= 0;
    });
    return snapshot != null
      && validLevels(snapshot.bids)
      && validLevels(snapshot.asks);
  }

  /** Emit standardized DepthEvent. */
  _emitDepth(type, bids, asks, ts, seq, meta = {}) {
    // Fail-closed: reject non-number, non-finite, or negative timestamps
    if (!this._isValidTimestamp(ts)) {
      this._stats.droppedDepthCount++;
      return;
    }
    this._stats.lastDepthMsgAt = Date.now();
    this._stats.depthMsgCount++;
    if (seq != null) this._stats.lastSeq = seq;
    this.emit('depth', {
      ...meta,
      market: this.market,
      type,
      bids,
      asks,
      ts,
      seq,
      connection_id: this._connectionId,
    });
    return true;
  }

  /** Emit raw depth frame for orderflow persistence. */
  _emitRawDepth(bids, asks, ts, seq, meta) {
    // Fail-closed: reject non-number, non-finite, or negative timestamps
    if (!this._isValidTimestamp(ts)) return;
    this._stats.lastDepthMsgAt = Date.now();
    this._stats.depthMsgCount++;
    if (seq != null) this._stats.lastSeq = seq;
    this.emit('rawDepth', {
      ...(meta || {}),
      market: this.market,
      bids,
      asks,
      ts,
      seq,
      connection_id: this._connectionId,
    });
  }

  /** Track when a depth event was actually applied to the book. */
  _noteDepthEventApplied(eventTime) {
    this._stats.lastDepthEventAppliedAt = eventTime;
  }

  /** Emit standardized TradeEvent. */
  _emitTrade(price, qty, side, ts, tradeId) {
    if (side !== 'buy' && side !== 'sell') {
      this._stats.droppedTradeCount++;
      return;
    }
    if (typeof price !== 'number' || !Number.isFinite(price) || price <= 0
      || typeof qty !== 'number' || !Number.isFinite(qty) || qty <= 0
      || !this._isValidTimestamp(ts)) {
      this._stats.droppedTradeCount++;
      return;
    }
    // Fail-closed: reject non-number, non-finite, or negative timestamps
    this._stats.lastTradeMsgAt = Date.now();
    this._stats.tradeMsgCount++;
    this.emit('trade', { market: this.market, price, qty, side, ts, tradeId });
  }

  /**
   * Emit standardized liquidation event.
   * @param {Object} opts
   * @param {string} opts.exchange
   * @param {string} opts.symbol
   * @param {'buy'|'sell'} opts.side
   * @param {number} opts.price
   * @param {number} opts.qty
   * @param {number|null} [opts.notional]
   * @param {string} [opts.raw_type]  e.g. 'forceOrder', 'liquidation'
   * @param {string} [opts.trade_id]
   * @param {number} [opts.source_ts]
   */
  _emitLiquidation({ exchange, symbol, side, price, qty, notional, raw_type, trade_id, source_ts }) {
    if (side !== 'buy' && side !== 'sell'
      || typeof price !== 'number' || !Number.isFinite(price) || price <= 0
      || typeof qty !== 'number' || !Number.isFinite(qty) || qty <= 0
      || notional != null && (typeof notional !== 'number' || !Number.isFinite(notional) || notional < 0)
      || source_ts != null && !this._isValidTimestamp(source_ts)) {
      this._stats.droppedLiquidationCount++;
      return false;
    }
    this.emit('liquidation', {
      ts: Date.now(),
      market: this.market,
      exchange: exchange || this.market.replace(/_.*$/, ''),
      symbol: symbol || this.config?.symbol || '',
      side,
      price,
      qty,
      notional: notional != null ? notional : price * qty,
      raw_type: raw_type || 'liquidation',
      trade_id: trade_id || null,
      source_ts: source_ts ?? null,
    });
    return true;
  }

  // ====== State machine ======

  /** @param {ConnectorState} newState */
  _setState(newState) {
    const old = this._state;
    this._state = newState;
    this._stats.state = newState;
    if (newState === 'running') this._startPeriodicSnapshotTimer();
    else this._stopPeriodicSnapshotTimer();
    this.emit('stateChange', old, newState);
  }

  _startPeriodicSnapshotTimer() {
    this._stopPeriodicSnapshotTimer();
    const interval = this.config?.raw_snapshot_interval_ms;
    if (!Number.isInteger(interval) || interval <= 0) return;
    this._periodicSnapshotTimer = setInterval(() => {
      if (this._state !== 'running' || !this.book || this.book.isEmpty?.()) return;
      const snapshot = this.book.toSnapshot(Date.now());
      this._emitDepth('snapshot', snapshot.bids, snapshot.asks, snapshot.ts, snapshot.seq ?? null, {
        snapshot_origin: 'periodic_book',
      });
    }, interval);
    this._periodicSnapshotTimer.unref?.();
  }

  _stopPeriodicSnapshotTimer() {
    if (!this._periodicSnapshotTimer) return;
    clearInterval(this._periodicSnapshotTimer);
    this._periodicSnapshotTimer = null;
  }

  /** Mark snapshot-sync failure and emit a standardized error event. */
  _failWsSnapshotSync(message) {
    this._setState('error');
    this.emit('error', { market: this.market, message });
  }

  // ====== Reconnect ======

  _scheduleReconnect() {
    if (this._isShuttingDown) return;
    if (this._reconnectTimer) {
      // Already scheduled a reconnect — only kill stale watchdog intervals,
      // preserve the existing reconnect timer and do not double-count.
      this._clearStaleTimer();
      return;
    }
    this._clearTimers();
    const rateLimited = /429|rate.?limit/i.test(String(this._lastWsError || ''));
    if (rateLimited) {
      // Rate limited: use the dedicated long curve and keep the generic
      // attempt counter untouched so the 30-attempt give-up sawtooth
      // (error → 60s recovery −10 attempts → retry) can never engage.
      this._reconnectAttempt = 0;
      this._rateLimitAttempt++;
      this._setState('reconnecting');
      this._stats.reconnectCount++;
      const delay = Math.min(RATE_LIMIT_BASE_MS * Math.pow(2, this._rateLimitAttempt - 1), RATE_LIMIT_MAX_MS);
      console.error(`[${this.market}] rate-limited (429), backing off ${Math.round(delay / 1000)}s (attempt ${this._rateLimitAttempt})`);
      this._reconnectTimer = setTimeout(() => {
        this._reconnectTimer = null;
        this._startReconnect();
      }, delay);
      return;
    }
    if (this._seqGapTimestamps.length) {
      const cutoff = Date.now() - SEQ_GAP_WINDOW_MS;
      this._seqGapTimestamps = this._seqGapTimestamps.filter((t) => t > cutoff);
    }
    let extraDelayMs = 0;
    if (this._seqGapTimestamps.length >= SEQ_GAP_THRESHOLD) {
      extraDelayMs = SEQ_GAP_EXTRA_DELAY_MS;
      console.error(`[${this.market}] seq-gap circuit breaker: ${this._seqGapTimestamps.length} gaps in ${Math.round(SEQ_GAP_WINDOW_MS / 1000)}s, adding ${Math.round(SEQ_GAP_EXTRA_DELAY_MS / 1000)}s cooldown`);
    }
    if (this._reconnectAttempt >= MAX_RECONNECT_ATTEMPTS) {
      this._setState('error');
      this.emit('error', { market: this.market, message: `reconnect failed after ${MAX_RECONNECT_ATTEMPTS} attempts` });
      // Schedule error recovery: wait cooldown then reduce count and retry
      if (!this._errorRecoveryTimer) {
        this._errorRecoveryTimer = setTimeout(() => {
          this._errorRecoveryTimer = null;
          this._reconnectAttempt = Math.max(0, this._reconnectAttempt - 10);
          this._scheduleReconnect();
        }, 60000);
      }
      return;
    }
    this._stats.reconnectCount++;
    this._setState('reconnecting');

    const delay = this._backoffDelay() + extraDelayMs;
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer = null;
      this._startReconnect();
    }, delay);
  }

  _backoffDelay() {
    this._reconnectAttempt++;
    const base = Math.min(
      RECONNECT_BASE_MS * Math.pow(2, this._reconnectAttempt - 1),
      RECONNECT_MAX_MS
    );
    const jitter = Math.random() * 1000;
    return Math.round(base + jitter);
  }

  async _startReconnect() {
    if (this._isShuttingDown) return;
    this._reconnectTimer = null; // Clear the timer that scheduled this call so future reconnects are not blocked
    this._resetBook();
    // Defensive: drop any socket that may still be tracked before connect()
    // opens a new one (connect() also does this, but belt-and-suspenders).
    this._closeCurrentSocket();
    try {
      await this.connect();
      await this._syncBook();
    } catch (err) {
      if (err?.message) this._lastWsError = String(err.message);
      this.emit('error', { market: this.market, message: `reconnect failed: ${err.message}` });
      this._scheduleReconnect();
    }
  }

  /** Handle non-JSON messages. Override if needed. */
  _preprocessRaw(raw) { return false; }

  /** Clear local book state and re-enter snapshot sync. */
  _resetBook() {
    if (this.book && typeof this.book.clear === 'function') {
      this.book.clear();
    }
    this._beginWsSnapshotSync();
  }

  /**
   * Handle sequence gap / out-of-order depth event.
   * Resets book, emits error, closes socket, and schedules reconnect/resync.
   * Avoids tight loop by checking state before scheduling.
   * @param {string} reason
   * @param {Object} event
   */
  _handleSequenceGap(reason, event) {
    if (this._isShuttingDown) return;
    if (this._state === 'reconnecting' || this._state === 'error') return;
    this._seqGapTimestamps.push(Date.now());
    if (this._seqGapTimestamps.length > 100) this._seqGapTimestamps.shift();
    this._resetBook();
    this.emit('error', { market: this.market, message: `seq gap: ${reason}` });
    // Close existing socket to stop incoming data during reconnection
    if (this._ws) {
      try { this._ws.close(1000, 'sequence gap'); } catch { /* ignore */ }
      this._ws = null;
    }
    this._scheduleReconnect();
  }

  // ====== Stale detection ======

  _startStaleTimer() {
    this._clearStaleTimer();
    this._staleTimer = setInterval(() => {
      if (['running', 'connected', 'syncing'].includes(this._state)) {
        const elapsed = Date.now() - this._lastMsgAt;
        if (elapsed > STALE_MSG_THRESHOLD_MS) {
          this.emit('error', { market: this.market, message: `no message for ${elapsed}ms, reconnecting` });
          // Close existing socket before scheduling reconnect to prevent
          // resource leak and double-connection where old socket events
          // trigger another reconnect cycle.
          if (this._ws) {
            try { this._ws.close(1000, 'stale'); } catch { /* ignore */ }
            this._ws = null;
          }
          this._scheduleReconnect();
        }
      }
    }, 5000);
  }

  _clearStaleTimer() {
    if (this._staleTimer) {
      clearInterval(this._staleTimer);
      this._staleTimer = null;
    }
  }

  _clearTimers() {
    this._clearStaleTimer();
    this._stopPeriodicSnapshotTimer();
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
    if (this._errorRecoveryTimer) {
      clearTimeout(this._errorRecoveryTimer);
      this._errorRecoveryTimer = null;
    }
  }

  /**
   * Lazy import of `ws` to avoid requiring it in test-only scenarios.
   * Subclasses may override for testing with mock WebSocket.
   */
  _getWebSocket() {
    // Use dynamic import so package.json 'ws' dependency is only needed at runtime
    // Use lazy import for ESM compatibility
    return globalThis.WebSocket || null;
  }

  /**
   * Override to provide mock WebSocket for testing.
   * @param {Function} wsCtor
   */
  _setWebSocket(wsCtor) {
    // This will be used if set, otherwise falls back to globalThis.WebSocket
    this._wsCtor = wsCtor;
  }

  async _getWsImpl() {
    if (this._wsCtor) return this._wsCtor;
    // NOTE: Do NOT use globalThis.WebSocket (Node built-in) — it uses addEventListener,
    // not the EventEmitter-style .on() API that ws package provides.
    try {
      const { default: WebSocket } = await import('ws');
      return WebSocket;
    } catch {
      return null;
    }
  }
}
