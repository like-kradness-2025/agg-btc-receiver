// lib/base-connector.mjs — Base connector class for btc-receiver v3.00
import { EventEmitter } from 'node:events';

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30000;
const MAX_RECONNECT_ATTEMPTS = 30;
const STALE_MSG_THRESHOLD_MS = 30000;
const RING_BUF_MAX = 65536;

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
    this._reconnectAttempt = 0;
    this._reconnectTimer = null;
    this._errorRecoveryTimer = null;
    this._staleTimer = null;
    this._isShuttingDown = false;
    this._firstRunningDiff = false;

    this._ringBuf = [];
    this._ringBufPos = 0;
    this._lastMsgAt = 0;

    this._wsSnapshotReceived = false;
    this._wsSnapshotSeq = null;
    this._wsSnapshotWaiter = null;

    this._stats = {
      state: 'init',
      connectedAt: 0,
      lastDepthMsgAt: 0,
      lastTradeMsgAt: 0,
      depthMsgCount: 0,
      tradeMsgCount: 0,
      reconnectCount: 0,
      resyncCount: 0,
      lastSeq: 0,
    };
  }

  // ====== Public API ======

  /**
   * Connect to WebSocket.
   * @returns {Promise<void>}
   */
  connect() {
    this._setState('connecting');
    return new Promise((resolve, reject) => {
      (async () => {
        try {
          const WebSocket = await this._getWsImpl();
          if (!WebSocket) throw new Error('No WebSocket implementation available (install ws or use global WebSocket)');
          this._ws = new WebSocket(this.wsUrl);

          /** Tracks whether the connect promise has settled (resolve/reject once). */
          let settled = false;
          const safeResolve = () => { if (!settled) { settled = true; resolve(); } };
          const safeReject = (err) => { if (!settled) { settled = true; reject(err); } };

          this._ws.on('open', () => {
            this._reconnectAttempt = 0;
            this._stats.connectedAt = Date.now();
            this._lastMsgAt = Date.now();
            this._setState('connected');
            this._startStaleTimer();
            this._onOpen();
            safeResolve();
          });

          this._ws.on('message', (raw) => {
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
            console.error(`[${this.market}] WS CLOSE code=${code} reason=${reason ? reason.toString() : '(empty)'} settled=${settled} state=${this._state}`);
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

  /** Graceful disconnect. */
  disconnect() {
    this._isShuttingDown = true;
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

  /**
   * Validate a single book level entry [price, qty].
   * price は正の有限数値、qty は 0 以上の有限数値でなければならない。
   * entry は配列であり、要素は文字列または数値としてパース可能であること。
   * @param {Array} level
   * @returns {boolean} valid なら true
   */
  _validateLevel(level) {
    if (!Array.isArray(level) || level.length < 2) return false;
    const price = typeof level[0] === 'number' ? level[0] : Number(level[0]);
    const qty = typeof level[1] === 'number' ? level[1] : Number(level[1]);
    if (!Number.isFinite(price) || price <= 0) return false;
    if (!Number.isFinite(qty) || qty < 0) return false;
    return true;
  }

  /**
   * Validate an array of book levels. 全ての level が有効なら true。
   * @param {Array} levels
   * @returns {boolean}
   */
  _validateLevels(levels) {
    if (!Array.isArray(levels)) return false;
    for (let i = 0; i < levels.length; i++) {
      if (!this._validateLevel(levels[i])) return false;
    }
    return true;
  }

  /** Emit standardized DepthEvent. */
  _emitDepth(type, bids, asks, ts, seq) {
    // Schema validation per C1 §5.2: type must be one of the recognised
    // depth event types (partial/delta/snapshot/update).  bids and asks
    // must be arrays.  Invalid payloads are silently dropped (logged) to
    // prevent corrupt data from reaching persistence.
    if (type !== 'partial' && type !== 'delta' && type !== 'snapshot' && type !== 'update') {
      console.error(`[${this.market}] _emitDepth: invalid type "${type}", dropping depth event`);
      return;
    }
    if (!Array.isArray(bids) || !Array.isArray(asks)) {
      console.error(`[${this.market}] _emitDepth: non-array bids/asks, dropping depth event`);
      return;
    }
    // FIX6: ネストした bids/asks の各 [price, qty] level を検証する。
    // 1 つでも不正な level があれば event 全体を reject する（fail-closed）。
    if (!this._validateLevels(bids)) {
      console.error(`[${this.market}] _emitDepth: invalid bid level(s) in bids, dropping depth event`);
      return;
    }
    if (!this._validateLevels(asks)) {
      console.error(`[${this.market}] _emitDepth: invalid ask level(s) in asks, dropping depth event`);
      return;
    }
    this._stats.lastDepthMsgAt = Date.now();
    this._stats.depthMsgCount++;
    if (seq != null) this._stats.lastSeq = seq;
    this.emit('depth', { market: this.market, type, bids, asks, ts, seq });
  }

  /** Emit standardized TradeEvent. */
  _emitTrade(price, qty, side, ts, tradeId) {
    // Schema validation per C1 §5.3: price と qty は正の有限数値。
    if (typeof price !== 'number' || price <= 0 || !Number.isFinite(price)) {
      console.error(`[${this.market}] _emitTrade: invalid price ${price}, dropping trade`);
      return;
    }
    if (typeof qty !== 'number' || qty <= 0 || !Number.isFinite(qty)) {
      console.error(`[${this.market}] _emitTrade: invalid qty ${qty}, dropping trade`);
      return;
    }
    if (side !== 'buy' && side !== 'sell') {
      console.error(`[${this.market}] _emitTrade: invalid side "${side}", dropping trade`);
      return;
    }
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
    // Schema validation per C1 §5.5: side must be buy/sell,
    // price and qty must be positive numbers.
    if (side !== 'buy' && side !== 'sell') {
      console.error(`[${this.market}] _emitLiquidation: invalid side "${side}", dropping liquidation`);
      return;
    }
    if (typeof price !== 'number' || price <= 0 || !Number.isFinite(price)) {
      console.error(`[${this.market}] _emitLiquidation: invalid price ${price}, dropping liquidation`);
      return;
    }
    if (typeof qty !== 'number' || qty <= 0 || !Number.isFinite(qty)) {
      console.error(`[${this.market}] _emitLiquidation: invalid qty ${qty}, dropping liquidation`);
      return;
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
      source_ts: source_ts || null,
    });
  }

  // ====== State machine ======

  /** @param {ConnectorState} newState */
  _setState(newState) {
    const old = this._state;
    this._state = newState;
    this._stats.state = newState;
    this.emit('stateChange', old, newState);
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

    const delay = this._backoffDelay();
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
    try {
      await this.connect();
      await this._syncBook();
    } catch (err) {
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
    this._staleTimer = setInterval(() => this._checkStale(), 5000);
  }

  /**
   * Run the stale detection check.
   * Exposed as a named method so tests can invoke it directly instead of
   * reimplementing source logic.
   */
  _checkStale() {
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
  }

  _clearStaleTimer() {
    if (this._staleTimer) {
      clearInterval(this._staleTimer);
      this._staleTimer = null;
    }
  }

  _clearTimers() {
    this._clearStaleTimer();
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
