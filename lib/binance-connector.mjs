// lib/binance-connector.mjs — Binance Spot + Perp connector for btc-receiver v3.00

import { BaseConnector } from './base-connector.mjs';
import { FullBook } from './full-book.mjs';

/**
 * Binance Spot connector (combined stream: no subscribe frame needed).
 */
export class BinanceSpotConnector extends BaseConnector {
  constructor(config) {
    super(config, {
      market: 'binance_spot',
      wsUrl: config.wsUrl || 'wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@depth@100ms',
      restUrl: config.restUrl || 'https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000',
    });
    /** @type {FullBook} */
    this.book = new FullBook('binance_spot', { maxLevels: config.depthLimit ?? 0 });
  }

  subscribe() {
    // Combined stream: already subscribed via URL param. No subscribe frame needed.
  }

  _onMessage(data) {
    // Spot combined stream: { stream: '...', data: { ... } }
    const event = data.data || data;
    if (event.e === 'depthUpdate') this._handleDepth(event);
    else if (event.e === 'trade') this._handleTrade(event);
  }

  _handleDepth(event) {
    if (!this._isValidTimestamp(event.E)) {
      this._stats.droppedDepthCount++;
      return;
    }
    const bids = event.b.map(([p, q]) => [p, q]);
    const asks = event.a.map(([p, q]) => [p, q]);
    // Persist the raw frame before deciding whether it can mutate the book.
    // The marker is kept on the buffered object so sync replay does not write
    // the same physical raw line twice.
    this._emitRawDepth(bids, asks, event.E, event.u, {
        seq_start: event.U,
        seq_end: event.u,
        prev_seq: event.pu,
        book_apply: 'candidate',
    });
    if (this._state === 'syncing') { this._bufferMsg(event); return; }
    // Never mutate an unseeded book while connected/error. The raw candidate
    // is already durable; a snapshot sync must establish the sequence anchor.
    if (this._state !== 'running') return;

    const localSeq = this.book._lastSeq;

    // Running state sequence validation
    if (localSeq !== null) {
      if (event.u <= localSeq) {
        // stale/duplicate — ignore silently
        return;
      }
      // Spot: event.U must overlap with (localSeq+1)
      if (!(event.U <= localSeq + 1 && localSeq + 1 <= event.u)) {
        // Gap or out-of-order
        this._handleSequenceGap(
          `Spot depth gap: U=${event.U}, u=${event.u}, localSeq=${localSeq}`,
          event
        );
        return;
      }
      // Valid diff — apply below
    }

    if (!this._emitDepth('update', bids, asks, event.E, event.u, {
      seq_start: event.U,
      seq_end: event.u,
      prev_seq: localSeq,
    })) return;
    // Clear first-running flag after the diff was actually emitted.
    this._firstRunningDiff = false;
    for (const [p, q] of bids) this.book.applyDiff('bid', p, q, event.u);
    for (const [p, q] of asks) this.book.applyDiff('ask', p, q, event.u);
    this._noteDepthEventApplied(event.E);
    // Ensure book._lastSeq advances even for empty diff
    this.book.setLastSeq(event.u);
  }

  _handleTrade(event) {
    const price = parseFloat(event.p);
    const qty = parseFloat(event.q);
    if (!price || !qty) return; // skip zero-price/qty
    this._emitTrade(
      price,
      qty,
      event.m ? 'sell' : 'buy',
      event.T,
      String(event.t)
    );
  }

  async _fetchSnapshot() {
    const res = await fetch(this.restUrl, {
      headers: { 'User-Agent': 'btc-receiver/v3.00' },
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) throw new Error(`REST snapshot ${res.status}`);
    return res.json();
  }

  _validateSync(snapshot) {
    const lastUpdateId = snapshot.lastUpdateId;
    if (!Number.isSafeInteger(lastUpdateId)) return false;
    if (this._ringBuf.length === 0) return false;

    let foundFirst = false;
    for (const msg of this._ringBuf) {
      if (msg.u <= lastUpdateId) continue; // discard stale diffs
      if (!foundFirst) {
        if (msg.U > lastUpdateId + 1) return false; // gap
        if (msg.U <= lastUpdateId + 1 && lastUpdateId + 1 <= msg.u) {
          foundFirst = true;
        }
        // else: partial overlap (U <= lastUpdateId, u > lastUpdateId), continue
      }
    }
    // A snapshot that is ahead of every buffered diff cannot be bridged.  The
    // old fallback accepted that stale ring buffer and started a book with a
    // sequence hole; force another REST/WS sync instead.
    return foundFirst;
  }

  _applyDiff(msg) {
    // Apply buffered diff to the book
    const bids = msg.b.map(([p, q]) => [p, q]);
    const asks = msg.a.map(([p, q]) => [p, q]);
    for (const [p, q] of bids) this.book.applyDiff('bid', p, q, msg.u);
    for (const [p, q] of asks) this.book.applyDiff('ask', p, q, msg.u);
    this._noteDepthEventApplied(msg.E);
  }

  /** Apply snapshot then ring-buf diffs from first valid onwards */
  _applyRingBuf(snapshot) {
    const lastUpdateId = snapshot.lastUpdateId;
    if (!Number.isSafeInteger(lastUpdateId) || !this._validateSnapshotLevels(snapshot)) {
      throw new Error(`Binance spot REST snapshot has invalid depth levels`);
    }
    // Apply snapshot
    const bids = (snapshot.bids || []).map(([p, q]) => [p, q]);
    const asks = (snapshot.asks || []).map(([p, q]) => [p, q]);
    this.book.applySnapshot(bids, asks, lastUpdateId);
    this._emitDepth('snapshot', bids, asks, Date.now(), lastUpdateId, {
      snapshot_origin: 'rest_sync',
      event_time_known: false,
      event_time_source: 'rest_snapshot',
      as_of_ts: null,
    });

    // Apply buffered diffs: discard stale, find first valid, apply subsequent
    let foundFirst = false;
    let lastAppliedSeq = lastUpdateId;
    for (const msg of this._ringBuf) {
      if (!Number.isSafeInteger(msg.U) || !Number.isSafeInteger(msg.u) || msg.U > msg.u) {
        throw new Error(`Binance spot buffered depth has invalid sequence: U=${msg.U}, u=${msg.u}`);
      }
      if (!this._isValidTimestamp(msg.E)) {
        throw new Error(`Binance spot buffered depth has invalid timestamp: E=${msg.E}`);
      }
      if (msg.u <= lastUpdateId) continue; // discard stale
      if (!foundFirst) {
        if (msg.U <= lastUpdateId + 1 && lastUpdateId + 1 <= msg.u) {
          foundFirst = true;
          if (!this._emitDepth('update', msg.b.map(([p, q]) => [p, q]), msg.a.map(([p, q]) => [p, q]), msg.E, msg.u, {
            seq_start: msg.U, seq_end: msg.u, prev_seq: lastAppliedSeq,
          })) throw new Error('Binance spot buffered depth emission rejected');
          this._applyDiff(msg);
          this.book.setLastSeq(msg.u);
          lastAppliedSeq = msg.u;
        }
        // else: partial overlap before first valid, skip
        continue;
      }

      // Binance depth updates may overlap, but they may not leave an
      // unobserved update-id interval.  The old code applied every message
      // after the bridge and silently accepted gaps here.
      if (msg.u <= lastAppliedSeq) continue;
      if (!(msg.U <= lastAppliedSeq + 1 && lastAppliedSeq + 1 <= msg.u)) {
        throw new Error(
          `Binance spot buffered depth gap: U=${msg.U}, u=${msg.u}, previous=${lastAppliedSeq}`
        );
      }
      if (!this._emitDepth('update', msg.b.map(([p, q]) => [p, q]), msg.a.map(([p, q]) => [p, q]), msg.E, msg.u, {
        seq_start: msg.U, seq_end: msg.u, prev_seq: lastAppliedSeq,
      })) throw new Error('Binance spot buffered depth emission rejected');
      this._applyDiff(msg);
      this.book.setLastSeq(msg.u);
      lastAppliedSeq = msg.u;
    }
    this._emitCurrentBookSnapshot('sync_replay');
  }

  /** Emit a depth snapshot from current book state. */
  _emitCurrentBookSnapshot(snapshotOrigin = 'sync_replay') {
    const snapshot = this.book.toSnapshot(Date.now());
    this._emitDepth('snapshot', snapshot.bids, snapshot.asks, snapshot.ts, snapshot.seq ?? null, {
      snapshot_origin: snapshotOrigin,
    });
  }

}

/**
 * Binance Perp connector (separate WS, sends subscribe frame).
 */
export class BinancePerpConnector extends BaseConnector {
  constructor(config) {
    super(config, {
      market: 'binance_perp',
      wsUrl: config.wsUrl || 'wss://fstream.binance.com/stream?streams=btcusdt@trade/btcusdt@depth@100ms/btcusdt@forceOrder',
      restUrl: config.restUrl || 'https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000',
    });
    /** @type {FullBook} */
    this.book = new FullBook('binance_perp', { maxLevels: config.depthLimit ?? 0 });
  }

  subscribe() {
    // Combined stream: already subscribed via URL param. No subscribe frame needed.
  }

  _onMessage(data) {
    // Perp combined stream: { stream: '...', data: { ... } }
    const event = data.data || data;
    if (event.e === 'depthUpdate') this._handleDepth(event);
    else if (event.e === 'trade' || event.e === 'aggTrade') this._handleTrade(event);
    else if (event.e === 'forceOrder') this._handleForceOrder(event);
  }

  _handleDepth(event) {
    if (!this._isValidTimestamp(event.E)) {
      this._stats.droppedDepthCount++;
      return;
    }
    const bids = event.b.map(([p, q]) => [p, q]);
    const asks = event.a.map(([p, q]) => [p, q]);
    this._emitRawDepth(bids, asks, event.E, event.u, {
        seq_start: event.U,
        seq_end: event.u,
        prev_seq: event.pu,
        book_apply: 'candidate',
    });
    if (this._state === 'syncing') { this._bufferMsg(event); return; }
    // Never mutate an unseeded book while connected/error. The raw candidate
    // is already durable; a snapshot sync must establish the sequence anchor.
    if (this._state !== 'running') return;

    const localSeq = this.book._lastSeq;
    const previousSeq = localSeq;

    // Running state sequence validation
    if (localSeq !== null) {
      if (event.u <= localSeq) {
        // stale/duplicate — ignore silently
        return;
      }

      if (this._firstRunningDiff) {
        // First diff after sync — allow bridge from snapshot if event covers localSeq
        // per USD-M futures docs: U <= lastUpdateId AND lastUpdateId <= u
        if (event.U <= localSeq && localSeq <= event.u) {
          // Bridge accepted — clear flag, proceed to apply
          this._firstRunningDiff = false;
        } else {
          // Cannot bridge — gap/out-of-order
          this._handleSequenceGap(
            `Perp depth bridge fail: U=${event.U}, u=${event.u}, localSeq=${localSeq}`,
            event
          );
          return;
        }
      } else {
        // Normal strict check: pu must match localSeq
        if (event.pu !== localSeq) {
          // pu mismatch — gap/out-of-order
          this._handleSequenceGap(
            `Perp depth pu mismatch: pu=${event.pu}, localSeq=${localSeq}`,
            event
          );
          return;
        }
        // Valid pu matches localSeq — apply below
      }
    }

    if (!this._emitDepth('update', bids, asks, event.E, event.u, {
      seq_start: event.U,
      seq_end: event.u,
      prev_seq: previousSeq,
    })) return;
    for (const [p, q] of bids) this.book.applyDiff('bid', p, q, event.u);
    for (const [p, q] of asks) this.book.applyDiff('ask', p, q, event.u);
    this._noteDepthEventApplied(event.E);
    // Ensure book._lastSeq advances even for empty diff
    this.book.setLastSeq(event.u);
  }

  _handleTrade(event) {
    const price = parseFloat(event.p);
    const qty = parseFloat(event.q);
    if (!price || !qty) return; // skip zero-price/qty (forceOrder edge cases)
    this._emitTrade(
      price,
      qty,
      event.m ? 'sell' : 'buy',
      event.T,
      String(event.t)
    );
  }

  /**
   * Handle forceOrder (liquidation) event from combined stream.
   * Binance perp forceOrder event structure:
   *   { e: 'forceOrder', E: <event_time>, o: { s, S, T, p, q, X, z, l, ap, f } }
   *   f = 'LIQUIDATION' / 'ROE_LIQUIDATION' for liquidation orders
   */
  _handleForceOrder(event) {
    const o = event.o || {};
    if (!o.s || !o.S || !o.p || !o.q) return;
    if (o.f !== 'LIQUIDATION' && o.f !== 'ROE_LIQUIDATION') return;

    const price = parseFloat(o.p);
    const qty = parseFloat(o.z || o.q);
    const notional = price * qty;

    this._emitLiquidation({
      exchange: 'binance',
      symbol: o.s,
      side: o.S === 'SELL' ? 'sell' : 'buy',
      price,
      qty,
      notional,
      raw_type: 'forceOrder',
      trade_id: null,
      source_ts: o.T || event.E || Date.now(),
    });
  }

  async _fetchSnapshot() {
    const res = await fetch(this.restUrl, {
      headers: { 'User-Agent': 'btc-receiver/v3.00' },
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) throw new Error(`REST snapshot ${res.status}`);
    return res.json();
  }

  _validateSync(snapshot) {
    const lastUpdateId = snapshot.lastUpdateId;
    if (!Number.isSafeInteger(lastUpdateId)) return false;
    if (this._ringBuf.length === 0) return false;

    let foundFirst = false;
    for (const msg of this._ringBuf) {
      if (msg.u <= lastUpdateId) continue; // discard stale diffs
      if (!foundFirst) {
        // USD-M futures bridge: the first event must cover lastUpdateId.
        // Spot uses lastUpdateId + 1 and is intentionally unchanged above.
        if (msg.U > lastUpdateId) return false; // gap
        if (msg.U <= lastUpdateId && lastUpdateId <= msg.u) {
          foundFirst = true;
        }
        // else: partial overlap (U <= lastUpdateId, u > lastUpdateId), continue
      }
    }
    // A snapshot that is ahead of every buffered diff cannot be bridged.  The
    // old fallback accepted that stale ring buffer and started a book with a
    // sequence hole; force another REST/WS sync instead.
    return foundFirst;
  }

  _applyDiff(msg) {
    const bids = msg.b.map(([p, q]) => [p, q]);
    const asks = msg.a.map(([p, q]) => [p, q]);
    for (const [p, q] of bids) this.book.applyDiff('bid', p, q, msg.u);
    for (const [p, q] of asks) this.book.applyDiff('ask', p, q, msg.u);
    this._noteDepthEventApplied(msg.E);
  }

  /** Apply snapshot then ring-buf diffs from first valid onwards */
  _applyRingBuf(snapshot) {
    const lastUpdateId = snapshot.lastUpdateId;
    if (!Number.isSafeInteger(lastUpdateId) || !this._validateSnapshotLevels(snapshot)) {
      throw new Error('Binance perp REST snapshot has invalid depth levels');
    }
    const bids = (snapshot.bids || []).map(([p, q]) => [p, q]);
    const asks = (snapshot.asks || []).map(([p, q]) => [p, q]);
    this.book.applySnapshot(bids, asks, lastUpdateId);
    this._emitDepth('snapshot', bids, asks, Date.now(), lastUpdateId, {
      snapshot_origin: 'rest_sync',
      event_time_known: false,
      event_time_source: 'rest_snapshot',
      as_of_ts: null,
    });

    this._ringBufApplied = false;
    let foundFirst = false;
    let lastAppliedSeq = lastUpdateId;
    for (const msg of this._ringBuf) {
      if (
        !Number.isSafeInteger(msg.U)
        || !Number.isSafeInteger(msg.u)
        || !Number.isSafeInteger(msg.pu)
        || msg.U > msg.u
      ) {
        throw new Error(
          `Binance perp buffered depth has invalid sequence: U=${msg.U}, u=${msg.u}, pu=${msg.pu}`
        );
      }
      if (!this._isValidTimestamp(msg.E)) {
        throw new Error(`Binance perp buffered depth has invalid timestamp: E=${msg.E}`);
      }
      if (msg.u <= lastUpdateId) continue; // discard stale
      if (!foundFirst) {
        if (msg.U <= lastUpdateId && lastUpdateId <= msg.u) {
          foundFirst = true;
          this._ringBufApplied = true;
          if (!this._emitDepth('update', msg.b.map(([p, q]) => [p, q]), msg.a.map(([p, q]) => [p, q]), msg.E, msg.u, {
            seq_start: msg.U, seq_end: msg.u, prev_seq: lastAppliedSeq,
          })) throw new Error('Binance perp buffered depth emission rejected');
          this._applyDiff(msg);
          this.book.setLastSeq(msg.u);
          lastAppliedSeq = msg.u;
        }
        // else: partial overlap before first valid, skip
        continue;
      }

      if (msg.u <= lastAppliedSeq) continue;
      if (msg.pu !== lastAppliedSeq) {
        throw new Error(
          `Binance perp buffered depth gap: pu=${msg.pu}, previous=${lastAppliedSeq}, U=${msg.U}, u=${msg.u}`
        );
      }
      if (!this._emitDepth('update', msg.b.map(([p, q]) => [p, q]), msg.a.map(([p, q]) => [p, q]), msg.E, msg.u, {
        seq_start: msg.U, seq_end: msg.u, prev_seq: lastAppliedSeq,
      })) throw new Error('Binance perp buffered depth emission rejected');
      this._applyDiff(msg);
      this.book.setLastSeq(msg.u);
      lastAppliedSeq = msg.u;
    }
    this._emitCurrentBookSnapshot('sync_replay');
  }

  /** Emit a depth snapshot from current book state. */
  _emitCurrentBookSnapshot(snapshotOrigin = 'sync_replay') {
    const snapshot = this.book.toSnapshot(Date.now());
    this._emitDepth('snapshot', snapshot.bids, snapshot.asks, snapshot.ts, snapshot.seq ?? null, {
      snapshot_origin: snapshotOrigin,
    });
  }

  /**
   * Override _syncBook to conditionally set _firstRunningDiff.
   * If _applyRingBuf applied >=1 buffered diff, the book already bridges
   * to the live stream -> use strict pu check for the next live diff.
   * If no diff was applied (snapshot ahead of buffer) -> keep bridge check.
   */
  async _syncBook() {
    this._setState('syncing');
    this.book.clear?.();
    this._ringBuf = [];
    this._ringBufPos = 0;

    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        this._stats.syncAttempts++;
        const snapshot = await this._fetchSnapshot();
        if (!Number.isSafeInteger(snapshot?.lastUpdateId) || !this._validateSnapshotLevels(snapshot)) {
          throw new Error('invalid REST depth snapshot');
        }
        if (this._ringBuf.length === 0) {
          // Give the websocket a short chance to deliver the first diff before
          // validation. Without this, fast REST snapshots on active USD-M pairs
          // can complete before any buffered diff exists, causing sync error or
          // first-diff bridge loops.
          await new Promise(resolve => setTimeout(resolve, 1500));
        }
        const valid = this._validateSync(snapshot);
        if (valid) {
          this._applyRingBuf(snapshot);
          this._stats.resyncCount++;
          this._stats.lastSeq = this.book._lastSeq || snapshot.lastUpdateId || 0;
          this._firstRunningDiff = !this._ringBufApplied;
          this._setState('running');
          this._ringBuf = [];
          return;
        }
        this._stats.syncValidationFailures++;
        this._stats.lastSyncFailure = 'snapshot has no contiguous buffered diff';
        this.emit('error', { market: this.market, message: `sync validation failed attempt ${attempt}: snapshot has no contiguous buffered diff` });
      } catch (err) {
        this._ringBuf = [];
        this._ringBufPos = 0;
        this._ringBufApplied = false;
        this.emit('error', { market: this.market, message: `sync attempt ${attempt} failed: ${err.message}` });
      }
      if (attempt < 2) await new Promise(resolve => setTimeout(resolve, 1000));
    }
    this._setState('error');
    this.emit('error', { market: this.market, message: 'init sync failed after 3 retries' });
    throw new Error(`init sync failed for ${this.market} after 3 retries`);
  }

}
