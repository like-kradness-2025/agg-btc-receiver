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
    if (this._state === 'syncing') { this._bufferMsg(event); return; }
    // During reconnecting, ignore depth events — fresh sync will provide correct state
    if (this._state === 'reconnecting') return;

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

    // Clear first-running flag after first accepted diff
    this._firstRunningDiff = false;

    const bids = event.b.map(([p, q]) => [p, q]);
    const asks = event.a.map(([p, q]) => [p, q]);
    this._emitDepth('update', bids, asks, event.E, event.u);
    for (const [p, q] of bids) this.book.applyDiff('bid', p, q, event.u);
    for (const [p, q] of asks) this.book.applyDiff('ask', p, q, event.u);
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
    // Valid if first valid diff found, or all buffered diffs are stale (snapshot ahead)
    return foundFirst || this._ringBuf.length > 0;
  }

  _applyDiff(msg) {
    // Apply buffered diff to the book
    const bids = msg.b.map(([p, q]) => [p, q]);
    const asks = msg.a.map(([p, q]) => [p, q]);
    for (const [p, q] of bids) this.book.applyDiff('bid', p, q, msg.u);
    for (const [p, q] of asks) this.book.applyDiff('ask', p, q, msg.u);
  }

  /** Apply snapshot then ring-buf diffs from first valid onwards */
  _applyRingBuf(snapshot) {
    const lastUpdateId = snapshot.lastUpdateId;
    // Apply snapshot
    const bids = (snapshot.bids || []).map(([p, q]) => [p, q]);
    const asks = (snapshot.asks || []).map(([p, q]) => [p, q]);
    this.book.applySnapshot(bids, asks, lastUpdateId);

    // Apply buffered diffs: discard stale, find first valid, apply subsequent
    let foundFirst = false;
    for (const msg of this._ringBuf) {
      if (msg.u <= lastUpdateId) continue; // discard stale
      if (!foundFirst) {
        if (msg.U <= lastUpdateId + 1 && lastUpdateId + 1 <= msg.u) {
          foundFirst = true;
          this._applyDiff(msg);
        }
        // else: partial overlap before first valid, skip
        continue;
      }
      this._applyDiff(msg);
    }
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
    if (this._state === 'syncing') { this._bufferMsg(event); return; }
    // During reconnecting, ignore depth events — fresh sync will provide correct state
    if (this._state === 'reconnecting') return;

    const localSeq = this.book._lastSeq;

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

    const bids = event.b.map(([p, q]) => [p, q]);
    const asks = event.a.map(([p, q]) => [p, q]);
    this._emitDepth('update', bids, asks, event.E, event.u);
    for (const [p, q] of bids) this.book.applyDiff('bid', p, q, event.u);
    for (const [p, q] of asks) this.book.applyDiff('ask', p, q, event.u);
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
    // Valid if first valid diff found, or all buffered diffs are stale (snapshot ahead)
    return foundFirst || this._ringBuf.length > 0;
  }

  _applyDiff(msg) {
    const bids = msg.b.map(([p, q]) => [p, q]);
    const asks = msg.a.map(([p, q]) => [p, q]);
    for (const [p, q] of bids) this.book.applyDiff('bid', p, q, msg.u);
    for (const [p, q] of asks) this.book.applyDiff('ask', p, q, msg.u);
  }

  /** Apply snapshot then ring-buf diffs from first valid onwards */
  _applyRingBuf(snapshot) {
    const lastUpdateId = snapshot.lastUpdateId;
    const bids = (snapshot.bids || []).map(([p, q]) => [p, q]);
    const asks = (snapshot.asks || []).map(([p, q]) => [p, q]);
    this.book.applySnapshot(bids, asks, lastUpdateId);

    this._ringBufApplied = false;
    let foundFirst = false;
    for (const msg of this._ringBuf) {
      if (msg.u <= lastUpdateId) continue; // discard stale
      if (!foundFirst) {
        if (msg.U <= lastUpdateId + 1 && lastUpdateId + 1 <= msg.u) {
          foundFirst = true;
          this._ringBufApplied = true;
          this._applyDiff(msg);
        }
        // else: partial overlap before first valid, skip
        continue;
      }
      this._applyDiff(msg);
    }
  }

  /**
   * Override _syncBook to conditionally set _firstRunningDiff.
   * If _applyRingBuf applied >=1 buffered diff, the book already bridges
   * to the live stream -> use strict pu check for the next live diff.
   * If no diff was applied (snapshot ahead of buffer) -> keep bridge check.
   */
  async _syncBook() {
    this._setState('syncing');
    this._ringBuf = [];
    this._ringBufPos = 0;

    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const snapshot = await this._fetchSnapshot();
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
      } catch (err) {
        this.emit('error', { market: this.market, message: `sync attempt ${attempt} failed: ${err.message}` });
      }
    }
    this._setState('error');
    this.emit('error', { market: this.market, message: 'init sync failed after 3 retries' });
    throw new Error(`init sync failed for ${this.market} after 3 retries`);
  }

}
