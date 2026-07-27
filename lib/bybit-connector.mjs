// lib/bybit-connector.mjs — Bybit V5 linear perpetual connector for btc-receiver v3.00

import { BaseConnector } from './base-connector.mjs';
import { FullBook } from './full-book.mjs';

const WS_SNAPSHOT_TIMEOUT_MS = 15000;

const toPairs = (rows = []) => rows.map(([p, q]) => [String(p), String(q)]);

export class BybitConnector extends BaseConnector {
  constructor(config) {
    super(config, {
      market: 'bybit_perp',
      wsUrl: config.wsUrl || 'wss://stream.bybit.com/v5/public/linear',
      restUrl: config.restUrl || 'https://api.bybit.com/v5/market/orderbook?category=linear&symbol=BTCUSDT&limit=1000',
    });
    /** @type {FullBook} */
    this.book = new FullBook('bybit_perp', { maxLevels: config.depthLimit ?? 0 });
  }

  subscribe() {
    this._ws.send(JSON.stringify({
      op: 'subscribe',
      args: ['publicTrade.BTCUSDT', 'orderbook.1000.BTCUSDT', 'allLiquidation.BTCUSDT'],
    }));
  }

  _onMessage(data) {
    // Handle op responses
    if (data.op === 'pong') return;
    if (data.op === 'subscribe') return; // subscription ack

    // Route by topic
    if (data.topic === 'publicTrade.BTCUSDT') {
      this._handleTrade(data);
    } else if (data.topic?.startsWith('orderbook.')) {
      this._handleDepth(data);
    } else if (data.topic === 'allLiquidation.BTCUSDT') {
      this._handleLiquidation(data);
    }
  }

  _handleDepth(data) {
    if (data.type === 'snapshot') {
      const seq = data.data?.u ?? data.data?.seq ?? data.u ?? data.seq;
      const ts = data.data?.ts || data.ts || Date.now();
      const bids = toPairs(data.data?.b || []);
      const asks = toPairs(data.data?.a || []);

      if (!this._emitDepth('snapshot', bids, asks, ts, seq, { snapshot_origin: 'ws_sync' })) return;
      this.book.applySnapshot(bids, asks, seq);
      this._notifyWsSnapshotReceived(seq);
      this._replayRingBufAfterSnapshot();
      this._ringBuf = [];
      return;
    }

    if (data.type === 'delta') {
      const ts = data.data?.ts || data.ts || Date.now();
      if (!this._isValidTimestamp(ts)) {
        this._stats.droppedDepthCount++;
        return;
      }
      if (!this._wsSnapshotReceived) {
        this._bufferMsg(data);
        return;
      }
      if (this._state === 'reconnecting') return;

      const hasBookUpdateId = data.data?.u != null || data.u != null;
      const seq = data.data?.u ?? data.u ?? data.data?.seq ?? data.seq;

      const localSeq = this.book._lastSeq;
      if (seq != null && localSeq != null) {
        if (seq <= localSeq) return;
        if (hasBookUpdateId && seq !== localSeq + 1) {
          this._handleSequenceGap(`Bybit sequence gap: seq=${seq}, localSeq=${localSeq}`, data);
          return;
        }
      }

      // Bybit delta entries: [price, qty, updateType] (V5 format)
      const bids = toPairs(data.data?.b || []);
      const asks = toPairs(data.data?.a || []);

      if (!this._emitDepth('update', bids, asks, ts, seq, { prev_seq: localSeq })) return;
      for (const [p, q] of bids) this.book.applyDiff('bid', p, q, seq);
      for (const [p, q] of asks) this.book.applyDiff('ask', p, q, seq);
      if (seq != null) this.book.setLastSeq(seq);
    }
  }

  _handleTrade(data) {
    const trades = data.data || [];
    for (const t of trades) {
      this._emitTrade(
        parseFloat(t.p),
        parseFloat(t.v),
        t.S === 'Buy' ? 'buy' : 'sell',
        t.T || Date.now(),
        String(t.i || '')
      );
    }
  }

  /**
   * Handle liquidation event from Bybit V5 public channel.
   * Supports both allLiquidation (data is array) and liquidation (data is object) formats.
   * Flexible field names: price/p, size/sz/v, side/S, symbol/s, updatedTime/T.
   */
  _handleLiquidation(data) {
    const raw = data.data;
    if (!raw) return;

    const items = Array.isArray(raw) ? raw : [raw];

    for (const liq of items) {
      if (!liq) continue;

      // Flexible field mapping
      const price = liq.price ?? liq.p;
      const qty = liq.size ?? liq.sz ?? liq.v;
      const side = liq.side ?? liq.S;
      const symbol = liq.symbol ?? liq.s;
      const updatedTime = liq.updatedTime ?? liq.T;

      if (price == null || qty == null || side == null) continue;

      const parsedPrice = parseFloat(price);
      const parsedQty = parseFloat(qty);

      this._emitLiquidation({
        exchange: 'bybit',
        symbol: symbol || 'BTCUSDT',
        side: String(side).toLowerCase() === 'sell' ? 'sell' : 'buy',
        price: parsedPrice,
        qty: parsedQty,
        notional: parsedPrice * parsedQty,
        raw_type: 'liquidation',
        trade_id: null,
        source_ts: updatedTime || Date.now(),
      });
    }
  }

  /** Replay ring buffer deltas whose seq is after snapshot seq */
  _replayRingBufAfterSnapshot() {
    for (const msg of this._ringBuf) {
      const hasBookUpdateId = msg.data?.u != null || msg.u != null;
      const msgSeq = msg.data?.u ?? msg.u ?? msg.data?.seq ?? msg.seq;
      const localSeq = this.book._lastSeq;
      if (msgSeq != null && localSeq != null && msgSeq <= localSeq) {
        continue; // stale
      }
      if (msg.type !== 'delta') continue;
      if (hasBookUpdateId && msgSeq != null && localSeq != null && msgSeq !== localSeq + 1) {
        this._handleSequenceGap(`Bybit replay sequence gap: seq=${msgSeq}, localSeq=${localSeq}`, msg);
        return;
      }
      const rawBids = msg.data?.b || [];
      const rawAsks = msg.data?.a || [];
      const ts = msg.data?.ts || msg.ts || Date.now();
      if (!this._emitDepth('update', toPairs(rawBids), toPairs(rawAsks), ts, msgSeq, { prev_seq: localSeq })) return;
      for (const [p, q] of rawBids) this.book.applyDiff('bid', String(p), String(q), msgSeq);
      for (const [p, q] of rawAsks) this.book.applyDiff('ask', String(p), String(q), msgSeq);
      if (msgSeq != null) this.book.setLastSeq(msgSeq);
    }
  }

  /** Override: wait for WS snapshot, fallback to REST on timeout */
  async _syncBook() {
    this._setState('syncing');

    if (this._wsSnapshotReceived) {
      this._finalizeWsSnapshotSync();
      return;
    }

    this._beginWsSnapshotSync();

    try {
      await this._waitForWsSnapshot(WS_SNAPSHOT_TIMEOUT_MS, 'ws snapshot timeout');
    } catch (err) {
      if (err?.code === 'WS_SNAPSHOT_ABORTED') return;

      let restored = false;
      for (let attempt = 0; attempt < 3; attempt++) {
        try {
          const snapshot = await this._fetchSnapshot();
          const rawBids = snapshot.result?.b;
          const rawAsks = snapshot.result?.a;
          if (!this._validateSnapshotLevels({ bids: rawBids, asks: rawAsks })) {
            throw new Error('invalid REST depth snapshot');
          }
          const bids = toPairs(rawBids);
          const asks = toPairs(rawAsks);
          const seq = snapshot.result?.u ?? snapshot.result?.seq ?? 0;
          if (!this._emitDepth('snapshot', bids, asks, Date.now(), seq, { snapshot_origin: 'rest_fallback' })) continue;
          this.book.applySnapshot(bids, asks, seq);
          this._notifyWsSnapshotReceived(seq);
          restored = true;
          break;
        } catch (restErr) {
          this.emit('error', { market: this.market, message: `sync REST fallback attempt ${attempt} failed: ${restErr.message}` });
        }
      }

      if (!restored) {
        this._failWsSnapshotSync('init sync failed after 3 retries');
        throw new Error(`init sync failed for ${this.market} after 3 retries`);
      }
    }

    if (this._state !== 'error') {
      this._finalizeWsSnapshotSync();
    }
  }

  async _fetchSnapshot() {
    const res = await fetch(this.restUrl, {
      headers: { 'User-Agent': 'btc-receiver/v3.00' },
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) throw new Error(`REST snapshot ${res.status}`);
    return res.json();
  }

}
