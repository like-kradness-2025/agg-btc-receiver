// lib/coinbase-connector.mjs — Coinbase spot connector for btc-receiver v3.00
// Uses Coinbase Advanced Trade WebSocket (public, no auth required for level2 / market_trades)

import { BaseConnector } from './base-connector.mjs';
import { FullBook } from './full-book.mjs';

const WS_SNAPSHOT_TIMEOUT_MS = 15000;

const toPairs = (rows = []) => rows.map(([p, q]) => [String(p), String(q)]);

export class CoinbaseConnector extends BaseConnector {
  constructor(config) {
    super(config, {
      market: 'coinbase_spot',
      wsUrl: config.wsUrl || 'wss://advanced-trade-ws.coinbase.com',
      restUrl: config.restUrl || 'https://api.exchange.coinbase.com/products/BTC-USD/book?level=3',
    });
    /** @type {FullBook} */
    this.book = new FullBook('coinbase_spot', { maxLevels: config.depthLimit ?? 0 });
  }

  subscribe() {
    // Advanced Trade WS: send two individual subscribe frames
    // (channel as string, not array — array form caused auth failure)
    this._ws.send(JSON.stringify({
      type: 'subscribe',
      product_ids: ['BTC-USD'],
      channel: 'level2',
    }));
    this._ws.send(JSON.stringify({
      type: 'subscribe',
      product_ids: ['BTC-USD'],
      channel: 'market_trades',
    }));
  }

  _onMessage(data) {
    // Handle error messages (old Exchange endpoint sent level2-auth errors)
    if (data.type === 'error') {
      this.emit('error', { market: this.market, message: `WS error: ${data.message || data.reason || JSON.stringify(data)}` });
      return;
    }
    // Subscription ack
    if (data.channel === 'subscriptions') return;

    if (data.channel === 'l2_data') {
      this._handleDepth(data);
    } else if (data.channel === 'market_trades') {
      this._handleTrade(data);
    }
  }

  _handleDepth(data) {
    const seq = data.sequence_num != null ? data.sequence_num : null;
    const ts = Date.now(); // Advanced Trade l2_data has no top-level timestamp

    const events = data.events || [];
    for (const event of events) {
      if (event.type === 'snapshot') {
        const updates = event.updates || [];
        const bids = [];
        const asks = [];
        for (const u of updates) {
          if (u.side === 'bid') {
            bids.push([String(u.price_level), String(u.new_quantity)]);
          } else if (u.side === 'ask' || u.side === 'offer') {
            asks.push([String(u.price_level), String(u.new_quantity)]);
          }
        }

        if (!this._emitDepth('snapshot', bids, asks, ts, seq, { snapshot_origin: 'ws_sync' })) return;
        this.book.applySnapshot(bids, asks, seq);
        this._notifyWsSnapshotReceived(seq);
        this._firstRunningDepth = true;
        this._replayRingBufAfterSnapshot();
        this._ringBuf = [];
        return;
      }

      if (event.type === 'update') {
        if (!this._wsSnapshotReceived) {
          this._bufferMsg(data);
          return;
        }
        if (this._state === 'reconnecting') return;

        // Coinbase l2_data for a single product is sequential, but the first
        // update after snapshot may skip seq numbers. Use relaxed stale check
        // (seq > localSeq) instead of strict +1 gap detection to avoid false
        // positive reconnects on the bridge event.
        const localSeq = this.book._lastSeq;
        if (seq != null && localSeq != null && seq <= localSeq) return;

        const updates = event.updates || [];
        const bids = [];
        const asks = [];
        for (const u of updates) {
          if (u.side === 'bid') {
            bids.push([String(u.price_level), String(u.new_quantity)]);
          } else if (u.side === 'ask' || u.side === 'offer') {
            asks.push([String(u.price_level), String(u.new_quantity)]);
          }
        }

        if (!this._emitDepth('update', bids, asks, ts, seq, { prev_seq: localSeq })) return;
        for (const [p, q] of bids) this.book.applyDiff('bid', p, q, seq);
        for (const [p, q] of asks) this.book.applyDiff('ask', p, q, seq);
        if (seq != null) this.book.setLastSeq(seq);
      }
    }
  }

  _handleTrade(data) {
    // Advanced Trade market_trades format
    const events = data.events || [];
    for (const event of events) {
      const trades = event.trades || [];
      for (const t of trades) {
        this._emitTrade(
          parseFloat(t.price),
          parseFloat(t.size),
          t.side === 'SELL' ? 'buy' : 'sell',
          Date.parse(t.time) || Date.now(),
          String(t.trade_id || '')
        );
      }
    }
  }

  /** Replay ring buffer l2_data updates after snapshot */
  _replayRingBufAfterSnapshot() {
    for (const msg of this._ringBuf) {
      const msgSeq = msg.sequence_num != null ? msg.sequence_num : null;
      const localSeq = this.book._lastSeq;
      if (msgSeq != null && localSeq != null && msgSeq <= localSeq) continue;
      const events = msg.events || [];
      for (const event of events) {
        if (event.type !== 'update') continue;
        const updates = event.updates || [];
        const bids = [];
        const asks = [];
        for (const u of updates) {
          if (u.side === 'bid') bids.push([String(u.price_level), String(u.new_quantity)]);
          else if (u.side === 'ask' || u.side === 'offer') asks.push([String(u.price_level), String(u.new_quantity)]);
        }
        this._emitDepth('update', bids, asks, Date.now(), msgSeq, { prev_seq: localSeq });
        for (const u of updates) {
          const qty = u.new_quantity;
          const price = u.price_level;
          if (u.side === 'bid') {
            this.book.applyDiff('bid', String(price), String(qty));
          } else if (u.side === 'ask' || u.side === 'offer') {
            this.book.applyDiff('ask', String(price), String(qty));
          }
        }
      }
      if (msgSeq != null) this.book.setLastSeq(msgSeq);
    }
  }

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
          // Coinbase REST returns {bids: [[price, qty, orderCount], ...], asks: [[price, qty, orderCount], ...], sequence}
          if (!this._validateSnapshotLevels(snapshot)) throw new Error('invalid REST depth snapshot');
          const bids = toPairs(snapshot.bids || []);
          const asks = toPairs(snapshot.asks || []);
          // Coinbase Exchange REST and Advanced Trade WS use different
          // sequence domains.  The REST value must not anchor the WS stream;
          // keep this fallback snapshot explicitly unsequenced so the next
          // Advanced Trade snapshot can establish its own cursor.
          const seq = null;
          this.book.applySnapshot(bids, asks, seq);
          this._emitDepth('snapshot', bids, asks, Date.now(), seq, { snapshot_origin: 'rest_fallback' });
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
