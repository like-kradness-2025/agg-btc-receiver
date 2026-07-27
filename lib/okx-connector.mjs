// lib/okx-connector.mjs — OKX perpetual swap connector for btc-receiver v3.00

import { BaseConnector } from './base-connector.mjs';
import { FullBook } from './full-book.mjs';

const WS_SNAPSHOT_TIMEOUT_MS = 15000;

const toPairs = (rows = []) => rows.map(([p, q]) => [String(p), String(q)]);

export class OkxConnector extends BaseConnector {
  constructor(config) {
    super(config, {
      market: 'okx_perp',
      wsUrl: config.wsUrl || 'wss://ws.okx.com:8443/ws/v5/public',
      restUrl: config.restUrl || 'https://www.okx.com/api/v5/market/books?instId=BTC-USDT-SWAP&sz=400',
    });
    this._contractValue = 0.01; // BTC-USDT-SWAP: 1 contract = 0.01 BTC
    /** @type {FullBook} */
    this.book = new FullBook('okx_perp', { maxLevels: config.depthLimit ?? 0 });
  }

  subscribe() {
    this._ws.send(JSON.stringify({
      op: 'subscribe',
      args: [
        { channel: 'trades', instId: 'BTC-USDT-SWAP' },
        { channel: 'books', instId: 'BTC-USDT-SWAP' },
        { channel: 'liquidation-orders', instType: 'SWAP', instId: 'BTC-USDT-SWAP' },
      ],
    }));
  }

  /** OKX sends raw 'ping' string — intercept before JSON parse */
  _preprocessRaw(raw) {
    const str = raw.toString();
    if (str === 'ping') {
      this._ws.send('pong');
      return true;
    }
    return false;
  }

  _onMessage(data) {
    // Ignore subscription confirmation events
    if (data.event === 'subscribe') return;
    if (data.event === 'error') {
      this.emit('error', { market: this.market, message: `subscription error: ${data.msg || JSON.stringify(data)}` });
      return;
    }

    const channel = data.arg?.channel;
    if (channel === 'books') {
      this._handleDepth(data);
    } else if (channel === 'trades') {
      this._handleTrade(data);
    } else if (channel === 'liquidation-orders') {
      this._handleLiquidation(data);
    }
  }

  _handleDepth(data) {
    const action = data.action; // 'snapshot' | 'update'
    const dataArr = data.data || [];
    if (dataArr.length === 0) return;

    const first = dataArr[0];
    // OKX level format: [px, sz, lv1, lv2] — only first two used
    const rawBids = first.bids || [];
    const rawAsks = first.asks || [];
    const ts = parseInt(first.ts, 10) || Date.now();
    const seqId = first.seqId || 0;

    if (action === 'snapshot') {
      const bids = toPairs(rawBids);
      const asks = toPairs(rawAsks);

      if (!this._emitDepth('snapshot', bids, asks, ts, seqId, { snapshot_origin: 'ws_sync' })) return;
      this.book.applySnapshot(bids, asks, seqId);
      this._notifyWsSnapshotReceived(seqId);
      this._replayRingBufAfterSnapshot();
      this._ringBuf = [];
      return;
    }

    if (action === 'update') {
      if (!this._wsSnapshotReceived) {
        this._bufferMsg(data);
        return;
      }
      if (this._state === 'reconnecting') return;

      const prevSeqId = first.prevSeqId ?? first.prevSeqID ?? null;
      const localSeq = this.book._lastSeq;
      if (localSeq != null && seqId != null && seqId <= localSeq) {
        return; // stale
      }
      if (localSeq != null && prevSeqId != null && prevSeqId !== localSeq) {
        this._handleSequenceGap(`OKX sequence gap: prevSeqId=${prevSeqId}, localSeq=${localSeq}, seqId=${seqId}`, data);
        return;
      }

      const bids = toPairs(rawBids);
      const asks = toPairs(rawAsks);

      if (!this._emitDepth('update', bids, asks, ts, seqId, { prev_seq: prevSeqId ?? localSeq })) return;
      for (const [p, q] of bids) this.book.applyDiff('bid', p, q, seqId);
      for (const [p, q] of asks) this.book.applyDiff('ask', p, q, seqId);
      if (seqId != null) this.book.setLastSeq(seqId);
    }
  }
  _handleTrade(data) {
    const trades = data.data || [];
    for (const t of trades) {
      const qty = parseFloat(t.sz) * this._contractValue;
      this._emitTrade(
        parseFloat(t.px),
        qty,
        t.side === 'buy' ? 'buy' : 'sell',
        parseInt(t.ts, 10) || Date.now(),
        String(t.tradeId || '')
      );
    }
  }

  /**
   * Handle liquidation-orders event from OKX public channel.
   * OKX liquidation-orders format:
   *   { arg: { channel: 'liquidation-orders', instType: 'SWAP', instId: 'BTC-USDT-SWAP' },
   *     data: [{ instType, instId, side, sz, ts, fillSz, fillPx, tdMode, uly }] }
   */
  _handleLiquidation(data) {
    const items = data.data || [];
    for (const liq of items) {
      if (!liq.side || !liq.sz) continue;

      const price = liq.fillPx ? parseFloat(liq.fillPx) : null;
      const qty = parseFloat(liq.sz) * this._contractValue;
      // If no fill price is available, we can still record the attempted liquidation
      if (!price) continue;

      this._emitLiquidation({
        exchange: 'okx',
        symbol: liq.instId || 'BTC-USDT-SWAP',
        side: liq.side === 'buy' ? 'buy' : 'sell',
        price,
        qty,
        notional: price * qty,
        raw_type: 'liquidation-orders',
        trade_id: null,
        source_ts: parseInt(liq.ts, 10) || Date.now(),
      });
    }
  }

  /** Replay ring buffer updates after snapshot */
  _replayRingBufAfterSnapshot() {
    for (const msg of this._ringBuf) {
      if (msg.action !== 'update') continue;
      const dataArr = msg.data || [];
      if (dataArr.length === 0) continue;
      const first = dataArr[0];
      const msgSeq = first.seqId || 0;
      const prevSeqId = first.prevSeqId ?? first.prevSeqID ?? null;
      const localSeq = this.book._lastSeq;
      if (localSeq != null && msgSeq != null && msgSeq <= localSeq) {
        continue; // stale
      }
      if (localSeq != null && prevSeqId != null && prevSeqId !== localSeq) {
        this._handleSequenceGap(`OKX replay sequence gap: prevSeqId=${prevSeqId}, localSeq=${localSeq}, seqId=${msgSeq}`, msg);
        return;
      }
      const rawBids = first.bids || [];
      const rawAsks = first.asks || [];
      this._emitDepth('update', toPairs(rawBids), toPairs(rawAsks), parseInt(first.ts, 10) || Date.now(), msgSeq, { prev_seq: prevSeqId ?? localSeq });
      for (const [p, q] of rawBids) this.book.applyDiff('bid', String(p), String(q), msgSeq);
      for (const [p, q] of rawAsks) this.book.applyDiff('ask', String(p), String(q), msgSeq);
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
          if (snapshot.data && snapshot.data.length > 0) {
            const d = snapshot.data[0];
            const rawBids = d.bids ?? d[0];
            const rawAsks = d.asks ?? d[1];
            if (!this._validateSnapshotLevels({ bids: rawBids, asks: rawAsks })) {
              throw new Error('invalid REST depth snapshot');
            }
            const sbids = toPairs(rawBids);
            const sasks = toPairs(rawAsks);
            const seqId = d.seqId ?? snapshot.seqId ?? 0;
            if (!this._emitDepth('snapshot', sbids, sasks, Date.now(), seqId, { snapshot_origin: 'rest_fallback' })) continue;
            this.book.applySnapshot(sbids, sasks, seqId);
            this._notifyWsSnapshotReceived(seqId);
            restored = true;
          }
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
