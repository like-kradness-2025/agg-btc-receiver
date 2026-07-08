// lib/hyperliquid-connector.mjs — Hyperliquid perpetual connector for btc-receiver v3.00
// NOTE: Hyperliquid does not provide a public liquidation/forceOrder stream.
//       Liquidation tracking for Hyperliquid is not implemented.
//       No stub is left — the `liquidation` event is never emitted.

import { BaseConnector } from './base-connector.mjs';
import { FullBook } from './full-book.mjs';

const WS_SNAPSHOT_TIMEOUT_MS = 15000;

/** Convert Hyperliquid nanosecond timestamp to milliseconds. */
const normalizeTs = (value) => {
  if (value == null) return Date.now();
  const n = Number(value);
  if (!Number.isFinite(n)) return Date.now();
  // Hyperliquid sends timestamps in nanoseconds (> 1e15 for current dates)
  return n > 1e15 ? Math.floor(n / 1e6) : Math.floor(n);
};

const toPairs = (rows = []) => rows.map(l => [String(l.px), String(l.sz)]);

export class HyperliquidConnector extends BaseConnector {
  constructor(config) {
    super(config, {
      market: 'hyperliquid_perp',
      wsUrl: config.wsUrl || 'wss://api.hyperliquid.xyz/ws',
      restUrl: '', // No REST snapshot needed; l2Book is full replace via WS
    });
    /** @type {FullBook} */
    this.book = new FullBook('hyperliquid_perp', { maxLevels: config.depthLimit ?? 0 });
  }

  subscribe() {
    this._ws.send(JSON.stringify({
      method: 'subscribe',
      subscription: { type: 'trades', coin: 'BTC' },
    }));
    this._ws.send(JSON.stringify({
      method: 'subscribe',
      subscription: { type: 'l2Book', coin: 'BTC' },
    }));
  }

  _onMessage(data) {
    if (data.channel === 'l2Book') {
      this._handleDepth(data);
    } else if (data.channel === 'trades') {
      this._handleTrade(data);
    }
    // subscription ack: { type: 'subscriptionResponse', ... } — ignore
  }

  _handleDepth(data) {
    // Hyperliquid l2Book is always a full snapshot (replace, not diff).
    // Compute diff vs previous book state to emit incremental update.
    const bookData = data.data;
    if (!bookData || !bookData.levels) return;

    // levels[0] = bids, levels[1] = asks
    // Each level: { px: number, sz: number, n: number }
    const rawBids = bookData.levels[0] || [];
    const rawAsks = bookData.levels[1] || [];
    const bids = toPairs(rawBids);
    const asks = toPairs(rawAsks);
    const ts = normalizeTs(bookData.time);

    // Capture old book state before applying new snapshot
    const oldBids = new Map(this.book.bids);
    const oldAsks = new Map(this.book.asks);

    this.book.applySnapshot(bids, asks);

    // Compute changed levels between old and new book state
    const changedBids = [];
    const changedAsks = [];

    // Bids: check new prices, then prices that existed in old but not new
    const allBidPrices = new Set([...oldBids.keys(), ...bids.map(p => p[0])]);
    for (const price of allBidPrices) {
      const oldQty = oldBids.get(price) ?? '0';
      const newQty = this.book.bids.get(price) ?? '0';
      if (oldQty !== newQty) changedBids.push([price, newQty]);
    }
    const allAskPrices = new Set([...oldAsks.keys(), ...asks.map(p => p[0])]);
    for (const price of allAskPrices) {
      const oldQty = oldAsks.get(price) ?? '0';
      const newQty = this.book.asks.get(price) ?? '0';
      if (oldQty !== newQty) changedAsks.push([price, newQty]);
    }

    // Only emit update if there are actual changes (avoids empty depth rows
    // when the snapshot is unchanged, e.g. idle market).
    if (changedBids.length || changedAsks.length) {
      this._emitDepth('update', changedBids, changedAsks, ts);
    }

    this._notifyWsSnapshotReceived();
    this._ringBuf = [];
  }

  _handleTrade(data) {
    const trades = data.data || [];
    for (const t of trades) {
      // Hyperliquid side: 'B' = buy, 'A' = sell
      this._emitTrade(
        parseFloat(t.px),
        parseFloat(t.sz),
        t.side === 'B' ? 'buy' : 'sell',
        normalizeTs(t.time),
        String(t.tid || '')
      );
    }
  }

  /** Override: l2Book is always full snapshot, no init sync needed */
  async _syncBook() {
    this._setState('syncing');

    if (this._wsSnapshotReceived) {
      this._finalizeWsSnapshotSync();
      return;
    }

    this._beginWsSnapshotSync();

    try {
      await this._waitForWsSnapshot(WS_SNAPSHOT_TIMEOUT_MS, 'ws l2Book timeout');
      if (this._state !== 'error') {
        this._finalizeWsSnapshotSync();
      }
    } catch (err) {
      if (err?.code === 'WS_SNAPSHOT_ABORTED') return;
      this._failWsSnapshotSync(`l2Book not received within ${WS_SNAPSHOT_TIMEOUT_MS}ms: ${err.message}`);
    }
  }

  /** Hyperliquid has no REST snapshot endpoint needed, but provide fallback */
  async _fetchSnapshot() {
    throw new Error('Hyperliquid uses WS l2Book only, no REST snapshot');
  }

}
