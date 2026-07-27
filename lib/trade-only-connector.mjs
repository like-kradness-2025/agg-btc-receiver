// lib/trade-only-connector.mjs — helper base for public trade-only markets

import { BaseConnector } from './base-connector.mjs';
import { FullBook } from './full-book.mjs';

export class TradeOnlyConnector extends BaseConnector {
  constructor(config, { market, wsUrl, restUrl = '' }) {
    super(config, { market, wsUrl, restUrl });
    this.supportsDepth = true;
    this.book = new FullBook(market, { maxLevels: config.depthLimit ?? 0 });
  }

  _resetBook() {
    if (this.book && typeof this.book.clear === 'function') {
      this.book.clear();
    }
    this._beginWsSnapshotSync();
  }

  async _syncBook() {
    if (this._state === 'reconnecting' || this._state === 'error') return;
    this._setState('running');
  }
}
