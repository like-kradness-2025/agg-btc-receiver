// lib/binance-usdc-connector.mjs — Binance Spot USDC connector for btc-receiver v3.00
//
// Independent BTCUSDC spot market, separate from the existing BTCUSDT spot.
// Reuses BinanceSpotConnector message-handling logic; only overrides market key and book.

import { BinanceSpotConnector } from './binance-connector.mjs';
import { FullBook } from './full-book.mjs';

export class BinanceSpotUsdcConnector extends BinanceSpotConnector {
  constructor(config) {
    super(config);
    // Override: market key and book must reflect binance_spot_usdc,
    // not the hardcoded binance_spot from the parent constructor.
    this.market = 'binance_spot_usdc';
    this.book = new FullBook('binance_spot_usdc', { maxLevels: config.depthLimit ?? 0 });
  }

  // BTCUSDC spot can be quiet during the first REST request. Reuse the same
  // socket and retry the complete bridge a few times instead of declaring the
  // optional market dead after one short three-attempt window.
  async _syncBook() {
    let lastError;
    for (let attempt = 0; attempt < 3; attempt += 1) {
      try {
        return await super._syncBook();
      } catch (error) {
        lastError = error;
        if (attempt < 2) await new Promise((resolve) => setTimeout(resolve, 1_000));
      }
    }
    throw lastError;
  }
}
