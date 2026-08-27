// lib/binance-fdusd-connector.mjs — Binance Spot FDUSD connector for btc-receiver v3.00
//
// Independent BTCFDUSD spot market, separate from the existing BTCUSDT/BTCUSDC spot.
// Reuses BinanceSpotConnector message-handling logic; only overrides market key and book.

import { BinanceSpotConnector } from './binance-connector.mjs';
import { FullBook } from './full-book.mjs';

export class BinanceSpotFdusdConnector extends BinanceSpotConnector {
  constructor(config) {
    super(config);
    // Override: market key and book must reflect binance_spot_fdusd,
    // not the hardcoded binance_spot from the parent constructor.
    this.market = 'binance_spot_fdusd';
    this.book = new FullBook('binance_spot_fdusd', { maxLevels: config.depthLimit ?? 0 });
  }
}
