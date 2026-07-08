#!/usr/bin/env node
/**
 * orderflow_monitor.mjs — btc-receiver v3.00 main entry point
 *
 * Usage:
 *   node orderflow_monitor.mjs --help
 *   node orderflow_monitor.mjs --config config.v3.json --seconds 5 --markets binance_spot,binance_perp
 *   node orderflow_monitor.mjs --config config.v3.json --seconds 5 --markets binance_spot,binance_perp --output data/live_v3_smoke
 */

import fs from 'node:fs';
import path from 'node:path';
import { BinanceSpotConnector, BinancePerpConnector } from './lib/binance-connector.mjs';
import { BinanceSpotUsdcConnector } from './lib/binance-usdc-connector.mjs';
import { BybitConnector } from './lib/bybit-connector.mjs';
import { OkxConnector } from './lib/okx-connector.mjs';
import { BinanceCoinmPerpConnector, BinancePerpBtcusdcConnector, BybitSpotConnector, OkxSpotConnector, KrakenSpotConnectorAlias } from './lib/market-connectors.mjs';
import { CoinbaseConnector } from './lib/coinbase-connector.mjs';
import { CoinbaseInternationalConnector } from './lib/coinbase-international-connector.mjs';
import { BitstampConnector } from './lib/bitstamp-connector.mjs';
import { CryptoComConnector } from './lib/crypto-com-connector.mjs';
import { BitfinexConnector } from './lib/bitfinex-connector.mjs';
import { GeminiConnector } from './lib/gemini-connector.mjs';
import { BitmexConnector } from './lib/bitmex-connector.mjs';
import { HyperliquidConnector } from './lib/hyperliquid-connector.mjs';
import { TradeAggregator } from './lib/trade-aggregator.mjs';
import { BufferedWriter } from './lib/buffered-writer.mjs';
import { RawRotationWriter } from './lib/raw-rotation-writer.mjs';
import { HealthMonitor } from './lib/health-monitor.mjs';
import { DerivativesHelper } from './lib/derivatives-helper.mjs';
import { MarketDataCollector } from './lib/market-data-collector.mjs';

// ====== Arg parser ======

function help() {
  console.log(`
btc-receiver v3.00 — BTC orderbook & trade receiver

Usage:
  node orderflow_monitor.mjs --config <path> [options]

Options:
  --help                          Show this help
  --config <path>                 Config JSON file (required)
  --seconds <N>                   Run for N seconds then exit (0 = run indefinitely)
  --markets <list>                Comma-separated market list (default: from config)
  --output <dir>                  Override output base path
  --selfTestReconnectAfterMs <N>  Close sockets after N ms for reconnect smoke test
`);
  process.exit(0);
}

function arg(name, def) {
  const idx = process.argv.indexOf(`--${name}`);
  if (idx >= 0 && idx + 1 < process.argv.length) return process.argv[idx + 1];
  // Also support --name=value
  for (const a of process.argv) {
    if (a.startsWith(`--${name}=`)) return a.slice(`--${name}=`.length);
  }
  return def;
}

function hasFlag(name) {
  return process.argv.includes(`--${name}`);
}

if (hasFlag('help')) help();

// ====== Load config ======
const configPath = arg('config', 'config.v3.json');
let config;
try {
  const raw = fs.readFileSync(configPath, 'utf-8');
  config = JSON.parse(raw);
} catch (err) {
  console.error(`[main] Failed to load config from ${configPath}: ${err.message}`);
  process.exit(1);
}

const outputBase = arg('output', config.output.base_path);
const seconds = parseInt(arg('seconds', '0'), 10);
const marketsArg = arg('markets', '');
const enabledMarkets = marketsArg
  ? marketsArg.split(',').map(s => s.trim()).filter(Boolean)
  : Object.keys(config.markets).filter(m => config.markets[m].enabled);
const selfTestReconnectAfterMs = parseInt(arg('selfTestReconnectAfterMs', '0'), 10);

function isMarketWritable(connector, book) {
  return Boolean(connector && connector.getState() === 'running' && book && !book.isEmpty());
}

// ====== Connector class map ======
const CONNECTOR_CLASSES = {
  binance_spot: BinanceSpotConnector,
  binance_spot_usdc: BinanceSpotUsdcConnector,
  binance_perp: BinancePerpConnector,
  binance_coinm_perp: BinanceCoinmPerpConnector,
  binance_perp_btcusdc: BinancePerpBtcusdcConnector,
  bybit_perp: BybitConnector,
  bybit_spot: BybitSpotConnector,
  okx_perp: OkxConnector,
  okx_spot: OkxSpotConnector,
  kraken_spot: KrakenSpotConnectorAlias,
  coinbase_spot: CoinbaseConnector,
  crypto_com_spot: CryptoComConnector,
  bitfinex_spot: BitfinexConnector,
  bitstamp_spot: BitstampConnector,
  gemini_spot: GeminiConnector,
  coinbase_international_perp: CoinbaseInternationalConnector,
  bitmex_perp: BitmexConnector,
  hyperliquid_perp: HyperliquidConnector,
};

// ====== Initialize components ======
const connectors = new Map();
const aggregators = new Map();
const books = new Map();
const STARTUP_STAGGER_MS = 50;
const STARTUP_MARKETS = enabledMarkets.filter(m => m !== 'binance_perp');
if (enabledMarkets.includes('binance_perp')) STARTUP_MARKETS.push('binance_perp');

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Writers: rotation writers for raw data, fixed-path for aggregated data
const rawTradeRotationWriters = new Map();   // market → RawRotationWriter (trades)
const bookUpdateRotationWriters = new Map();  // market → RawRotationWriter (book_updates)
const liquidationRotationWriters = new Map(); // market → RawRotationWriter (liquidations)
const snapshotRotationWriters = new Map();    // market → RawRotationWriter (snapshots)
const tradeWriters = new Map();               // aggregated trades (fixed path)
const bookWriters = new Map();                // book snapshots (fixed path)
const healthMonitor = new HealthMonitor(path.join(outputBase, 'health.jsonl'), {
  intervalMs: 1000,
});

const derivativesHelper = new DerivativesHelper(outputBase, {
  intervalMs: 5000,
});

const marketDataCollector = new MarketDataCollector(outputBase, {
  intervalMs: config.tick?.market_data_ms ?? 60000,
});

// ====== Start connectors ======
async function startConnector(market) {
  const ConnectorClass = CONNECTOR_CLASSES[market];
  if (!ConnectorClass) {
    console.error(`[main] unknown market: ${market}`);
    return;
  }
  const cfg = config.markets[market];
  const connector = new ConnectorClass(cfg);

  // Create aggregator and book ref
  const aggregator = new TradeAggregator(market, 1000);
  aggregators.set(market, aggregator);
  books.set(market, connector.book);

  // Create writers — raw data uses rotation writers, aggregated data uses fixed-path
  const basePath = outputBase;
  rawTradeRotationWriters.set(market, new RawRotationWriter(basePath, market, 'trades', {
    flushIntervalMs: config.output.flush_trades_ms ?? 200,
  }));
  bookUpdateRotationWriters.set(market, new RawRotationWriter(basePath, market, 'book_updates', {
    flushIntervalMs: config.output.flush_book_ms ?? 1000,
  }));
  liquidationRotationWriters.set(market, new RawRotationWriter(basePath, market, 'liquidations', {
    flushIntervalMs: config.output.flush_liquidations_ms ?? 200,
  }));
  snapshotRotationWriters.set(market, new RawRotationWriter(basePath, market, 'snapshots', {
    flushIntervalMs: config.output.flush_book_ms ?? 1000,
  }));
  tradeWriters.set(market, new BufferedWriter(path.join(basePath, 'trades', `${market}.jsonl`), {
    flushIntervalMs: config.output.flush_trades_ms ?? 200,
  }));
  bookWriters.set(market, new BufferedWriter(path.join(basePath, 'book', `${market}.jsonl`), {
    flushIntervalMs: config.output.flush_book_ms ?? 1000,
  }));

  // Wire events — raw data goes to rotation writers
  connector.on('trade', async (tradeEvent) => {
    aggregator.addTrade(tradeEvent);
    rawTradeRotationWriters.get(market)?.write(tradeEvent, tradeEvent.ts);
  });

  connector.on('depth', async (depthEvent) => {
    bookUpdateRotationWriters.get(market)?.write(depthEvent, depthEvent.ts);
  });

  connector.on('liquidation', async (row) => {
    liquidationRotationWriters.get(market)?.write(row, row.ts);
  });

  connector.on('error', ({ message }) => {
    console.error(`[${market}] error:`, message);
  });

  connector.on('stateChange', (from, to) => {
    console.log(`[${market}] state: ${from} → ${to}`);
    healthMonitor.updateConnector(market, connector.getStats());
  });

  connectors.set(market, connector);
  healthMonitor.updateConnector(market, connector.getStats());

  // Connect
  try {
    await connector.connect();
    await connector._syncBook();
  } catch (err) {
    console.error(`[main] ${market} initial connect failed:`, err.message);
  }
}

// ====== Main loop ======
async function main() {
  console.log(`[main] btc-receiver v3.00 starting with markets: ${enabledMarkets.join(', ')}`);
  console.log(`[main] output base: ${outputBase}`);

  // Start all connectors with a tiny stagger to reduce burst contention.
  for (const [index, market] of STARTUP_MARKETS.entries()) {
    if (index > 0) await sleep(STARTUP_STAGGER_MS);
    await startConnector(market);
  }

  // Startup recovery: scan existing files and restore state for all rotation writers
  const startupNowMs = Date.now();
  for (const [market, writer] of rawTradeRotationWriters) {
    await writer.startupRecovery(startupNowMs);
  }
  for (const [market, writer] of bookUpdateRotationWriters) {
    await writer.startupRecovery(startupNowMs);
  }
  for (const [market, writer] of liquidationRotationWriters) {
    await writer.startupRecovery(startupNowMs);
  }
  for (const [market, writer] of snapshotRotationWriters) {
    await writer.startupRecovery(startupNowMs);
  }

  // Register perp markets for auxiliary data collection
  const PERP_MARKETS = ['binance_perp', 'binance_coinm_perp', 'binance_perp_btcusdc', 'bybit_perp', 'okx_perp', 'hyperliquid_perp'];
  for (const market of enabledMarkets) {
    if (PERP_MARKETS.includes(market)) {
      derivativesHelper.registerMarket(market, {});
    }
  }

  // Register all markets for REST market data collection
  let hasCoinbase = false;
  for (const market of enabledMarkets) {
    const md = config.markets[market]?.marketData;
    if (!md) continue;
    const type = PERP_MARKETS.includes(market) ? 'perp' : 'spot';
    const collect = {};
    if (md.lsratio) collect.lsratio = true;
    if (md.takervol) collect.takervol = true;
    marketDataCollector.registerMarket(market, { type, urls: md, collect });
    if (market === 'coinbase_spot') hasCoinbase = true;
  }
  // Coinbase Premium Index (needs binance_spot + coinbase_spot both registered)
  if (hasCoinbase && enabledMarkets.includes('binance_spot')) {
    marketDataCollector.registerPremium();
  }

  // Start health monitor
  healthMonitor.start();

  // Start derivatives helper
  derivativesHelper.start();

  // Start market data collector
  marketDataCollector.start();

  // Self-test reconnect trigger
  if (selfTestReconnectAfterMs > 0) {
    setTimeout(() => {
      for (const [market, conn] of connectors) {
        console.log(`[main] self-test: closing ${market} socket`);
        if (conn._ws) {
          try { conn._ws.close(1000, 'self-test reconnect'); } catch {}
        }
      }
    }, selfTestReconnectAfterMs);
  }

  // Tick loop
  const tickMs = config.tick?.feature_ms ?? 1000;
  const bookSnapshotMs = config.tick?.book_snapshot_ms ?? 30000;
  let lastBookSnapshot = 0;

  const tick = () => {
    const now = Date.now();

    // Flush trade aggregators (always, preserves buffer during reconnect)
    for (const [market, aggregator] of aggregators) {
      const aggTrade = aggregator.flushIfDue(now);
      if (aggTrade) {
        // Write aggregated trade row
        tradeWriters.get(market)?.write(aggTrade);
      }
    }

    // Book snapshot — skip for non-running connectors or empty books
    if (now - lastBookSnapshot >= bookSnapshotMs) {
      lastBookSnapshot = now;
      for (const [market, book] of books) {
        const connector = connectors.get(market);
        if (isMarketWritable(connector, book)) {
          const snap = book.toSnapshot(now);
          bookWriters.get(market)?.write(snap);
          // Also write snapshot to rotation writer (wall-clock bucketing)
          snapshotRotationWriters.get(market)?.write(snap, now);
        }
      }
    }

    // Update health
    for (const [market, conn] of connectors) {
      healthMonitor.updateConnector(market, conn.getStats());
    }
  };

  // Run tick every tickMs
  const tickTimer = setInterval(tick, tickMs);

  // Periodic stale check for rotation writers (every 15s)
  const staleCheckMs = 15000;
  const staleCheckTimer = setInterval(async () => {
    const now = Date.now();
    for (const [, writer] of rawTradeRotationWriters) await writer.checkStale(now);
    for (const [, writer] of bookUpdateRotationWriters) await writer.checkStale(now);
    for (const [, writer] of liquidationRotationWriters) await writer.checkStale(now);
    for (const [, writer] of snapshotRotationWriters) await writer.checkStale(now);
  }, staleCheckMs);
  if (staleCheckTimer.unref) staleCheckTimer.unref();

  // Graceful shutdown
  const shutdown = async () => {
    console.log('[main] shutting down...');
    clearInterval(tickTimer);
    clearInterval(staleCheckTimer);

    for (const [, conn] of connectors) {
      conn.disconnect();
    }

    // Flush remaining trade aggregator buffers before closing writers
    for (const [market, aggregator] of aggregators) {
      const aggTrade = aggregator.flushNow();
      if (aggTrade) {
        tradeWriters.get(market)?.write(aggTrade);
      }
    }

    // Flush all writers
    const writerFlushPromises = [];
    for (const w of tradeWriters.values()) writerFlushPromises.push(w.close());
    for (const w of bookWriters.values()) writerFlushPromises.push(w.close());
    for (const w of rawTradeRotationWriters.values()) writerFlushPromises.push(w.finalize());
    for (const w of bookUpdateRotationWriters.values()) writerFlushPromises.push(w.finalize());
    for (const w of liquidationRotationWriters.values()) writerFlushPromises.push(w.finalize());
    for (const w of snapshotRotationWriters.values()) writerFlushPromises.push(w.finalize());
    writerFlushPromises.push(healthMonitor.close());
    writerFlushPromises.push(derivativesHelper.close());
    writerFlushPromises.push(marketDataCollector.close());
    await Promise.allSettled(writerFlushPromises);

    console.log('[main] shutdown complete');
    process.exit(0);
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);

  // Duration limit
  if (seconds > 0) {
    setTimeout(shutdown, seconds * 1000);
  }
}

main().catch(err => {
  console.error('[main] fatal error:', err);
  process.exit(1);
});
