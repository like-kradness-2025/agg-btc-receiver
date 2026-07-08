#!/usr/bin/env node
/**
 * fairprice_monitor.mjs — agg-btc-receiver: fair price / book / raw trades receiver
 *
 * Usage:
 *   node fairprice_monitor.mjs --help
 *   node fairprice_monitor.mjs --config config.v3.json --seconds 5 --markets binance_spot,binance_perp
 *   node fairprice_monitor.mjs --config config.v3.json --seconds 5 --markets binance_spot,binance_perp --output data/live_fairprice_smoke
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
import { FairPriceCollector } from './lib/fair-price-collector.mjs';
import { BufferedWriter } from './lib/buffered-writer.mjs';

function help() {
  console.log(`
btc-receiver fairprice — BTC fair price / orderbook / raw trades receiver

Usage:
  node fairprice_monitor.mjs --config <path> [options]

Options:
  --help                    Show this help
  --config <path>           Config JSON file (required)
  --seconds <N>             Run for N seconds then exit (0 = run indefinitely)
  --markets <list>          Comma-separated market list (default: from config)
  --output <dir>            Override output base path
`);
  process.exit(0);
}

function arg(name, def) {
  const idx = process.argv.indexOf(`--${name}`);
  if (idx >= 0 && idx + 1 < process.argv.length) return process.argv[idx + 1];
  for (const a of process.argv) {
    if (a.startsWith(`--${name}=`)) return a.slice(`--${name}=`.length);
  }
  return def;
}

function hasFlag(name) {
  return process.argv.includes(`--${name}`);
}

if (hasFlag('help')) help();

const configPath = arg('config', 'config.v3.json');
let config;
try {
  const raw = fs.readFileSync(configPath, 'utf-8');
  config = JSON.parse(raw);
} catch (err) {
  console.error(`[agg-btc] Failed to load config from ${configPath}: ${err.message}`);
  process.exit(1);
}

const outputBase = arg('output', config.output?.fairprice_base_path ?? 'data/raw_hot');
const seconds = parseInt(arg('seconds', '0'), 10);
const marketsArg = arg('markets', '');
const enabledMarkets = marketsArg
  ? marketsArg.split(',').map(s => s.trim()).filter(Boolean)
  : Object.keys(config.markets).filter(m => config.markets[m].enabled);

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

const collectors = new Map();
const connectors = new Map();
const liquidationWriters = new Map();
const STARTUP_STAGGER_MS = 50;
const STARTUP_MARKETS = enabledMarkets.filter(m => m !== 'binance_perp');
if (enabledMarkets.includes('binance_perp')) STARTUP_MARKETS.push('binance_perp');

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function startConnector(market) {
  const ConnectorClass = CONNECTOR_CLASSES[market];
  if (!ConnectorClass) {
    console.error(`[agg-btc] unknown market: ${market}`);
    return;
  }

  const cfg = config.markets[market];
  const connector = new ConnectorClass(cfg);
  const collector = collectors.get('main');

  connectors.set(market, connector);
  collector.registerMarket(market, {
    connector,
    book: connector.book,
  });

  // Liquidation writer
  liquidationWriters.set(market, new BufferedWriter(
    path.join(outputBase, 'liquidations', `${market}.jsonl`),
    { flushIntervalMs: 200 }
  ));

  connector.on('error', ({ message }) => {
    console.error(`[${market}] error: ${message}`);
  });

  connector.on('stateChange', (from, to) => {
    console.log(`[${market}] state: ${from} → ${to}`);
  });

  connector.on('liquidation', async (row) => {
    liquidationWriters.get(market)?.write(row);
  });

  try {
    await connector.connect();
    await connector._syncBook();
  } catch (err) {
    console.error(`[agg-btc] ${market} initial connect failed: ${err.message}`);
  }
}

async function main() {
  console.log(`[agg-btc] agg-btc-receiver starting with markets: ${enabledMarkets.join(', ')}`);
  console.log(`[agg-btc] output base: ${outputBase}`);

  const collector = new FairPriceCollector(outputBase, {
    snapshotIntervalMs: config.tick?.feature_ms ?? 1000,
    bookSnapshotMs: config.tick?.book_snapshot_ms ?? 30000,
  });
  collectors.set('main', collector);

  // Output directories for raw receiver streams
  for (const dir of ['liquidations']) {
    fs.mkdirSync(path.join(outputBase, dir), { recursive: true });
  }

  // Start collector timer BEFORE connector startup loop.
  // The tick handler already skips markets whose connector is not running or
  // whose book is empty, so data flows for fast-connecting markets while
  // slow-connecting ones are still syncing.
  collector.start();

  for (const [index, market] of STARTUP_MARKETS.entries()) {
    if (index > 0) await sleep(STARTUP_STAGGER_MS);
    await startConnector(market);
  }

  const shutdown = async () => {
    console.log('[agg-btc] shutting down...');
    for (const [, conn] of connectors) {
      conn.disconnect();
    }
    await collector.close();
    // Flush liquidation writers
    const promises = [];
    for (const [, w] of liquidationWriters) promises.push(w.close());
    await Promise.allSettled(promises);
    console.log('[agg-btc] shutdown complete');
    process.exit(0);
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);

  if (seconds > 0) {
    setTimeout(shutdown, seconds * 1000);
  }
}

main().catch(err => {
  console.error('[agg-btc] fatal error:', err);
  process.exit(1);
});