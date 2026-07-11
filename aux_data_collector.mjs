#!/usr/bin/env node
/**
 * aux_data_collector.mjs — BTC auxiliary data collector (standalone)
 *
 * Collects REST-based market data independently from the orderflow receiver:
 *   - DerivativesHelper: mark price, funding rate, open interest (perp only)
 *   - MarketDataCollector: OHLCV, 24h ticker, LS ratio, taker volume, premium
 *
 * Usage:
 *   node aux_data_collector.mjs --help
 *   node aux_data_collector.mjs --config config.v3.json
 *   node aux_data_collector.mjs --config config.v3.json --seconds 5 --markets binance_spot,binance_perp --output data/live_v3_aux_smoke
 */

import fs from 'node:fs';
import path from 'node:path';
import { DerivativesHelper } from './lib/derivatives-helper.mjs';
import { MarketDataCollector } from './lib/market-data-collector.mjs';

// ====== Heartbeat ======

/**
 * Write heartbeat JSON atomically to outputBase/health/aux_collector.json.
 * Errors are caught silently — heartbeat must never crash the collector.
 */
function writeHeartbeat(outputBase, status, enabledMarkets) {
  try {
    const healthDir = path.join(outputBase, 'health');
    fs.mkdirSync(healthDir, { recursive: true });
    const tmpPath = path.join(healthDir, '.aux_collector.json.tmp');
    const finalPath = path.join(healthDir, 'aux_collector.json');
    const payload = JSON.stringify({
      status,
      pid: process.pid,
      ts: new Date().toISOString(),
      updated_at_ms: Date.now(),
      markets: enabledMarkets,
      output_base: outputBase,
    }, null, 2);
    fs.writeFileSync(tmpPath, payload, 'utf8');
    fs.renameSync(tmpPath, finalPath);
  } catch (_err) {
    // Heartbeat failure must never crash the collector
  }
}

// ====== Arg parser ======

function help() {
  console.log(`
btc-aux-collector — standalone REST auxiliary data collector

Usage:
  node aux_data_collector.mjs --config <path> [options]

Options:
  --help                          Show this help
  --config <path>                 Config JSON file (required)
  --seconds <N>                   Run for N seconds then exit (0 = run indefinitely)
  --markets <list>                Comma-separated market list (default: from config)
  --output <dir>                  Override output base path
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

// ====== Load config ======

const configPath = arg('config', 'config.v3.json');
let config;
try {
  const raw = fs.readFileSync(configPath, 'utf-8');
  config = JSON.parse(raw);
} catch (err) {
  console.error(`[aux-collector] Failed to load config from ${configPath}: ${err.message}`);
  process.exit(1);
}

const outputBase = arg('output', config.output.base_path);
const seconds = parseInt(arg('seconds', '0'), 10);
const marketsArg = arg('markets', '');
const enabledMarkets = marketsArg
  ? marketsArg.split(',').map(s => s.trim()).filter(Boolean)
  : Object.keys(config.markets).filter(m => config.markets[m].enabled);

// ====== Initialize auxiliary data collectors ======

const PERP_MARKETS = ['binance_perp', 'binance_coinm_perp', 'binance_perp_btcusdc', 'bybit_perp', 'okx_perp', 'hyperliquid_perp'];

const derivativesHelper = new DerivativesHelper(outputBase, {
  intervalMs: 5000,
});

const marketDataCollector = new MarketDataCollector(outputBase, {
  intervalMs: config.tick?.market_data_ms ?? 60000,
});

// ====== Main ======

async function main() {
  console.log(`[aux-collector] starting`);
  console.log(`[aux-collector] enabled markets: ${enabledMarkets.join(', ')}`);
  console.log(`[aux-collector] output base: ${outputBase}`);

  // Register perp markets for derivatives collection
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
  if (hasCoinbase && enabledMarkets.includes('binance_spot')) {
    marketDataCollector.registerPremium();
  }

  // Start auxiliary services
  derivativesHelper.start();
  marketDataCollector.start();

  console.log('[aux-collector] auxiliary data collection started');

  // Start heartbeat timer
  // Use 1s interval for smoke runs (seconds <= 10), otherwise 5s
  const heartbeatIntervalMs = (seconds > 0 && seconds <= 10) ? 1000 : 5000;
  const heartbeatTimer = setInterval(() => {
    writeHeartbeat(outputBase, 'running', enabledMarkets);
  }, heartbeatIntervalMs);
  // Write initial heartbeat immediately
  writeHeartbeat(outputBase, 'running', enabledMarkets);

  // Graceful shutdown
  let shuttingDown = false;
  const shutdown = async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log('[aux-collector] shutting down...');

    // Stop heartbeat timer and write final stopped status
    clearInterval(heartbeatTimer);
    writeHeartbeat(outputBase, 'stopped', enabledMarkets);

    const promises = [];
    promises.push(derivativesHelper.close());
    promises.push(marketDataCollector.close());
    await Promise.allSettled(promises);

    console.log('[aux-collector] shutdown complete');
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
  console.error('[aux-collector] fatal error:', err);
  process.exit(1);
});
