// lib/orderflow-worker.mjs — Worker thread for orderflow monitor
//
// Each worker handles a group of markets:
//   - Creates connectors and raw rotation writers (trades/book_updates/liquidations)
//   - Saves raw trade/depth/liquidation events to rotation files
//   - Routes stateChange/liquidation events → main via IPC
//   - Recovery-before-connect: prepareMarket → startupRecovery → connectMarket

import { parentPort } from 'node:worker_threads';
import path from 'node:path';
import { BinanceSpotConnector, BinancePerpConnector } from './binance-connector.mjs';
import { BinanceSpotUsdcConnector } from './binance-usdc-connector.mjs';
import { BybitConnector } from './bybit-connector.mjs';
import { OkxConnector } from './okx-connector.mjs';
import { BinanceCoinmPerpConnector, BinancePerpBtcusdcConnector, BybitSpotConnector, OkxSpotConnector, KrakenSpotConnectorAlias } from './market-connectors.mjs';
import { CoinbaseConnector } from './coinbase-connector.mjs';
import { CoinbaseInternationalConnector } from './coinbase-international-connector.mjs';
import { BitstampConnector } from './bitstamp-connector.mjs';
import { CryptoComConnector } from './crypto-com-connector.mjs';
import { BitfinexConnector } from './bitfinex-connector.mjs';
import { GeminiConnector } from './gemini-connector.mjs';
import { BitmexConnector } from './bitmex-connector.mjs';
import { HyperliquidConnector } from './hyperliquid-connector.mjs';
import { RawRotationWriter } from './raw-rotation-writer.mjs';

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

const STARTUP_STAGGER_MS = 50;
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ── State ────────────────────────────────────────────────────────────────
const connectors = new Map();
let outputBase = null;
let markets = [];
let configMarkets = {};
let configOutput = {};
let workerId = 'unknown';
let staleCheckTimer = null;
let healthPushTimer = null;

// Raw rotation writers (per market, per kind) — raw-only contract
const rawTradeRotationWriters = new Map();
const bookUpdateRotationWriters = new Map();
const liquidationRotationWriters = new Map();

// ── Phase 1: prepare market (create connector + writers + wire events) ───

async function prepareMarket(market) {
  const ConnectorClass = CONNECTOR_CLASSES[market];
  if (!ConnectorClass) {
    console.error(`[worker:${workerId}] unknown market: ${market}`);
    return;
  }
  const cfg = configMarkets[market];
  if (!cfg) return;
  const connector = new ConnectorClass(cfg);

  const basePath = outputBase;
  rawTradeRotationWriters.set(market, new RawRotationWriter(basePath, market, 'trades', {
    flushIntervalMs: configOutput.flush_trades_ms ?? 200,
  }));
  bookUpdateRotationWriters.set(market, new RawRotationWriter(basePath, market, 'book_updates', {
    flushIntervalMs: configOutput.flush_book_ms ?? 1000,
  }));
  liquidationRotationWriters.set(market, new RawRotationWriter(basePath, market, 'liquidations', {
    flushIntervalMs: configOutput.flush_liquidations_ms ?? 200,
  }));

  // Wire events — save to writers + IPC to main
  connector.on('trade', async (tradeEvent) => {
    rawTradeRotationWriters.get(market)?.write(tradeEvent, tradeEvent.ts);
  });

  connector.on('depth', async (depthEvent) => {
    bookUpdateRotationWriters.get(market)?.write(depthEvent, depthEvent.ts);
  });

  connector.on('liquidation', async (row) => {
    liquidationRotationWriters.get(market)?.write(row, row.ts);
    parentPort.postMessage({ type: 'liquidation', market, payload: row });
  });

  connector.on('error', ({ message }) => {
    console.error(`[worker:${workerId}][${market}] error:`, message);
  });

  connector.on('stateChange', (from, to) => {
    console.log(`[worker:${workerId}][${market}] state: ${from} → ${to}`);
    parentPort.postMessage({
      type: 'stateChange',
      market,
      from,
      to,
      stats: connector.getStats(),
    });
  });

  connectors.set(market, connector);
}

// ── Phase 3: connect market (recovery already done) ──────────────────────

/**
 * Connect a single market. On failure, notifies parent via IPC and throws.
 * Returns void on success; throws on failure.
 */
async function connectMarket(market) {
  const connector = connectors.get(market);
  if (!connector) return;

  // Connect
  try {
    await connector.connect();
    await connector._syncBook();
    parentPort.postMessage({
      type: 'stateChange',
      market,
      from: 'initializing',
      to: connector.getState(),
      stats: connector.getStats(),
    });
  } catch (err) {
    console.error(`[worker:${workerId}] ${market} initial connect failed:`, err.message);
    // Notify parent of startup failure for this market
    parentPort.postMessage({
      type: 'startupFailed',
      workerId,
      market,
      reason: err.message,
    });
    throw err;
  }
}

// ── Init handler ─────────────────────────────────────────────────────────

async function doInit(msg) {
  workerId = msg.workerId || 'unknown';
  markets = msg.markets || [];
  configMarkets = msg.configMarkets || {};
  configOutput = msg.configOutput || {};
  outputBase = msg.outputBase;

  console.log(`[worker:${workerId}] starting with markets: ${markets.join(', ')}`);

  // Phase 1: Prepare all markets (create connectors + writers + wire events)
  for (const [index, market] of markets.entries()) {
    if (index > 0) await sleep(STARTUP_STAGGER_MS);
    await prepareMarket(market);
  }

  // Phase 2: Startup recovery for raw rotation writers (before connecting)
  const startupNowMs = Date.now();
  for (const [, writer] of rawTradeRotationWriters) {
    await writer.startupRecovery(startupNowMs);
  }
  for (const [, writer] of bookUpdateRotationWriters) {
    await writer.startupRecovery(startupNowMs);
  }
  for (const [, writer] of liquidationRotationWriters) {
    await writer.startupRecovery(startupNowMs);
  }

  // Phase 3: Connect all markets (recovery done, now safe to receive).
  // ALL markets must connect successfully before timers/ready are started.
  let connectFailed = false;
  for (const market of markets) {
    try {
      await connectMarket(market);
    } catch (err) {
      // connectMarket already notified parent via startupFailed IPC.
      // Mark failure and stop iterating — remaining markets won't be connected.
      connectFailed = true;
      break;
    }
  }

  // If any market failed to connect, do NOT start timers or send ready.
  if (connectFailed) {
    console.error(`[worker:${workerId}] startup failed — not all markets connected`);
    // Exit with non-zero to signal failure (process.exit is intercepted by worker_threads)
    process.exit(1);
    return;
  }

  // Start stale check timer (raw 3 writers only, every 15s)
  staleCheckTimer = setInterval(async () => {
    const now = Date.now();
    for (const [, writer] of rawTradeRotationWriters) await writer.checkStale(now);
    for (const [, writer] of bookUpdateRotationWriters) await writer.checkStale(now);
    for (const [, writer] of liquidationRotationWriters) await writer.checkStale(now);
  }, 15000);
  if (staleCheckTimer.unref) staleCheckTimer.unref();

  // Start health stats push timer (every 2s)
  healthPushTimer = setInterval(() => {
    for (const [market, connector] of connectors) {
      parentPort.postMessage({
        type: 'stats',
        market,
        payload: connector.getStats(),
      });
    }
  }, 2000);
  if (healthPushTimer.unref) healthPushTimer.unref();

  // Signal ready
  parentPort.postMessage({ type: 'ready', workerId });
}

// ── Shutdown ─────────────────────────────────────────────────────────────

async function doShutdown() {
  console.log(`[worker:${workerId}] shutting down...`);

  // Stop timers
  if (staleCheckTimer) { clearInterval(staleCheckTimer); staleCheckTimer = null; }
  if (healthPushTimer) { clearInterval(healthPushTimer); healthPushTimer = null; }

  // Disconnect all connectors
  for (const [, conn] of connectors) {
    conn.disconnect();
  }

  // Finalize raw 3 writers only
  const promises = [];
  for (const w of rawTradeRotationWriters.values()) promises.push(w.finalize());
  for (const w of bookUpdateRotationWriters.values()) promises.push(w.finalize());
  for (const w of liquidationRotationWriters.values()) promises.push(w.finalize());
  await Promise.allSettled(promises);

  console.log(`[worker:${workerId}] shutdown complete`);
  process.exit(0);
}

// ── IPC message handler ──────────────────────────────────────────────────

parentPort.on('message', async (msg) => {
  try {
    switch (msg.cmd) {
      case 'init':
        await doInit(msg);
        break;
      case 'shutdown':
        await doShutdown();
        break;
      case 'selfTestReconnect':
        console.log(`[worker:${workerId}] self-test: closing all sockets`);
        for (const [, conn] of connectors) {
          if (conn._ws) {
            try { conn._ws.close(1000, 'self-test reconnect'); } catch {}
          }
        }
        break;
      default:
        console.error(`[worker:${workerId}] unknown command: ${msg.cmd}`);
    }
  } catch (err) {
    console.error(`[worker:${workerId}] IPC handler error:`, err.message);
  }
});
