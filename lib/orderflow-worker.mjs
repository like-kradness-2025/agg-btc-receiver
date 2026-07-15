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
import { RawRotationWriter, acquireOutputRootLock, releaseOutputRootLock } from './raw-rotation-writer.mjs';

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

// テスト seam: market → { trades?, book_updates?, liquidations? } の上書きマップ。
// セットすると prepareMarket は新規 RawRotationWriter を作成せず、既存インスタンスを使用する。
// startupRecovery 失敗のテストなど、writer 動作を制御したい場合に使う。
let _testWriterOverrides = null;
/** @returns {object|null} writer override for (market, kind) or null */
function _getWriterOverride(market, kind) {
  if (!_testWriterOverrides) return null;
  const entry = _testWriterOverrides.get(market);
  if (!entry) return null;
  return entry[kind] || null;
}

// FIX7a: doInit で登録した process.on('exit') リスナーの参照。
// _testReset で解除してテスト間のリークを防ぐ。
let _exitListener = null;

/**
 * FIX4: writer のエラー状態を確認し、connector の state と IPC に伝搬する。
 * テストから直接呼べるよう export している。
 */
export function _propagateWriterError(market, writer) {
  const errCount = writer.getWriteErrorCount();
  if (errCount === 0) return;

  const conn = connectors.get(market);
  if (!conn) return;

  // すでに error 状態なら再通知しない
  if (conn.getState() === 'error') return;

  const lastErr = writer.getLastWriteError();
  const msg = lastErr ? lastErr.message : `writer errors: ${errCount}`;
  console.error(`[worker:${workerId}] ${market}: writer I/O error detected (count=${errCount}), marking connector error: ${msg}`);

  // connector を error に遷移 → HealthMonitor に伝搬される
  if (typeof conn._setState === 'function') {
    conn._setState('error');
  } else {
    conn._state = 'error';
  }

  // IPC でエビデンスを送る
  (_testParentPort || parentPort).postMessage({
    type: 'writerError',
    workerId,
    market,
    errorCount: errCount,
    lastErrorMessage: msg,
  });
}

// FIX5: テスト用 — process.exit を上書きする。null で解除。
let _testExitFn = null;
export function _setTestExitFn(fn) { _testExitFn = fn; }

// ── Phase 1: prepare market (create connector + writers + wire events) ───

async function prepareMarket(market) {
  const ConnectorClass = (_testConnectorClasses || CONNECTOR_CLASSES)[market];
  if (!ConnectorClass) {
    console.error(`[worker:${workerId}] unknown market: ${market}`);
    // C2 G1: fail-closed — 不明marketはworkerレベルでも報告し、main threadに通知する。
    // ただしworker全体は停止せず、後続marketの処理を継続する（defense-in-depth）。
    (_testParentPort || parentPort).postMessage({
      type: 'startupFailed',
      workerId,
      market,
      reason: `unknown market: ${market} (no connector class)`,
    });
    return;
  }
  const cfg = configMarkets[market];
  if (!cfg) return;
  const connector = new ConnectorClass(cfg);

  const basePath = outputBase;
  // テスト writer 上書きがあれば優先する（startupRecovery 失敗などの注入用）
  rawTradeRotationWriters.set(market,
    _getWriterOverride(market, 'trades') ??
    new RawRotationWriter(basePath, market, 'trades', {
      flushIntervalMs: configOutput.flush_trades_ms ?? 200,
    }));
  bookUpdateRotationWriters.set(market,
    _getWriterOverride(market, 'book_updates') ??
    new RawRotationWriter(basePath, market, 'book_updates', {
      flushIntervalMs: configOutput.flush_book_ms ?? 1000,
    }));
  liquidationRotationWriters.set(market,
    _getWriterOverride(market, 'liquidations') ??
    new RawRotationWriter(basePath, market, 'liquidations', {
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
    (_testParentPort || parentPort).postMessage({ type: 'liquidation', market, payload: row });
  });

  connector.on('error', ({ message }) => {
    console.error(`[worker:${workerId}][${market}] error:`, message);
  });

  connector.on('stateChange', (from, to) => {
    console.log(`[worker:${workerId}][${market}] state: ${from} → ${to}`);
    (_testParentPort || parentPort).postMessage({
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
    // B2/G4: if sync failed (connector in error state), treat as connect failure
    if (connector.getState() === 'error') {
      throw new Error('sync failed after retries');
    }
    (_testParentPort || parentPort).postMessage({
      type: 'stateChange',
      market,
      from: 'initializing',
      to: connector.getState(),
      stats: connector.getStats(),
    });
  } catch (err) {
    console.error(`[worker:${workerId}] ${market} initial connect failed:`, err.message);
    // Notify parent of startup failure for this market
    (_testParentPort || parentPort).postMessage({
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

  // FIX5: 出力ルートのマルチインスタンスロック — 最初の worker が locks/receiver.lock/ を取得する
  if (outputBase) {
    const lockResult = acquireOutputRootLock(outputBase);
    if (!lockResult.ok) {
      console.error(
        `[worker:${workerId}] output root lock failed: ${lockResult.reason}` +
        (lockResult.holderPid ? ` (held by PID ${lockResult.holderPid})` : '') +
        (lockResult.error ? `: ${lockResult.error}` : '')
      );
      (_testParentPort || parentPort).postMessage({
        type: 'startupFailed',
        workerId,
        market: '*',
        reason: `output-root-lock: ${lockResult.reason}`,
      });
      // FIX5: テスト用 seam — process.exit を上書き可能にする
      if (_testExitFn) { _testExitFn(1); return; }
      process.exit(1);
      return;
    }
    // ロックを実際に取得した worker のみが cleanup を担当する
    if (lockResult.acquired) {
      _exitListener = () => releaseOutputRootLock(outputBase);
      process.on('exit', _exitListener);
    }
  }

  console.log(`[worker:${workerId}] starting with markets: ${markets.join(', ')}`);

  // Phase 1: Prepare all markets (create connectors + writers + wire events)
  for (const [index, market] of markets.entries()) {
    if (index > 0) await sleep(STARTUP_STAGGER_MS);
    await prepareMarket(market);
  }

  // Phase 2: Startup recovery for raw rotation writers (before connecting).
  // 単一 writer の回復失敗が worker 全体をクラッシュさせないよう try/catch で保護する。
  const startupNowMs = Date.now();
  /** @type {Set<string>} recovery に失敗した market — Phase 3 でスキップする。 */
  const recoveryFailed = new Set();
  for (const [market, writer] of rawTradeRotationWriters) {
    try {
      await writer.startupRecovery(startupNowMs);
    } catch (err) {
      console.error(`[worker:${workerId}] ${market} trade writer recovery failed:`, err.message);
      recoveryFailed.add(market);
    }
  }
  for (const [market, writer] of bookUpdateRotationWriters) {
    try {
      await writer.startupRecovery(startupNowMs);
    } catch (err) {
      console.error(`[worker:${workerId}] ${market} book writer recovery failed:`, err.message);
      recoveryFailed.add(market);
    }
  }
  for (const [market, writer] of liquidationRotationWriters) {
    try {
      await writer.startupRecovery(startupNowMs);
    } catch (err) {
      console.error(`[worker:${workerId}] ${market} liquidation writer recovery failed:`, err.message);
      recoveryFailed.add(market);
    }
  }

  // Phase 3: Connect all markets (recovery done, now safe to receive).
  // B2/G4: per-market isolation — a single market failure does NOT block others.
  // recoveryFailed の market はスキップ（writer が使えないため）。
  let anyConnected = false;
  for (const market of markets) {
    if (recoveryFailed.has(market)) {
      console.error(`[worker:${workerId}] ${market} skipping connect — writer recovery failed`);
      // 親に startupFailed を通知する
      (_testParentPort || parentPort).postMessage({
        type: 'startupFailed',
        workerId,
        market,
        reason: 'writer recovery failed',
      });
      continue;
    }
    try {
      await connectMarket(market);
      anyConnected = true;
    } catch (err) {
      // connectMarket already notified parent via startupFailed IPC.
      // Isolate the failed market: update connector state to error.
      const connector = connectors.get(market);
      if (connector) {
        if (typeof connector._setState === 'function') {
          connector._setState('error');
        } else {
          connector._state = 'error';
        }
      }
    }
  }

  // If NO markets connected, the worker has nothing to do.
  if (!anyConnected) {
    console.error(`[worker:${workerId}] no markets connected — exiting`);
    process.exit(1);
    return;
  }

  // Start stale check timer (raw 3 writers only, every 15s)
  staleCheckTimer = setInterval(async () => {
    const now = Date.now();
    for (const [market, writer] of rawTradeRotationWriters) {
      await writer.checkStale(now);
      _propagateWriterError(market, writer);
    }
    for (const [market, writer] of bookUpdateRotationWriters) {
      await writer.checkStale(now);
      _propagateWriterError(market, writer);
    }
    for (const [market, writer] of liquidationRotationWriters) {
      await writer.checkStale(now);
      _propagateWriterError(market, writer);
    }
  }, 15000);
  if (staleCheckTimer.unref) staleCheckTimer.unref();

  // Start health stats push timer (every 2s)
  healthPushTimer = setInterval(() => {
    for (const [market, connector] of connectors) {
      (_testParentPort || parentPort).postMessage({
        type: 'stats',
        market,
        payload: connector.getStats(),
      });
    }
  }, 2000);
  if (healthPushTimer.unref) healthPushTimer.unref();

  // Signal ready
  (_testParentPort || parentPort).postMessage({ type: 'ready', workerId });
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

// ── Test seam exports ─────────────────────────────────────────────────────
//
// These exports allow tests to inject mock connector classes and a mock
// parentPort, and to invoke prepareMarket directly without running the
// real Worker thread.  Use _setTestConnectorClasses(null) and
// _setTestParentPort(null) to restore defaults.

let _testConnectorClasses = null;
let _testParentPort = null;

/**
 * Override the connector class map for testing.
 * Pass a map of market → constructor, or null to use real classes.
 * @param {Object|null} map
 */
export function _setTestConnectorClasses(map) { _testConnectorClasses = map; }

/**
 * Override writer instances per market for testing.
 * Pass a Map<string, {trades?, book_updates?, liquidations?}> where each
 * value is an object with optional writer instances for each data kind.
 * Pass null to clear.
 * @param {Map<string, object>|null} overrides
 */
export function _testSetWriterOverrides(overrides) { _testWriterOverrides = overrides; }

/**
 * Override the parentPort for testing (capture IPC messages).
 * Pass an object with postMessage(msg) method, or null to use real parentPort.
 * @param {Object|null} pp
 */
export function _setTestParentPort(pp) { _testParentPort = pp; }

/**
 * Set module-level state for testing (outputBase, configMarkets, configOutput).
 * Call before _testPrepareMarket to configure the Worker-like environment.
 * @param {Object} opts
 * @param {string} [opts.outputBase]
 * @param {Object} [opts.configMarkets]
 * @param {Object} [opts.configOutput]
 * @param {string} [opts.workerId]
 */
export function _testInit(opts = {}) {
  if (opts.outputBase !== undefined) outputBase = opts.outputBase;
  if (opts.configMarkets !== undefined) configMarkets = opts.configMarkets;
  if (opts.configOutput !== undefined) configOutput = opts.configOutput;
  if (opts.workerId !== undefined) workerId = opts.workerId;
}

/**
 * Expose prepareMarket for testing.  Sets up connector + writers + event
 * wiring for a single market, using whatever connector classes and
 * parentPort are currently injected via test seams.
 * @param {string} market
 */
export async function _testPrepareMarket(market) {
  return prepareMarket(market);
}

/**
 * Expose connectMarket for testing.  Connects a single market through the
 * real connect path (connector.connect → _syncBook), using injected test
 * seams for parentPort and connector classes.
 * @param {string} market
 */
export async function _testConnectMarket(market) {
  return connectMarket(market);
}

/**
 * Expose the full init handler for testing.  Runs Phase 1-3 (prepare,
 * recovery, connect) with injected test seams.  Useful for integration
 * tests that verify per-market isolation and multi-market startup.
 * @param {Object} msg  Same shape as the real init IPC message.
 */
export async function _testDoInit(msg) {
  return doInit(msg);
}

/**
 * Test-reset: finalize all writers, clear maps and timers.
 */
export async function _testFinalizeAll() {
  for (const w of rawTradeRotationWriters.values()) await w.finalize();
  for (const w of bookUpdateRotationWriters.values()) await w.finalize();
  for (const w of liquidationRotationWriters.values()) await w.finalize();
}

/**
 * Reset module state between tests.
 */
export function _testReset() {
  connectors.clear();
  rawTradeRotationWriters.clear();
  bookUpdateRotationWriters.clear();
  liquidationRotationWriters.clear();
  outputBase = null;
  markets = [];
  configMarkets = {};
  configOutput = {};
  workerId = 'unknown';
  if (staleCheckTimer) { clearInterval(staleCheckTimer); staleCheckTimer = null; }
  if (healthPushTimer) { clearInterval(healthPushTimer); healthPushTimer = null; }
  // FIX7a: 前のテストで登録された process.on('exit') リスナーを解除する
  if (_exitListener) { process.removeListener('exit', _exitListener); _exitListener = null; }
  _testConnectorClasses = null;
  _testParentPort = null;
  _testWriterOverrides = null;
  _testExitFn = null;
}

if (parentPort) {
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
}
