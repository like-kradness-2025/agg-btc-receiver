// lib/orderflow-worker.mjs — Worker thread for orderflow monitor
//
// Each worker handles a group of markets:
//   - Creates connectors and raw writers (DuckDB or legacy file writers)
//   - Saves raw trade/depth/liquidation events to rotation files
//   - Routes stateChange/liquidation events → main via IPC
//   - Recovery-before-connect: prepareMarket → startupRecovery → connectMarket

import { parentPort } from 'node:worker_threads';
import path from 'node:path';
import { BinanceSpotConnector, BinancePerpConnector } from './binance-connector.mjs';
import { BinanceSpotUsdcConnector } from './binance-usdc-connector.mjs';
import { BinanceSpotFdusdConnector } from './binance-fdusd-connector.mjs';
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
// RawV4Writer and SnapshotWriter are legacy — dynamically imported only on
// non-SQLite/DuckDB paths.  See docs/current/legacy-artifacts.md.

const CONNECTOR_CLASSES = {
  binance_spot: BinanceSpotConnector,
  binance_spot_usdc: BinanceSpotUsdcConnector,
  binance_spot_fdusd: BinanceSpotFdusdConnector,
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
// Kept for CLI/config compatibility. Initial connect failures are now
// isolated for every market, so this no longer controls readiness.
let optionalMarkets = new Set();
let staleCheckTimer = null;
let healthPushTimer = null;
let rawFlushTimer = null;
let rawQueueFailureReported = false;
let rawWriteFailed = false;

const RAW_WRITE_BATCH_SIZE = 256;
const RAW_WORKER_PENDING_MAX_EVENTS = 65_536;
const RAW_IPC_BATCH_SIZE = 256;
const RAW_IPC_FLUSH_MS = 50;
// A failed initial snapshot is a market-local outage.  Do not immediately
// hammer the exchange REST endpoint: wait for the stream to have time to
// accumulate a fresh contiguous diff, then enter the connector's normal
// backoff/reconnect path.
const INITIAL_RECONNECT_DELAY_MS = 5 * 60 * 1000;
const rawWriteStates = new Map();
const rawDepthKeys = new Map();
const rawIpcPending = [];
let rawIpcFlushTimer = null;
let rawIngestSeq = 0;

// Raw rotation writers (per market, per kind) — raw-only contract
const rawTradeRotationWriters = new Map();
const bookUpdateRotationWriters = new Map();
const liquidationRotationWriters = new Map();
const snapshotWriters = new Map();
const pendingSnapshotWrites = new Set();
const marketRestartPromises = new Map();
const rawDbStorage = () => ['duckdb', 'sqlite'].includes(configOutput.raw_storage);

function getRawWriteState(writer) {
  let state = rawWriteStates.get(writer);
  if (!state) {
    state = { writer, pending: [], drainPromise: null };
    rawWriteStates.set(writer, state);
  }
  return state;
}

async function createRawWriter(basePath, market, kind, options = {}) {
  if (rawDbStorage()) return { storage: configOutput.raw_storage, market, kind };
  if (configOutput.raw_layout === 'v4') {
    const { RawV4Writer } = await import('./raw-v4-writer.mjs');
    return new RawV4Writer({
      root: basePath,
      market,
      kind,
      maxSegmentBytes: configOutput.max_segment_bytes,
    });
  }
  return new RawRotationWriter(basePath, market, kind, options);
}

function drainRawWriteState(state) {
  if (state.drainPromise) return state.drainPromise;

  state.drainPromise = (async () => {
    while (state.pending.length) {
      if (rawWriteFailed) throw new Error('raw writer already failed');
      const batch = state.pending.splice(0, RAW_WRITE_BATCH_SIZE);
      try {
        await state.writer.writeBatch(batch);
      } catch (error) {
        state.pending.unshift(...batch);
        if (!rawQueueFailureReported) {
          rawQueueFailureReported = true;
          parentPort.postMessage({ type: 'rawQueueFailure', workerId, reason: error.message });
        }
        rawWriteFailed = true;
        throw error;
      }
    }
  })().finally(() => {
    state.drainPromise = null;
    if (!rawWriteFailed && state.pending.length) void drainRawWriteState(state).catch(() => {});
  });

  return state.drainPromise;
}

function queueRawWrite(writer, obj, eventTimestampMs) {
  if (!writer) return;
  if (rawWriteFailed) return;
  if (writer.storage === 'duckdb' || writer.storage === 'sqlite') {
    const workerSeq = ++rawIngestSeq;
    const rawMetadata = {
      worker_seq: workerSeq,
      connection_id: obj?.connection_id ?? null,
      sequence_order: obj?.sequence_order ?? workerSeq,
    };
    const envelope = obj?.schema === 'raw_v4' ? obj : {
      schema: writer.storage === 'sqlite' ? 'raw_v6_sqlite' : 'raw_v5_duckdb',
      market: writer.market,
      stream: writer.kind,
      event_ts_ms: Number(obj?.event_ts_ms ?? obj?.ts ?? eventTimestampMs),
      recv_ts_ms: Number(obj?.recv_ts_ms ?? Date.now()),
      writer_session_id: `${process.pid}:${workerId}`,
      ingest_seq: workerSeq,
      ...rawMetadata,
      source_id: obj?.source_id ?? obj?.source?.id ?? obj?.trade_id ?? obj?.tradeId ?? null,
      payload: { ...obj, ...rawMetadata },
    };
    rawIpcPending.push(envelope);
    if (rawIpcPending.length >= RAW_IPC_BATCH_SIZE) flushRawIpc();
    else scheduleRawIpcFlush();
    return;
  }
  const state = getRawWriteState(writer);
  if (state.pending.length >= RAW_WORKER_PENDING_MAX_EVENTS) {
    throw new Error(`raw worker pending queue limit exceeded: ${RAW_WORKER_PENDING_MAX_EVENTS}`);
  }
  const sourceId = obj?.source_id ?? obj?.source?.id ?? obj?.trade_id ?? obj?.tradeId ?? null;
  const workerSeq = ++rawIngestSeq;
  state.pending.push([{
    ...obj,
    worker_seq: obj?.worker_seq ?? workerSeq,
    connection_id: obj?.connection_id ?? null,
    sequence_order: obj?.sequence_order ?? workerSeq,
    ingest_seq: obj?.ingest_seq ?? workerSeq,
    source_id: obj?.source_id ?? sourceId,
  }, eventTimestampMs]);
  if (state.pending.length >= RAW_WRITE_BATCH_SIZE) void drainRawWriteState(state).catch(() => {});
}

function flushRawIpc() {
  if (!rawIpcPending.length) return;
  const envelopes = rawIpcPending.splice(0, rawIpcPending.length);
  parentPort.postMessage({ type: 'rawEvents', envelopes });
}

function scheduleRawIpcFlush() {
  if (rawIpcFlushTimer) return;
  rawIpcFlushTimer = setTimeout(() => {
    rawIpcFlushTimer = null;
    flushRawIpc();
  }, RAW_IPC_FLUSH_MS);
  rawIpcFlushTimer.unref?.();
}

function rawDepthKey(event) {
  if (!event?.connection_id || event.seq_end == null) return null;
  return `${event.connection_id}:${event.seq_start ?? ''}:${event.seq_end}`;
}

function rememberRawDepth(market, event) {
  const key = rawDepthKey(event);
  if (!key) return false;
  let keys = rawDepthKeys.get(market);
  if (!keys) {
    keys = new Set();
    rawDepthKeys.set(market, keys);
  }
  const existed = keys.has(key);
  keys.add(key);
  if (keys.size > RAW_WORKER_PENDING_MAX_EVENTS) {
    keys.delete(keys.values().next().value);
  }
  return existed;
}

async function drainRawWrites() {
  const states = [...rawWriteStates.values()];
  await Promise.all(states.map((state) => drainRawWriteState(state)));
}

// ── Phase 1: prepare market (create connector + writers + wire events) ───

async function prepareMarket(market) {
  const ConnectorClass = CONNECTOR_CLASSES[market];
  if (!ConnectorClass) {
    throw new Error(`unknown market connector: ${market}`);
  }
  const cfg = configMarkets[market];
  if (!cfg) throw new Error(`missing market config: ${market}`);
  const connector = new ConnectorClass(cfg);
  const safeQueueRawWrite = (writer, obj, eventTs) => {
    try { queueRawWrite(writer, obj, eventTs); }
    catch (error) {
      if (!rawQueueFailureReported) {
        rawQueueFailureReported = true;
        parentPort.postMessage({ type: 'rawQueueFailure', workerId, market, reason: error.message });
      }
    }
  };

  const basePath = outputBase;
  rawTradeRotationWriters.set(market, await createRawWriter(basePath, market, 'trades', {
    flushIntervalMs: configOutput.flush_trades_ms ?? 200,
  }));
  bookUpdateRotationWriters.set(market, await createRawWriter(basePath, market, 'book_updates', {
    flushIntervalMs: configOutput.flush_book_ms ?? 1000,
  }));
  liquidationRotationWriters.set(market, await createRawWriter(basePath, market, 'liquidations', {
    flushIntervalMs: configOutput.flush_liquidations_ms ?? 200,
  }));
  snapshotWriters.set(market, rawDbStorage()
    ? await createRawWriter(basePath, market, 'snapshots')
    : await (async () => {
        const { SnapshotWriter } = await import('./snapshot-writer.mjs');
        return new SnapshotWriter(basePath, market);
      })());

  // Wire events — save to writers + IPC to main
  connector.on('trade', (tradeEvent) => {
    safeQueueRawWrite(rawTradeRotationWriters.get(market), tradeEvent, tradeEvent.ts);
  });

  connector.on('depth', (depthEvent) => {
    if (depthEvent.type === 'snapshot') {
      safeQueueRawWrite(bookUpdateRotationWriters.get(market), depthEvent, depthEvent.ts);
      const snapshotWriter = snapshotWriters.get(market);
      if (snapshotWriter?.storage === 'duckdb' || snapshotWriter?.storage === 'sqlite') {
        safeQueueRawWrite(snapshotWriter, depthEvent, depthEvent.ts);
      } else {
        const writePromise = snapshotWriter?.write(depthEvent).catch((error) => {
          console.error(`[worker:${workerId}][${market}] snapshot write error:`, error.message);
        });
        if (writePromise) {
          pendingSnapshotWrites.add(writePromise);
          void writePromise.finally(() => pendingSnapshotWrites.delete(writePromise));
        }
      }
    } else if (depthEvent.type === 'update') {
      // Non-Binance connectors emit only `depth`; persist those updates too.
      // Binance emits a durable rawDepth candidate immediately before the
      // validated depth event, so skip the latter when the same sequence key
      // is already present.
      if (!rememberRawDepth(market, depthEvent)) {
        safeQueueRawWrite(bookUpdateRotationWriters.get(market), depthEvent, depthEvent.ts);
      }
    }
  });

  connector.on('rawDepth', (depthEvent) => {
    if (!rememberRawDepth(market, depthEvent)) {
      safeQueueRawWrite(bookUpdateRotationWriters.get(market), depthEvent, depthEvent.ts);
    }
  });

  connector.on('liquidation', (row) => {
    safeQueueRawWrite(liquidationRotationWriters.get(market), row, row.ts);
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

/** Schedule a slow first retry after an initial connect/sync failure. */
function scheduleInitialReconnect(market, connector, reason) {
  if (!connector) return;

  // A failed sync can leave the old socket open in an error state. Close it
  // before the later retry so the failed market cannot leak a stale socket or
  // race the new snapshot boundary.
  if (connector._ws) {
    try { connector._ws.close(1000, 'initial sync failed'); } catch { /* ignore */ }
    connector._ws = null;
  }
  connector._clearTimers?.();

  if (!connector._initialReconnectTimer) {
    connector._initialReconnectTimer = setTimeout(() => {
      connector._initialReconnectTimer = null;
      connector._scheduleReconnect?.();
    }, INITIAL_RECONNECT_DELAY_MS);
    connector._initialReconnectTimer.unref?.();
  }

  parentPort.postMessage({
    type: 'marketDegraded',
    workerId,
    market,
    reason,
    retryDelayMs: INITIAL_RECONNECT_DELAY_MS,
  });
}

/** Connect a single market. Throws on failure; the caller isolates the market. */
async function connectMarket(market) {
  const connector = connectors.get(market);
  if (!connector) throw new Error(`market connector not prepared: ${market}`);

  // Connect
  try {
    await connector.connect();
    await connector._syncBook();
    if (connector.getState() !== 'running') {
      throw new Error(`${market}: initial sync did not reach running (state=${connector.getState()})`);
    }
    parentPort.postMessage({
      type: 'stateChange',
      market,
      from: 'initializing',
      to: connector.getState(),
      stats: connector.getStats(),
    });
  } catch (err) {
    console.error(`[worker:${workerId}] ${market} initial connect failed:`, err.message);
    throw err;
  }
}

/** Re-seed one connector without disturbing sibling markets in this worker. */
async function restartMarket(market, reason = 'operator request') {
  if (!connectors.has(market)) throw new Error(`market connector not found: ${market}`);
  if (marketRestartPromises.has(market)) return marketRestartPromises.get(market);
  const promise = (async () => {
    const connector = connectors.get(market);
    console.warn(`[worker:${workerId}][${market}] module restart: ${reason}`);
    connector.disconnect();
    connector._isShuttingDown = false;
    try {
      await connector.connect();
      await connector._syncBook();
      parentPort.postMessage({ type: 'marketRestarted', workerId, market });
    } catch (error) {
      parentPort.postMessage({ type: 'marketRestartFailed', workerId, market, reason: error.message });
      connector._scheduleReconnect?.();
      throw error;
    }
  })().finally(() => marketRestartPromises.delete(market));
  marketRestartPromises.set(market, promise);
  return promise;
}

// ── Init handler ─────────────────────────────────────────────────────────

async function doInit(msg) {
  workerId = msg.workerId || 'unknown';
  markets = msg.markets || [];
  optionalMarkets = new Set(msg.optionalMarkets || []);
  configMarkets = msg.configMarkets || {};
  configOutput = msg.configOutput || {};
  outputBase = msg.outputBase;

  console.log(`[worker:${workerId}] starting with markets: ${markets.join(', ')}`);

  // Phase 1: Prepare all markets (create connectors + writers + wire events)
  for (const [index, market] of markets.entries()) {
    if (index > 0) await sleep(STARTUP_STAGGER_MS);
    try {
      await prepareMarket(market);
    } catch (err) {
      console.error(`[worker:${workerId}] ${market} preparation failed:`, err.message);
      parentPort.postMessage({
        type: 'startupFailed',
        workerId,
        market,
        reason: err.message,
      });
      await doShutdown(1);
      return;
    }
  }

  // Phase 2: Startup recovery for raw rotation writers (before connecting)
  const startupNowMs = Date.now();
  for (const [, writer] of rawTradeRotationWriters) {
    if (writer.startupRecovery) await writer.startupRecovery(startupNowMs);
  }
  for (const [, writer] of bookUpdateRotationWriters) {
    if (writer.startupRecovery) await writer.startupRecovery(startupNowMs);
  }
  for (const [, writer] of liquidationRotationWriters) {
    if (writer.startupRecovery) await writer.startupRecovery(startupNowMs);
  }

  // Phase 3: Connect all markets (recovery done, now safe to receive).
  // A network/snapshot failure is market-local.  The worker remains alive so
  // healthy markets continue ingesting and the failed market can recover in
  // the background.  Preparation/writer failures above remain fail-closed.
  for (const market of markets) {
    try {
      await connectMarket(market);
    } catch (err) {
      console.error(`[worker:${workerId}] market ${market} unavailable at startup; background reconnect will continue`);
      scheduleInitialReconnect(market, connectors.get(market), err.message);
    }
  }

  // Start stale check timer (raw 3 writers only, every 15s)
  staleCheckTimer = setInterval(async () => {
    const now = Date.now();
    for (const [, writer] of rawTradeRotationWriters) if (writer.checkStale) await writer.checkStale(now);
    for (const [, writer] of bookUpdateRotationWriters) if (writer.checkStale) await writer.checkStale(now);
    for (const [, writer] of liquidationRotationWriters) if (writer.checkStale) await writer.checkStale(now);
  }, 15000);
  if (staleCheckTimer.unref) staleCheckTimer.unref();

  // RawRotationWriter deliberately disables BufferedWriter's shared
  // auto-flush because rotation/finalization must stay serialized per
  // market/kind. Flush active windows explicitly on the configured cadence.
  const configuredFlushMs = [
    configOutput.flush_trades_ms,
    configOutput.flush_book_ms,
    configOutput.flush_liquidations_ms,
  ].filter(Number.isInteger).filter((value) => value > 0);
  const rawFlushIntervalMs = configuredFlushMs.length > 0
    ? Math.min(...configuredFlushMs)
    : 1000;
  rawFlushTimer = setInterval(async () => {
    const writers = [
      ...rawTradeRotationWriters.values(),
      ...bookUpdateRotationWriters.values(),
      ...liquidationRotationWriters.values(),
    ];
    if (!rawDbStorage()) {
      await drainRawWrites();
      await Promise.allSettled(writers.map((writer) => writer.flush()));
    }
  }, rawFlushIntervalMs);
  if (rawFlushTimer.unref) rawFlushTimer.unref();

  // Start health stats push timer (every 2s)
  healthPushTimer = setInterval(() => {
    for (const [market, connector] of connectors) {
      parentPort.postMessage({
        type: 'stats',
        market,
        payload: connector.getStats(),
      });

      // Report writer I/O status alongside connector stats
      const tradeFail = rawTradeRotationWriters.get(market)?.getIoFailure?.();
      const bookFail = bookUpdateRotationWriters.get(market)?.getIoFailure?.();
      const liqFail = liquidationRotationWriters.get(market)?.getIoFailure?.();
      parentPort.postMessage({
        type: 'writerStatus',
        market,
        payload: {
          count: (tradeFail?.count ?? 0) + (bookFail?.count ?? 0) + (liqFail?.count ?? 0),
          message: tradeFail?.message ?? bookFail?.message ?? liqFail?.message ?? null,
        },
      });
    }
  }, 2000);
  if (healthPushTimer.unref) healthPushTimer.unref();

  // Signal ready
  parentPort.postMessage({ type: 'ready', workerId });
}

// ── Shutdown ─────────────────────────────────────────────────────────────

async function doShutdown(exitCode = 0) {
  console.log(`[worker:${workerId}] shutting down...`);

  // Stop timers
  if (staleCheckTimer) { clearInterval(staleCheckTimer); staleCheckTimer = null; }
  if (healthPushTimer) { clearInterval(healthPushTimer); healthPushTimer = null; }
  if (rawFlushTimer) { clearInterval(rawFlushTimer); rawFlushTimer = null; }
  if (rawIpcFlushTimer) { clearTimeout(rawIpcFlushTimer); rawIpcFlushTimer = null; }

  // Disconnect all connectors
  for (const [, conn] of connectors) {
    conn.disconnect();
  }

  // Drain receiver-side batches before closing the raw writers.
  flushRawIpc();
  if (!rawDbStorage()) await drainRawWrites();
  await Promise.allSettled([...pendingSnapshotWrites]);

  // Keep active windows open across a restart. Finalizing the current window
  // advances its watermark; events emitted immediately after reconnect can
  // still belong to that same 30s window and must remain appendable.
  const promises = [];
  if (!rawDbStorage()) {
    for (const w of rawTradeRotationWriters.values()) promises.push(w.flush());
    for (const w of bookUpdateRotationWriters.values()) promises.push(w.flush());
    for (const w of liquidationRotationWriters.values()) promises.push(w.flush());
  }
  await Promise.allSettled(promises);

  console.log(`[worker:${workerId}] shutdown complete`);
  process.exit(exitCode);
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
      case 'restartMarket':
        await restartMarket(msg.market, msg.reason);
        break;
      default:
        console.error(`[worker:${workerId}] unknown command: ${msg.cmd}`);
    }
  } catch (err) {
    console.error(`[worker:${workerId}] IPC handler error:`, err.message);
  }
});
