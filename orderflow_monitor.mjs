#!/usr/bin/env node
/**
 * orderflow_monitor.mjs — btc-receiver v3.11 multi-worker orchestrator
 *
 * Main thread keeps: HealthMonitor
 * Spawns market-isolated workers with stable B-F groups, routes IPC events.
 * Pure receive + save — no feature computation, no REST auxiliary collection.
 *
 * Usage:
 *   node orderflow_monitor.mjs --help
 *   node orderflow_monitor.mjs --config config.v3.json --seconds 5 --markets binance_spot,binance_perp
 *   node orderflow_monitor.mjs --config config.v3.json --seconds 5 --markets binance_spot,binance_perp --output data/live_v3_smoke
 *   node orderflow_monitor.mjs --config config.v3.json --raw-layout v4 --output data/live_v4
 *   node orderflow_monitor.mjs --config config.v3.json --storage duckdb --database data/agg-btc-receiver.duckdb
 */

import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { Worker } from 'node:worker_threads';
import { HealthMonitor } from './lib/health-monitor.mjs';
import { validateConfig } from './lib/config-validator.mjs';
import { acquireOutputRootLock, releaseOutputRootLock } from './lib/lock.mjs';
import { RawDbWriter, DEFAULT_RAW_RETENTION_DAYS } from './lib/raw-db-writer.mjs';
import { RawSqliteWriter } from './lib/raw-sqlite-writer.mjs';
import { DerivativesHelper } from './lib/derivatives-helper.mjs';
import { getOICapability } from './lib/oi-schema.mjs';

// ====== Market grouping ======

const WORKER_MARKET_GROUPS = {
  A: ['binance_spot'],
  B: ['bybit_perp', 'bybit_spot', 'okx_perp', 'okx_spot'],
  // Keep high-volume Kraken isolated from the other reconnect-sensitive spot
  // feeds.  One overloaded worker must not starve unrelated sockets.
  C: ['coinbase_spot'],
  D: ['crypto_com_spot', 'bitfinex_spot', 'bitmex_perp', 'coinbase_international_perp', 'hyperliquid_perp'],
  E: ['kraken_spot'],
  F: ['bitstamp_spot', 'gemini_spot'],
  G: ['binance_perp'],
  H: ['binance_coinm_perp', 'binance_perp_btcusdc'],
  I: ['binance_spot_usdc'],
};
const KNOWN_MARKETS = new Set(Object.values(WORKER_MARKET_GROUPS).flat());
// Only exchange feeds with an independently verified recovery path may be
// degraded at startup. This is not a user-controlled bypass for required
// markets; the list is reviewed code/config policy.
const OPTIONAL_MARKET_ALLOWLIST = new Set([
  'kraken_spot',
  'binance_spot',
  'binance_spot_usdc',
]);

// ====== Arg parser ======

function help() {
  console.log(`
btc-receiver v3.10 — multi-worker BTC orderbook & trade receiver

Usage:
  node orderflow_monitor.mjs --config <path> [options]

Options:
  --help                          Show this help
  --config <path>                 Config JSON file (required)
  --seconds <N>                   Run for N seconds then exit (0 = run indefinitely)
  --markets <list>                Comma-separated market list (default: from config)
  --output <dir>                  Override output base path
  --raw-layout <v3|v4>           Select raw storage layout (v4 is opt-in)
  --storage <files|duckdb|sqlite> Select raw storage backend
  --database <path>               DuckDB path (default: data/agg-btc-receiver.duckdb)
  --database-dir <dir>            SQLite market DB directory (default: data/sqlite)
  --retention-days <N>            DuckDB raw retention (default: 90)
  --optional-markets <list>       Legacy compatibility option; initial market failures now recover per-market
  --selfTestReconnectAfterMs <N>  Close sockets after N ms for reconnect smoke test
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
  console.error(`[main] Failed to load config from ${configPath}: ${err.message}`);
  process.exit(1);
}

// Structural validation before any config access.
// Fail-closed: any violation prevents worker startup and output creation.
const validation = validateConfig(config);
if (!validation.valid) {
  console.error(`[main] config validation failed:\n${validation.errors.map(e => `  - ${e}`).join('\n')}`);
  process.exit(1);
}

// ====== Acquire output-root lock ======

const rawLayoutArg = arg('raw-layout', '');
const rawStorageArg = arg('storage', '');
const rawStorage = rawStorageArg || config.output.raw_storage || 'files';
const outputDefault = rawStorage === 'duckdb'
  ? 'data/live_db'
  : rawStorage === 'sqlite' ? 'data/live_sqlite'
  : rawLayoutArg === 'v4' ? 'data/live_v4' : config.output.base_path;
const outputBase = arg('output', outputDefault);
const effectiveOutput = {
  ...config.output,
  ...(rawLayoutArg ? { raw_layout: rawLayoutArg } : {}),
  raw_storage: rawStorage,
};
const rawDatabasePath = arg('database', 'data/agg-btc-receiver.duckdb');
const rawDatabaseDir = arg('database-dir', 'data/sqlite');
const rawEnvelopeSchema = rawStorage === 'sqlite' ? 'raw_v6_sqlite' : 'raw_v5_duckdb';
const rawRetentionDays = parseInt(arg('retention-days', String(DEFAULT_RAW_RETENTION_DAYS)), 10);
if (!['files', 'duckdb', 'sqlite'].includes(rawStorage)) {
  console.error(`[main] unsupported storage backend: ${rawStorage}`);
  process.exit(1);
}
if (!Number.isInteger(rawRetentionDays) || rawRetentionDays < 1) {
  console.error(`[main] retention-days must be a positive integer: ${rawRetentionDays}`);
  process.exit(1);
}
const lockResult = await acquireOutputRootLock(outputBase);
if (!lockResult.ok) {
  console.error(`[main] failed to acquire output-root lock: ${lockResult.status}${lockResult.holder ? ` (holder: ${lockResult.holder})` : ''}`);
  process.exit(1);
}
process.on('exit', () => { try { releaseOutputRootLock(outputBase); } catch (_) {} });
const seconds = parseInt(arg('seconds', '0'), 10);
const marketsArg = arg('markets', '');
const enabledMarkets = marketsArg
  ? marketsArg.split(',').map(s => s.trim()).filter(Boolean)
  : Object.keys(config.markets).filter(m => config.markets[m].enabled);
const selfTestReconnectAfterMs = parseInt(arg('selfTestReconnectAfterMs', '0'), 10);
const optionalMarkets = new Set((arg('optional-markets', '') || '')
  .split(',').map(s => s.trim()).filter(Boolean));
const unknownEnabledMarkets = enabledMarkets.filter((market) => !KNOWN_MARKETS.has(market));
const unsupportedOptionalMarkets = [...optionalMarkets].filter((market) => !OPTIONAL_MARKET_ALLOWLIST.has(market));
if (unknownEnabledMarkets.length > 0) {
  console.error(`[main] unknown enabled market(s): ${unknownEnabledMarkets.join(', ')}`);
  process.exit(1);
}
if (unsupportedOptionalMarkets.length > 0) {
  console.error(`[main] optional market is not allowlisted: ${unsupportedOptionalMarkets.join(', ')}`);
  process.exit(1);
}

// ====== Initialize main-thread components ======

const healthMonitor = new HealthMonitor(path.join(outputBase, 'health.jsonl'), {
  intervalMs: 1000,
});
const rawDbWriter = rawStorage === 'duckdb'
  ? await new RawDbWriter({ databasePath: rawDatabasePath, retentionDays: rawRetentionDays }).open()
  : rawStorage === 'sqlite'
    ? await new RawSqliteWriter({ databaseDir: rawDatabaseDir, retentionDays: rawRetentionDays }).open()
  : null;
if (rawDbWriter) await rawDbWriter.pruneExpired();

// ====== Worker management ======

/** @type {Map<string, Worker>} workerId → Worker */
const workers = new Map();
/** @type {Map<string, string[]>} workerId → markets */
const workerMarkets = new Map();
/** @type {Set<string>} workers that have signalled ready */
const readyWorkers = new Set();
/** @type {Set<string>} workers that have finished replay */
const replayDoneWorkers = new Set();

/** Number of workers expected after spawning. Used for fail-closed startup. */
let expectedWorkerCount = 0;
/** Set to true when a worker exits or errors before ready — triggers fail-closed. */
let startupFailed = false;
/** True while the main process is intentionally stopping all workers. */
let plannedShutdown = false;
/** Prevent multiple runtime-failure shutdown paths from racing. */
let runtimeFailureHandled = false;
/** Assigned once the normal shutdown handler has been constructed. */
let shutdownHandler = null;
/** Runtime worker failure that occurred before shutdownHandler was assigned. */
let pendingRuntimeFailure = null;
const MODULE_RESTART_REQUEST = path.join(process.env.XDG_RUNTIME_DIR || `/run/user/${os.userInfo().uid}`, 'agg-btc-receiver-module-restart.json');
const rawDbPending = [];
const RAW_DB_PENDING_MAX_EVENTS = 65536;
let rawIngestSeq = 0;
const RAW_DB_FLUSH_INTERVAL_MS = 10_000;
const RAW_DB_FLUSH_MAX_EVENTS = 16_384;
let rawDbFlushPromise = Promise.resolve();
let rawDbFailure = null;
let rawDbFlushTimer = null;
let rawDbRetentionTimer = null;
const derivativesHelper = rawDbWriter
  ? new DerivativesHelper(outputBase, {
    intervalMs: 30_000,
    onRow: (row) => {
      if (rawDbPending.length >= RAW_DB_PENDING_MAX_EVENTS) return reportRawDbFailure(new Error('raw DB pending queue limit exceeded'));
      rawDbPending.push({
        schema: rawEnvelopeSchema,
        market: row.market,
        stream: 'open_interest',
        event_ts_ms: row.source_ts ?? row.ts,
        recv_ts_ms: row.ts,
        writer_session_id: `main:${process.pid}:oi`,
        ingest_seq: ++rawIngestSeq,
        source_id: row.source ?? row.source_id ?? null,
        payload: row,
      });
      if (rawDbPending.length >= RAW_DB_FLUSH_MAX_EVENTS) void flushRawDbQueue();
    },
  })
  : null;

const STARTUP_STAGGER_MS = 50;
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

function reportRawDbFailure(error) {
  if (rawDbFailure) return;
  rawDbFailure = error;
  const reason = `raw DB write failed: ${error.message}`;
  console.error(`[main] ${reason}`);
  startupFailed = true;
  pendingRuntimeFailure = reason;
  if (shutdownHandler) void shutdownHandler(1, reason);
}

function flushRawDbQueue() {
  if (!rawDbWriter || rawDbFailure) return rawDbFlushPromise;
  rawDbFlushPromise = rawDbFlushPromise.then(async () => {
    while (rawDbPending.length) {
      const batch = rawDbPending.splice(0, RAW_DB_FLUSH_MAX_EVENTS);
      try {
        await rawDbWriter.append(batch);
      } catch (error) {
        rawDbPending.unshift(...batch);
        throw error;
      }
    }
  }).catch((error) => {
    reportRawDbFailure(error);
  });
  return rawDbFlushPromise;
}

function enqueueRawEnvelopes(envelopes) {
  if (!rawDbWriter) return;
  for (const envelope of envelopes ?? []) {
    if (rawDbPending.length >= RAW_DB_PENDING_MAX_EVENTS) {
      reportRawDbFailure(new Error('raw DB pending queue limit exceeded'));
      return;
    }
    envelope.ingest_seq = ++rawIngestSeq;
    rawDbPending.push(envelope);
  }
  if (rawDbPending.length >= RAW_DB_FLUSH_MAX_EVENTS) void flushRawDbQueue();
}

async function closeRawDb() {
  if (!rawDbWriter) return;
  if (derivativesHelper) await derivativesHelper.close();
  if (rawDbFlushTimer) clearInterval(rawDbFlushTimer);
  if (rawDbRetentionTimer) clearInterval(rawDbRetentionTimer);
  rawDbFlushTimer = null;
  rawDbRetentionTimer = null;
  await flushRawDbQueue();
  await rawDbWriter.close();
}

function handleUnexpectedWorkerFailure(workerId, reason) {
  if (plannedShutdown || runtimeFailureHandled) return;

  if (!readyWorkers.has(workerId)) {
    startupFailed = true;
    return;
  }

  runtimeFailureHandled = true;
  // Also break the startup readiness wait when this happens before the
  // normal shutdown handler has been installed.
  startupFailed = true;
  pendingRuntimeFailure = `worker ${workerId} failed after ready: ${reason}`;
  console.error(`[main] ${pendingRuntimeFailure}`);
  if (shutdownHandler) {
    void shutdownHandler(1, pendingRuntimeFailure);
  }
}

function createWorker(workerId, groupMarkets) {
  const filtered = groupMarkets.filter(m => enabledMarkets.includes(m));
  if (filtered.length === 0) {
    console.log(`[main] worker ${workerId}: no enabled markets in group, skipping`);
    return null;
  }

  console.log(`[main] spawning worker ${workerId} with markets: ${filtered.join(', ')}`);

  const worker = new Worker(
    new URL('./lib/orderflow-worker.mjs', import.meta.url)
  );

  workers.set(workerId, worker);
  workerMarkets.set(workerId, filtered);

  // ── IPC: worker → main ────────────────────────────────────────────

  worker.on('message', (msg) => {
    switch (msg.type) {
      case 'rawEvent':
        enqueueRawEnvelopes([msg.envelope]);
        break;

      case 'rawEvents':
        enqueueRawEnvelopes(msg.envelopes);
        break;

      case 'rawQueueFailure':
        reportRawDbFailure(new Error(`worker raw queue failure: ${msg.reason}`));
        break;

      case 'liquidation':
        // Log liquidation events
        break;

      case 'stateChange':
        console.log(`[${msg.market}] state: ${msg.from} → ${msg.to}`);
        if (msg.stats) {
          healthMonitor.updateConnector(msg.market, msg.stats);
        }
        break;

      case 'marketDegraded':
        console.error(
          `[main] market ${msg.market} degraded in worker ${msg.workerId}: ${msg.reason}; ` +
          `initial retry in ${Math.round((msg.retryDelayMs ?? 0) / 1000)}s`,
        );
        break;

      case 'stats':
        healthMonitor.updateConnector(msg.market, msg.payload);
        break;

      case 'replayDone':
        replayDoneWorkers.add(msg.workerId);
        console.log(`[main] worker ${msg.workerId} replay done`);
        break;

      case 'ready':
        readyWorkers.add(msg.workerId);
        console.log(`[main] worker ${msg.workerId} ready`);
        break;

      case 'startupFailed':
        console.error(`[main] worker ${msg.workerId} startup failed for market ${msg.market}: ${msg.reason}`);
        startupFailed = true;
        break;

      case 'marketRestarted':
        console.log(`[main] module restart complete: ${msg.market} (worker ${msg.workerId})`);
        break;
      case 'marketRestartFailed':
        console.error(`[main] module restart failed: ${msg.market}: ${msg.reason}`);
        break;

      case 'writerStatus':
        healthMonitor.updateWriterHealth(msg.market, msg.payload);
        if (msg.payload.count > 0) {
          console.error(
            `[main] writer I/O failure detected for ${msg.market}: ${msg.payload.count} error(s), last: ${msg.payload.message}`,
          );
        }
        break;

      default:
        // ignore unknown types
        break;
    }
  });

  worker.on('error', (err) => {
    console.error(`[main] worker ${workerId} error:`, err.message);
    handleUnexpectedWorkerFailure(workerId, `error: ${err.message}`);
  });

  worker.on('exit', (code) => {
    console.log(`[main] worker ${workerId} exited with code ${code}`);
    handleUnexpectedWorkerFailure(workerId, `exit code ${code}`);
    readyWorkers.delete(workerId);
    workers.delete(workerId);
  });

  // Send init to worker
  worker.postMessage({
    cmd: 'init',
    workerId,
    markets: filtered,
    configMarkets: config.markets,
    configOutput: effectiveOutput,
    configTick: config.tick || {},
    outputBase,
    optionalMarkets: [...optionalMarkets],
  });

  return worker;
}

// ====== Main setup ======

async function main() {
  console.log(`[main] btc-receiver v3.10 multi-worker starting`);
  console.log(`[main] enabled markets: ${enabledMarkets.join(', ')}`);
  console.log(`[main] output base: ${outputBase}`);

  // Spawn workers with stagger
  const groupEntries = Object.entries(WORKER_MARKET_GROUPS);
  for (const [index, [workerId, groupMarkets]] of groupEntries.entries()) {
    if (index > 0) await sleep(STARTUP_STAGGER_MS);
    createWorker(workerId, groupMarkets);
  }

  if (workers.size === 0) {
    console.error('[main] no workers spawned — no enabled markets match any group');
    process.exit(1);
  }

  expectedWorkerCount = workers.size;

  // Wait for all workers to be ready (with timeout, fail-closed)
  const readyTimeout = 60000;
  const readyStart = Date.now();
  while (readyWorkers.size < expectedWorkerCount && !startupFailed) {
    if (Date.now() - readyStart > readyTimeout) {
      console.error(`[main] timeout waiting for workers to be ready (${readyWorkers.size}/${expectedWorkerCount})`);
      startupFailed = true;
      break;
    }
    await sleep(100);
  }

  if (startupFailed) {
    console.error('[main] startup failed — shutting down all workers');
    plannedShutdown = true;
    // Send shutdown to all surviving workers and wait for their finalizers.
    // A fixed sleep can terminate the main process while BufferedWriter or
    // RawRotationWriter still has data queued.
    const shutdownPromises = [];
    for (const [, worker] of workers) {
      shutdownPromises.push(new Promise((resolve) => {
        const timer = setTimeout(resolve, 10000);
        worker.once('exit', () => {
          clearTimeout(timer);
          resolve();
        });
        try { worker.postMessage({ cmd: 'shutdown' }); } catch (_) {
          clearTimeout(timer);
          resolve();
        }
      }));
    }
    await Promise.allSettled(shutdownPromises);
    await closeRawDb();
    await healthMonitor.close();
    process.exit(1);
  }

  console.log(`[main] ${readyWorkers.size}/${expectedWorkerCount} workers ready`);

  // Graceful shutdown
  let shuttingDown = false;
  const shutdown = async (exitCode = 0, reason = '') => {
    if (shuttingDown) return;
    shuttingDown = true;
    plannedShutdown = true;
    if (reason) console.error(`[main] ${reason}`);
    console.log('[main] shutting down...');

    // Send shutdown to all workers and wait for them
    const workerExitPromises = [];
    for (const [workerId, worker] of workers) {
      workerExitPromises.push(new Promise((resolve) => {
        worker.once('exit', resolve);
        setTimeout(resolve, 10000); // 10s timeout
      }));
      try {
        worker.postMessage({ cmd: 'shutdown' });
      } catch (_) { /* worker may have exited */ }
    }
    await Promise.allSettled(workerExitPromises);

    // Flush main-thread components
    const promises = [];
    promises.push(closeRawDb());
    promises.push(healthMonitor.close());
    await Promise.allSettled(promises);

    console.log(`[main] shutdown complete (exit ${exitCode})`);
    process.exit(exitCode);
  };

  shutdownHandler = shutdown;

  // A worker can fail in the small interval between the ready loop and the
  // handler assignment above. Handle that failure before declaring the
  // receiver healthy.
  if (pendingRuntimeFailure) {
    await shutdown(1, pendingRuntimeFailure);
    return;
  }

  // Start auxiliary services
  healthMonitor.start();
  if (rawDbWriter) {
    for (const market of enabledMarkets) {
      if (getOICapability(market)) {
        derivativesHelper.registerMarket(market, config.markets[market]?.derivatives ?? {});
      }
    }
    derivativesHelper.start();
    rawDbFlushTimer = setInterval(() => { void flushRawDbQueue(); }, RAW_DB_FLUSH_INTERVAL_MS);
    rawDbRetentionTimer = setInterval(() => {
      void flushRawDbQueue().then(() => rawDbWriter.pruneExpired()).catch(reportRawDbFailure);
    }, 6 * 60 * 60 * 1000);
    rawDbFlushTimer.unref?.();
    rawDbRetentionTimer.unref?.();
  }

  // Self-test reconnect trigger
  if (selfTestReconnectAfterMs > 0) {
    setTimeout(() => {
      console.log('[main] self-test: sending reconnect command to all workers');
      for (const [, worker] of workers) {
        worker.postMessage({ cmd: 'selfTestReconnect' });
      }
    }, selfTestReconnectAfterMs);
  }

  process.on('SIGUSR2', () => {
    try {
      const request = JSON.parse(fs.readFileSync(MODULE_RESTART_REQUEST, 'utf8'));
      fs.unlinkSync(MODULE_RESTART_REQUEST);
      const entry = [...workerMarkets.entries()].find(([, markets]) => markets.includes(request.market));
      if (!entry) throw new Error(`market is not assigned to a live worker: ${request.market}`);
      const [workerId] = entry;
      const worker = workers.get(workerId);
      if (!worker) throw new Error(`worker is not live: ${workerId}`);
      console.warn(`[main] module restart requested: ${request.market} (worker ${workerId})`);
      worker.postMessage({ cmd: 'restartMarket', market: request.market, reason: request.reason || 'watchdog' });
    } catch (error) {
      console.error(`[main] module restart request failed: ${error.message}`);
    }
  });
  process.on('SIGTERM', () => shutdown(0, 'SIGTERM'));
  process.on('SIGINT', () => shutdown(0, 'SIGINT'));

  // Duration limit
  if (seconds > 0) {
    setTimeout(shutdown, seconds * 1000);
  }
}

main().catch(err => {
  console.error('[main] fatal error:', err);
  process.exit(1);
});
