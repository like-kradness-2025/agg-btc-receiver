#!/usr/bin/env node
/**
 * orderflow_monitor.mjs — btc-receiver v3.11 multi-worker orchestrator
 *
 * Main thread keeps: HealthMonitor
 * Spawns 4 worker threads with market groups, routes IPC events.
 * Pure receive + save — no feature computation, no REST auxiliary collection.
 *
 * Usage:
 *   node orderflow_monitor.mjs --help
 *   node orderflow_monitor.mjs --config config.v3.json --seconds 5 --markets binance_spot,binance_perp
 *   node orderflow_monitor.mjs --config config.v3.json --seconds 5 --markets binance_spot,binance_perp --output data/live_v3_smoke
 */

import fs from 'node:fs';
import path from 'node:path';
import { Worker } from 'node:worker_threads';
import { HealthMonitor } from './lib/health-monitor.mjs';
import { validateConfig } from './lib/config-validator.mjs';
import { acquireOutputRootLock, releaseOutputRootLock } from './lib/lock.mjs';

// ====== Market grouping (4 workers) ======

const WORKER_MARKET_GROUPS = {
  A: ['binance_spot', 'binance_perp', 'binance_coinm_perp', 'binance_perp_btcusdc', 'binance_spot_usdc'],
  B: ['bybit_perp', 'bybit_spot', 'okx_perp', 'okx_spot'],
  C: ['coinbase_spot', 'kraken_spot', 'bitstamp_spot', 'gemini_spot'],
  D: ['crypto_com_spot', 'bitfinex_spot', 'bitmex_perp', 'coinbase_international_perp', 'hyperliquid_perp'],
};

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

const outputBase = arg('output', config.output.base_path);
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

// ====== Initialize main-thread components ======

const healthMonitor = new HealthMonitor(path.join(outputBase, 'health.jsonl'), {
  intervalMs: 1000,
});

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

const STARTUP_STAGGER_MS = 50;
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

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
      case 'liquidation':
        // Log liquidation events
        break;

      case 'stateChange':
        console.log(`[${msg.market}] state: ${msg.from} → ${msg.to}`);
        if (msg.stats) {
          healthMonitor.updateConnector(msg.market, msg.stats);
        }
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
    // Worker error before ready is fatal
    if (!readyWorkers.has(workerId)) {
      startupFailed = true;
    }
  });

  worker.on('exit', (code) => {
    console.log(`[main] worker ${workerId} exited with code ${code}`);
    // Worker exit before ready is fatal
    if (!readyWorkers.has(workerId)) {
      startupFailed = true;
    }
    workers.delete(workerId);
  });

  // Send init to worker
  worker.postMessage({
    cmd: 'init',
    workerId,
    markets: filtered,
    configMarkets: config.markets,
    configOutput: config.output,
    configTick: config.tick || {},
    outputBase,
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
    // Send shutdown to all workers
    for (const [, worker] of workers) {
      try { worker.postMessage({ cmd: 'shutdown' }); } catch (_) {}
    }
    // Give workers a moment to flush, then exit
    await sleep(2000);
    process.exit(1);
  }

  console.log(`[main] ${readyWorkers.size}/${expectedWorkerCount} workers ready`);

  // Start auxiliary services
  healthMonitor.start();

  // Self-test reconnect trigger
  if (selfTestReconnectAfterMs > 0) {
    setTimeout(() => {
      console.log('[main] self-test: sending reconnect command to all workers');
      for (const [, worker] of workers) {
        worker.postMessage({ cmd: 'selfTestReconnect' });
      }
    }, selfTestReconnectAfterMs);
  }

  // Graceful shutdown
  let shuttingDown = false;
  const shutdown = async () => {
    if (shuttingDown) return;
    shuttingDown = true;
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
    promises.push(healthMonitor.close());
    await Promise.allSettled(promises);

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
