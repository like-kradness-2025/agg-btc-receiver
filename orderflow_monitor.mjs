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
import { StallProbe, createStallLog } from './lib/stall-probe.mjs';
import {
  MarketStatusTracker,
  defaultStatusFilePath,
  formatMarketStatusV1,
  writeMarketStatusFile,
} from './lib/market-status.mjs';
import { validateConfig } from './lib/config-validator.mjs';
import { acquireOutputRootLock, releaseOutputRootLock } from './lib/lock.mjs';
import { RawDbWriter, DEFAULT_RAW_RETENTION_DAYS } from './lib/raw-db-writer.mjs';
import {
  RawSqliteWriter,
  DEFAULT_SLOW_APPEND_MS,
} from './lib/raw-sqlite-writer.mjs';
import { DerivativesHelper } from './lib/derivatives-helper.mjs';
import { getOICapability, openInterestEventTimestamp } from './lib/oi-schema.mjs';
import {
  accumulatePostDrainCanonicalDrops,
  admitPendingEnvelopes,
  buildRawDbDropReport,
  drainPendingQueueWithBoundedRetry,
  ingestSeqRange,
  resolveCanonicalRawEnabled,
  resolveRawDbPendingMaxEvents,
  resolveRawDbPendingOverflowMode,
  writeRawDbDropReport,
} from './lib/raw-db-pending.mjs';

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
  I: ['binance_spot_usdc', 'binance_spot_fdusd'],
};
const KNOWN_MARKETS = new Set(Object.values(WORKER_MARKET_GROUPS).flat());
// Only exchange feeds with an independently verified recovery path may be
// degraded at startup. This is not a user-controlled bypass for required
// markets; the list is reviewed code/config policy.
const OPTIONAL_MARKET_ALLOWLIST = new Set([
  'kraken_spot',
  'binance_spot',
  'binance_spot_usdc',
  'binance_spot_fdusd',
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
  --status-file <path>            Market-status.json path for downstream (default: <database-dir>/../market-status.json)
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
/** R-13: durable report for raw-DB events the shutdown drain could not write. */
const RAW_DB_DROP_REPORT_PATH = path.join(outputBase, 'raw-db-drop-report.json');
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

// ====== Issue #16 + Issue #9 (Done条件#8): market data-completeness ======
// MarketStatusTracker is the SINGLE source of truth for per-market state. It
// separates "worker process ready" from "every required market streams with a
// synced book" (expected = all enabled markets; optional markets never block
// data_complete) and feeds BOTH projections below:
//   1. health.jsonl  — refreshHealthCompleteness() pushes the snapshot into
//      the HealthMonitor (additive expected/running/degraded_markets,
//      data_complete, change-only completeness_transitions), and
//   2. market-status.json — publishMarketStatusFile() formats the SAME
//      snapshot into the downstream receiver-market-status/v1 contract and
//      atomically writes it (state changes + 2s periodic flush).
// There is deliberately NO second tracker: every IPC event applies a tracker
// method and then re-derives both outputs from the snapshot.
const marketStatus = new MarketStatusTracker({
  expectedMarkets: enabledMarkets,
  optionalMarkets: [...optionalMarkets],
});

/** Push the latest aggregate view into the HealthMonitor's next report. */
function refreshHealthCompleteness() {
  const snap = marketStatus.snapshot();
  healthMonitor.setCompleteness({
    expected_markets: snap.expected_markets,
    running_markets: snap.running_markets,
    degraded_markets: snap.degraded_markets,
    data_complete: snap.data_complete,
    transitions: snap.transitions,
  });
  return snap;
}

/** Last completeness value actually logged — log only on transitions. */
let lastLoggedDataComplete = null;

/** Log a completeness flip with the offending markets; returns dataComplete. */
function logCompletenessChange(snap, reason = '') {
  if (lastLoggedDataComplete === snap.data_complete) return snap.data_complete;
  lastLoggedDataComplete = snap.data_complete;
  if (snap.data_complete) {
    console.log(
      `[main] data_complete=true (${snap.running_markets.length}/${snap.expected_markets.length} markets running)${reason ? ` (${reason})` : ''}`,
    );
  } else {
    const degraded = Object.entries(snap.degraded_markets)
      .map(([m, r]) => `${m}:${r}`).join('; ');
    console.error(
      `[main] data_complete=false — degraded/missing markets must not be presented as complete: ${degraded || '(none reported running)'}`,
    );
  }
  return snap.data_complete;
}

// ====== Initialize main-thread components ======

// ====== Stall observer (2026-09-21) ======
// 目的: プロセスが数秒〜20 秒ブロックする事象 (health.jsonl に最大 19.3 秒の穴、
// 各市場が同時に "no message for ~30s" で再接続) の「どこで止まっているか」を
// 記録する。**挙動は変えない** (タイムアウトや書き込み経路には触らない)。
// 記録は異常時のみ: 通常運転ではファイルを 1 バイトも書かない。
const stallLog = createStallLog({
  filePath: path.join(outputBase, 'stall-events.jsonl'),
  fsModule: fs,
});
function resolveSlowAppendMs() {
  const raw = process.env.RECEIVER_SLOW_APPEND_MS;
  if (raw === undefined || raw === '') return DEFAULT_SLOW_APPEND_MS;
  const value = Number(raw);
  return Number.isFinite(value) ? value : DEFAULT_SLOW_APPEND_MS;
}

const stallProbe = new StallProbe({
  label: 'main',
  // 既定 1500ms。検証や調整のために env で上書きできる (RECEIVER_STALL_LAG_MS)。
  lagThresholdMs: Number(process.env.RECEIVER_STALL_LAG_MS) || 1500,
  sampleMs: 250,
  // 遅い append の内訳は 1 回で数十 span になるため、リングを広めに取る。
  ringSize: 2000,
  onAnomaly: (record) => {
    stallLog.write(record);
    if (record.kind === 'slow_append') {
      const top = [...record.spans].sort((a, b) => b.dur_ms - a.dur_ms).slice(0, 4)
        .map((s) => `${s.name}:${Math.round(s.dur_ms)}ms`).join(',');
      console.error(
        `[stall] slow_append #${record.append} total=${Math.round(record.total_ms)}ms `
        + `events=${record.events} queue=${Math.round(record.queue_wait_ms)}ms top=${top}`,
      );
      return;
    }
    const detail = record.kind === 'stall'
      ? `max_lag=${record.max_lag_ms}ms spans=${record.spans.map((s) => `${s.name}:${s.dur_ms}ms`).join(',') || 'none'}`
      : `duration=${record.duration_ms}ms`;
    console.error(`[stall] ${record.kind} label=${record.label} ${detail}`);
  },
});
stallProbe.start();

const healthMonitor = new HealthMonitor(path.join(outputBase, 'health.jsonl'), {
  intervalMs: 1000,
});
const rawDbWriter = rawStorage === 'duckdb'
  ? await new RawDbWriter({ databasePath: rawDatabasePath, retentionDays: rawRetentionDays }).open()
  : rawStorage === 'sqlite'
    ? await new RawSqliteWriter({
      databaseDir: rawDatabaseDir,
      retentionDays: rawRetentionDays,
      // 遅い append の内訳を観測ログ (stall-events.jsonl) へ出す。
      observer: stallProbe,
      // 閾値の既定は 1000ms。検証・調整用に env で上書きできる。
      slowAppendMs: resolveSlowAppendMs(),
    }).open()
  : null;
if (rawDbWriter) await stallProbe.wrap('raw.pruneExpired', () => rawDbWriter.pruneExpired());

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
const RAW_DB_PENDING_MAX_EVENTS = resolveRawDbPendingMaxEvents();
/**
 * Operator switch for the immutable full-history copy (`canonical_frames`).
 * Turned off with RECEIVER_CANONICAL_RAW=0: the canonical layer is TTL-exempt
 * by design and grows without bound, and no downstream consumer reads it
 * (agg-btc-downstream parses raw_batches). raw_batches keeps every event.
 */
const CANONICAL_RAW_ENABLED = resolveCanonicalRawEnabled();
/**
 * R14: how envelopes rejected at the pending-queue cap are accounted for.
 * `count` (default) counts every rejected envelope; `legacy` reproduces the
 * pre-fix silent drop and exists only for A/B measurement and rollback.
 */
const RAW_DB_PENDING_OVERFLOW_MODE = resolveRawDbPendingOverflowMode();
/**
 * R14: envelopes rejected at the cap. Pre-fix these were dropped with no counter
 * anywhere — reportRawDbFailure latched on the first one and every later
 * rejection short-circuited on that latch — so an overflow looked exactly like a
 * genuine no-trade interval downstream. Counted per queue, surfaced at shutdown
 * in health.jsonl + the drop report.
 */
const rawDbPendingOverflow = { raw: 0, canonical: 0, open_interest: 0, first_ts_ms: null };
let rawIngestSeq = 0;
const RAW_DB_FLUSH_INTERVAL_MS = 10_000;
const RAW_DB_FLUSH_MAX_EVENTS = 16_384;
let rawDbFlushPromise = Promise.resolve();
let rawDbFailure = null;
let rawDbFlushTimer = null;
let rawDbRetentionTimer = null;
// Canonical raw frames (issues #10/#11) get their own pending queue so the
// append-only writer is fed independently of the legacy mutable batches.
const canonicalDbPending = [];
let canonicalDbFlushPromise = Promise.resolve();
const CANONICAL_DB_FLUSH_MAX_EVENTS = 16_384;
/**
 * O-02: canonical frames that arrived AFTER the shutdown drain had run.
 *
 * Until the drain there is still a recovery path — drainRawDbPendingOnShutdown()
 * retries the canonical queue regardless of the latch — so a frame received
 * after the latch belongs in the queue (and is then either written or counted by
 * the R-13/R14 accounting). Once the drain is done no further attempt exists, so
 * those frames are counted explicitly instead of vanishing the way the pre-fix
 * guard let them.
 * @type {{frames: number, first_ts_ms: number|null, last_ts_ms: number|null}}
 */
const canonicalPostDrainDrops = { frames: 0, first_ts_ms: null, last_ts_ms: null };
/** Set once drainRawDbPendingOnShutdown() has run: no drain remains after it. */
let rawDbDrainComplete = false;
const derivativesHelper = rawDbWriter
  ? new DerivativesHelper(outputBase, {
    intervalMs: 30_000,
    onRow: (row) => {
      const result = admitPendingEnvelopes({
        pending: rawDbPending,
        envelopes: [{
          schema: rawEnvelopeSchema,
          market: row.market,
          stream: 'open_interest',
          event_ts_ms: openInterestEventTimestamp(row),
          recv_ts_ms: row.ts,
          writer_session_id: `main:${process.pid}:oi`,
          source_id: row.source ?? row.source_id ?? null,
          payload: row,
        }],
        maxEvents: RAW_DB_PENDING_MAX_EVENTS,
        mode: RAW_DB_PENDING_OVERFLOW_MODE,
        prepare: (envelope) => { envelope.ingest_seq = ++rawIngestSeq; },
      });
      recordPendingQueueOverflow('open_interest', result);
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

/**
 * R14: account for a pending-queue admission result and keep the fail-closed
 * latch the pre-fix code had. Every rejected envelope is counted (the label says
 * which queue), the first overflow of the process timestamps itself, and the
 * failure is reported once — later rejections only add to the count.
 *
 * @param {'raw'|'canonical'|'open_interest'} queue
 * @param {{admitted: number, rejected: number, overflowed: boolean}} result
 */
function recordPendingQueueOverflow(queue, result) {
  if (!result?.overflowed) return;
  if (result.rejected > 0) {
    rawDbPendingOverflow[queue] += result.rejected;
    if (rawDbPendingOverflow.first_ts_ms === null) rawDbPendingOverflow.first_ts_ms = Date.now();
  }
  const label = queue === 'canonical' ? 'canonical raw' : queue === 'open_interest' ? 'open-interest raw' : 'raw DB';
  // Keep the pre-fix message prefix so existing log parsers still match, and
  // append the counted loss (0 in legacy mode, where nothing is counted).
  const counted = result.rejected > 0
    ? `: ${result.rejected} envelope(s) counted as dropped at the ${RAW_DB_PENDING_MAX_EVENTS}-event cap`
    : ` (cap=${RAW_DB_PENDING_MAX_EVENTS}, mode=${result.mode})`;
  reportRawDbFailure(new Error(`${label} pending queue limit exceeded${counted}`));
}

/**
 * R14: totals for the queues whose envelopes were rejected at the cap, or null
 * when nothing overflowed. `dropped_events` is the exact counted loss.
 */
function pendingQueueOverflowSummary() {
  const total = rawDbPendingOverflow.raw + rawDbPendingOverflow.canonical + rawDbPendingOverflow.open_interest;
  if (total <= 0) return null;
  return {
    dropped_events: total,
    cap_events: RAW_DB_PENDING_MAX_EVENTS,
    mode: RAW_DB_PENDING_OVERFLOW_MODE,
    raw: rawDbPendingOverflow.raw,
    canonical: rawDbPendingOverflow.canonical,
    open_interest: rawDbPendingOverflow.open_interest,
    first_ts_ms: rawDbPendingOverflow.first_ts_ms,
  };
}

function flushRawDbQueue() {
  if (!rawDbWriter || rawDbFailure) return rawDbFlushPromise;
  rawDbFlushPromise = rawDbFlushPromise.then(async () => {
    while (rawDbPending.length) {
      const batch = rawDbPending.splice(0, RAW_DB_FLUSH_MAX_EVENTS);
      try {
        await stallProbe.wrap('raw.append', () => rawDbWriter.append(batch), { events: batch.length });
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

// Canonical raw flush (issues #10/#11): append-only sink. Batches are
// re-queued and the failure is reported fail-closed when the append rejects.
function flushCanonicalDbQueue() {
  if (!rawDbWriter || rawDbFailure) return canonicalDbFlushPromise;
  canonicalDbFlushPromise = canonicalDbFlushPromise.then(async () => {
    while (canonicalDbPending.length) {
      const batch = canonicalDbPending.splice(0, CANONICAL_DB_FLUSH_MAX_EVENTS);
      try {
        await stallProbe.wrap('canonical.append', () => rawDbWriter.appendCanonical(batch), { frames: batch.length });
      } catch (error) {
        canonicalDbPending.unshift(...batch);
        throw error;
      }
    }
  }).catch((error) => {
    reportRawDbFailure(error);
  });
  return canonicalDbFlushPromise;
}

function enqueueCanonicalFrames(envelopes) {
  if (!rawDbWriter) return;
  if (rawDbWriter.appendCanonical === undefined) return; // duckdb path: no canonical table
  // Operator switch (RECEIVER_CANONICAL_RAW=0): the canonical layer is an
  // immutable, TTL-exempt full-history copy that nothing downstream reads.
  // Skipping admission here leaves the canonical queue and its R13/R14/O-02
  // counters untouched (they account for the queue, not for the archive), so
  // the loss surfaces keep reporting the truth for the bytes we do persist.
  if (!CANONICAL_RAW_ENABLED) return;
  // O-02: the `rawDbFailure` latch must NOT short-circuit this path. Pre-fix the
  // guard read `if (!rawDbWriter || rawDbFailure) return;`, so a frame that
  // arrived after the latch was discarded BEFORE the queue admission and before
  // any counter — invisible in health.jsonl and in the drop report, exactly like
  // a genuine quiet interval. Admitting it instead keeps the frame recoverable:
  // the R-13 shutdown drain retries the canonical queue regardless of the latch,
  // so the frame is either written or counted by the existing R-13/R14 fields
  // (cap rejections via recordPendingQueueOverflow, drain failures via
  // `queues.canonical.dropped_events`). Only a frame that arrives after the drain
  // has run has no recovery left and is counted here.
  if (rawDbDrainComplete) {
    const frames = (envelopes ?? []).length;
    accumulatePostDrainCanonicalDrops(canonicalPostDrainDrops, frames);
    healthMonitor.noteRawDbPostDrainDroppedEvents(canonicalPostDrainDrops.frames, canonicalPostDrainDrops);
    console.error(
      `[main] canonical raw frame(s) arrived after the shutdown drain: ${frames} DROPPED `
      + `(total=${canonicalPostDrainDrops.frames}): no drain remains to write them`,
    );
    return;
  }
  const result = admitPendingEnvelopes({
    pending: canonicalDbPending,
    envelopes,
    maxEvents: RAW_DB_PENDING_MAX_EVENTS,
    mode: RAW_DB_PENDING_OVERFLOW_MODE,
  });
  recordPendingQueueOverflow('canonical', result);
  if (canonicalDbPending.length >= CANONICAL_DB_FLUSH_MAX_EVENTS) void flushCanonicalDbQueue();
}

function enqueueRawEnvelopes(envelopes) {
  if (!rawDbWriter) return;
  const result = admitPendingEnvelopes({
    pending: rawDbPending,
    envelopes,
    maxEvents: RAW_DB_PENDING_MAX_EVENTS,
    mode: RAW_DB_PENDING_OVERFLOW_MODE,
    prepare: (envelope) => { envelope.ingest_seq = ++rawIngestSeq; },
  });
  recordPendingQueueOverflow('raw', result);
  if (rawDbPending.length >= RAW_DB_FLUSH_MAX_EVENTS) void flushRawDbQueue();
}

/**
 * R-13: shutdown drain of the raw-DB pending queues.
 *
 * flushRawDbQueue()/flushCanonicalDbQueue() both short-circuit once
 * `rawDbFailure` is latched, so closeRawDb() previously reached
 * rawDbWriter.close() with up to RAW_DB_PENDING_MAX_EVENTS (65,536) events per
 * queue still pending — dropped at process exit with no marker, which is
 * indistinguishable downstream from a genuine no-trade interval.
 *
 * This runs a bounded retry (the latch does not block a shutdown retry: the
 * original failure may have been transient) and, when events still cannot be
 * written, counts them explicitly in a durable drop report plus the final
 * health row.
 *
 * R14: the same report/row also carry the envelopes the pending queues rejected
 * at their cap during the run (see recordPendingQueueOverflow), because those
 * never reached a queue and would otherwise be lost without a trace.
 *
 * @returns {Promise<Object|null>} the drop report, or null when nothing was lost
 */
async function drainRawDbPendingOnShutdown() {
  if (!rawDbWriter) return null;

  const rawRange = ingestSeqRange(rawDbPending);
  const rawResult = await drainPendingQueueWithBoundedRetry({
    pending: rawDbPending,
    append: (batch) => stallProbe.wrap('raw.append.shutdown', () => rawDbWriter.append(batch), { events: batch.length }),
    maxBatch: RAW_DB_FLUSH_MAX_EVENTS,
    onAttemptError: (error, attempt) => console.error(
      `[main] raw DB shutdown drain attempt ${attempt} failed: ${error.message} (${rawDbPending.length} event(s) still pending)`,
    ),
  });

  let canonicalResult = { flushed: 0, remaining: 0, attempts: 0 };
  if (canonicalDbPending.length > 0 && rawDbWriter.appendCanonical !== undefined) {
    canonicalResult = await drainPendingQueueWithBoundedRetry({
      pending: canonicalDbPending,
      append: (batch) => stallProbe.wrap('canonical.append.shutdown', () => rawDbWriter.appendCanonical(batch), { frames: batch.length }),
      maxBatch: CANONICAL_DB_FLUSH_MAX_EVENTS,
      onAttemptError: (error, attempt) => console.error(
        `[main] canonical raw shutdown drain attempt ${attempt} failed: ${error.message} (${canonicalDbPending.length} event(s) still pending)`,
      ),
    });
  }

  const remaining = rawResult.remaining + canonicalResult.remaining;
  const overflow = pendingQueueOverflowSummary();
  if (remaining === 0 && !overflow) return null;

  const report = buildRawDbDropReport({
    queues: {
      raw: {
        ...rawResult,
        firstIngestSeq: rawRange.first,
        lastIngestSeq: rawRange.last,
      },
      canonical: canonicalResult,
    },
    reason: rawDbFailure ? rawDbFailure.message : null,
    overflow,
  });

  // Explicit counted drops, each durable in the same report file:
  //   - R-13 `remaining`: queued events the bounded drain could not write;
  //   - R14 `overflow`:  envelopes the pending queue rejected at the cap.
  if (remaining > 0) {
    healthMonitor.noteRawDbDroppedEvents(remaining, report);
    console.error(
      `[main] raw DB shutdown drain failed after bounded retries: ${remaining} event(s) ` +
      `DROPPED (raw=${rawResult.remaining}, canonical=${canonicalResult.remaining}); ` +
      `reason=${report.reason ?? 'unknown'}; report=${RAW_DB_DROP_REPORT_PATH}`,
    );
  }
  if (overflow) {
    healthMonitor.noteRawDbPendingOverflow(overflow.dropped_events, overflow);
    console.error(
      `[main] raw DB pending queue overflow: ${overflow.dropped_events} envelope(s) ` +
      `DROPPED at the ${overflow.cap_events}-event cap ` +
      `(raw=${overflow.raw}, canonical=${overflow.canonical}, open_interest=${overflow.open_interest}); ` +
      `report=${RAW_DB_DROP_REPORT_PATH}`,
    );
  }
  try {
    await writeRawDbDropReport(RAW_DB_DROP_REPORT_PATH, report);
  } catch (error) {
    console.error(`[main] raw DB drop report write failed: ${error.message}`);
  }
  return report;
}

async function closeRawDb() {
  if (!rawDbWriter) return;
  if (derivativesHelper) await derivativesHelper.close();
  if (rawDbFlushTimer) clearInterval(rawDbFlushTimer);
  if (rawDbRetentionTimer) clearInterval(rawDbRetentionTimer);
  rawDbFlushTimer = null;
  rawDbRetentionTimer = null;
  await flushRawDbQueue();
  await flushCanonicalDbQueue();
  // R-13: bounded retry + explicit counted drop for whatever the latched
  // failure blocked above. Must run before rawDbWriter.close().
  await drainRawDbPendingOnShutdown();
  // O-02: the drain above is the last recovery attempt, so from this point a
  // canonical frame that still arrives must be counted (no drain can reach it).
  rawDbDrainComplete = true;
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

      case 'canonicalFrames':
        enqueueCanonicalFrames(msg.envelopes);
        break;

      case 'rawQueueFailure':
        reportRawDbFailure(new Error(`worker raw queue failure: ${msg.reason}`));
        break;

      // Stall observer: ワーカースレッド側の停止を main が集約して 1 ファイルに書く
      // (worker ごとにファイルを持たせない = 記録は異常時のみ、単一ライター)。
      case 'stallAnomaly': {
        const record = msg.record;
        if (record && typeof record === 'object') {
          stallLog.write(record);
          const detail = record.kind === 'stall'
            ? `max_lag=${record.max_lag_ms}ms spans=${(record.spans ?? []).map((sp) => `${sp.name}:${sp.dur_ms}ms`).join(',') || 'none'}`
            : `duration=${record.duration_ms}ms`;
          console.error(`[stall] ${record.kind} label=${record.label} ${detail}`);
        }
        break;
      }

      case 'liquidation':
        // Log liquidation events
        break;

      case 'stateChange':
        console.log(`[${msg.market}] state: ${msg.from} → ${msg.to}`);
        if (msg.stats) {
          healthMonitor.updateConnector(msg.market, msg.stats);
        }
        marketStatus.observeState(msg.market, msg.to);
        refreshHealthCompleteness();
        publishMarketStatusFile();
        break;

      case 'marketStatus': {
        // Issue #16: per-market state pushed by the worker on every
        // transition (including degraded→running recovery). `ready` no longer
        // implies data completeness — this message keeps the aggregate view
        // current between the 2s stats ticks.
        const prev = marketStatus.snapshot().data_complete;
        if (msg.degradedReason) {
          marketStatus.markDegraded(msg.market, msg.degradedReason);
        } else if (msg.state) {
          const result = marketStatus.observeState(msg.market, msg.state);
          if (result.recovered && result.dataComplete && !prev) {
            console.log(`[main] market ${msg.market} recovered → running (worker ${msg.workerId})`);
          }
        }
        const snap = refreshHealthCompleteness();
        if (snap.data_complete !== prev) logCompletenessChange(snap, `worker ${msg.workerId} market ${msg.market}`);
        publishMarketStatusFile();
        break;
      }

      case 'marketDegraded':
        console.error(
          `[main] market ${msg.market} degraded in worker ${msg.workerId}: ${msg.reason}; ` +
          `initial retry in ${Math.round((msg.retryDelayMs ?? 0) / 1000)}s`,
        );
        // Issue #16: an isolated market is not running — data_complete=false.
        marketStatus.markDegraded(msg.market, msg.reason);
        logCompletenessChange(refreshHealthCompleteness(), `market ${msg.market} degraded`);
        publishMarketStatusFile();
        break;

      case 'stats':
        healthMonitor.updateConnector(msg.market, msg.payload);
        // Periodic stats reflect the connector's current state so a mid-run
        // reconnect/error downgrades data_complete without waiting for a
        // stateChange frame — but a stats tick must never CLEAR an active
        // degradation (observeStatsState keeps the degraded flag until an
        // explicit stateChange/marketRestarted recovery event arrives).
        if (msg.payload?.state) marketStatus.observeStatsState(msg.market, msg.payload.state);
        refreshHealthCompleteness();
        publishMarketStatusFile();
        break;

      case 'replayDone':
        replayDoneWorkers.add(msg.workerId);
        console.log(`[main] worker ${msg.workerId} replay done`);
        break;

      case 'ready':
        readyWorkers.add(msg.workerId);
        // Issue #16: the ready report carries per-market state + the worker's
        // own data_complete. Worker ready ≠ data complete; log both and seed
        // the aggregate tracker so health output separates the concepts.
        marketStatus.applyReady(msg.workerId, msg);
        logCompletenessChange(refreshHealthCompleteness(), `worker ${msg.workerId} ready`);
        publishMarketStatusFile();
        console.log(
          `[main] worker ${msg.workerId} ready ` +
          `(processReady=true, data_complete=${msg.dataComplete === true})`,
        );
        break;

      case 'startupFailed':
        console.error(`[main] worker ${msg.workerId} startup failed for market ${msg.market}: ${msg.reason}`);
        startupFailed = true;
        break;

      case 'marketRestarted':
        console.log(`[main] module restart complete: ${msg.market} (worker ${msg.workerId})`);
        marketStatus.observeState(msg.market, 'running');
        logCompletenessChange(refreshHealthCompleteness(), `market ${msg.market} restarted`);
        publishMarketStatusFile();
        break;
      case 'marketRestartFailed':
        console.error(`[main] module restart failed: ${msg.market}: ${msg.reason}`);
        // PR #20 re-audit: the watchdog only restarts a market whose connector
        // still reports state='running' with stale data, so after a FAILED
        // restart the 2s stats tick would keep re-asserting a stale
        // 'running' and data_complete=true forever. A failed restart leaves
        // the market isolated from its feed — degrade it exactly like the
        // marketDegraded path (fail-visible: data_complete=false until an
        // explicit recovery event arrives).
        marketStatus.markDegraded(msg.market, `module restart failed: ${msg.reason}`);
        logCompletenessChange(refreshHealthCompleteness(), `market ${msg.market} restart failed`);
        publishMarketStatusFile();
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
    // Qwen P1-2: the crashed worker's markets stop streaming — degrade them
    // through the tracker so data_complete=false and market-status.json
    // reflects the isolation before the fail-closed shutdown below.
    degradeWorkerMarkets(workerId, `error: ${err.message}`);
    handleUnexpectedWorkerFailure(workerId, `error: ${err.message}`);
  });

  worker.on('exit', (code) => {
    console.log(`[main] worker ${workerId} exited with code ${code}`);
    degradeWorkerMarkets(workerId, `exit code ${code}`);
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

// ====== Issue #9 (Done条件#8): receiver-market-status/v1 file emission ======
// The downstream status file is derived from the SAME MarketStatusTracker
// snapshot as health.jsonl (see the tracking section above). Contract:
//   path:   --status-file <path> | defaultStatusFilePath(rawDatabaseDir)
//           (default: sibling of the receiver SQLite dir, i.e.
//            dirname(--database-dir)/market-status.json)
//   schema: receiver-market-status/v1 (docs/current/data-contract.md)
// Downstream (agg-btc-downstream src/receiver-completeness.mjs) polls this
// file; a missing/unparsable/stale (>15s) file must never be read as
// "complete" — fail-visible.
const marketStatusFilePath = path.resolve(arg('status-file', defaultStatusFilePath(rawDatabaseDir)));
/** Periodic flush cadence while running (state changes write immediately). */
const STATUS_REFRESH_INTERVAL_MS = 2000;
/** Debounce: identical documents are not re-written more often than this. */
const STATUS_MIN_WRITE_INTERVAL_MS = 1000;
let statusFileTimer = null;
let statusWriteChain = Promise.resolve();
let lastStatusJson = null;
let lastStatusWriteAtMs = 0;

/** All workers ready and no startup failure — the receiver process is up. */
function isProcessReady() {
  return expectedWorkerCount > 0 && readyWorkers.size >= expectedWorkerCount && !startupFailed;
}

/** Current tracker snapshot formatted as the downstream contract document. */
function currentStatusDocument() {
  return formatMarketStatusV1(marketStatus.snapshot(), { processReady: isProcessReady() });
}

/**
 * Persist market-status.json atomically (tmp+rename). Called on every
 * state-changing IPC event and by a 2s periodic timer. The document carries a
 * fresh ts_ms (Date.now() re-stamped on every snapshot), so consecutive
 * publishes always differ and the file stays fresh — the freshness contract
 * for the downstream staleness window (15s) depends on this. The interval arm
 * below only guards the (practically unreachable) identical-document case as
 * a disk-safety backstop. Returns the document that was (or would be)
 * written.
 */
function publishMarketStatusFile(force = false) {
  const doc = currentStatusDocument();
  const json = JSON.stringify(doc);
  const changed = json !== lastStatusJson;
  lastStatusJson = json;
  const now = Date.now();
  if (force || changed || now - lastStatusWriteAtMs >= STATUS_MIN_WRITE_INTERVAL_MS) {
    lastStatusWriteAtMs = now;
    statusWriteChain = statusWriteChain
      .catch(() => {})
      .then(() => writeMarketStatusFile(marketStatusFilePath, doc))
      .catch((error) => console.error(`[main] market-status file write failed: ${error.message}`));
  }
  return doc;
}

/**
 * Qwen P1-2: a worker crash (exit/error) means every market that worker owned
 * is no longer streaming. Degrade them all through the tracker so
 * data_complete flips false and the status file shows the isolation
 * immediately; the main process then fail-closes via the existing runtime
 * failure policy and the shutdown flush persists this last view. Graceful
 * shutdowns (planned) never degrade — downstream detects those by staleness.
 */
function degradeWorkerMarkets(workerId, reason) {
  if (plannedShutdown || !readyWorkers.has(workerId)) return;
  const assigned = workerMarkets.get(workerId);
  if (!assigned || assigned.length === 0) return;
  for (const market of assigned) {
    marketStatus.markDegraded(market, `worker ${workerId} lost: ${reason}`);
  }
  const snap = refreshHealthCompleteness();
  logCompletenessChange(snap, `worker ${workerId} crashed`);
  publishMarketStatusFile(true);
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
  {
    // Issue #16: worker readiness and market data completeness are separate.
    // A fully ready receiver can still be data_complete=false (e.g. an
    // isolated degraded market) — publish the aggregate once at startup.
    const snap = refreshHealthCompleteness();
    logCompletenessChange(snap);
    console.log(
      `[main] readiness summary: process_ready=true data_complete=${snap.data_complete} ` +
      `expected=${snap.expected_markets.length} running=${snap.running_markets.length} ` +
      `degraded=${Object.keys(snap.degraded_markets).length}`,
    );
    // Issue #9 (Done条件#8): publish the initial market-status.json and keep
    // it fresh — every state change writes immediately, and this 2s timer is
    // the periodic flush (unchanged documents are debounced to ≥1s).
    const doc = publishMarketStatusFile(true);
    console.log(
      `[main] market-status summary: process_ready=${doc.process_ready} data_complete=${doc.data_complete} ` +
      `expected=${doc.expected_markets.length} running=${doc.running_markets.length} ` +
      `degraded=${Object.keys(doc.degraded_markets).length}`,
    );
    statusFileTimer = setInterval(() => publishMarketStatusFile(), STATUS_REFRESH_INTERVAL_MS);
    if (statusFileTimer.unref) statusFileTimer.unref();
  }

  // Graceful shutdown
  let shuttingDown = false;
  const shutdown = async (exitCode = 0, reason = '') => {
    if (shuttingDown) return;
    shuttingDown = true;
    plannedShutdown = true;
    if (reason) console.error(`[main] ${reason}`);
    console.log('[main] shutting down...');
    if (statusFileTimer) {
      clearInterval(statusFileTimer);
      statusFileTimer = null;
    }

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

    // Flush main-thread components. closeRawDb() is awaited FIRST: its R-13
    // shutdown drain can note a counted raw-DB drop that HealthMonitor.close()
    // then persists in its final health.jsonl row (parallel closes would race
    // that final write). Each step keeps its own failure isolated.
    try {
      await closeRawDb();
    } catch (error) {
      console.error(`[main] raw DB close failed: ${error.message}`);
    }
    const promises = [];
    promises.push(healthMonitor.close());
    // Last market-status.json write (e.g. the P1-2 crash degradation view) is
    // flushed before exit so the file never outlives the process claiming a
    // completeness the receiver no longer guarantees.
    promises.push(statusWriteChain.catch(() => {}));
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
    const stallGaugeTimer = setInterval(() => {
      stallProbe.note('rawDbPending', rawDbPending.length);
      stallProbe.note('canonicalDbPending', canonicalDbPending.length);
    }, 1000);
    stallGaugeTimer.unref?.();
    rawDbFlushTimer = setInterval(() => {
      void flushRawDbQueue();
      void flushCanonicalDbQueue();
    }, RAW_DB_FLUSH_INTERVAL_MS);
    rawDbRetentionTimer = setInterval(() => {
      void Promise.all([flushRawDbQueue(), flushCanonicalDbQueue()])
        .then(() => stallProbe.wrap('raw.pruneExpired', () => rawDbWriter.pruneExpired()))
        .catch(reportRawDbFailure);
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
