#!/usr/bin/env node
// scripts/burst-agg.mjs — btc-burst-aggregator entry point
//
// Reads raw trade + book data, detects bursts, produces 30s summary + 1s features.
// Output: data/burst_agg/.staging/<run_id>/ (summary + features + run-report.json)

import fs from 'node:fs';
import path from 'node:path';
import { BurstBuilder } from '../lib/burst-builder.mjs';
import { replayBestBookState } from '../lib/replay-book-state.mjs';

// ── Constants ──────────────────────────────────────────────────────────

const SCHEMA_VERSION = 'burst_agg.v1';
const DEFAULT_GAP_THRESHOLD_MS = 100;
const DEFAULT_MAX_BURST_DURATION_MS = 3000;
const DEFAULT_BOOK_RANGE_USD = 10_000;
const SEC_MS = 1000;
const WIN_MS = 30 * SEC_MS;
const DATA_DIR_DEFAULT = 'data/live_v3';
const OUT_DIR_DEFAULT = 'data/burst_agg';

// ── Argument parsing ───────────────────────────────────────────────────

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = {
    data: DATA_DIR_DEFAULT,
    out: OUT_DIR_DEFAULT,
    markets: null,    // null = auto-detect
    from: null,       // ISO string
    to: null,         // ISO string
    bookRangeUsd: DEFAULT_BOOK_RANGE_USD,
    runId: null,      // null = auto-generate
    gapThresholdMs: DEFAULT_GAP_THRESHOLD_MS,
    maxBurstDurationMs: DEFAULT_MAX_BURST_DURATION_MS,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    const next = args[i + 1];
    switch (arg) {
      case '--data':       opts.data = next; i++; break;
      case '--out':        opts.out = next; i++; break;
      case '--markets':    opts.markets = next; i++; break;
      case '--from':       opts.from = next; i++; break;
      case '--to':         opts.to = next; i++; break;
      case '--book-range-usd': opts.bookRangeUsd = parseFloat(next); i++; break;
      case '--run-id':     opts.runId = next; i++; break;
      case '--gap-threshold-ms': opts.gapThresholdMs = parseInt(next, 10); i++; break;
      case '--max-burst-duration-ms': opts.maxBurstDurationMs = parseInt(next, 10); i++; break;
      case '--delete-processed': opts.deleteProcessed = true; break;
      case '--help':
        console.log(`
Usage: node scripts/burst-agg.mjs [options]

Options:
  --data <path>           Raw data directory (default: ${DATA_DIR_DEFAULT})
  --out <path>            Output directory (default: ${OUT_DIR_DEFAULT})
  --markets <list>        Comma-separated markets (default: auto-detect from ${DATA_DIR_DEFAULT}/trades/)
  --from <ISO>            Start time (ISO 8601, required)
  --to <ISO>              End time (ISO 8601, required)
  --book-range-usd <N>    Book depth range in USD (default: ${DEFAULT_BOOK_RANGE_USD})
  --run-id <str>          Run identifier (default: auto-generated)
  --gap-threshold-ms <N>  Burst gap threshold ms (default: ${DEFAULT_GAP_THRESHOLD_MS})
  --max-burst-duration-ms <N>  Max burst duration ms (default: ${DEFAULT_MAX_BURST_DURATION_MS})
  --delete-processed       Delete processed raw files after successful aggregation
`);
        process.exit(0);
      default:
        if (arg.startsWith('--')) {
          console.error(`Unknown option: ${arg}`);
          process.exit(1);
        }
    }
  }

  if (!opts.from || !opts.to) {
    console.error('--from and --to are required');
    process.exit(1);
  }

  return opts;
}

// ── Helpers ────────────────────────────────────────────────────────────

function isoToMs(iso) {
  const ms = new Date(iso).getTime();
  if (isNaN(ms)) throw new Error(`Invalid ISO timestamp: ${iso}`);
  return ms;
}

function msToIso(ms) {
  return new Date(ms).toISOString();
}

function assert30sBoundary(ms, label) {
  if (ms % WIN_MS !== 0) {
    throw new Error(`${label} must be on a 30-second boundary (got ${msToIso(ms)}, remainder ${ms % WIN_MS}ms)`);
  }
}

/** Generate all 30s window start times in [startMs, endMs) */
function* generateWindows(startMs, endMs) {
  for (let ws = startMs; ws < endMs; ws += WIN_MS) {
    yield ws;
  }
}

/** Generate all 1s bucket start times in [startMs, endMs) */
function* generateSeconds(startMs, endMs) {
  for (let s = startMs; s < endMs; s += SEC_MS) {
    yield s;
  }
}

function msToDate(ms) {
  return new Date(ms).toISOString().slice(0, 10);
}

function msToHhMmSs(ms) {
  const d = new Date(ms);
  return [
    String(d.getUTCHours()).padStart(2, '0'),
    String(d.getUTCMinutes()).padStart(2, '0'),
    String(d.getUTCSeconds()).padStart(2, '0'),
  ].join('-');
}

/** List all 30s-file paths within a date range for a given subdirectory */
function listDataFiles(dataDir, subDir, market, fromMs, toMs) {
  const files = [];
  const fromDate = msToDate(fromMs);
  const toDate = msToDate(toMs);

  // Walk date directories
  const marketDir = path.join(dataDir, subDir, market);
  if (!fs.existsSync(marketDir)) return files;

  const dateDirs = fs.readdirSync(marketDir).filter(d => {
    // Match YYYY-MM-DD format and check range
    if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return false;
    return d >= fromDate && d <= toDate;
  });

  for (const dateDir of dateDirs) {
    const datePath = path.join(marketDir, dateDir);
    const timeFiles = fs.readdirSync(datePath).filter(f => f.endsWith('.jsonl'));

    for (const tf of timeFiles) {
      const base = tf.replace('.jsonl', '');
      // filename is HH-MM-SS — this is the 30s window start
      const [h, m, s] = base.split('-').map(Number);
      const fileMs = Date.UTC(
        parseInt(dateDir.slice(0, 4), 10),
        parseInt(dateDir.slice(5, 7), 10) - 1,
        parseInt(dateDir.slice(8, 10), 10),
        h, m, s
      );
      // Include if file window [fileMs, fileMs+30s) overlaps scan range
      if (fileMs < toMs && fileMs + WIN_MS > fromMs) {
        files.push({ ms: fileMs, fullPath: path.join(datePath, tf) });
      }
    }
  }

  files.sort((a, b) => a.ms - b.ms);
  return files;
}

// ── Data reading ───────────────────────────────────────────────────────

/** Read all trades within scan range for a market */
function readTrades(dataDir, market, fromMs, toMs) {
  const tradeFiles = listDataFiles(dataDir, 'trades', market, fromMs, toMs);
  const trades = [];
  let idx = 0;

  for (const { fullPath } of tradeFiles) {
    try {
      const content = fs.readFileSync(fullPath, 'utf8');
      for (const line of content.trim().split('\n')) {
        if (!line) continue;
        try {
          const t = JSON.parse(line);
          trades.push({
            _idx: idx++,
            ts: t.ts,
            side: t.side,
            price: t.price,
            qty: t.qty,
            market: t.market,
            tradeId: t.tradeId,
          });
        } catch (_) { /* skip malformed lines */ }
      }
    } catch (_) { /* file missing or unreadable — skip */ }
  }

  // Sort by ts for correct BurstBuilder monotonic input.
  // Keep stable original order for equal-ts trades (no price secondary sort).
  trades.sort((a, b) => a.ts - b.ts);
  return trades;
}

/** Read all book events within scan range for a market */
function readBookEvents(dataDir, market, fromMs, toMs) {
  const bookFiles = listDataFiles(dataDir, 'book_updates', market, fromMs, toMs);
  /** @type {import('../lib/replay-book-state.mjs').BookEvent[]} */
  const events = [];

  for (const { fullPath } of bookFiles) {
    try {
      const content = fs.readFileSync(fullPath, 'utf8');
      let lineNo = 0;
      for (const line of content.trim().split('\n')) {
        lineNo++;
        if (!line) continue;
        try {
          const raw = JSON.parse(line);

          const subtype = raw.type === 'snapshot' ? 'book_update_snapshot' : 'book_update_update';

          events.push({
            effective_ts_ms: raw.ts,
            subtype,
            file_path: fullPath,
            line_no: lineNo,
            data: {
              type: raw.type,       // 'snapshot' or 'update'
              bids: raw.bids || [],  // [[price, qty], ...]
              asks: raw.asks || [],  // [[price, qty], ...]
            },
          });
        } catch (_) { /* skip malformed */ }
      }
    } catch (_) { /* skip unreadable */ }
  }

  // Sort by effective_ts_ms; for same-ts, snapshots before updates
  events.sort((a, b) => {
    if (a.effective_ts_ms !== b.effective_ts_ms) return a.effective_ts_ms - b.effective_ts_ms;
    const subtypePriority = { snapshot_file: 0, book_update_snapshot: 0, book_update_update: 1 };
    const pa = subtypePriority[a.subtype] ?? 2;
    const pb = subtypePriority[b.subtype] ?? 2;
    if (pa !== pb) return pa - pb;
    // Compare file_path first, then line_no numerically (not string compare)
    const fpCmp = a.file_path.localeCompare(b.file_path);
    if (fpCmp !== 0) return fpCmp;
    return a.line_no - b.line_no;
  });

  return events;
}

/** Auto-detect markets from trades directory */
function detectMarkets(dataDir) {
  const tradesDir = path.join(dataDir, 'trades');
  if (!fs.existsSync(tradesDir)) return [];
  return fs.readdirSync(tradesDir, { withFileTypes: true })
    .filter(d => d.isDirectory())
    .map(d => d.name);
}

// ── Burst helpers ──────────────────────────────────────────────────────

/** Check if a burst is multilevel (spans multiple price levels) */
function isMultilevel(burst) {
  return burst.distinct_price_count >= 2;
}

/** Compute notional of a print: price * qty, signed by side (buy=正, sell=負) */
function signedNotional(print) {
  const notional = print.price * print.qty;
  return print.side === 'buy' ? notional : -notional;
}

// ── Compute summary for one 30s window ──────────────────────────────────

function sanitizeBookState(b) {
  if (!b || b.bestBid === null || b.bestAsk === null) {
    return { bestBid: null, bestAsk: null };
  }
  if (!(b.bestBid < b.bestAsk)) {
    return { bestBid: null, bestAsk: null };
  }
  return b;
}

function computeSummary({
  ws, we, market, trades, bursts, bookAtTime, bookRangeUsd,
}) {
  // OHLCV from trades in [ws, we)
  let open = null, high = -Infinity, low = Infinity, close = null;
  let volume = 0, tradeCount = 0;
  let buyNotional = 0, sellNotional = 0;

  for (const t of trades) {
    if (t.ts < ws || t.ts >= we) continue;
    tradeCount++;
    const notional = t.price * t.qty;
    volume += t.qty;

    if (open === null) open = t.price;
    close = t.price;
    if (t.price > high) high = t.price;
    if (t.price < low) low = t.price;

    if (t.side === 'buy') buyNotional += notional;
    else sellNotional += notional;
  }

  if (open === null) {
    // No trades — use 0 or null
    open = null; high = null; low = null; close = null;
  }

  // Burst assignment: bursts whose start_ts is in [ws, we)
  const windowBursts = bursts.filter(b => b.burst_start_ts >= ws && b.burst_start_ts < we);
  let burstCount = windowBursts.length;
  let burstDeltaNotional = 0;
  const burstPrintSizes = [];
  const nonBurstPrintSizes = []; // trades not in any burst
  let multilevelCount = 0;
  let maxBurstNotional = 0;
  let maxSpanTicks = 0;

  // Build a set of _idx values for burst membership check.
  // Use ALL bursts (not just windowBursts) to correctly exclude prints
  // from bursts that started in a previous window but extend into this one.
  const burstPrintIdx = new Set();
  for (const b of bursts) {
    for (const p of b.prints) {
      if (p._idx !== undefined) burstPrintIdx.add(p._idx);
    }
  }

  for (const b of windowBursts) {
    const delta = b.burst_notional * (b.side === 'buy' ? 1 : -1);
    burstDeltaNotional += delta;

    if (b.burst_notional > maxBurstNotional) maxBurstNotional = b.burst_notional;
    if (b.span_ticks > maxSpanTicks) maxSpanTicks = b.span_ticks;
    if (isMultilevel(b)) multilevelCount++;

    for (const p of b.prints) {
      burstPrintSizes.push(signedNotional(p));
    }
  }

  // Non-burst trades: trades in [ws, we) not in any burst (cross-window safe)
  for (const t of trades) {
    if (t.ts < ws || t.ts >= we) continue;
    if (!burstPrintIdx.has(t._idx)) {
      nonBurstPrintSizes.push(signedNotional(t));
    }
  }

  // Book stats
  const bookOpenRaw = bookAtTime(ws);
  const bookCloseRaw = bookAtTime(we);
  const bookOpen = sanitizeBookState(bookOpenRaw);
  const bookClose = sanitizeBookState(bookCloseRaw);
  let spreadBpsSum = 0;
  let spreadCount = 0;

  // 30-point sampling: midpoints of each second within the window.
  // Only count usable book states. Crossed/locked books are treated as unavailable.
  for (let s = 0; s < 30; s++) {
    const sampleTs = ws + s * SEC_MS + SEC_MS / 2;
    const b = sanitizeBookState(bookAtTime(sampleTs));
    if (b.bestBid !== null && b.bestAsk !== null) {
      const spreadBps = (b.bestAsk - b.bestBid) / b.bestBid * 10000;
      spreadBpsSum += spreadBps;
      spreadCount++;
    }
  }

  return {
    ts: ws,
    market,
    schema_version: SCHEMA_VERSION,
    open,
    high,
    low,
    close,
    volume,
    trade_count: tradeCount,
    buy_notional: buyNotional,
    sell_notional: sellNotional,
    delta_notional: buyNotional - sellNotional,
    burst_count: burstCount,
    burst_delta_notional: burstDeltaNotional,
    burst_print_sizes: burstPrintSizes,
    non_burst_print_sizes: nonBurstPrintSizes,
    multilevel_burst_count: multilevelCount,
    max_burst_notional: maxBurstNotional,
    max_span_ticks: maxSpanTicks,
    best_bid_open: bookOpen?.bestBid ?? null,
    best_ask_open: bookOpen?.bestAsk ?? null,
    best_bid_close: bookClose?.bestBid ?? null,
    best_ask_close: bookClose?.bestAsk ?? null,
    spread_bps_avg: spreadCount > 0 ? spreadBpsSum / spreadCount : null,
    book_available_count: spreadCount,
  };
}

// ── Compute features for one 1s bucket ─────────────────────────────────

function computeFeatures({
  ts,         // 1s bucket start
  trades,     // all trades in scan range
  bursts,     // all closed bursts
}) {
  const bucketStart = ts;
  const bucketEnd = ts + SEC_MS;

  // Trades in this 1s bucket
  const bucketTrades = trades.filter(t => t.ts >= bucketStart && t.ts < bucketEnd);
  const tradeCount = bucketTrades.length;
  let volume = 0;
  let deltaNotional = 0;
  for (const t of bucketTrades) {
    volume += t.qty;
    const n = t.price * t.qty;
    deltaNotional += t.side === 'buy' ? n : -n;
  }

  // Overlap-based bursts
  const overlappingBursts = bursts.filter(b =>
    b.burst_start_ts < bucketEnd && b.burst_end_ts >= bucketStart,
  );

  let burstDeltaNotional1s = 0;
  let burstCount1s = overlappingBursts.length;
  let buyBurstNotional1s = 0;
  let sellBurstNotional1s = 0;
  let multilevelCount1s = 0;

  for (const b of overlappingBursts) {
    burstDeltaNotional1s += b.burst_notional * (b.side === 'buy' ? 1 : -1);
    if (b.side === 'buy') buyBurstNotional1s += b.burst_notional;
    else sellBurstNotional1s += b.burst_notional;
    if (isMultilevel(b)) multilevelCount1s++;
  }

  // Bucket-local print sizes (prints whose ts falls in this bucket)
  const burstPrintSizes = [];
  const nonBurstPrintSizes = [];

  // Build a set of _idx values from all burst prints for lossless membership
  const allBurstPrintIdx = new Set();
  for (const b of bursts) {
    for (const p of b.prints) {
      if (p._idx !== undefined) allBurstPrintIdx.add(p._idx);
    }
  }

  for (const t of bucketTrades) {
    if (allBurstPrintIdx.has(t._idx)) {
      burstPrintSizes.push(signedNotional(t));
    } else {
      nonBurstPrintSizes.push(signedNotional(t));
    }
  }

  // Max same-side run prints: longest consecutive same-side run in this bucket
  let maxSameSideRunPrints = 0;
  if (bucketTrades.length > 0) {
    let runLen = 1;
    for (let i = 1; i < bucketTrades.length; i++) {
      if (bucketTrades[i].side === bucketTrades[i - 1].side) {
        runLen++;
      } else {
        if (runLen > maxSameSideRunPrints) maxSameSideRunPrints = runLen;
        runLen = 1;
      }
    }
    if (runLen > maxSameSideRunPrints) maxSameSideRunPrints = runLen;
  }

  return {
    ts,
    delta_notional: deltaNotional,
    burst_delta_notional_1s: burstDeltaNotional1s,
    non_burst_delta_notional_1s: deltaNotional - burstDeltaNotional1s, // approximate
    burst_print_sizes: burstPrintSizes,
    non_burst_print_sizes: nonBurstPrintSizes,
    burst_count_1s: burstCount1s,
    buy_burst_notional_1s: buyBurstNotional1s,
    sell_burst_notional_1s: sellBurstNotional1s,
    multilevel_burst_count_1s: multilevelCount1s,
    max_same_side_run_prints_1s: maxSameSideRunPrints,
    trade_count: tradeCount,
    volume,
  };
}

// ── Output helpers ─────────────────────────────────────────────────────

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function writeJsonl(filePath, row) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.appendFileSync(filePath, JSON.stringify(row) + '\n', 'utf8');
}

/**
 * Append a JSONL row to a file only if its ts is not already present.
 * On first call for a given file, reads existing rows into a Set.
 */
function appendJsonlIfNew(filePath, row, tsMap, market, ts) {
  // Initialize dedup set for this market if not yet loaded
  if (!tsMap.has(market)) {
    const tsSet = new Set();
    if (fs.existsSync(filePath)) {
      const content = fs.readFileSync(filePath, 'utf8');
      for (const line of content.trim().split('\n')) {
        if (line) {
          try {
            const existing = JSON.parse(line);
            tsSet.add(existing.ts);
          } catch { /* skip malformed */ }
        }
      }
    }
    tsMap.set(market, tsSet);
  }

  const tsSet = tsMap.get(market);
  if (tsSet.has(ts)) return; // skip duplicate

  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.appendFileSync(filePath, JSON.stringify(row) + '\n', 'utf8');
  tsSet.add(ts);
}

// ── Main ───────────────────────────────────────────────────────────────

async function main() {
  const opts = parseArgs();

  // Parse and validate from/to
  const fromMs = isoToMs(opts.from);
  const toMs = isoToMs(opts.to);
  assert30sBoundary(fromMs, '--from');
  assert30sBoundary(toMs, '--to');
  if (fromMs >= toMs) throw new Error('--from must be before --to');

  // Compute scan range with lookback/lookahead
  const lookback = opts.gapThresholdMs + opts.maxBurstDurationMs;
  const lookahead = Math.max(opts.gapThresholdMs, opts.maxBurstDurationMs);
  const scanFromMs = fromMs - lookback;
  const scanToMs = toMs + lookahead;

  // Markets
  const markets = opts.markets
    ? opts.markets.split(',').map(s => s.trim()).filter(Boolean)
    : detectMarkets(opts.data);

  if (markets.length === 0) {
    console.error('No markets found. Use --markets to specify.');
    process.exit(1);
  }

  // Run ID
  const runId = opts.runId || `run-${Date.now()}`;
  const stagingDir = path.join(opts.out, '.staging', runId);

  const summaryDir = path.join(opts.out, 'summary');
  const featuresDir = path.join(opts.out, 'features');
  fs.mkdirSync(summaryDir, { recursive: true });
  fs.mkdirSync(featuresDir, { recursive: true });

  console.log(`Run ID: ${runId}`);
  console.log(`Markets: ${markets.join(', ')}`);
  console.log(`Range: ${msToIso(fromMs)} → ${msToIso(toMs)}`);
  console.log(`Scan:  ${msToIso(scanFromMs)} → ${msToIso(scanToMs)}`);
  console.log(`Gap threshold: ${opts.gapThresholdMs}ms, Max duration: ${opts.maxBurstDurationMs}ms`);

  const runReport = {
    run_id: runId,
    from: msToIso(fromMs),
    to: msToIso(toMs),
    scan_from: msToIso(scanFromMs),
    scan_to: msToIso(scanToMs),
    gap_threshold_ms: opts.gapThresholdMs,
    max_burst_duration_ms: opts.maxBurstDurationMs,
    schema_version: SCHEMA_VERSION,
    started_at: new Date().toISOString(),
    markets: {},
  };

  // Process all markets in parallel
  const results = await Promise.all(markets.map(async (market) => {
    console.log(`\n── Processing ${market} ──`);
    return processMarket(market, opts, fromMs, toMs, scanFromMs, scanToMs, summaryDir, featuresDir);
  }));
  for (const result of results) {
    if (result) {
      runReport.markets[result.market] = result;
    }
  }

  // Write run report
  runReport.completed_at = new Date().toISOString();
  const reportPath = path.join(stagingDir, 'run-report.json');
  writeJsonl(reportPath, runReport);
  // run-report.json should be pretty-printed
  fs.writeFileSync(reportPath, JSON.stringify(runReport, null, 2) + '\n', 'utf8');
  console.log('\nRun report: ' + reportPath);
  console.log('Done.');

  // Optional: delete processed raw files
  if (opts.deleteProcessed) {
    deleteProcessedRawFiles(opts.data, markets, fromMs, toMs, runReport);
  }
}

/**
 * Process a single market: read trades, build bursts, compute summary+features, write output.
 */
async function processMarket(market, opts, fromMs, toMs, scanFromMs, scanToMs, summaryDir, featuresDir) {
  // Dedup ts sets for this file (lazy initialized per market)
  const summaryTsSet = new Set();
  const featureTsSet = new Set();

  // Load existing ts if file exists
  const summaryFile = path.join(summaryDir, `${market}.jsonl`);
  if (fs.existsSync(summaryFile)) {
    for (const line of fs.readFileSync(summaryFile, 'utf8').trim().split('\n')) {
      if (line) try { summaryTsSet.add(JSON.parse(line).ts); } catch {}
    }
  }
  const featureFile = path.join(featuresDir, `${market}.jsonl`);
  if (fs.existsSync(featureFile)) {
    for (const line of fs.readFileSync(featureFile, 'utf8').trim().split('\n')) {
      if (line) try { featureTsSet.add(JSON.parse(line).ts); } catch {}
    }
  }

  // Read trades
  const trades = readTrades(opts.data, market, scanFromMs, scanToMs);

  if (trades.length === 0) {
    console.log(`  ${market}: 0 trades, skipping`);
    return null;
  }

  // Build bursts
  console.log(`  ${market}: ${trades.length} trades, building bursts...`);
  const builder = new BurstBuilder({
    market,
    gap_threshold_ms: opts.gapThresholdMs,
    max_burst_duration_ms: opts.maxBurstDurationMs,
  });
  for (const t of trades) {
    builder.feedTrade(t);
  }
  builder.flushAll();

  // Collect all unique bursts
  const burstMap = new Map();
  for (const s of generateSeconds(scanFromMs, scanToMs)) {
    const obs = builder.getClosedBurstsOverlapping(s);
    for (const b of obs) {
      if (!burstMap.has(b.burst_id)) {
        burstMap.set(b.burst_id, b);
      }
    }
  }
  const allBursts = Array.from(burstMap.values());

  // Read book events
  const bookEvents = readBookEvents(opts.data, market, scanFromMs, scanToMs);

  let bookAtTime;
  if (bookEvents.length === 0) {
    bookAtTime = () => ({ bestBid: null, bestAsk: null, seeded: false, bestBidQty: null, bestAskQty: null });
  } else {
    bookAtTime = replayBestBookState(bookEvents);
  }

  // Process each 30s window
  let summaryWritten = 0;
  let featuresWritten = 0;

  for (const ws of generateWindows(fromMs, toMs)) {
    const we = ws + WIN_MS;

    const summaryRow = computeSummary({
      ws, we, market, trades, bursts: allBursts, bookAtTime, bookRangeUsd: opts.bookRangeUsd,
    });

    if (!summaryTsSet.has(summaryRow.ts)) {
      fs.mkdirSync(path.dirname(summaryFile), { recursive: true });
      fs.appendFileSync(summaryFile, JSON.stringify(summaryRow) + '\n', 'utf8');
      summaryTsSet.add(summaryRow.ts);
    }
    summaryWritten++;

    for (const secTs of generateSeconds(ws, we)) {
      const featureRow = computeFeatures({ ts: secTs, trades, bursts: allBursts });
      if (!featureTsSet.has(featureRow.ts)) {
        fs.mkdirSync(path.dirname(featureFile), { recursive: true });
        fs.appendFileSync(featureFile, JSON.stringify(featureRow) + '\n', 'utf8');
        featureTsSet.add(featureRow.ts);
      }
      featuresWritten++;
    }
  }

  console.log(`  ${market}: ${summaryWritten} windows, ${featuresWritten} features`);

  return {
    market,
    trades_read: trades.length,
    bursts_detected: allBursts.length,
    book_events_read: bookEvents.length,
    summary_windows: summaryWritten,
    feature_seconds: featuresWritten,
  };
}

// ── Smoke test (triggered with --smoke) ────────────────────────────────

async function smokeTest() {
  console.log('=== Smoke test ===');

  // Use a small slice of real data if available
  const dataDir = DATA_DIR_DEFAULT;
  const market = 'hyperliquid_perp';
  const fromMs = isoToMs('2026-07-07T00:12:30.000Z');
  const toMs = isoToMs('2026-07-07T00:13:00.000Z');

  console.log(`Range: ${msToIso(fromMs)} → ${msToIso(toMs)}`);

  const trades = readTrades(dataDir, market, fromMs, toMs);
  console.log(`Trades: ${trades.length}`);

  const builder = new BurstBuilder({ market, gap_threshold_ms: 100, max_burst_duration_ms: 3000 });
  for (const t of trades) builder.feedTrade(t);
  builder.flushAll();

  // Collect bursts
  const burstMap = new Map();
  for (const s of generateSeconds(fromMs, toMs + 3000)) {
    for (const b of builder.getClosedBurstsOverlapping(s)) {
      burstMap.set(b.burst_id, b);
    }
  }
  const bursts = Array.from(burstMap.values());
  console.log(`Bursts: ${bursts.length}`);
  for (const b of bursts.slice(0, 3)) {
    console.log(`  ${b.burst_id}: side=${b.side} notional=${b.burst_notional.toFixed(2)} prints=${b.burst_print_count} duration=${b.burst_duration_ms}ms multilevel=${isMultilevel(b)}`);
  }
  if (bursts.length > 3) console.log(`  ... and ${bursts.length - 3} more`);

  // Test book
  const bookEvents = readBookEvents(dataDir, market, fromMs, toMs);
  console.log(`Book events: ${bookEvents.length}`);
  if (bookEvents.length > 0) {
    const bookAtTime = replayBestBookState(bookEvents);
    const s0 = bookAtTime(fromMs);
    const s1 = bookAtTime(fromMs + 15000);
    console.log(`Book at start: bid=${s0.bestBid} ask=${s0.bestAsk}`);
    console.log(`Book at +15s:   bid=${s1.bestBid} ask=${s1.bestAsk}`);
  }

  // Test computeSummary
  if (bookEvents.length > 0 && bursts.length > 0) {
    const bookAtTime = replayBestBookState(bookEvents);
    const summary = computeSummary({
      ws: fromMs, we: fromMs + WIN_MS, market, trades, bursts, bookAtTime, bookRangeUsd: DEFAULT_BOOK_RANGE_USD,
    });
    console.log('\nSample summary:');
    console.log(JSON.stringify(summary, null, 2));

    // Test features
    const feature = computeFeatures({ ts: fromMs, trades, bursts });
    console.log('\nSample feature (first second):');
    console.log(JSON.stringify(feature, null, 2));
  }

  console.log('\nSmoke test passed.');
}

// ── Processed raw file cleanup ──────────────────────────────────────────

function deleteProcessedRawFiles(dataDir, markets, fromMs, toMs, runReport) {
  let deleted = 0;
  const subdirs = ['trades', 'book_updates'];
  for (const market of markets) {
    const mktReport = runReport.markets[market];
    if (!mktReport || mktReport.trades_read === 0) continue;

    for (const subdir of subdirs) {
      const fromDate = formatDate(fromMs);
      const toDate = formatDate(toMs);
      let d = new Date(fromDate + 'T00:00:00.000Z');
      const endDate = new Date(toDate + 'T00:00:00.000Z');
      while (d <= endDate) {
        const dateStr = formatDate(d);
        const dir = path.join(dataDir, subdir, market, dateStr);
        if (fs.existsSync(dir)) {
          const files = fs.readdirSync(dir).filter(f => f.endsWith('.jsonl') && !f.endsWith('.open'));
          for (const f of files) {
            const parts = f.replace('.jsonl', '').split('-');
            if (parts.length === 3) {
              const h = parseInt(parts[0], 10), m = parseInt(parts[1], 10), s = parseInt(parts[2], 10);
              const fileMs = d.getTime() + (h * 3600 + m * 60 + s) * 1000;
              if (fileMs >= fromMs && fileMs < toMs) {
                const fp = path.join(dir, f);
                try {
                  fs.unlinkSync(fp);
                  deleted++;
                } catch (e) {
                  // ignore if already deleted
                }
              }
            }
          }
        }
        d.setTime(d.getTime() + 86400000);
      }
    }
  }
  if (deleted > 0) {
    console.log(`Deleted ${deleted} processed raw file(s)`);
  }
}

function formatDate(ms) {
  const d = new Date(ms);
  return d.toISOString().slice(0, 10);
}

// ── Entry ──────────────────────────────────────────────────────────────

if (process.argv.includes('--smoke')) {
  smokeTest().catch(e => { console.error(e); process.exit(1); });
} else {
  main().catch(e => { console.error(e); process.exit(1); });
}
