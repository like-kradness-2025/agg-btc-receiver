#!/usr/bin/env node
// scripts/tfp.mjs — TradeFlow Pipeline (TFP) CLI entry point
// Follows plan Task 9 + P0-4 finalized input horizon + P0-1 kind/horizon-proof
// Gate A: direct-entry per-market flock protection via lock-helper.sh subprocess

import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';
import { runPipeline } from '../lib/burst-reducer/pipeline.mjs';
import { INPUT_KIND, VALID_INPUT_KINDS, BLOCK_DURATION_MS } from '../lib/burst-reducer/schema.mjs';

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = {
    data: 'data/live_v3',
    markets: null,
    from: null,
    to: null,
    outputRoot: null,
    finalizedThrough: null,      // P0-4: ISO timestamp
    frozenInventory: null,       // P0-4: path to JSON inventory file
    kind: 'trades',              // P0-1: 'trades' or 'book_updates'
  };

  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--data': opts.data = args[++i]; break;
      case '--markets': opts.markets = args[++i]; break;
      case '--from': opts.from = args[++i]; break;
      case '--to': opts.to = args[++i]; break;
      case '--output-root': opts.outputRoot = args[++i]; break;
      case '--finalized-through': opts.finalizedThrough = args[++i]; break;
      case '--frozen-inventory': opts.frozenInventory = args[++i]; break;
      case '--kind': opts.kind = args[++i]; break;
      case '--help':
        console.error(`Usage: node scripts/tfp.mjs --from <ISO|epoch_ms> --to <ISO|epoch_ms> [options]`);
        console.error(`  --markets <csv>            Comma-separated market names`);
        console.error(`  --data <dir>               Input data dir (default: data/live_v3)`);
        console.error(`  --output-root <dir>        Output root dir (default: data/derived/burst_features_v1)`);
        console.error(`  --finalized-through <ISO>  P0-4: exclusive 30s-aligned horizon (live EOF authority)`);
        console.error(`  --frozen-inventory <path>  P0-4: path to frozen inventory JSON manifest`);
        console.error(`  --kind <trades|book_updates> P0-1: input kind (default: trades)`);
        process.exit(0);
    }
  }

  if (!opts.from || !opts.to) {
    console.error('ERROR: --from and --to are required');
    process.exit(1);
  }

  // Validate kind
  if (!VALID_INPUT_KINDS.has(opts.kind)) {
    console.error(`ERROR: --kind must be one of: ${[...VALID_INPUT_KINDS].join(', ')} (got: ${opts.kind})`);
    process.exit(1);
  }

  return opts;
}

function isoToMs(iso) {
  const ms = new Date(iso).getTime();
  if (isNaN(ms)) throw new Error(`Invalid ISO timestamp: ${iso}`);
  return ms;
}

/**
 * Detect markets by scanning a kind-specific subdirectory.
 * @param {string} dataDir
 * @param {string} kind - 'trades' or 'book_updates'
 * @returns {string[]}
 */
function detectMarkets(dataDir, kind = 'trades') {
  const kindDir = join(dataDir, kind);
  if (!existsSync(kindDir)) return [];
  return readdirSync(kindDir, { withFileTypes: true })
    .filter(d => d.isDirectory())
    .map(d => d.name);
}

/**
 * Validate a single frozen inventory entry's structure and content.
 * @param {object} entry
 * @param {number} index - entry index for error messages
 * @returns {string[]} array of error messages (empty if valid)
 */
function validateInventoryEntry(entry, index) {
  const prefix = `entry[${index}]`;
  const errors = [];

  if (!entry || typeof entry !== 'object') {
    errors.push(`${prefix}: must be an object`);
    return errors;
  }

  // market: required non-empty string
  if (typeof entry.market !== 'string' || entry.market.trim() === '') {
    errors.push(`${prefix}.market: required non-empty string`);
  }

  // kind: must be valid input kind
  if (!VALID_INPUT_KINDS.has(entry.kind)) {
    errors.push(`${prefix}.kind: must be one of ${[...VALID_INPUT_KINDS].join(', ')} (got: ${entry.kind})`);
  }

  // block_start_ms: required number, 30s-aligned
  if (typeof entry.block_start_ms !== 'number' || !Number.isFinite(entry.block_start_ms)) {
    errors.push(`${prefix}.block_start_ms: required finite number`);
  } else if (entry.block_start_ms % BLOCK_DURATION_MS !== 0) {
    errors.push(`${prefix}.block_start_ms: must be ${BLOCK_DURATION_MS}-aligned (got ${entry.block_start_ms}, remainder=${entry.block_start_ms % BLOCK_DURATION_MS})`);
  }

  // path: required string matching kind/market/date/time pattern
  if (typeof entry.path !== 'string' || entry.path.trim() === '') {
    errors.push(`${prefix}.path: required non-empty string`);
  } else {
    const pathParts = entry.path.split('/');
    if (pathParts.length < 4) {
      errors.push(`${prefix}.path: expected format <kind>/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl`);
    } else {
      const pathKind = pathParts[0];
      const pathMarket = pathParts[1];
      // Verify path-kind matches entry.kind
      if (entry.kind && pathKind !== entry.kind) {
        errors.push(`${prefix}.path: leading directory "${pathKind}" does not match kind "${entry.kind}"`);
      }
      // Verify path-market matches entry.market
      if (entry.market && pathMarket !== entry.market) {
        errors.push(`${prefix}.path: market "${pathMarket}" does not match entry.market "${entry.market}"`);
      }
      // Extract block_start_ms from path and verify match
      if (entry.block_start_ms && typeof entry.block_start_ms === 'number') {
        try {
          const datePart = pathParts[2]; // YYYY-MM-DD
          const timePart = pathParts[3].replace('.jsonl', ''); // HH-MM-SS
          const [hh, mm, ss] = timePart.split('-').map(Number);
          const pathMs = Date.UTC(
            parseInt(datePart.slice(0, 4)),
            parseInt(datePart.slice(5, 7)) - 1,
            parseInt(datePart.slice(8, 10)),
            hh, mm, ss
          );
          if (pathMs !== entry.block_start_ms) {
            errors.push(`${prefix}.path: derived block_start_ms ${pathMs} does not match entry.block_start_ms ${entry.block_start_ms}`);
          }
        } catch (_) {
          errors.push(`${prefix}.path: could not parse date/time from "${entry.path}"`);
        }
      }
    }
  }

  // sha256: optional, but if present must be 64-char hex or empty string
  if (entry.sha256 !== undefined && entry.sha256 !== null && entry.sha256 !== '') {
    if (typeof entry.sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(entry.sha256)) {
      errors.push(`${prefix}.sha256: must be 64-char hex string or empty (got: ${entry.sha256})`);
    }
  }

  return errors;
}

/**
 * Validate inventory entries for cross-reference consistency.
 * @param {Array} entries
 * @returns {string[]} array of error messages
 */
function validateInventoryCrossReferences(entries) {
  const errors = [];
  const seen = new Map();

  for (let i = 0; i < entries.length; i++) {
    const e = entries[i];
    if (e.market && e.kind && typeof e.block_start_ms === 'number') {
      const key = `${e.market}::${e.kind}::${e.block_start_ms}`;
      if (seen.has(key)) {
        errors.push(`duplicate entry: (market=${e.market}, kind=${e.kind}, block_start_ms=${e.block_start_ms}) at index ${i} (first at ${seen.get(key)})`);
      } else {
        seen.set(key, i);
      }
    }
  }

  return errors;
}

/**
 * Load and validate a frozen inventory JSON file.
 * Returns a structured result:
 *   { byKindAndMarket: Map<string, Map<string, Map<number, object>>>, entries: Array, errors: Array }
 * Exits process with code 1 on fatal errors.
 */
function loadAndValidateFrozenInventory(path) {
  if (!path) return null;
  if (!existsSync(path)) {
    console.error(`ERROR: frozen inventory file not found: ${path}`);
    process.exit(1);
  }

  let data;
  try {
    data = JSON.parse(readFileSync(path, 'utf8'));
  } catch (e) {
    console.error(`ERROR: failed to parse frozen inventory: ${e.message}`);
    process.exit(1);
  }

  // Extract blocks array (support both array and {blocks: [...]} formats)
  let entries;
  if (Array.isArray(data)) {
    entries = data;
  } else if (data && Array.isArray(data.blocks)) {
    entries = data.blocks;
  } else {
    console.error('ERROR: frozen inventory must be a JSON array or {blocks: [...]} object');
    process.exit(1);
  }

  const errors = [];

  // Validate each entry
  for (let i = 0; i < entries.length; i++) {
    const entryErrors = validateInventoryEntry(entries[i], i);
    errors.push(...entryErrors);
  }

  // Cross-reference validation
  const xrefErrors = validateInventoryCrossReferences(entries);
  errors.push(...xrefErrors);

  if (errors.length > 0) {
    console.error('ERROR: frozen inventory validation failed:');
    for (const err of errors) {
      console.error(`  - ${err}`);
    }
    process.exit(1);
  }

  // Build structured lookup: byKindAndMarket[kind][market][block_start_ms] = entry
  const byKindAndMarket = new Map();
  for (const e of entries) {
    if (!e.kind || !e.market || typeof e.block_start_ms !== 'number') continue;
    if (!byKindAndMarket.has(e.kind)) {
      byKindAndMarket.set(e.kind, new Map());
    }
    const byMarket = byKindAndMarket.get(e.kind);
    if (!byMarket.has(e.market)) {
      byMarket.set(e.market, new Map());
    }
    const byBlock = byMarket.get(e.market);
    byBlock.set(e.block_start_ms, e);
  }

  return { byKindAndMarket, entries, errors: [] };
}

const LOCK_ACQUIRE_TIMEOUT_MS = parseInt(process.env.TFP_LOCK_ACQUIRE_TIMEOUT_MS || '15000', 10);
const LOCK_PRE_ACQUIRE_DELAY_MS = parseInt(process.env.TFP_LOCK_PRE_ACQUIRE_DELAY_MS || '0', 10);

/**
 * Acquire an exclusive non-blocking flock for a market via lock-helper.sh subprocess.
 * The subprocess holds the lock FD open until killed (kernel releases on process death).
 *
 * @param {string} market - market name
 * @param {string} outputRoot - output root dir (determines lock file path)
 * @returns {Promise<import('child_process').ChildProcess|null>}
 *   ChildProcess on success (lock held), null on flock contention (exit 2).
 * @throws {Error} on source failure, helper missing, spawn error, unexpected exit, or timeout.
 */
function acquireLock(market, outputRoot) {
  const __dirname = fileURLToPath(new URL('.', import.meta.url));
  const repoRoot = resolve(__dirname, '..');
  const lockHelperPath = join(repoRoot, 'scripts', 'lock-helper.sh');

  return new Promise((resolve, reject) => {
    const preDelayLine = LOCK_PRE_ACQUIRE_DELAY_MS > 0
      ? `sleep ${(LOCK_PRE_ACQUIRE_DELAY_MS / 1000).toFixed(1)}`
      : '';
    const script =
      `${preDelayLine}${preDelayLine ? '\n' : ''}test -r "${lockHelperPath}" || { echo '{"level":"FATAL","reason":"lock-helper-not-found","path":"${lockHelperPath}"}' >&2; exit 3; }
source "${lockHelperPath}" || { echo '{"level":"FATAL","reason":"lock-helper-source-failed","path":"${lockHelperPath}"}' >&2; exit 3; }
acquire_market_lock "$TFP_LOCK_MARKET" "$TFP_LOCK_OUTPUT_ROOT"
RET=$?
case $RET in
  0) ;;
  1) exit 2 ;;
  *) exit 3 ;;
esac
echo '{"status":"ACQUIRED"}'
sleep 86400`;

    const child = spawn('/bin/bash', ['-c', script], {
      stdio: ['ignore', 'pipe', 'inherit'],  // inherit stderr → lock-helper SKIP/INFO visible
      env: {
        ...process.env,
        TFP_LOCK_MARKET: market,
        TFP_LOCK_OUTPUT_ROOT: outputRoot,
      },
      detached: true,  // separate proc group for process group kill on release
    });

    let resolved = false;
    const timer = setTimeout(() => {
      if (!resolved) {
        resolved = true;
        const pid = child.pid;
        // Process group TERM like releaseLock, then KILL fallback
        try { process.kill(-pid, 'SIGTERM'); } catch (_) {}
        try { child.kill('SIGTERM'); } catch (_) {}
        setTimeout(() => {
          try { process.kill(-pid, 'SIGKILL'); } catch (_) {}
          try { child.kill('SIGKILL'); } catch (_) {}
        }, 3000);
        reject(new Error(`lock acquisition timed out after ${LOCK_ACQUIRE_TIMEOUT_MS}ms for market ${market}`));
      }
    }, LOCK_ACQUIRE_TIMEOUT_MS);

    child.stdout.on('data', (data) => {
      const line = data.toString().trim();
      try {
        const msg = JSON.parse(line);
        if (msg.status === 'ACQUIRED' && !resolved) {
          resolved = true;
          clearTimeout(timer);
          resolve(child);
        }
      } catch (_) { /* not JSON yet, wait */ }
    });

    child.on('exit', (code) => {
      if (!resolved) {
        resolved = true;
        clearTimeout(timer);
        if (code === 2) {
          // exit 2 = lock contention (from acquire_market_lock returning 1 → exit 2)
          resolve(null);
        } else {
          reject(new Error(`lock helper exited with code ${code} for market ${market}`));
        }
      }
    });

    child.on('error', (err) => {
      if (!resolved) {
        resolved = true;
        clearTimeout(timer);
        reject(new Error(`lock helper spawn error for market ${market}: ${err.message}`));
      }
    });
  });
}

/**
 * Release a held market lock by killing the holder subprocess.
 * Kernel releases the advisory flock on process death.
 *
 * @param {import('child_process').ChildProcess|null} lockProc - holder from acquireLock()
 */
async function releaseLock(lockProc) {
  if (!lockProc || lockProc.killed) return;
  const pid = lockProc.pid;
  // Send TERM to process group (bash + sleep), then KILL fallback after timeout
  try { process.kill(-pid, 'SIGTERM'); } catch (_) {}
  try { lockProc.kill('SIGTERM'); } catch (_) {}
  await new Promise((resolve) => {
    const timer = setTimeout(() => {
      try { process.kill(-pid, 'SIGKILL'); } catch (_) {}
      try { lockProc.kill('SIGKILL'); } catch (_) {}
      resolve();
    }, 3000);
    lockProc.on('exit', () => { clearTimeout(timer); resolve(); });
    lockProc.on('error', () => { clearTimeout(timer); resolve(); });
  });
}

async function main() {
  const opts = parseArgs();
  const fromMs = isoToMs(opts.from);
  const toMs = isoToMs(opts.to);
  const runId = `run-${new Date().toISOString().replace(/[:.]/g, '-')}`;
  const outputRoot = opts.outputRoot || 'data/derived/burst_features_v1';

  // P0-4: Parse finalized-through
  let finalizedThroughMs = null;
  if (opts.finalizedThrough) {
    finalizedThroughMs = isoToMs(opts.finalizedThrough);
    // Validate 30s alignment
    if (finalizedThroughMs % BLOCK_DURATION_MS !== 0) {
      console.error(`ERROR: --finalized-through must be 30s-aligned (got ${opts.finalizedThrough}, ms=${finalizedThroughMs}, remainder=${finalizedThroughMs % BLOCK_DURATION_MS})`);
      process.exit(1);
    }
  }

  // P0-1: Load and validate frozen inventory
  const frozenInventory = loadAndValidateFrozenInventory(opts.frozenInventory);

  const markets = opts.markets
    ? opts.markets.split(',').map(s => s.trim())
    : detectMarkets(opts.data, opts.kind);

  if (markets.length === 0) {
    console.error('ERROR: no markets found');
    process.exit(1);
  }

  process.stderr.write(JSON.stringify({
    level: 'INFO', msg: `Starting reducer v1`, runId, markets,
    fromMs, toMs,
    kind: opts.kind,
    finalizedThroughMs: finalizedThroughMs,
    frozenInventory: frozenInventory ? `${frozenInventory.entries.length} entries` : null,
  }) + '\n');

  let totalProcessed = 0;
  let totalErrors = 0;
  let anyBlocked = false;
  let exitCode = 0;

  for (const market of markets) {
    /** @type {import('child_process').ChildProcess|null} */
    let lockProc = null;
    try {
      // Gate A: acquire per-market exclusive flock via lock-helper.sh subprocess
      // Lock FD is held by subprocess until killed; kernel releases on process death.
      lockProc = await acquireLock(market, outputRoot);
      if (!lockProc) {
        // Contention or timeout — lock-helper.sh already emitted structured SKIP to stderr.
        // Emit additional machine-readable SKIP for log parsers.
        process.stderr.write(JSON.stringify({
          ts: new Date().toISOString(),
          level: 'SKIP',
          reason: 'lock-contention',
          market,
          lock_file: `${outputRoot}/locks/${market}.lock`,
        }) + '\n');
        // IMPORTANT: continue (not exit) — cursor unchanged, commit untouched
        continue;
      }

      const result = await runPipeline({
        dataDir: opts.data,
        market,
        fromMs,
        toMs,
        runId,
        outputRoot,
        finalizedThroughMs,
        frozenInventory,
        kind: opts.kind,
      });

      totalProcessed += result.processed;
      totalErrors += result.errors;
      if (result.blocked) {
        anyBlocked = true;
      }
    } catch (e) {
      process.stderr.write(JSON.stringify({ level: 'FATAL', market, error: e.message }) + '\n');
      exitCode = 1;
      break;  // let finally release the lock
    } finally {
      // Gate A: release lock on normal exit, exception, or skip
      await releaseLock(lockProc);
    }
  }

  process.stderr.write(JSON.stringify({
    level: 'INFO', msg: 'Reducer complete',
    processed: totalProcessed, errors: totalErrors,
    blocked: anyBlocked,
  }) + '\n');

  // P0-4: blocked exit = 0 (not an error)
  if (exitCode === 0 && totalErrors > 0) {
    exitCode = 1;
  }
  process.exit(exitCode);
}

// Only run main when executed directly (not when imported as module)
const __filename = fileURLToPath(import.meta.url);
const isDirectRun = process.argv[1] && resolve(process.argv[1]) === __filename;
if (isDirectRun) {
  main().catch(e => {
    process.stderr.write(JSON.stringify({ level: 'FATAL', error: e.message }) + '\n');
    process.exit(1);
  });
}

export { validateInventoryEntry, validateInventoryCrossReferences, loadAndValidateFrozenInventory };
