// lib/burst-reducer/pipeline.mjs — 1-block lag pipeline with safety remediation (P0-2,P0-3,P0-4,P1-1,P0-1)
// Follows plan Task 8 + remediation plan + P0-1 horizon proof / frozen inventory validation

import { readFileSync, existsSync, mkdirSync, writeFileSync, rmSync, renameSync } from 'node:fs';
import { join } from 'node:path';
import { randomUUID, createHash } from 'node:crypto';
import { scanBlocks } from './block-scanner.mjs';
import { validateAndParseTrades } from './input-validator.mjs';
import { BurstDetector } from './burst-detector.mjs';
import { computeFeatures1s } from './feature-computer-1s.mjs';
import { OutputCommitter } from './output-committer.mjs';
import { RollupOutputCommitter } from './rollup-output-committer.mjs';
import { Rollup5minCommitter } from './rollup-5min-committer.mjs';
import { reconcileMarketState } from './recovery.mjs';
import { loadCheckpoint, writeManifestRecord, writeCheckpoint, writeVerifiedMissingRecord } from './manifest-manager.mjs';
import { CHECKPOINT_CORRUPT } from './manifest-manager.mjs';
import { DERIVED_DIR, BLOCK_DURATION_MS } from './schema.mjs';
import { buildRawTradedNotionalLookup } from './raw-trades-notional-reader.mjs';
import { validateRawTradeLookback } from './raw-trades-notional-reader.mjs';
import { stateAt } from '../book-state-machine.mjs';
import { toCanonicalBookEnvelope } from '../book-updates-adapter.mjs';

function sha256File(path) {
  if (!existsSync(path)) return null;
  return createHash('sha256').update(readFileSync(path, 'utf8')).digest('hex');
}

function sha256Content(content) {
  return createHash('sha256').update(content).digest('hex');
}

function formatBlockTime(blockStartMs) {
  const d = new Date(blockStartMs);
  return `${String(d.getUTCHours()).padStart(2,'0')}-${String(d.getUTCMinutes()).padStart(2,'0')}-${String(d.getUTCSeconds()).padStart(2,'0')}`;
}

function formatDate(blockStartMs) {
  const d = new Date(blockStartMs);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')}`;
}

/**
 * Write a quarantine report for a failed block.
 * Optional kind parameter added in P0-1.
 */
function writeQuarantineReport(derivedDir, market, blockStartMs, reason, details, kind) {
  const path = join(derivedDir, 'quarantine', market, `${blockStartMs}.json`);
  mkdirSync(join(derivedDir, 'quarantine', market), { recursive: true });
  const report = {
    ts: new Date().toISOString(),
    market,
    block_start_ms: blockStartMs,
    reason,
  };
  if (kind) report.kind = kind;
  if (details) report.details = details;
  writeFileSync(path, JSON.stringify(report, null, 2) + '\n', 'utf8');
}

function emitStructured(record) {
  process.stderr.write(JSON.stringify({ ts: new Date().toISOString(), ...record }) + '\n');
}

function log(level, market, msg, extra) {
  emitStructured({ level, market, msg, ...(extra || {}) });
}

// ── P0-3: Recovery reconcile (delegated to recovery.mjs) ─────────────────

// ── P0-4 / P0-1: Finalized horizon helpers ───────────────────────────────

function is30sAligned(ms) {
  return (ms % BLOCK_DURATION_MS) === 0;
}

/**
 * Determine if the pending block can be EOF-finalized.
 * P0-1: kind-aware with 3-state model returned via `state` field.
 *
 * @param {object|null} pendingBlock
 * @param {number|null} finalizedThroughMs
 * @param {object|null} frozenInventory - object with byKindAndMarket Map (from loadAndValidateFrozenInventory)
 * @param {string} dataDir
 * @param {string} market
 * @param {string} [kind='trades'] - 'trades' or 'book_updates'
 * @returns {{ canFinalize: boolean, reason: string, state: string, horizonProof: object|null }}
 */
function checkFinalizedHorizon(pendingBlock, finalizedThroughMs, frozenInventory, dataDir, market, kind = 'trades') {
  if (!pendingBlock) return { canFinalize: false, state: 'no-pending', reason: 'no-pending-block', horizonProof: null };

  const blockStart = pendingBlock.block_start_ms;

  // ── Frozen inventory path ──
  if (frozenInventory !== null && frozenInventory.byKindAndMarket) {
    const byMarket = frozenInventory.byKindAndMarket.get(kind);
    const byBlock = byMarket ? byMarket.get(market) : undefined;
    const pendingEntry = byBlock ? byBlock.get(blockStart) : undefined;
    const nextEntry = byBlock ? byBlock.get(blockStart + BLOCK_DURATION_MS) : undefined;

    if (!pendingEntry) {
      return { canFinalize: false, state: 'verified-missing', reason: 'pending-not-in-inventory', horizonProof: null };
    }

    // sha256 check when declared
    if (pendingEntry.sha256 && pendingEntry.sha256 !== '') {
      const actualSha = sha256File(pendingBlock.replay_identity?.input_path);
      if (actualSha === null) {
        // File not found at the recorded path — quarantine as verified-missing
        return { canFinalize: false, state: 'verified-missing', reason: 'pending-block-file-not-found', quarantine: true, horizonProof: null };
      }
      if (actualSha && actualSha !== pendingEntry.sha256) {
        return { canFinalize: false, state: 'hash-mismatch', reason: 'pending-block-hash-mismatch', quarantine: true, horizonProof: null };
      }
    }

    if (!nextEntry) {
      // Inventory ends at pending boundary — EOF is valid
      return { canFinalize: true, state: 'frozen-inventory-boundary', reason: 'frozen-inventory-boundary', horizonProof: { inventoryEndsAt: blockStart + BLOCK_DURATION_MS } };
    }
    // Next block exists in inventory — don't EOF yet
    return { canFinalize: false, state: 'next-block-in-inventory', reason: 'next-block-in-inventory', horizonProof: null };
  }

  // ── finalizedThroughMs path ──
  if (finalizedThroughMs !== null) {
    const nextBoundaryStart = blockStart + BLOCK_DURATION_MS;

    if (nextBoundaryStart < finalizedThroughMs) {
      // Next block boundary is strictly within finalized horizon
      const nextBlockPath = join(dataDir, kind, market, formatDate(nextBoundaryStart), `${formatBlockTime(nextBoundaryStart)}.jsonl`);

      if (existsSync(nextBlockPath)) {
        return { canFinalize: false, state: 'next-block-exists', reason: 'next-block-exists', horizonProof: null };
      }
      // Next block absent within finalized horizon
      if (kind === 'trades') {
        // ASSUMED_EMPTY_GAP: gap in trades is EOF-completable
        return { canFinalize: true, state: 'data-none-gap', reason: 'data-none-gap', horizonProof: { finalizedThroughMs, missingBlock: nextBoundaryStart } };
      }
      // book_updates: absent file within horizon → verified-missing quarantine
      return { canFinalize: false, state: 'verified-missing', reason: 'verified-missing', quarantine: true, horizonProof: null };
    }
    if (nextBoundaryStart === finalizedThroughMs) {
      // Next block boundary === finalizedThrough — this is the EOF boundary
      return { canFinalize: true, state: 'finalized-through-boundary', reason: 'finalized-through-boundary', horizonProof: { finalizedThroughMs } };
    }
    // nextBoundaryStart > finalizedThroughMs — pending is at/after horizon, can EOF
    if (blockStart >= finalizedThroughMs) {
      return { canFinalize: true, state: 'pending-at-horizon', reason: 'pending-at-horizon', horizonProof: { finalizedThroughMs } };
    }
    // Pending is before horizon but next boundary is after — blocked
    return { canFinalize: false, state: 'not-yet-arrived', reason: 'not-yet-arrived', horizonProof: null };
  }

  // No horizon proof at all — live/unfinalized
  return { canFinalize: false, state: 'no-horizon-proof', reason: 'no-horizon-proof', horizonProof: null };
}

/**
 * Load book snapshot for the same block at pipeline boundary.
 * Reads book_updates/{market}/{date}/{time}.jsonl, parses canonical envelopes,
 * and calls strict stateAt(events, blockStartMs + 30000).
 * Returns unavailable metadata on missing/corrupt/quarantined input.
 *
 * @param {string} dataDir
 * @param {string} market
 * @param {number} blockStartMs
 * @returns {{available: boolean, book_seeded: boolean, state?: object|null}}
 */
function loadBookSnapshot(dataDir, market, blockStartMs) {
  const bookPath = join(dataDir, 'book_updates', market, formatDate(blockStartMs), `${formatBlockTime(blockStartMs)}.jsonl`);

  if (!existsSync(bookPath)) {
    return { available: false, book_seeded: false };
  }

  let content;
  try {
    content = readFileSync(bookPath, 'utf8');
  } catch {
    return { available: false, book_seeded: false };
  }

  const events = [];
  const lines = content.split('\n');
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    let parsed;
    try {
      parsed = JSON.parse(trimmed);
    } catch {
      return { available: false, book_seeded: false };
    }
    const result = toCanonicalBookEnvelope(parsed);
    if (!result.valid) {
      return { available: false, book_seeded: false };
    }
    events.push(result.envelope);
  }

  const stateResult = stateAt(events, blockStartMs + 30000);

  // stateAt returns { state: null, quarantined: true } on quarantine
  if (stateResult && stateResult.quarantined) {
    return { available: false, book_seeded: false };
  }

  // stateResult is snapshotState(): { seeded, best_bid, ... }
  const bookSeeded = stateResult ? stateResult.seeded : false;
  return { available: true, book_seeded: bookSeeded, state: stateResult || null };
}

// ── Main pipeline ────────────────────────────────────────────────────────

/**
 * 1-block lag pipeline with safety remediation.
 *
 * @param {Object} params
 * @param {string} params.dataDir
 * @param {string} params.market
 * @param {number} params.fromMs - lower bound for initial block scan (CLI --from)
 * @param {number} params.toMs - upper bound for block scan (CLI --to)
 * @param {string} params.runId
 * @param {string} [params.outputRoot] - override output root
 * @param {number|null} [params.finalizedThroughMs] - P0-4: exclusive 30s-aligned horizon
 * @param {object|null} [params.frozenInventory] - P0-4/P0-1: validated inventory result
 * @param {string} [params.kind='trades'] - P0-1: 'trades' or 'book_updates'
 */
export async function runPipeline({ dataDir, market, fromMs, toMs, runId, outputRoot, finalizedThroughMs = null, frozenInventory = null, kind = 'trades' }) {
  const derivedDir = outputRoot || DERIVED_DIR;

  // ── P0-4: Validate finalizedThrough alignment ──
  if (finalizedThroughMs !== null && !is30sAligned(finalizedThroughMs)) {
    emitStructured({ level: 'FATAL', market, msg: '--finalized-through must be 30s-aligned', finalizedThroughMs });
    throw new Error(`E040: --finalized-through must be 30s-aligned (got ${finalizedThroughMs})`);
  }

  // ── P0-3: Recovery reconcile before processing ──
  // B5: Kind-aware recovery — reconcile all kinds
  let reconcileResult = reconcileMarketState(market, derivedDir, kind);
  if (reconcileResult.status === 'corrupt-checkpoint') {
    throw new Error(`E024: corrupt-checkpoint: ${kind} checkpoint file was empty or invalid JSON, backed up as .bak`);
  }
  if (reconcileResult.errors && reconcileResult.errors.length > 0) {
    throw new Error(`E023: recovery reconciliation failed: ${reconcileResult.errors.join('; ')}`);
  }
  const reconciledCp = reconcileResult.cursor;

  // ── P0-2: Authoritative cursor from checkpoint ──
  // B5: kind-aware checkpoint loading
  const cp = reconciledCp || loadCheckpoint(market, derivedDir, kind);

  // Determine effective scan range: checkpoint cursor is authoritative
  let effectiveFromMs = fromMs;
  if (cp) {
    if (cp.pending_block) {
      effectiveFromMs = cp.pending_block.block_start_ms;
      log('INFO', market, `resuming from checkpoint pending block`, { cursorMs: effectiveFromMs, kind });
    } else if (cp.last_committed_block_start !== null && cp.last_committed_block_start !== undefined) {
      effectiveFromMs = cp.last_committed_block_start + BLOCK_DURATION_MS;
      log('INFO', market, `resuming after last committed`, { cursorMs: effectiveFromMs, kind });
    }
    // Checkpoint cursor is authoritative — reject --from that skips past
    if (fromMs > effectiveFromMs) {
      throw new Error(`E022: --from (${fromMs}) skips past checkpoint cursor (${effectiveFromMs})`);
    }
  }

  const blocks = scanBlocks(dataDir, kind, market, effectiveFromMs, toMs);
  if (blocks.length === 0) {
    // P3-C3: recovery must also run on an idle restart (no new 1s blocks).
    const idle5minCommitter = new Rollup5minCommitter(market, `${runId}-5min-idle`, derivedDir);
    try {
      idle5minCommitter.reconcileCommitted30s();
    } catch (error) {
      emitStructured({ level: 'ERROR', market, msg: `5min idle recovery blocked: ${error.code || error.message}` });
    }
    // P0-4: Check if we have a pending block but no new blocks — blocked exit
    if (cp && cp.pending_block) {
      const horizon = checkFinalizedHorizon(cp.pending_block, finalizedThroughMs, frozenInventory, dataDir, market, kind);
      if (horizon.canFinalize) {
        // Re-feed pending block to finalize it at horizon
        log('INFO', market, `EOF-finalizing pending block at horizon`, { reason: horizon.reason, kind });
        const pendingBlocks = scanBlocks(dataDir, kind, market, cp.pending_block.block_start_ms, cp.pending_block.block_start_ms + BLOCK_DURATION_MS);
        if (pendingBlocks.length > 0) {
          return processBlocks({ dataDir, market, runId, derivedDir, blocks: pendingBlocks, cp, finalizedThroughMs, frozenInventory, isEofRun: true, kind });
        }
      }
      // Blocked: no new blocks, no horizon proof
      emitStructured({
        level: 'BLOCKED',
        processed: 0,
        blocked_reason: horizon.reason || 'not-yet-arrived',
        blocked_state: horizon.state || horizon.reason || 'not-yet-arrived',
        kind,
        market,
        cursor_ms: cp.pending_block.block_start_ms,
        expected_block_start_ms: cp.pending_block.block_start_ms + BLOCK_DURATION_MS,
      });
      return { processed: 0, errors: 0, manifestUpdates: [], blocked: true, blockedReason: horizon.reason || 'not-yet-arrived', blockedState: horizon.state || horizon.reason || 'not-yet-arrived' };
    }
    log('INFO', market, 'no blocks to process');
    return { processed: 0, errors: 0, manifestUpdates: [] };
  }

  return processBlocks({ dataDir, market, runId, derivedDir, blocks, cp, finalizedThroughMs, frozenInventory, isEofRun: false, kind });
}

/**
 * Core block processing loop for non-trade kinds (book_updates).
 * Simplified: no content parsing, no feature computation, no commit.
 * Only ordering checks, gap detection, and horizon validation.
 */
function processBlocksNonTrade({ dataDir, market, derivedDir, blocks, cp, finalizedThroughMs, frozenInventory, kind }) {
  let pendingBlock = null;
  let processed = 0, errors = 0;
  const manifestUpdates = [];

  // ── B5: On entry, handle checkpoint pending block for book_updates ──
  if (cp?.pending_block && kind !== 'trades') {
    const pendingPath = cp.pending_block.replay_identity?.input_path;
    if (pendingPath && !existsSync(pendingPath)) {
      // Pending block file is missing — quarantine and track verified-missing
      writeQuarantineReport(derivedDir, market, cp.pending_block.block_start_ms,
        'MISSING_FINALIZED_INPUT',
        { reason: 'pending-block-file-not-found', kind, path: pendingPath },
        kind);
      writeVerifiedMissingRecord(market, cp.pending_block.block_start_ms, {
        kind,
        reason: 'pending-block-file-not-found',
        input_path: pendingPath,
      }, derivedDir);
      emitStructured({
        level: 'VERIFIED_MISSING', market, kind,
        block_start_ms: cp.pending_block.block_start_ms,
        reason: 'pending-block-file-not-found',
      });
      // Restore pendingBlock from cp so loop advances past it in ordering check
      // Next candidate must be > this block's start
      pendingBlock = {
        block_start_ms: cp.pending_block.block_start_ms,
        replay_identity: {
          market,
          block_start_ms: cp.pending_block.block_start_ms,
          input_path: null,
        },
      };
      log('WARN', market, `pending block file not found, marked verified-missing`, {
        blockStartMs: cp.pending_block.block_start_ms, kind,
      });
    } else if (pendingPath) {
      // Pending block exists — restore state so loop skips it
      pendingBlock = {
        block_start_ms: cp.pending_block.block_start_ms,
        replay_identity: cp.pending_block.replay_identity,
      };
      log('INFO', market, `restored pending block from checkpoint`, {
        blockStartMs: cp.pending_block.block_start_ms, kind,
      });
    }
  }

  for (let i = 0; i < blocks.length; i++) {
    const candidateBlock = blocks[i];

    // Skip blocks at or before pending
    if (pendingBlock !== null && candidateBlock.ms <= pendingBlock.block_start_ms) continue;

    if (pendingBlock !== null) {
      const expectedNext = pendingBlock.block_start_ms + BLOCK_DURATION_MS;
      if (candidateBlock.ms !== expectedNext) {
        if (candidateBlock.ms > expectedNext) {
          // Gap: verified-missing for book_updates
          // Loop over all missing gap blocks: quarantine and track each as verified-missing
          for (let missing = expectedNext; missing < candidateBlock.ms; missing += BLOCK_DURATION_MS) {
            writeQuarantineReport(derivedDir, market, missing,
              'MISSING_FINALIZED_INPUT',
              { gap_from: missing, gap_to_exclusive: missing + BLOCK_DURATION_MS, kind },
              kind);
            writeVerifiedMissingRecord(market, missing, {
              kind,
              reason: 'verified-missing-gap',
              gap_range: { start_ms: expectedNext, end_ms_exclusive: candidateBlock.ms },
            }, derivedDir);
            emitStructured({
              level: 'VERIFIED_MISSING', market, kind,
              block_start_ms: pendingBlock.block_start_ms,
              missing_block_start_ms: missing,
              gap_range: { start_ms: expectedNext, end_ms_exclusive: candidateBlock.ms },
            });
            processed++;
          }
          // Advance pending block past the gap to the incoming candidate
          pendingBlock = {
            block_start_ms: candidateBlock.ms,
            replay_identity: {
              market,
              block_start_ms: candidateBlock.ms,
              input_path: candidateBlock.fullPath,
            },
          };
          continue;
        }
        // candidateBlock.ms < expectedNext — should not happen (blocks are sorted)
        writeQuarantineReport(derivedDir, market, candidateBlock.ms,
          'out-of-order block', { expected_ms: expectedNext, got_ms: candidateBlock.ms }, kind);
        throw new Error(`E006: out-of-order block ${candidateBlock.ms} (expected ${expectedNext})`);
      }
    }

    // Advance pending block (no content processing)
    pendingBlock = {
      block_start_ms: candidateBlock.ms,
      replay_identity: {
        market,
        block_start_ms: candidateBlock.ms,
        input_path: candidateBlock.fullPath,
      },
    };
  }

  // ═══ EOF: horizon check ═══
  if (pendingBlock !== null) {
    const horizon = checkFinalizedHorizon(pendingBlock, finalizedThroughMs, frozenInventory, dataDir, market, kind);

    if (!horizon.canFinalize) {
      if (horizon.state === 'verified-missing' || horizon.quarantine) {
        writeQuarantineReport(derivedDir, market, pendingBlock.block_start_ms,
          'MISSING_FINALIZED_INPUT',
          { kind, reason: horizon.reason },
          kind);
      }
      emitStructured({
        level: 'BLOCKED',
        processed,
        blocked_reason: horizon.reason || 'not-yet-arrived',
        blocked_state: horizon.state || horizon.reason || 'not-yet-arrived',
        kind,
        market,
        cursor_ms: pendingBlock.block_start_ms,
        expected_block_start_ms: pendingBlock.block_start_ms + BLOCK_DURATION_MS,
      });
      return { processed, errors, manifestUpdates, blocked: true,
        blockedReason: horizon.reason || 'not-yet-arrived',
        blockedState: horizon.state || horizon.reason || 'not-yet-arrived' };
    }

    // Horizon proof valid — write checkpoint for book_updates kind
    if (kind !== 'trades') {
      writeCheckpoint({ market, pending_block: null, last_committed_block_start: pendingBlock.block_start_ms, generation: 0, open_burst: null, kind, derivedDir });
    }
    log('INFO', market, `horizon proof valid, pending=${pendingBlock.block_start_ms}, state=${horizon.state}`, { kind });
  }

  return { processed, errors, manifestUpdates };
}

/**
 * Core block processing loop for trades.
 */
function processBlocks({ dataDir, market, runId, derivedDir, blocks, cp, finalizedThroughMs, frozenInventory, isEofRun, kind = 'trades' }) {
  // P0-1: non-trades path uses simplified processing loop
  if (kind !== 'trades') {
    return processBlocksNonTrade({ dataDir, market, derivedDir, blocks, cp, finalizedThroughMs, frozenInventory, kind });
  }

  const detector = new BurstDetector(market, cp?.open_burst ?? null);

  // P1-1: If restarting from minimal checkpoint, restore open burst and re-feed pending block trades
  const pendingBlockFromCp = cp?.pending_block ?? null;
  let pendingBlock = pendingBlockFromCp;
  let pendingSetViaGap = false;
  let checkpointGeneration = cp?.generation ?? 0;
  // warmup: true only when no prior committed blocks exist (first block ever)
  const hasPriorCommits = cp && (cp.generation > 0 || cp.last_committed_block_start !== null);
  let warmup = !hasPriorCommits;

  // P1-1: On restart with pending block, re-feed trades to rebuild closedBursts
  if (pendingBlock !== null && cp && cp.open_burst) {
    // Validate pending block immutability first
    const replayPath = pendingBlock.replay_identity.input_path;
    const currentSha = sha256File(replayPath);
    if (currentSha !== pendingBlock.trade_input_sha256) {
      // P0-3: hash mismatch on pending block → quarantine, don't advance
      writeQuarantineReport(derivedDir, market, pendingBlock.block_start_ms,
        'E021: pending block input changed on restart',
        { expected: pendingBlock.trade_input_sha256, got: currentSha, path: replayPath });
      throw new Error(`E021: pending block ${pendingBlock.block_start_ms} input changed (expected ${pendingBlock.trade_input_sha256}, got ${currentSha})`);
    }

    // Re-feed pending block trades to rebuild closedBursts in-memory
    const pendingContent = readFileSync(replayPath, 'utf8');
    const { trades: pendingTrades, reordered_input: pr, timestamp_inversion_count: pic } = validateAndParseTrades(pendingContent, pendingBlock.block_start_ms);
    // §4.2: capture reorder audit for legacy checkpoint pending blocks
    pendingBlock.reordered_input = pr;
    pendingBlock.timestamp_inversion_count = pic;
    detector.feedTrades(pendingTrades);
    log('INFO', market, `re-fed ${pendingTrades.length} pending block trades to rebuild state`);
  }

  const committer = new OutputCommitter(market, runId, derivedDir);
  const rollupCommitter = new RollupOutputCommitter(market, `${runId}-30s`, derivedDir);
  // A crash can occur after the durable 1s commit and before its 30s commit.
  // Repair only committed 1s source shards before processing new blocks.
  rollupCommitter.reconcileCommitted1s();
  let fiveMinRecoveryHealthy = true;
  const rollup5minCommitter = new Rollup5minCommitter(market, `${runId}-5min`, derivedDir);
  try {
    rollup5minCommitter.reconcileCommitted30s();
  } catch (error) {
    fiveMinRecoveryHealthy = false;
    emitStructured({ level: 'ERROR', market, msg: `5min recovery blocked: ${error.code || error.message}` });
  }

  // Committed 30s row buffer for inline 5min commit
  const thirtySBuffer = [];
  const thirtySProvenance = [];
  function tryCommit5min(row, sourceMeta = {}) {
    if (!fiveMinRecoveryHealthy) {
      emitStructured({ level: 'ERROR', market, msg: '5min commit blocked because source reconciliation failed' });
      return;
    }
    const sourceQuality = row?._quality;
    if (!sourceQuality || sourceQuality.source_layer !== 'features_1s'
        || !['arrived-valid', 'arrived-empty-valid'].includes(sourceQuality.input_status)
        || sourceQuality.has_missing_input !== false
        || sourceQuality.coverage !== 1
        || sourceQuality.coverage_seconds !== 30
        || sourceQuality.expected_seconds !== 30
        || sourceQuality.finalized !== true) {
      const error = new Error('E_FIVEMIN_INVALID_SOURCE_PROVENANCE: committed 30s source quality is invalid');
      error.code = 'E_FIVEMIN_INVALID_SOURCE_PROVENANCE';
      throw error;
    }
    const normalizedRow = {
      ...row,
      _quality: {
        ...row._quality,
        source_layer: 'features_30s',
        finalized: sourceQuality.finalized,
      },
    };
    thirtySBuffer.push(normalizedRow);
    thirtySProvenance.push(sourceMeta);
    if (thirtySBuffer.length === 10) {
      const aligned = thirtySBuffer.every((r, i) => r.ts === thirtySBuffer[0].ts + i * 30000);
      if (!aligned) {
        const error = new Error('E_FIVEMIN_UNALIGNED_BUFFER: 10 committed 30s rows are not consecutive');
        error.code = 'E_FIVEMIN_UNALIGNED_BUFFER';
        throw error;
      }
    const sourceOutputPaths = thirtySProvenance.map((p) => p.sourceOutputPath);
    const sourceManifestKeys = thirtySProvenance.map((p) => p.sourceManifestKey || p.key);
    if (sourceOutputPaths.length !== 10 || sourceOutputPaths.some((p) => !p)
        || sourceManifestKeys.length !== 10 || sourceManifestKeys.some((p) => !p)) {
      const error = new Error('E_FIVEMIN_PROVENANCE_INCOMPLETE: all 10 source paths and manifest keys are required');
      error.code = 'E_FIVEMIN_PROVENANCE_INCOMPLETE';
      throw error;
    }
    const sourceMetaKey = sourceManifestKeys[9];
    const sourceRowsHash = sha256Content(JSON.stringify(thirtySBuffer));
    rollup5minCommitter.commitWindow({
      rows: thirtySBuffer,
      sourceInputSha256: sourceRowsHash,
      sourceOutputPath: sourceMeta.sourceOutputPath || null,
      sourceOutputHash: sourceMeta.sourceOutputHash || sourceRowsHash,
      sourceManifestKey: sourceMetaKey,
      sourceOutputPaths,
      sourceManifestKeys,
    });
      thirtySBuffer.length = 0;
      thirtySProvenance.length = 0;
    }
  }

  const commitRollupAfter1s = (result, finalizedBlock, rows) => {
    if (rows.some((row) => row._quality?.finalized === false)) {
      const error = new Error('E_ROLLUP_CONTRADICTORY_FINALIZED: input row has finalized=false after durable commit');
      error.code = 'E_ROLLUP_CONTRADICTORY_FINALIZED';
      throw error;
    }
    return rollupCommitter.commitWindow({
      rows: rows.map((row) => ({ ...row, _quality: { ...row._quality, finalized: true } })),
      sourceInputSha256: finalizedBlock.input_sha256,
      sourceOutputPath: result.output_path,
      sourceOutputHash: result.finalHash,
      sourceManifestKey: result.key,
    });
  };
  let processed = 0, errors = 0;
  const manifestUpdates = [];

  for (let i = 0; i < blocks.length; i++) {
    const candidateBlock = blocks[i];  // current N+1 from pending's perspective

    // P1-1 restart: skip blocks already consumed (at or before pending block)
    if (pendingBlock !== null && candidateBlock.ms <= pendingBlock.block_start_ms) {
      continue;
    }

    // Read candidate block content (needed for sha256 in gap handler + trades in normal flow)
    const content = readFileSync(candidateBlock.fullPath, 'utf8');
    const { trades, inputSha256, reordered_input, timestamp_inversion_count } = validateAndParseTrades(content, candidateBlock.ms);

    // P0-2: Validate contiguous 30s order — AFTER reading candidate
    if (pendingBlock !== null) {
      const expectedNext = pendingBlock.block_start_ms + BLOCK_DURATION_MS;
      if (candidateBlock.ms !== expectedNext) {
        // Gap or out-of-order
        if (candidateBlock.ms > expectedNext) {
          // ═══ Gap: candidate N+k > pending N+1 — commit N and continue ═══
          const gapRanges = [{ start_ms: expectedNext, end_ms_exclusive: candidateBlock.ms }];

          // Validate raw lookback for pending N (absent blocks are valid-empty)
          const rawLookback = validateRawTradeLookback(dataDir, market, pendingBlock.block_start_ms);

          // Read pending N's trades for feature computation
          const nTradesContent = readFileSync(pendingBlock.replay_identity.input_path, 'utf8');
          const { trades: nTrades } = validateAndParseTrades(nTradesContent, pendingBlock.block_start_ms);

          // Build notional lookup (absent lookback → zero contribution)
          const tradedNotional30s = buildRawTradedNotionalLookup(dataDir, market, pendingBlock.block_start_ms);

          // 🐛 FIX (Task 7 review blocker): feed candidate N+k trades BEFORE computing N features.
          // This closes N's open burst (if N+k trades are same-side with gap > threshold or
          // different-side), so the burst state is correct when N is committed. The same detector
          // state is saved to the checkpoint, making N+k's subsequent feature computation accurate.
          detector.feedTrades(trades);

          // Compute N features (candidate feed above closes N's open burst correctly)
          const bookSnapshot = loadBookSnapshot(dataDir, market, pendingBlock.block_start_ms);
          const nRows = computeFeatures1s({
            detector,
            blockStartMs: pendingBlock.block_start_ms,
            tradeTsList: nTrades.map(t => t.ts),
            warmup,
            inputBlockIds: [String(pendingBlock.block_start_ms)],
            lookupTradedNotional30s: tradedNotional30s,
            bookSnapshot,
          });

          // Build next pending info from candidate N+k
          const nextPendingInfo = {
            block_start_ms: candidateBlock.ms,
            trade_input_sha256: inputSha256,
            auxiliary_input_hashes: {},
            replay_identity: {
              market,
              block_start_ms: candidateBlock.ms,
              input_path: candidateBlock.fullPath,
            },
            reordered_input,
            timestamp_inversion_count,
          };

          // Commit N with gap audit fields
          const commitId = randomUUID();
          const result = committer.commitFinalizedBlock(
            { block_start_ms: pendingBlock.block_start_ms, input_sha256: pendingBlock.trade_input_sha256, date: formatDate(pendingBlock.block_start_ms), time: formatBlockTime(pendingBlock.block_start_ms) },
            nextPendingInfo,
            detector.getMinimalBurstState(),
            nRows,
            {
              auxiliary_input_hashes: rawLookback.hashes || {},
              assumed_empty_input_blocks: rawLookback.assumedEmptyBlockStarts || [],
              assumed_empty_gap_ranges: gapRanges,
              reordered_input: pendingBlock.reordered_input || false,
              timestamp_inversion_count: pendingBlock.timestamp_inversion_count || 0,
            },
            checkpointGeneration,
            commitId,
            false  // isEofFinalization: false
          );

          const _30r = commitRollupAfter1s(
            result,
            { input_sha256: pendingBlock.trade_input_sha256 },
            nRows,
          );
          tryCommit5min(JSON.parse(readFileSync(_30r.output_path, 'utf8').trim()), {
            sourceOutputPath: _30r.output_path,
            sourceOutputHash: _30r.output_row_hash,
            sourceManifestKey: _30r.key,
          });

          checkpointGeneration = result.nextGeneration;
          manifestUpdates.push({ blockMs: pendingBlock.block_start_ms, ...result });
          processed++;
          warmup = false;
          detector.pruneClosedBurstsBefore(candidateBlock.ms);

          // Emit structured ASSUMED_EMPTY_GAP
          emitStructured({
            level: 'ASSUMED_EMPTY_GAP',
            market,
            gap_ranges: gapRanges,
            committed_block_start_ms: pendingBlock.block_start_ms,
            next_candidate_block_start_ms: candidateBlock.ms,
          });

          // §4.2: ASSUMED_REORDERED_INPUT
          if (pendingBlock.reordered_input) {
            emitStructured({
              level: 'ASSUMED_REORDERED_INPUT',
              market,
              block_start_ms: pendingBlock.block_start_ms,
              input_sha256: pendingBlock.trade_input_sha256,
              timestamp_inversion_count: pendingBlock.timestamp_inversion_count,
            });
          }

          // Set candidate as new pending (skip normal feed/commit)
          pendingBlock = {
            block_start_ms: candidateBlock.ms,
            trade_input_sha256: inputSha256,
            auxiliary_input_hashes: {},
            replay_identity: {
              market,
              block_start_ms: candidateBlock.ms,
              input_path: candidateBlock.fullPath,
            },
            reordered_input,
            timestamp_inversion_count,
          };

          log('INFO', market, `block ${candidateBlock.ms}: ${trades.length} trades, pending after gap (1-block lag)`);
          pendingSetViaGap = true;
          continue;
        }
        // candidateBlock.ms < expectedNext — should not happen (blocks are sorted)
        writeQuarantineReport(derivedDir, market, candidateBlock.ms,
          'out-of-order block', { expected_ms: expectedNext, got_ms: candidateBlock.ms });
        throw new Error(`E006: out-of-order block ${candidateBlock.ms} (expected ${expectedNext})`);
      }
    }

    // ═══ Step 3: validate finalized N required raw trade lookback blocks ═══
    let rawLookback = null;
    if (pendingBlock !== null) {
      rawLookback = validateRawTradeLookback(dataDir, market, pendingBlock.block_start_ms);
      // Absent blocks are now valid-empty with assumedEmptyBlockStarts recorded
      // Never throws E007 for absent blocks (only for malformed existing files)
    }

    // Feed ALL sorted N+1 trades (open burst may close)
    detector.feedTrades(trades);

    // ═══ Step 6: compute N first time with complete #12 lookup ═══
    if (pendingBlock !== null) {
      const tradedNotional30s = buildRawTradedNotionalLookup(dataDir, market, pendingBlock.block_start_ms);

      const nTradesContent = readFileSync(pendingBlock.replay_identity.input_path, 'utf8');
      const { trades: nTrades } = validateAndParseTrades(nTradesContent, pendingBlock.block_start_ms);

      const bookSnapshot = loadBookSnapshot(dataDir, market, pendingBlock.block_start_ms);

      const nRows = computeFeatures1s({
        detector,
        blockStartMs: pendingBlock.block_start_ms,
        tradeTsList: nTrades.map(t => t.ts),
        warmup,
        inputBlockIds: [String(pendingBlock.block_start_ms)],
        lookupTradedNotional30s: tradedNotional30s,
        bookSnapshot,
      });

      // ═══ Step 7: commit N once with N+1 next pending ═══
      const gen = checkpointGeneration;
      const commitId = randomUUID();

      // P1-1: Use minimal state for checkpoint persistence
      const nextPendingInfo = {
        block_start_ms: candidateBlock.ms,
        trade_input_sha256: inputSha256,
        auxiliary_input_hashes: {},
        replay_identity: {
          market,
          block_start_ms: candidateBlock.ms,
          input_path: candidateBlock.fullPath,
        },
        // P1-1: removed open_burst_before_N1 (duplicate of checkpoint open_burst)
        reordered_input,
        timestamp_inversion_count,
      };

      const result = committer.commitFinalizedBlock(
        { block_start_ms: pendingBlock.block_start_ms, input_sha256: pendingBlock.trade_input_sha256, date: formatDate(pendingBlock.block_start_ms), time: formatBlockTime(pendingBlock.block_start_ms) },
        nextPendingInfo,
        detector.getMinimalBurstState(), // P1-1: minimal state for checkpoint
        nRows,
        { 
          auxiliary_input_hashes: rawLookback.hashes || {},
          assumed_empty_input_blocks: rawLookback.assumedEmptyBlockStarts || [],
          assumed_empty_gap_ranges: [],
          reordered_input: pendingBlock.reordered_input || false,
          timestamp_inversion_count: pendingBlock.timestamp_inversion_count || 0,
        },
        gen,
        commitId,
        false  // isEofFinalization: false
      );

      const _30r = commitRollupAfter1s(
        result,
        { input_sha256: pendingBlock.trade_input_sha256 },
        nRows,
      );
      tryCommit5min(JSON.parse(readFileSync(_30r.output_path, 'utf8').trim()), {
            sourceOutputPath: _30r.output_path,
            sourceOutputHash: _30r.output_row_hash,
            sourceManifestKey: _30r.key,
          });

      checkpointGeneration = result.nextGeneration;
      manifestUpdates.push({ blockMs: pendingBlock.block_start_ms, ...result });
      processed++;
      pendingSetViaGap = false;
      warmup = false;

      // P1-2: Prune closed bursts beyond retention window after each commit
      detector.pruneClosedBurstsBefore(candidateBlock.ms);

      // §4.2: ASSUMED_REORDERED_INPUT
      if (pendingBlock.reordered_input) {
        emitStructured({
          level: 'ASSUMED_REORDERED_INPUT',
          market,
          block_start_ms: pendingBlock.block_start_ms,
          input_sha256: pendingBlock.trade_input_sha256,
          timestamp_inversion_count: pendingBlock.timestamp_inversion_count,
        });
      }
    }

    // N+1 becomes new pending — NO rows computed yet
    pendingBlock = {
      block_start_ms: candidateBlock.ms,
      trade_input_sha256: inputSha256,
      auxiliary_input_hashes: {},
      replay_identity: {
        market,
        block_start_ms: candidateBlock.ms,
        input_path: candidateBlock.fullPath,
      },
      reordered_input,
      timestamp_inversion_count,
    };

    log('INFO', market, `block ${candidateBlock.ms}: ${trades.length} trades, pending (1-block lag)`);
  }

  // ═══ P0-4 / P0-1: EOF — only with finalized horizon proof ═══
  if (pendingBlock !== null) {
    const horizon = checkFinalizedHorizon(pendingBlock, finalizedThroughMs, frozenInventory, dataDir, market, kind);
    if (pendingSetViaGap && horizon.state === 'data-none-gap') {
      // Pending block was set via a gap handler, and the EOF check says
      // there's a data-none-gap ahead. Don't finalize — leave pending so
      // the next run can pick up what actually arrives.
      emitStructured({
        level: 'BLOCKED',
        processed,
        blocked_reason: 'next-block-exists',
        blocked_state: 'next-block-exists',
        kind,
        market,
        cursor_ms: pendingBlock.block_start_ms,
        expected_block_start_ms: pendingBlock.block_start_ms + BLOCK_DURATION_MS,
      });
      return { processed, errors, manifestUpdates, blocked: true,
        blockedReason: 'next-block-exists', blockedState: 'next-block-exists' };
    }

    if (!horizon.canFinalize) {
      // No proof — keep pending, exit with blocked reason
      emitStructured({
        level: 'BLOCKED',
        processed,
        blocked_reason: horizon.reason || 'not-yet-arrived',
        blocked_state: horizon.state || horizon.reason || 'not-yet-arrived',
        kind,
        market,
        cursor_ms: pendingBlock.block_start_ms,
        expected_block_start_ms: pendingBlock.block_start_ms + BLOCK_DURATION_MS,
        horizon: finalizedThroughMs,
      });
      return { processed, errors, manifestUpdates, blocked: true, blockedReason: horizon.reason || 'not-yet-arrived', blockedState: horizon.state || horizon.reason || 'not-yet-arrived' };
    }

    // Horizon proof valid — EOF finalize
    detector.flushAll();
    const finalTradesContent = readFileSync(pendingBlock.replay_identity.input_path, 'utf8');
    const { trades: finalTrades } = validateAndParseTrades(finalTradesContent, pendingBlock.block_start_ms);

    const rawLookback = validateRawTradeLookback(dataDir, market, pendingBlock.block_start_ms);
    // Absent blocks are valid-empty at EOF too — no E007 for absent lookback

    const tradedNotional30s = buildRawTradedNotionalLookup(dataDir, market, pendingBlock.block_start_ms);

    const bookSnapshot = loadBookSnapshot(dataDir, market, pendingBlock.block_start_ms);

    const finalRows = computeFeatures1s({
      detector,
      blockStartMs: pendingBlock.block_start_ms,
      tradeTsList: finalTrades.map(t => t.ts),
      warmup,
      inputBlockIds: [String(pendingBlock.block_start_ms)],
      lookupTradedNotional30s: tradedNotional30s,
      bookSnapshot,
    });

    const gen = checkpointGeneration;
    const commitId = randomUUID();
    const result = committer.commitFinalizedBlock(
      { block_start_ms: pendingBlock.block_start_ms, input_sha256: pendingBlock.trade_input_sha256, date: formatDate(pendingBlock.block_start_ms), time: formatBlockTime(pendingBlock.block_start_ms) },
      null,  // nextPendingBlock: null at EOF
      null,  // P1-1: null open_burst at EOF
      finalRows,
      { 
        auxiliary_input_hashes: rawLookback.hashes || {},
        assumed_empty_input_blocks: rawLookback.assumedEmptyBlockStarts || [],
        assumed_empty_gap_ranges: [],
        reordered_input: pendingBlock.reordered_input || false,
        timestamp_inversion_count: pendingBlock.timestamp_inversion_count || 0,
      },
      gen,
      commitId,
      true  // isEofFinalization: true
    );
    checkpointGeneration = result.nextGeneration;
    manifestUpdates.push({ blockMs: pendingBlock.block_start_ms, ...result });
    const _30r = commitRollupAfter1s(
      result,
      { input_sha256: pendingBlock.trade_input_sha256 },
      finalRows,
    );
    tryCommit5min(JSON.parse(readFileSync(_30r.output_path, 'utf8').trim()), {
            sourceOutputPath: _30r.output_path,
            sourceOutputHash: _30r.output_row_hash,
            sourceManifestKey: _30r.key,
          });
    processed++;

    // §4.2: ASSUMED_REORDERED_INPUT
    if (pendingBlock.reordered_input) {
      emitStructured({
        level: 'ASSUMED_REORDERED_INPUT',
        market,
        block_start_ms: pendingBlock.block_start_ms,
        input_sha256: pendingBlock.trade_input_sha256,
        timestamp_inversion_count: pendingBlock.timestamp_inversion_count,
      });
    }
  }

  return { processed, errors, manifestUpdates };
}
