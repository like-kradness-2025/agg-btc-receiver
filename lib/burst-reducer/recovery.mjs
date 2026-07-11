// lib/burst-reducer/recovery.mjs — Fail-closed crash recovery with committed-state verification
// Follows plan Task 3 (P1-2)

import { existsSync, readFileSync, mkdirSync, renameSync, rmSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { createHash } from 'node:crypto';
import { loadManifest, writeManifestRecord, loadCheckpoint, MANIFEST_CORRUPT, CHECKPOINT_CORRUPT } from './manifest-manager.mjs';

function sha256Content(content) {
  return createHash('sha256').update(content).digest('hex');
}

function sha256File(path) {
  if (!existsSync(path)) return null;
  return createHash('sha256').update(readFileSync(path, 'utf8')).digest('hex');
}

/**
 * Reconcile a single intent record:
 * - If final exists: verify SHA-256 of final against staged_row_hash before committing.
 *   If checkpoint exists, also verify generation consistency.
 * - If staged exists + no final: verify hash, rename to final, mark committed.
 * - If neither exists: quarantine.
 *
 * @returns {{ completed: boolean, quarantined: boolean }}
 */
function reconcileIntentRecord(market, key, rec, manifest, derivedDir, cp) {
  const stagedPath = rec.staged_path;
  const outputPath = rec.output_path;
  const stagedExists = stagedPath && existsSync(stagedPath);
  const finalExists = outputPath && existsSync(outputPath);

  if (finalExists) {
    // Bug 3 fix: verify final shard SHA-256 against staged_row_hash
    if (rec.staged_row_hash) {
      const actualHash = sha256File(outputPath);
      if (actualHash !== rec.staged_row_hash) {
        // Hash mismatch → quarantine, clean up staged if present
        try { if (stagedExists) rmSync(stagedPath, { force: true }); } catch (_) {}
        writeManifestRecord(market, key, { ...rec, status: 'quarantined' }, 'quarantined', manifest, derivedDir);
        return { completed: false, quarantined: true };
      }
    }

    // Checkpoint generation consistency — intent record uses > (not >=)
    // because checkpoint is written AFTER renaming staged→final, so
    // rec.gen === cp.gen is the normal case for a successfully-completed commit
    // that crashed before checkpoint write.
    if (cp && rec.checkpoint_generation !== undefined && cp.generation !== undefined) {
      if (rec.checkpoint_generation > cp.generation) {
        // Generation inconsistency → quarantine
        try { if (stagedExists) rmSync(stagedPath, { force: true }); } catch (_) {}
        writeManifestRecord(market, key, { ...rec, status: 'quarantined' }, 'quarantined', manifest, derivedDir);
        return { completed: false, quarantined: true };
      }
    }

    // Final shard verified — mark committed
    if (stagedExists) {
      // Both exist — staged is orphan, final is valid
      try { rmSync(stagedPath, { force: true }); } catch (_) {}
    }
    writeManifestRecord(market, key, {
      ...rec,
      status: 'committed',
      output_row_hash: rec.staged_row_hash,
    }, 'committed', manifest, derivedDir);
    return { completed: true, quarantined: false };
  }

  if (!finalExists && stagedExists) {
    // Staged exists but final doesn't — crash before rename
    // PDD safety fix 2: verify checkpoint consistency before completing
    if (cp) {
      // Generation check: record's checkpoint_generation must not be in the future
      if (rec.checkpoint_generation !== undefined && cp.generation !== undefined) {
        if (rec.checkpoint_generation > cp.generation) {
          // Generation mismatch → quarantine
          try { rmSync(stagedPath, { force: true }); } catch (_) {}
          writeManifestRecord(market, key, { ...rec, status: 'quarantined' }, 'quarantined', manifest, derivedDir);
          return { completed: false, quarantined: true };
        }
      }
      // Cursor check: if cp has a pending block, staged block must match
      if (cp.pending_block && rec.block_start_ms !== cp.pending_block.block_start_ms) {
        // Cursor mismatch → quarantine
        try { rmSync(stagedPath, { force: true }); } catch (_) {}
        writeManifestRecord(market, key, { ...rec, status: 'quarantined' }, 'quarantined', manifest, derivedDir);
        return { completed: false, quarantined: true };
      }
    }
    try {
      const stagedContent = readFileSync(stagedPath, 'utf8');
      const stagedHash = sha256Content(stagedContent);
      if (stagedHash === rec.staged_row_hash) {
        // Hash matches: complete the rename
        mkdirSync(dirname(outputPath), { recursive: true });
        renameSync(stagedPath, outputPath);
        writeManifestRecord(market, key, {
          ...rec,
          status: 'committed',
          output_row_hash: stagedHash,
        }, 'committed', manifest, derivedDir);
        return { completed: true, quarantined: false };
      }
      // Hash mismatch: corrupted
      try { rmSync(stagedPath, { force: true }); } catch (_) {}
    } catch (_) {
      try { rmSync(stagedPath, { force: true }); } catch (_) {}
    }
  }

  // Neither exists (orphan intent) or hash mismatch — quarantine
  // Bug 1 fix: pass 'quarantined' as status, not 'intent'
  writeManifestRecord(market, key, { ...rec, status: 'quarantined' }, 'quarantined', manifest, derivedDir);
  return { completed: false, quarantined: true };
}

/**
 * Verify a committed record: final shard must exist + hash must match +
 * checkpoint generation must be consistent.
 *
 * @returns {{ ok: boolean, reason: string|null }}
 */
function verifyCommittedRecord(rec, cp) {
  // Check final shard file exists
  if (!rec.output_path || !existsSync(rec.output_path)) {
    return { ok: false, reason: 'missing-final-shard' };
  }

  // Check final shard hash matches manifest
  if (rec.output_row_hash) {
    const actualHash = sha256File(rec.output_path);
    if (actualHash !== rec.output_row_hash) {
      return { ok: false, reason: `final-shard-hash-mismatch: expected ${rec.output_row_hash}, got ${actualHash}` };
    }
  }

  // Check checkpoint generation consistency
  if (cp && rec.checkpoint_generation !== undefined && cp.generation !== undefined) {
    // The manifest's checkpoint_generation is the gen from BEFORE this commit.
    // The checkpoint's generation is the gen AFTER this commit.
    // So they should differ by 1 if this is the most recent commit.
    // But for robustness, just check they're not wildly inconsistent.
    // The committed record's generation should be < the checkpoint's generation
    // (since checkpoint generation advances on each commit).
    if (rec.checkpoint_generation >= cp.generation) {
      return { ok: false, reason: `generation-consistency: record gen ${rec.checkpoint_generation} >= cp gen ${cp.generation}` };
    }
  }

  return { ok: true, reason: null };
}

/**
 * Reconcile ALL manifest records (intent + committed) for a market.
 *
 * For intent records: attempt to complete the rename; quarantine on failure.
 * For committed records: verify final shard exists, hash matches, checkpoint consistent.
 *
 * @param {string} market
 * @param {string} derivedDir - derived output root
 * @returns {{ cursor: Object|null, generation: number, quarantinedKeys: string[], errors: string[] }}
 *   cursor: loaded checkpoint (or null)
 *   generation: checkpoint generation (0 if no checkpoint)
 *   quarantinedKeys: keys that were quarantined during reconciliation
 *   errors: descriptive errors for quarantined committed records
 */
export function reconcileMarketState(market, derivedDir) {
  const manifest = loadManifest(market, derivedDir);
  if (manifest === MANIFEST_CORRUPT) {
    process.stderr.write(JSON.stringify({
      ts: new Date().toISOString(),
      level: 'FATAL',
      market,
      msg: 'corrupt-manifest: cannot start processing, manifest backed up as .bak',
    }) + '\n');
    return {
      cursor: null,
      generation: 0,
      quarantinedKeys: [],
      errors: ['corrupt-manifest: manifest file was empty or invalid JSON, backed up as .bak'],
      status: 'corrupt-manifest',
    };
  }
  if (!manifest || !manifest.processed_blocks) {
    // No manifest — nothing to reconcile
    const cp = loadCheckpoint(market, derivedDir);
    if (cp === CHECKPOINT_CORRUPT) {
      process.stderr.write(JSON.stringify({
        ts: new Date().toISOString(),
        level: 'FATAL',
        market,
        msg: 'corrupt-checkpoint: cannot start processing, checkpoint backed up as .bak',
      }) + '\n');
      return {
        cursor: null,
        generation: 0,
        quarantinedKeys: [],
        errors: ['corrupt-checkpoint: checkpoint file was empty or invalid JSON, backed up as .bak'],
        status: 'corrupt-checkpoint',
      };
    }
    return {
      cursor: cp,
      generation: cp?.generation ?? 0,
      quarantinedKeys: [],
      errors: [],
    };
  }

  const cp = loadCheckpoint(market, derivedDir);
  if (cp === CHECKPOINT_CORRUPT) {
    process.stderr.write(JSON.stringify({
      ts: new Date().toISOString(),
      level: 'FATAL',
      market,
      msg: 'corrupt-checkpoint: cannot start processing, checkpoint backed up as .bak',
    }) + '\n');
    return {
      cursor: null,
      generation: 0,
      quarantinedKeys: [],
      errors: ['corrupt-checkpoint: checkpoint file was empty or invalid JSON, backed up as .bak'],
      status: 'corrupt-checkpoint',
    };
  }
  const entries = Object.entries(manifest.processed_blocks);
  const quarantinedKeys = [];
  const errors = [];
  let reconciledCount = 0;
  let quarantinedCount = 0;

  for (const [key, rec] of entries) {
    if (rec.status === 'intent') {
      const result = reconcileIntentRecord(market, key, rec, manifest, derivedDir, cp);
      if (result.completed) reconciledCount++;
      if (result.quarantined) {
        quarantinedKeys.push(key);
        quarantinedCount++;
        errors.push(`intent record ${key}: staged file missing or hash mismatch`);
      }
    } else if (rec.status === 'committed') {
      const result = verifyCommittedRecord(rec, cp);
      if (!result.ok) {
        // Quarantine the record — Bug 1 fix: pass 'quarantined' not 'committed'
        writeManifestRecord(market, key, {
          ...rec,
          status: 'quarantined',
        }, 'quarantined', manifest, derivedDir);
        quarantinedKeys.push(key);
        quarantinedCount++;
        errors.push(`committed record ${key}: ${result.reason}`);
      }
    }
    // quarantined status records are left as-is
  }

  if (reconciledCount > 0 || quarantinedCount > 0) {
    // Use process.stderr for structured logging (same as pipeline)
    process.stderr.write(JSON.stringify({
      ts: new Date().toISOString(),
      level: 'INFO',
      market,
      msg: `reconcile: ${reconciledCount} completed, ${quarantinedCount} quarantined`,
    }) + '\n');
  }

  return {
    cursor: loadCheckpoint(market, derivedDir),  // reload in case reconcile wrote files
    generation: cp?.generation ?? 0,
    quarantinedKeys,
    errors,
  };
}
