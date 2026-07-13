// lib/burst-reducer/consumer-5min.mjs — Manifest committed-only reader with validation
// P3-C2: reads only features_5min committed rows, validates schema, supports range queries

import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { FEATURES_5MIN_DIR, FEATURES_30S_DIR, MANIFESTS_DIR } from './schema.mjs';
import { load5minManifest, FIVEMIN_SCHEMA_VERSION } from './rollup-5min-committer.mjs';

// ── Error codes ────────────────────────────────────────────────────────

const E_MISSING_MANIFEST = 'E_FIVEMIN_CONS_MISSING_MANIFEST';
const E_OUTPUT_MISSING = 'E_FIVEMIN_CONS_OUTPUT_MISSING';
const E_INVALID_ROW = 'E_FIVEMIN_CONS_INVALID_ROW';

// ── Diagnostics ────────────────────────────────────────────────────────

export const RECORD_STATUS = Object.freeze({
  COMMITTED: 'committed',
  BLOCKED: 'blocked',
  QUARANTINED: 'quarantined',
  INTENT: 'intent',
});

// ── Helpers ────────────────────────────────────────────────────────────

function sha256(value) {
  return createHash('sha256').update(value).digest('hex');
}

function readJsonl(path) {
  if (!existsSync(path)) throw Object.assign(new Error(`Output file not found: ${path}`), { code: E_OUTPUT_MISSING });
  const content = readFileSync(path, 'utf8');
  const rows = [];
  for (const [idx, line] of content.split('\n').entries()) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      rows.push(JSON.parse(trimmed));
    } catch (err) {
      throw Object.assign(
        new Error(`Invalid JSON at ${path}:${idx + 1}: ${err.message}`),
        { code: E_INVALID_ROW, cause: err }
      );
    }
  }
  return rows;
}

// ── Row validation per contract ────────────────────────────────────────

/**
 * Validate a single 5min row against the approved schema.
 * Contract: source_layer=features_30s, source_window_count=10,
 * coverage=1, has_missing_input=false, finalized=true, ts=5min-aligned.
 *
 * @param {Object} row
 * @param {string} market
 * @returns {{ valid: boolean, reasons: string[] }}
 */
export function validate5minRow(row, market) {
  const reasons = [];

  if (!row || typeof row !== 'object') {
    return { valid: false, reasons: ['row is not an object'] };
  }
  if (typeof row.ts !== 'number') reasons.push('ts must be a number');
  else if (row.ts % 300_000 !== 0) reasons.push('ts must be 5min-aligned');
  if (row.market !== market) reasons.push(`market mismatch: expected ${market}, got ${row.market}`);

  const q = row._quality;
  if (!q || typeof q !== 'object') {
    reasons.push('_quality is required');
  } else {
    if (q.source_layer !== 'features_30s') reasons.push(`source_layer must be features_30s, got ${q.source_layer}`);
    if (q.source_window_count !== 10) reasons.push(`source_window_count must be 10, got ${q.source_window_count}`);
    if (typeof q.source_window_start_ms !== 'number') reasons.push('source_window_start_ms required');
    if (typeof q.source_window_end_ms !== 'number') reasons.push('source_window_end_ms required');
    if (q.coverage !== 1) reasons.push(`coverage must be 1, got ${q.coverage}`);
    if (q.coverage_seconds !== 300) reasons.push(`coverage_seconds must be 300, got ${q.coverage_seconds}`);
    if (q.has_missing_input !== false) reasons.push('has_missing_input must be false');
    if (q.finalized !== true) reasons.push('finalized must be true');
    if (q.input_status !== 'arrived-valid' && q.input_status !== 'arrived-empty-valid') {
      reasons.push(`input_status must be arrived-valid or arrived-empty-valid, got ${q.input_status}`);
    }
    if (typeof q.has_empty_input !== 'boolean') reasons.push('has_empty_input must be boolean');
  }

  return { valid: reasons.length === 0, reasons };
}

// ── Consumer class ─────────────────────────────────────────────────────

export class FiveMinConsumer {
  /**
   * @param {string} market
   * @param {string} derivedDir
   */
  constructor(market, derivedDir) {
    this._market = market;
    this._derivedDir = derivedDir;
    this._manifestDir = join(derivedDir, MANIFESTS_DIR, FEATURES_5MIN_DIR);
    this._featuresDir = join(derivedDir, FEATURES_5MIN_DIR);
  }

  /**
   * Load the 5min manifest. Throws if missing or corrupt.
   * @returns {Object}
   */
  loadManifest() {
    const manifest = load5minManifest(this._market, this._derivedDir);
    if (manifest === null) {
      throw Object.assign(new Error(`No 5min manifest for ${this._market}`), { code: E_MISSING_MANIFEST });
    }
    return manifest;
  }

  /**
   * Enumerate all committed records from the manifest.
   * Returns only records where status === 'committed'.
   * @returns {Array<{ key, window_start_ms, output_path, output_row_hash, source_output_hash, record }>}
   */
  getCommittedRecords() {
    const manifest = this.loadManifest();
    if (!manifest.processed_windows) return [];

    const results = [];
    for (const [key, rec] of Object.entries(manifest.processed_windows)) {
      if (rec?.status === 'committed') {
        results.push({
          key,
          window_start_ms: rec.window_start_ms,
          output_path: rec.output_path,
          output_row_hash: rec.output_row_hash,
          source_output_hash: rec.source_output_hash,
          record: rec,
        });
      }
    }
    results.sort((a, b) => a.window_start_ms - b.window_start_ms);
    return results;
  }

  /**
   * Read a committed record's output file and validate its row content.
   * @param {Object} record — from getCommittedRecords()
   * @returns {{ rows: Array, hash_ok: boolean, validation: Array<{ valid, reasons }> }}
   */
  readAndValidate(record) {
    const outputHash = sha256(readFileSync(record.output_path, 'utf8'));
    const hashOk = outputHash === record.output_row_hash;

    const rows = readJsonl(record.output_path);
    // 5min output should be exactly 1 row
    const validations = rows.map(row => validate5minRow(row, this._market));

    return { rows, hash_ok: hashOk, validation: validations };
  }

  /**
   * Range query: return committed rows within [fromMs, toMs).
   * Each result includes validated row data and integrity info.
   *
   * @param {number} fromMs — inclusive
   * @param {number} toMs — exclusive
   * @param {Object} [opts]
   * @param {boolean} [opts.requireHashOk=false] — skip records with hash mismatch
   * @param {boolean} [opts.requireValid=true] — skip records with validation failures
   * @returns {Array<{ ts: number, market: string, row: Object, hash_ok: boolean, validation: Object }>}
   */
  queryRange(fromMs, toMs, opts = {}) {
    const { requireHashOk = false, requireValid = true } = opts;
    const records = this.getCommittedRecords().filter(
      r => r.window_start_ms >= fromMs && r.window_start_ms < toMs
    );

    const results = [];
    for (const rec of records) {
      const { rows, hash_ok, validation } = this.readAndValidate(rec);
      if (requireHashOk && !hash_ok) continue;
      for (let i = 0; i < rows.length; i++) {
        if (requireValid && !validation[i].valid) continue;
        results.push({
          ts: rows[i].ts,
          market: this._market,
          row: rows[i],
          hash_ok,
          validation: validation[i],
        });
      }
    }
    return results.sort((a, b) => a.ts - b.ts);
  }

  /**
   * Diagnostic status: report all windows in the manifest with their status,
   * output existence, and hash integrity.
   *
   * Blocked/quarantined/missing windows get a status entry but no data row.
   *
   * @returns {Array<{ window_start_ms: number, status: string, output_exists: boolean, hash_ok: boolean|null, error?: string }>}
   */
  getStatus() {
    const manifest = this.loadManifest();
    if (!manifest.processed_windows) return [];

    const diagnostics = [];
    for (const [key, rec] of Object.entries(manifest.processed_windows)) {
      const diag = {
        key,
        window_start_ms: rec.window_start_ms,
        status: rec.status || 'unknown',
        output_exists: false,
        hash_ok: null,
      };
      if (rec.output_path && existsSync(rec.output_path)) {
        diag.output_exists = true;
        if (rec.output_row_hash) {
          diag.hash_ok = sha256(readFileSync(rec.output_path, 'utf8')) === rec.output_row_hash;
        }
      }
      if (rec.status === 'quarantined' && rec.quarantined_reason) {
        diag.error = rec.quarantined_reason;
      }
      diagnostics.push(diag);
    }
    return diagnostics.sort((a, b) => (a.window_start_ms || 0) - (b.window_start_ms || 0));
  }
}
