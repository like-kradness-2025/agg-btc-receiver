import { createHash } from 'node:crypto';
import { existsSync, readFileSync, writeFileSync, mkdirSync, readdirSync, renameSync, unlinkSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { aggregate5min } from './rollup.mjs';
import { sha256 } from './schema.mjs';

const FIVEMIN_SCHEMA_VERSION = 'burst_features_5min_v1';
const FEATURES_5MIN_DIR = 'features_5min';
const FEATURES_30S_DIR = 'features_30s';
const MANIFESTS_DIR = 'manifests';
const CHECKPOINTS_DIR = 'checkpoints';

function sha256Content(value) {
  return createHash('sha256').update(value).digest('hex');
}

function parseJsonl(content, path) {
  if (!content || !content.trim()) return [];
  return content.trim().split('\n').filter(Boolean).map((line, i) => {
    try { return JSON.parse(line); }
    catch { throw Object.assign(new Error(`E_FIVEMIN_PARSE: invalid JSONL at ${path}:${i + 1}`), { code: 'E_FIVEMIN_PARSE' }); }
  });
}

function readJsonOrNull(path) {
  try { return JSON.parse(readFileSync(path, 'utf8')); } catch { return null; }
}

export class Rollup5minCommitter {
  constructor(market, runId, derivedDir) {
    this._market = market;
    this._runId = runId;
    this._derivedDir = derivedDir;
    this._featuresDir = FEATURES_5MIN_DIR;
  }

  commitWindow({ rows, sourceInputSha256, sourceOutputPath, sourceOutputHash, sourceManifestKey = null, sourceOutputPaths = [], sourceManifestKeys = [] }) {
    if ((sourceOutputPaths.length > 0 && (sourceOutputPaths.length !== 10 || sourceOutputPaths.some((p) => !p)))
        || (sourceManifestKeys.length > 0 && (sourceManifestKeys.length !== 10 || sourceManifestKeys.some((k) => !k)))) {
      const err = new Error('E_FIVEMIN_PROVENANCE_INCOMPLETE: source provenance arrays must contain 10 non-empty entries');
      err.code = 'E_FIVEMIN_PROVENANCE_INCOMPLETE';
      throw err;
    }
    const [row] = aggregate5min(rows);
    const content = `${JSON.stringify(row)}\n`;
    const outputHash = sha256Content(content);
    const inputHash = sourceInputSha256 || sourceOutputHash || sha256Content(JSON.stringify(rows));
    const key = `${FIVEMIN_SCHEMA_VERSION}:${this._market}:${row.ts}:${inputHash}`;
    const date = new Date(row.ts).toISOString().slice(0, 10);
    const time = new Date(row.ts).toISOString().slice(11, 19).replace(/:/g, '-');

    const manifest = this._loadManifest();
    const existing = manifest.processed_windows?.[key];
    if (existing && existing.status === 'committed' && existing.output_row_hash === outputHash) {
      const checkpoint = { schema_version: FIVEMIN_SCHEMA_VERSION, namespace: this._featuresDir, source_layer: FEATURES_30S_DIR, market: this._market, last_committed_window_start: row.ts, generation: manifest.generation || 1, output_path: existing.output_path, output_row_hash: outputHash, updated_at: new Date().toISOString() };
      this._writeCheckpoint(checkpoint);
      return { key, output_path: existing.output_path, output_row_hash: outputHash, nextGeneration: null, idempotent: true };
    }
    if (existing && existing.status === 'committed' && existing.output_row_hash !== outputHash) {
      const quarantinePath = existing.output_path + '.quarantine.' + Date.now();
      if (existsSync(existing.output_path)) renameSync(existing.output_path, quarantinePath);
      const err = new Error(`E_FIVEMIN_HASH_CONFLICT: re-commit produced different hash for key ${key}`);
      err.code = 'E_FIVEMIN_HASH_CONFLICT';
      throw err;
    }

    const pathPrefix = join(this._derivedDir, this._featuresDir, this._market, date);
    mkdirSync(join(this._derivedDir, this._featuresDir, this._market, date), { recursive: true });
    const stagingDir = join(this._derivedDir, this._featuresDir, this._market, date, '.staging');
    mkdirSync(stagingDir, { recursive: true });
    const stagedPath = join(stagingDir, `${this._runId}-5min/${time}.jsonl`);
    mkdirSync(dirname(stagedPath), { recursive: true });
    writeFileSync(stagedPath, content);

    const intentDir = join(this._derivedDir, this._featuresDir, 'intents');
    mkdirSync(intentDir, { recursive: true });
    writeFileSync(join(intentDir, `${this._market}-${row.ts}.json`), JSON.stringify({ key, stagedPath, sourceInputSha256: inputHash, sourceOutputHash, sourceManifestKey, sourceOutputPaths, sourceManifestKeys, ts: row.ts, market: this._market }));

    const outputPath = join(pathPrefix, `${time}.jsonl`);
    renameSync(stagedPath, outputPath);

    const actualHash = sha256Content(readFileSync(outputPath, 'utf8'));
    if (actualHash !== outputHash) {
      renameSync(outputPath, outputPath + '.quarantine.' + Date.now());
      const err = new Error(`E_FIVEMIN_RENAME_HASH: output file hash mismatch after rename`);
      err.code = 'E_FIVEMIN_RENAME_HASH';
      throw err;
    }

    const gen = (manifest.generation || 0) + 1;
    manifest.schema_version = FIVEMIN_SCHEMA_VERSION;
    manifest.namespace = this._featuresDir;
    manifest.source_layer = FEATURES_30S_DIR;
    manifest.market = this._market;
    if (!manifest.processed_windows) manifest.processed_windows = {};
    manifest.processed_windows[key] = {
      window_start_ms: row.ts,
      source_layer: FEATURES_30S_DIR,
      source_input_sha256: inputHash,
      source_output_hash: inputHash,
      source_output_path: sourceOutputPath || null,
      source_manifest_key: sourceManifestKey,
      source_output_paths: sourceOutputPaths,
      source_manifest_keys: sourceManifestKeys,
      source_row_count: rows.length,
      output_row_hash: outputHash,
      staged_path: stagedPath,
      output_path: outputPath,
      checkpoint_generation: gen,
      status: 'committed',
    };
    manifest.generation = gen;
    this._writeManifest(manifest);

    try { unlinkSync(join(intentDir, `${this._market}-${row.ts}.json`)); } catch {}

    const checkpoint = {
      schema_version: FIVEMIN_SCHEMA_VERSION,
      namespace: this._featuresDir,
      source_layer: FEATURES_30S_DIR,
      market: this._market,
      last_committed_window_start: row.ts,
      generation: gen,
      output_path: outputPath,
      output_row_hash: outputHash,
      updated_at: new Date().toISOString(),
    };
    this._writeCheckpoint(checkpoint);

    return { key, output_path: outputPath, output_row_hash: outputHash, nextGeneration: gen, idempotent: false };
  }

  reconcileCommitted30s() {
    const sourceManifestPath = join(this._derivedDir, MANIFESTS_DIR, `${FEATURES_30S_DIR}`, `${this._market}.json`);
    const sourceManifest = readJsonOrNull(sourceManifestPath);
    if (!sourceManifest) return { repaired: 0, checked: 0 };

    const thirtySRecords = [];
    for (const [sourceKey, record] of Object.entries(sourceManifest.processed_windows || {})) {
      if (record?.status !== 'committed') continue;
      if (!existsSync(record.output_path)) continue;
      const sourceContent = readFileSync(record.output_path, 'utf8');
      const actualHash = sha256Content(sourceContent);
      if (record.output_row_hash && actualHash !== record.output_row_hash) {
        const err = new Error(`E_FIVEMIN_SOURCE_HASH: committed 30s window hash mismatch at ${record.output_path}`);
        err.code = 'E_FIVEMIN_SOURCE_HASH';
        throw err;
      }
      const rows = parseJsonl(sourceContent, record.output_path);
      const normalizedRows = rows.map((row) => {
        const sourceLayer = row?._quality?.source_layer;
        if (!row?._quality || !['features_1s', FEATURES_30S_DIR].includes(sourceLayer)
            || !['arrived-valid', 'arrived-empty-valid'].includes(row._quality.input_status)
            || row._quality.has_missing_input !== false || row._quality.coverage !== 1
            || row._quality.coverage_seconds !== 30 || row._quality.expected_seconds !== 30
            || row._quality.finalized !== true) {
          const err = new Error(`E_FIVEMIN_INVALID_SOURCE_PROVENANCE: invalid 30s quality at ${record.output_path}`);
          err.code = 'E_FIVEMIN_INVALID_SOURCE_PROVENANCE';
          throw err;
        }
        return { ...row, _quality: { ...row._quality, source_layer: FEATURES_30S_DIR, finalized: row._quality.finalized } };
      });
      for (const row of normalizedRows) {
        if (typeof row.ts === 'number') {
          thirtySRecords.push({ ts: row.ts, rows: normalizedRows, key: sourceKey, outputPath: record.output_path, hash: actualHash });
        }
      }
    }

    thirtySRecords.sort((a, b) => a.ts - b.ts);
    let checked = 0;
    let repaired = 0;
    let firstRecoveryError = null;
    let i = 0;
    while (i + 9 < thirtySRecords.length) {
      const bucket = thirtySRecords.slice(i, i + 10);
      let aligned = true;
      for (let j = 1; j < 10; j++) {
        if (bucket[j].ts !== bucket[0].ts + j * 30_000) { aligned = false; break; }
      }
      if (aligned) {
        checked++;
        const combinedRows = bucket.flatMap(b => b.rows);
        const sourceHash = sha256Content(JSON.stringify(combinedRows));
        try {
          const result = this.commitWindow({
            rows: combinedRows,
            sourceInputSha256: null,
            sourceOutputHash: sourceHash,
            sourceOutputPath: bucket[bucket.length - 1].outputPath,
            sourceManifestKey: bucket[bucket.length - 1].key,
            sourceOutputPaths: bucket.map((b) => b.outputPath),
            sourceManifestKeys: bucket.map((b) => b.key),
          });
          if (!result.idempotent) repaired++;
        } catch (error) {
          if (!firstRecoveryError) firstRecoveryError = error;
        }
      }
      i += 10;
    }
    if (firstRecoveryError) throw firstRecoveryError;
    return { repaired, checked };
  }

  _loadManifest() {
    const path = join(this._derivedDir, MANIFESTS_DIR, this._featuresDir, `${this._market}.json`);
    return readJsonOrNull(path) || { schema_version: FIVEMIN_SCHEMA_VERSION, namespace: this._featuresDir, source_layer: FEATURES_30S_DIR, market: this._market, processed_windows: {}, generation: 0 };
  }

  _writeManifest(manifest) {
    const dir = join(this._derivedDir, MANIFESTS_DIR, this._featuresDir);
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, `${this._market}.json`), JSON.stringify(manifest, null, 2));
  }

  _writeCheckpoint(checkpoint) {
    const dir = join(this._derivedDir, MANIFESTS_DIR, CHECKPOINTS_DIR, this._featuresDir);
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, `${this._market}.json`), JSON.stringify(checkpoint, null, 2));
  }
}
