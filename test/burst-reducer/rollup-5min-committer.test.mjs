// test/burst-reducer/rollup-5min-committer.test.mjs — P3-C2 5min committer tests
import assert from 'node:assert/strict';
import { describe, it, beforeEach, after } from 'node:test';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { Rollup5minCommitter, load5minManifest, load5minCheckpoint } from '../../lib/burst-reducer/rollup-5min-committer.mjs';

const ROOT = join('test', 'fixtures', 'burst-v1', 'tmp-c2-5min-committer');
const MARKET = 'c2_5min_test';
const START = 300_000; // 5min-aligned

function sha256(value) {
  return createHash('sha256').update(value).digest('hex');
}

function makeSourceRows({ start = START, empty = false } = {}) {
  return Array.from({ length: 10 }, (_, i) => ({
    ts: start + i * 30_000,
    market: MARKET,
    burst_count_mean_30s: empty ? 0 : i + 1,
    burst_count_max_30s: empty ? 0 : (i % 4) + 1,
    burst_notional_overlap_sum_30s: empty ? 0 : (i + 1) * 10,
    burst_notional_overlap_max_30s: empty ? 0 : (i === 9 ? 200 : (i + 1) * 10),
    burst_notional_overlap_p95_30s: empty ? 0 : (i + 1) * 5,
    max_burst_notional_max_30s: empty ? 0 : (i === 5 ? 500 : (i + 1) * 20),
    max_burst_notional_mean_30s: empty ? 0 : i + 0.5,
    max_burst_prints_max_30s: empty ? 0 : (i % 3) + 1,
    max_burst_duration_max_30s: empty ? 0 : (i === 7 ? 999 : i * 50),
    _quality: {
      source_layer: 'features_30s',
      input_block_ids: ['source-30s-block'],
      input_status: empty ? 'arrived-empty-valid' : 'arrived-valid',
      has_empty_input: empty,
      has_missing_input: false,
      coverage: 1,
      coverage_seconds: 30,
      expected_seconds: 30,
      finalized: true,
      warmup: false,
    },
  }));
}

function make30sManifest(sourceManifestDir, records) {
  mkdirSync(sourceManifestDir, { recursive: true });
  writeFileSync(join(sourceManifestDir, `${MARKET}.json`), JSON.stringify({
    schema_version: 'burst_features_30s_v1',
    namespace: 'features_30s',
    source_layer: 'features_1s',
    market: MARKET,
    last_checkpoint_window_start: START + 9 * 30_000,
    processed_windows: Object.fromEntries(records.map((r, idx) => [
      `burst_features_30s_v1:${MARKET}:${r.ts}:src-hash-${idx}`,
      {
        window_start_ms: r.ts,
        source_layer: 'features_1s',
        source_output_path: `/dev/null/${r.ts}.jsonl`,
        source_row_count: 30,
        output_row_hash: r.fileHash,
        output_path: r.outputPath,
        checkpoint_generation: idx + 1,
        status: 'committed',
      },
    ])),
  }) + '\n');
}

beforeEach(() => {
  rmSync(ROOT, { recursive: true, force: true });
  mkdirSync(ROOT, { recursive: true });
});

after(() => rmSync(ROOT, { recursive: true, force: true }));

describe('P3-C2 Rollup5minCommitter', () => {

  it('commits a normal 5min window with isolated namespace', () => {
    const committer = new Rollup5minCommitter(MARKET, 'run-1', ROOT);
    const result = committer.commitWindow({
      rows: makeSourceRows(),
      sourceInputSha256: 'source-input-hash',
      sourceOutputHash: 'source-output-hash',
    });

    assert.ok(result.key.startsWith('burst_features_5min_v1'));
    assert.ok(existsSync(result.output_path));
    const content = readFileSync(result.output_path, 'utf8').trim();
    const rows = content.split('\n');
    assert.equal(rows.length, 1);
    const parsed = JSON.parse(rows[0]);
    assert.equal(parsed.ts, START);
    assert.equal(parsed.market, MARKET);
    assert.ok(!result.idempotent);

    // Verify manifest
    const manifest = load5minManifest(MARKET, ROOT);
    assert.equal(manifest.namespace, 'features_5min');
    assert.equal(manifest.source_layer, 'features_30s');
    assert.equal(manifest.processed_windows[result.key].status, 'committed');
    assert.equal(manifest.processed_windows[result.key].source_layer, 'features_30s');

    // Verify checkpoint
    const checkpoint = load5minCheckpoint(MARKET, ROOT);
    assert.equal(checkpoint.namespace, 'features_5min');
    assert.equal(checkpoint.last_committed_window_start, START);
    assert.equal(checkpoint.generation, 1);

    // Verify no namespace cross-contamination
    assert.ok(!existsSync(join(ROOT, 'features_30s')));
    assert.ok(!existsSync(join(ROOT, 'features_1s')));
    assert.ok(!existsSync(join(ROOT, 'manifests', `${MARKET}.json`)));
  });

  it('is idempotent — same key and hash returns idempotent=true', () => {
    const committer = new Rollup5minCommitter(MARKET, 'run-1', ROOT);
    const input = { rows: makeSourceRows(), sourceOutputHash: 'input-hash' };
    const first = committer.commitWindow(input);
    const second = new Rollup5minCommitter(MARKET, 'run-2', ROOT).commitWindow(input);

    assert.equal(second.key, first.key);
    assert.equal(second.idempotent, true);
    const manifest = load5minManifest(MARKET, ROOT);
    assert.equal(Object.keys(manifest.processed_windows).length, 1);
  });

  it('rejects hash conflict — same window, different source hash → quarantine', () => {
    const input1 = { rows: makeSourceRows(), sourceOutputHash: 'hash-a' };
    const committer1 = new Rollup5minCommitter(MARKET, 'run-1', ROOT);
    committer1.commitWindow(input1);

    const input2 = { rows: makeSourceRows(), sourceOutputHash: 'hash-b' };
    const committer2 = new Rollup5minCommitter(MARKET, 'run-2', ROOT);
    assert.throws(
      () => committer2.commitWindow(input2),
      (err) => err.code === 'E_FIVEMIN_HASH_CONFLICT'
    );

    // Original data should still exist
    const manifest = load5minManifest(MARKET, ROOT);
    const conflictingKey = Object.keys(manifest.processed_windows).find(k => k.includes('hash-a'));
    assert.ok(conflictingKey, 'original key must exist');
    // The new attempt should be rejected before any write
  });

  it('commits empty-valid window without error', () => {
    const committer = new Rollup5minCommitter(MARKET, 'run-empty', ROOT);
    const result = committer.commitWindow({
      rows: makeSourceRows({ empty: true }),
      sourceOutputHash: 'empty-hash',
    });

    assert.ok(existsSync(result.output_path));
    const parsed = JSON.parse(readFileSync(result.output_path, 'utf8'));
    assert.equal(parsed._quality.input_status, 'arrived-empty-valid');
    assert.equal(parsed._quality.has_empty_input, true);
    assert.equal(result.idempotent, false);
  });

  it('rejects missing input (empty array or null rows)', () => {
    const committer = new Rollup5minCommitter(MARKET, 'run-missing', ROOT);
    assert.throws(
      () => committer.commitWindow({ rows: [], sourceOutputHash: 'x' }),
      (err) => err.code === 'E_5MIN_MISSING_INPUT'
    );
    assert.throws(
      () => committer.commitWindow({ rows: null, sourceOutputHash: 'x' }),
      (err) => err.code === 'E_5MIN_MISSING_INPUT'
    );
  });

  it('recovers missing checkpoint without rewriting 5min shard', () => {
    const committer = new Rollup5minCommitter(MARKET, 'run-1', ROOT);
    const result = committer.commitWindow({
      rows: makeSourceRows(),
      sourceOutputHash: 'repair-hash',
    });

    // Delete checkpoint
    const cpPath = join(ROOT, 'manifests/checkpoints/features_5min', `${MARKET}.json`);
    rmSync(cpPath, { force: true });
    assert.ok(!existsSync(cpPath));

    // Re-run same input → idempotent repair
    const repairResult = new Rollup5minCommitter(MARKET, 'run-2', ROOT).commitWindow({
      rows: makeSourceRows(),
      sourceOutputHash: 'repair-hash',
    });

    assert.equal(repairResult.idempotent, true);
    assert.ok(existsSync(cpPath));
    const checkpoint = load5minCheckpoint(MARKET, ROOT);
    assert.equal(checkpoint.last_committed_window_start, START);
  });

  it('reconciles committed 30s windows into missing 5min rows', () => {
    // Create committed 30s windows covering one 5min bucket
    const sourceDir = join(ROOT, 'features_30s', MARKET, '1970-01-01');
    mkdirSync(sourceDir, { recursive: true });
    const sourceFiles = [];
    for (let i = 0; i < 10; i++) {
      const ts = START + i * 30_000;
      const rows = [{
        ts, market: MARKET,
        burst_count_mean_30s: i + 1,
        burst_count_max_30s: (i % 4) + 1,
        burst_notional_overlap_sum_30s: (i + 1) * 10,
        burst_notional_overlap_max_30s: (i === 9 ? 200 : (i + 1) * 10),
        burst_notional_overlap_p95_30s: (i + 1) * 5,
        max_burst_notional_max_30s: (i === 5 ? 500 : (i + 1) * 20),
        max_burst_notional_mean_30s: i + 0.5,
        max_burst_prints_max_30s: (i % 3) + 1,
        max_burst_duration_max_30s: (i === 7 ? 999 : i * 50),
        _quality: {
          source_layer: 'features_30s',
          input_block_ids: [`src-${i}`],
          input_status: 'arrived-valid',
          has_empty_input: false,
          has_missing_input: false,
          coverage: 1, coverage_seconds: 30, expected_seconds: 30,
          finalized: true, warmup: false,
        },
      }];
      const path = join(sourceDir, `${String(i).padStart(2, '0')}.jsonl`);
      const fileContent = rows.map(r => JSON.stringify(r)).join('\n') + '\n';
      const fileHash = sha256(fileContent);
      writeFileSync(path, fileContent);
      sourceFiles.push({ ts, rows, outputPath: path, fileHash });
    }

    // Write 30s manifest
    const sourceManifestDir = join(ROOT, 'manifests', 'features_30s');
    make30sManifest(sourceManifestDir, sourceFiles);

    // Run reconciliation
    const committer = new Rollup5minCommitter(MARKET, 'recovery-run', ROOT);
    const result = committer.reconcileCommitted30s();

    assert.equal(result.checked, 1);
    assert.equal(result.repaired, 1);
    assert.ok(existsSync(join(ROOT, 'features_5min', MARKET, '1970-01-01')));
    const manifest = load5minManifest(MARKET, ROOT);
    assert.equal(Object.keys(manifest.processed_windows).length, 1);
  });

  it('cleans up orphan staging files on scanOrphanStaging', () => {
    // Create an orphan staging file
    const orphanDir = join(ROOT, 'features_5min', MARKET, '1970-01-01', '.staging', 'orphan-run');
    mkdirSync(orphanDir, { recursive: true });
    writeFileSync(join(orphanDir, 'orphan.jsonl'), '{"ts":1}\n');
    assert.ok(existsSync(join(orphanDir, 'orphan.jsonl')));

    const committer = new Rollup5minCommitter(MARKET, 'scan-run', ROOT);
    const result = committer.scanOrphanStaging();
    assert.equal(result.cleaned, 1);
    assert.ok(!existsSync(orphanDir));
  });
});
