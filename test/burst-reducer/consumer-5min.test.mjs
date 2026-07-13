// test/burst-reducer/consumer-5min.test.mjs — P3-C2 5min consumer tests
import assert from 'node:assert/strict';
import { describe, it, beforeEach, after } from 'node:test';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, rmSync, writeFileSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { FiveMinConsumer, validate5minRow } from '../../lib/burst-reducer/consumer-5min.mjs';
import { Rollup5minCommitter } from '../../lib/burst-reducer/rollup-5min-committer.mjs';

const ROOT = join('test', 'fixtures', 'burst-v1', 'tmp-c2-5min-consumer');
const MARKET = 'c2_5min_consumer_test';
const START = 300_000;

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
      input_block_ids: ['src-block'],
      input_status: empty ? 'arrived-empty-valid' : 'arrived-valid',
      has_empty_input: empty,
      has_missing_input: false,
      coverage: 1, coverage_seconds: 30, expected_seconds: 30,
      finalized: true, warmup: false,
    },
  }));
}

function commitWindow(market, rows, hash) {
  const committer = new Rollup5minCommitter(market, 'consumer-test', ROOT);
  return committer.commitWindow({ rows, sourceOutputHash: hash || 'test-hash' });
}

beforeEach(() => {
  rmSync(ROOT, { recursive: true, force: true });
  mkdirSync(ROOT, { recursive: true });
});

after(() => rmSync(ROOT, { recursive: true, force: true }));

describe('FiveMinConsumer', () => {

  it('reads only committed records from the manifest', () => {
    const result = commitWindow(MARKET, makeSourceRows(), 'commit-a');
    const consumer = new FiveMinConsumer(MARKET, ROOT);
    const records = consumer.getCommittedRecords();

    assert.equal(records.length, 1);
    assert.equal(records[0].window_start_ms, START);
    assert.equal(records[0].output_path, result.output_path);
    assert.equal(records[0].output_row_hash, result.output_row_hash);
  });

  it('validates row schema correctly', () => {
    commitWindow(MARKET, makeSourceRows(), 'valid');
    const consumer = new FiveMinConsumer(MARKET, ROOT);
    const records = consumer.getCommittedRecords();
    const { rows, hash_ok, validation } = consumer.readAndValidate(records[0]);

    assert.equal(rows.length, 1);
    assert.equal(hash_ok, true);
    assert.equal(validation[0].valid, true);

    // Test validate5minRow directly
    const badRow = { ts: START, market: MARKET, _quality: {} };
    const result = validate5minRow(badRow, MARKET);
    assert.equal(result.valid, false);
    assert.ok(result.reasons.length > 0);
  });

  it('performs range query correctly', () => {
    // Commit two windows
    const start2 = START + 300_000; // second 5min window
    commitWindow(MARKET, makeSourceRows(), 'range-1');
    commitWindow(MARKET, makeSourceRows({ start: start2 }), 'range-2');

    const consumer = new FiveMinConsumer(MARKET, ROOT);
    const all = consumer.queryRange(0, Infinity);
    assert.equal(all.length, 2);

    const firstOnly = consumer.queryRange(START, start2);
    assert.equal(firstOnly.length, 1);
    assert.equal(firstOnly[0].ts, START);

    const empty = consumer.queryRange(start2 + 1, Infinity);
    assert.equal(empty.length, 0);
  });

  it('reports diagnostic status correctly', () => {
    commitWindow(MARKET, makeSourceRows(), 'diag-a');

    const consumer = new FiveMinConsumer(MARKET, ROOT);
    const status = consumer.getStatus();

    assert.equal(status.length, 1);
    assert.equal(status[0].status, 'committed');
    assert.equal(status[0].output_exists, true);
    assert.equal(status[0].hash_ok, true);
  });

  it('detects hash mismatch in diagnostic status', () => {
    const result = commitWindow(MARKET, makeSourceRows(), 'hash-test');

    // Corrupt the output file
    writeFileSync(result.output_path, '{"corrupted":true}\n');

    const consumer = new FiveMinConsumer(MARKET, ROOT);
    const status = consumer.getStatus();

    assert.equal(status[0].status, 'committed');
    assert.equal(status[0].output_exists, true);
    assert.equal(status[0].hash_ok, false);
  });

  it('handles missing manifest gracefully with error', () => {
    const consumer = new FiveMinConsumer('nonexistent_market', ROOT);
    assert.throws(
      () => consumer.loadManifest(),
      (err) => err.code === 'E_FIVEMIN_CONS_MISSING_MANIFEST'
    );
    assert.throws(
      () => consumer.queryRange(0, Infinity),
      (err) => err.code === 'E_FIVEMIN_CONS_MISSING_MANIFEST'
    );
  });

  it('excludes blocked/quarantined records from committed output', () => {
    // Normal commit
    commitWindow(MARKET, makeSourceRows(), 'good-window');

    // Add blocked and quarantined entries directly to manifest
    const consumer = new FiveMinConsumer(MARKET, ROOT);
    const manifestPath = join(ROOT, 'manifests', 'features_5min', `${MARKET}.json`);
    const fullManifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
    fullManifest.processed_windows['blocked:key'] = {
      window_start_ms: START + 600_000,
      status: 'blocked',
      output_path: '/dev/null/nope.jsonl',
    };
    fullManifest.processed_windows['quarantined:key'] = {
      window_start_ms: START + 900_000,
      status: 'quarantined',
      quarantined_reason: 'hash-conflict',
    };
    writeFileSync(manifestPath, JSON.stringify(fullManifest, null, 2) + '\n');

    const committed = consumer.getCommittedRecords();
    assert.equal(committed.length, 1);
    assert.equal(committed[0].window_start_ms, START);

    // Status should still show all records
    const status = consumer.getStatus();
    assert.equal(status.length, 3);
    const blockedStatus = status.find(s => s.status === 'blocked');
    assert.ok(blockedStatus);
    const quarantinedStatus = status.find(s => s.status === 'quarantined');
    assert.ok(quarantinedStatus);

    // Range query should only return committed
    const rangeResults = consumer.queryRange(0, Infinity);
    assert.equal(rangeResults.length, 1);
  });
});
