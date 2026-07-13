// test/burst-reducer/rollup-output-committer.test.mjs — C2 isolated 30s output
import assert from 'node:assert/strict';
import { describe, it, beforeEach, after } from 'node:test';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { createBaseRow } from '../../lib/burst-reducer/schema.mjs';
import {
  RollupOutputCommitter,
  loadRollupManifest,
  loadRollupCheckpoint,
} from '../../lib/burst-reducer/rollup-output-committer.mjs';

const ROOT = join('test', 'fixtures', 'burst-v1', 'tmp-c2-rollup-output');
const MARKET = 'c2_committer';

function sha256(value) {
  return createHash('sha256').update(value).digest('hex');
}

function makeRows(start = 30_000) {
  return Array.from({ length: 30 }, (_, i) => createBaseRow(start + i * 1000, MARKET, {
    trade_count_this_second: i === 0 ? 1 : 0,
    input_block_ids: [String(start)],
    finalized: true,
    warmup: false,
  }));
}

beforeEach(() => {
  rmSync(ROOT, { recursive: true, force: true });
  mkdirSync(ROOT, { recursive: true });
});

after(() => rmSync(ROOT, { recursive: true, force: true }));

describe('C2 RollupOutputCommitter', () => {
  it('writes one 30s row under an isolated features_30s namespace', () => {
    const committer = new RollupOutputCommitter(MARKET, 'c2-run-1', ROOT);
    const result = committer.commitWindow({
      rows: makeRows(),
      sourceInputSha256: 'source-input-hash',
      sourceOutputPath: join(ROOT, 'features_1s', MARKET, '1970-01-01', '00-00-30.jsonl'),
      sourceOutputHash: 'source-output-hash',
    });

    assert.equal(result.output_path, join(ROOT, 'features_30s', MARKET, '1970-01-01', '00-00-30.jsonl'));
    assert.ok(existsSync(result.output_path));
    assert.equal(readFileSync(result.output_path, 'utf8').trim().split('\n').length, 1);
    assert.ok(!existsSync(join(ROOT, 'features_30s', 'features_1s')));
    assert.ok(!existsSync(join(ROOT, 'features_1s')));

    const manifest = loadRollupManifest(MARKET, ROOT);
    assert.equal(manifest.namespace, 'features_30s');
    assert.equal(manifest.processed_windows[result.key].status, 'committed');
    assert.equal(manifest.processed_windows[result.key].source_layer, 'features_1s');

    const checkpoint = loadRollupCheckpoint(MARKET, ROOT);
    assert.equal(checkpoint.namespace, 'features_30s');
    assert.equal(checkpoint.last_committed_window_start, 30_000);
  });

  it('is idempotent for the same durable 1s source and does not duplicate rows', () => {
    const committer = new RollupOutputCommitter(MARKET, 'c2-run-1', ROOT);
    const input = {
      rows: makeRows(),
      sourceInputSha256: 'source-input-hash',
      sourceOutputPath: 'features_1s/source.jsonl',
      sourceOutputHash: 'source-output-hash',
    };

    const first = committer.commitWindow(input);
    const second = new RollupOutputCommitter(MARKET, 'c2-run-2', ROOT).commitWindow(input);

    assert.equal(second.key, first.key);
    assert.equal(second.idempotent, true);
    assert.equal(readFileSync(first.output_path, 'utf8').trim().split('\n').length, 1);
    const manifest = loadRollupManifest(MARKET, ROOT);
    assert.equal(Object.keys(manifest.processed_windows).length, 1);
  });

  it('fails closed before writing for partial or missing-quality windows', () => {
    const committer = new RollupOutputCommitter(MARKET, 'c2-run-1', ROOT);
    assert.throws(() => committer.commitWindow({
      rows: makeRows().slice(0, 29),
      sourceInputSha256: 'partial',
      sourceOutputPath: 'features_1s/partial.jsonl',
      sourceOutputHash: 'partial-output',
    }), (error) => error.code === 'E_ROLLUP_PARTIAL_WINDOW');

    const missing = makeRows();
    missing[3]._quality.input_status = 'verified-missing';
    assert.throws(() => committer.commitWindow({
      rows: missing,
      sourceInputSha256: 'missing',
      sourceOutputPath: 'features_1s/missing.jsonl',
      sourceOutputHash: 'missing-output',
    }), (error) => error.code === 'E_ROLLUP_INVALID_INPUT_STATUS');

    assert.ok(!existsSync(join(ROOT, 'features_30s')));
  });

  it('repairs a missing 30s row from a committed 1s manifest without touching 1s bytes', () => {
    const sourceDir = join(ROOT, 'features_1s', MARKET, '1970-01-01');
    const sourcePath = join(sourceDir, '00-00-30.jsonl');
    mkdirSync(sourceDir, { recursive: true });
    const rows = makeRows();
    const sourceContent = rows.map(row => JSON.stringify(row)).join('\n') + '\n';
    writeFileSync(sourcePath, sourceContent);
    const sourceManifestDir = join(ROOT, 'manifests');
    mkdirSync(sourceManifestDir, { recursive: true });
    writeFileSync(join(sourceManifestDir, `${MARKET}.json`), JSON.stringify({
      schema_version: 'burst_features_v1',
      market: MARKET,
      processed_blocks: {
        'burst_features_v1:c2_committer:30000:input': {
          block_start_ms: 30_000,
          input_sha256: 'source-input-hash',
          output_row_hash: sha256(sourceContent),
          output_path: sourcePath,
          status: 'committed',
        },
      },
    }));
    const before = readFileSync(sourcePath, 'utf8');

    const result = new RollupOutputCommitter(MARKET, 'repair-run', ROOT).reconcileCommitted1s();

    assert.equal(result.repaired, 1);
    assert.equal(readFileSync(sourcePath, 'utf8'), before);
    assert.ok(existsSync(join(ROOT, 'features_30s', MARKET, '1970-01-01', '00-00-30.jsonl')));
    assert.equal(loadRollupCheckpoint(MARKET, ROOT).last_committed_window_start, 30_000);
  });
});
