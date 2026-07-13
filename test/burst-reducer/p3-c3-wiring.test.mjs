// test/burst-reducer/p3-c3-wiring.test.mjs — P3-C3 5min rollup pipeline wiring
// Focused test: no separate fixture directory, all data inline.
import assert from 'node:assert/strict';
import { describe, it, beforeEach, after } from 'node:test';
import { existsSync, mkdirSync, rmSync, writeFileSync, readFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { runPipeline } from '../../lib/burst-reducer/pipeline.mjs';
import { load5minManifest } from '../../lib/burst-reducer/rollup-5min-committer.mjs';

const TEST_ROOT = join('test', 'fixtures', 'burst-v1', 'tmp-p3-c3-wiring');
const MARKET = 'p3_c3_wiring_test';
const RUN_ID = 'p3-c3-test';
const OUTPUT_ROOT = join(TEST_ROOT, 'derived');

function blockPath(start) {
  const d = new Date(start);
  const date = `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')}`;
  const time = `${String(d.getUTCHours()).padStart(2,'0')}-${String(d.getUTCMinutes()).padStart(2,'0')}-${String(d.getUTCSeconds()).padStart(2,'0')}`;
  return join(TEST_ROOT, 'trades', MARKET, date, `${time}.jsonl`);
}

function writeBlock(start, trades) {
  const p = blockPath(start);
  mkdirSync(join(p, '..'), { recursive: true });
  writeFileSync(p, trades.map(t => JSON.stringify(t)).join('\n') + '\n');
}

beforeEach(() => {
  rmSync(TEST_ROOT, { recursive: true, force: true });
  mkdirSync(TEST_ROOT, { recursive: true });

  // Lookback block
  writeBlock(-30000, [{ ts: -29000, side: 'buy', price: 100, qty: 1 }]);

  // 11 blocks: 10 for one 5min bucket + 1 pending
  for (let i = 0; i < 11; i++) {
    const start = i * 30_000;
    writeBlock(start, [
      { ts: start + 500, side: 'buy', price: 100, qty: 1 },
      { ts: start + 520, side: 'buy', price: 100, qty: 2 },
    ]);
  }
});

after(() => {
  rmSync(TEST_ROOT, { recursive: true, force: true });
});

describe('P3-C3 5min pipeline wiring', () => {
  it('commits a 5min window after 10 consecutive 30s commits', async () => {
    const result = await runPipeline({
      dataDir: TEST_ROOT,
      market: MARKET,
      fromMs: 0,
      toMs: 330_000,
      runId: RUN_ID,
      outputRoot: OUTPUT_ROOT,
      finalizedThroughMs: 330_000,
    });

    assert.equal(result.processed, 11, 'all 11 blocks should be committed (10 normal + 1 EOF)');

    // Verify 5min output exists
    const features5min = join(OUTPUT_ROOT, 'features_5min', MARKET, '1970-01-01');
    const fiveMinFiles = existsSync(features5min) ? readdirSync(features5min).filter(f => f.endsWith('.jsonl')).sort() : [];
    assert.ok(fiveMinFiles.length >= 1, 'at least one 5min file should exist');

    // Verify 5min manifest has a committed window
    const manifest = load5minManifest(MARKET, OUTPUT_ROOT);
    assert.ok(manifest, '5min manifest should exist');
    assert.ok(manifest.processed_windows, '5min manifest should have processed_windows');

    const committedWindows = Object.values(manifest.processed_windows).filter(r => r.status === 'committed');
    assert.equal(committedWindows.length, 1, 'exactly one committed 5min window');
    assert.equal(committedWindows[0].window_start_ms, 0, '5min window should start at 0');
    assert.equal(committedWindows[0].source_layer, 'features_30s');
    assert.equal(committedWindows[0].status, 'committed');
  });
});
