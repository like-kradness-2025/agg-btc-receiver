// C2 pipeline wiring: roll up only after durable 1s commits.
import assert from 'node:assert/strict';
import { after, beforeEach, describe, it } from 'node:test';
import { existsSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { runPipeline } from '../../lib/burst-reducer/pipeline.mjs';

const DATA_ROOT = join('test', 'fixtures', 'burst-v1', 'tmp-c2-pipeline-wiring');
const OUTPUT_ROOT = join(DATA_ROOT, 'derived');
const MARKET = 'c2_pipeline_wiring';

function blockPath(start) {
  const date = new Date(start);
  const day = `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}-${String(date.getUTCDate()).padStart(2, '0')}`;
  const time = `${String(date.getUTCHours()).padStart(2, '0')}-${String(date.getUTCMinutes()).padStart(2, '0')}-${String(date.getUTCSeconds()).padStart(2, '0')}`;
  return join(DATA_ROOT, 'trades', MARKET, day, `${time}.jsonl`);
}

function writeBlock(start, trades = []) {
  mkdirSync(join(DATA_ROOT, 'trades', MARKET, '1970-01-01'), { recursive: true });
  writeFileSync(blockPath(start), trades.map((trade) => JSON.stringify(trade)).join('\n') + '\n');
}

function setupData(starts) {
  rmSync(DATA_ROOT, { recursive: true, force: true });
  mkdirSync(join(DATA_ROOT, 'trades', MARKET, '1969-12-31'), { recursive: true });
  writeFileSync(join(DATA_ROOT, 'trades', MARKET, '1969-12-31', '23-59-30.jsonl'),
    JSON.stringify({ ts: -29_000, side: 'buy', price: 100, qty: 1 }) + '\n');
  for (const start of starts) writeBlock(start, start === 0
    ? [{ ts: 500, side: 'buy', price: 100, qty: 1 }]
    : [{ ts: start + 500, side: 'sell', price: 101, qty: 1 }]);
}

function filesUnder(path) {
  return existsSync(path) ? readdirSync(path).filter((name) => name.endsWith('.jsonl')).sort() : [];
}

beforeEach(() => rmSync(DATA_ROOT, { recursive: true, force: true }));
after(() => rmSync(DATA_ROOT, { recursive: true, force: true }));

describe('C2 pipeline rollup wiring', () => {
  it('writes 30s rows after normal durable 1s commits and preserves 1s bytes', async () => {
    setupData([0, 30_000]);
    const result = await runPipeline({
      dataDir: DATA_ROOT,
      market: MARKET,
      fromMs: 0,
      toMs: 60_000,
      runId: 'c2-normal',
      outputRoot: OUTPUT_ROOT,
      finalizedThroughMs: 60_000,
    });

    assert.equal(result.processed, 2);
    const features1s = join(OUTPUT_ROOT, 'features_1s', MARKET, '1970-01-01');
    const features30s = join(OUTPUT_ROOT, 'features_30s', MARKET, '1970-01-01');
    const oneSecondBytes = filesUnder(features1s).map((name) => [name, readFileSync(join(features1s, name), 'utf8')]);
    assert.deepEqual(filesUnder(features30s), ['00-00-00.jsonl', '00-00-30.jsonl']);
    assert.deepEqual(oneSecondBytes.map(([name]) => name), ['00-00-00.jsonl', '00-00-30.jsonl']);
    assert.equal(readFileSync(join(features30s, '00-00-00.jsonl'), 'utf8').trim().split('\n').length, 1);
    assert.equal(readFileSync(join(features30s, '00-00-30.jsonl'), 'utf8').trim().split('\n').length, 1);
  });

  it('rolls up present blocks across a trade gap without creating a gap window', async () => {
    setupData([0, 60_000]);
    const result = await runPipeline({
      dataDir: DATA_ROOT,
      market: MARKET,
      fromMs: 0,
      toMs: 90_000,
      runId: 'c2-gap',
      outputRoot: OUTPUT_ROOT,
      finalizedThroughMs: 90_000,
    });

    assert.equal(result.processed, 2);
    const features30s = join(OUTPUT_ROOT, 'features_30s', MARKET, '1970-01-01');
    assert.deepEqual(filesUnder(features30s), ['00-00-00.jsonl', '00-01-00.jsonl']);
    assert.ok(!existsSync(join(features30s, '00-00-30.jsonl')));
  });

  it('does not write a rollup when EOF is blocked without horizon proof', async () => {
    setupData([0]);
    const result = await runPipeline({
      dataDir: DATA_ROOT,
      market: MARKET,
      fromMs: 0,
      toMs: 30_000,
      runId: 'c2-eof-blocked',
      outputRoot: OUTPUT_ROOT,
    });

    assert.equal(result.processed, 0);
    assert.equal(result.blocked, true);
    assert.ok(!existsSync(join(OUTPUT_ROOT, 'features_1s')));
    assert.ok(!existsSync(join(OUTPUT_ROOT, 'features_30s')));
  });
});
