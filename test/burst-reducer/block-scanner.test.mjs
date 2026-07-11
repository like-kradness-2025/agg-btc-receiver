// test/burst-reducer/block-scanner.test.mjs — BlockScanner tests
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { existsSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { scanTradeBlocks } from '../../lib/burst-reducer/block-scanner.mjs';

function ensureEmptyTestDir(market) {
  const dataDir = join('test', 'fixtures', 'burst-v1', 'tmp-scan');
  const dir = join(dataDir, 'trades', market);
  rmSync(dir, { recursive: true, force: true });
  mkdirSync(join(dir, '1970-01-01'), { recursive: true });
  return { dataDir, dateDir: join(dir, '1970-01-01') };
}

describe('BlockScanner', () => {
  it('returns empty for non-existent market', () => {
    const result = scanTradeBlocks('data/live_v3', 'nonexistent_market_xyz', 0, 9999999999999);
    assert.deepEqual(result, []);
  });

  it('detects 30s files from temp fixture dir', () => {
    const { dataDir, dateDir } = ensureEmptyTestDir('test_scan');
    writeFileSync(join(dateDir, '00-00-00.jsonl'), '{"ts":0}\n');
    writeFileSync(join(dateDir, '00-00-30.jsonl'), '{"ts":30000}\n');
    writeFileSync(join(dateDir, '00-01-00.jsonl'), '{"ts":60000}\n');

    const result = scanTradeBlocks(dataDir, 'test_scan', 0, 9999999999999);
    assert.equal(result.length, 3);
    for (const b of result) {
      assert.ok(b.ms % 30000 === 0, `block ${b.ms} not on 30s boundary`);
    }
    assert.equal(result[0].ms, 0);
    assert.equal(result[1].ms, 30000);
    assert.equal(result[2].ms, 60000);
  });

  it('throws E006 on non-00/30 boundary filename (quarantine/fail)', () => {
    const { dataDir, dateDir } = ensureEmptyTestDir('test_e006');
    writeFileSync(join(dateDir, '00-00-15.jsonl'), '{"ts":15000}\n');

    assert.throws(() => scanTradeBlocks(dataDir, 'test_e006', 0, 120000), /E006/);
  });

  it('filters blocks outside time range', () => {
    const { dataDir, dateDir } = ensureEmptyTestDir('test_range');
    writeFileSync(join(dateDir, '00-00-00.jsonl'), '{"ts":0}\n');
    writeFileSync(join(dateDir, '00-00-30.jsonl'), '{"ts":30000}\n');
    writeFileSync(join(dateDir, '00-01-00.jsonl'), '{"ts":60000}\n');

    // Range [30000, 90000) should include 00-00-30 (ms=30000) and 00-01-00 (ms=60000)
    // 00-00-00 (ms=0) is excluded because ms+30000=30000 is not > fromMs=30000
    const result = scanTradeBlocks(dataDir, 'test_range', 30000, 90000);
    assert.equal(result.length, 2);
  });
});
