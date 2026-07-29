import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { HealthMonitor } from '../lib/health-monitor.mjs';
import { validateHealthGenerations } from '../scripts/verify-health-generations.mjs';

async function tempHealth() {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'agg-health-'));
  return { dir, file: path.join(dir, 'health.jsonl') };
}

describe('HealthMonitor generation retention and acceptance', () => {
  it('rotates at the configured limit, preserves dashboard JSONL, and writes a manifest', async () => {
    const { file } = await tempHealth();
    const monitor = new HealthMonitor(file, { rotateBytes: 220 });
    for (let i = 0; i < 12; i++) {
      monitor.updateConnector('binance_spot', {
        state: 'running', connectedAt: 1, lastDepthMsgAt: i, lastTradeMsgAt: i,
        depthMsgCount: i, tradeMsgCount: i, droppedDepthCount: 0,
        droppedTradeCount: 0, droppedLiquidationCount: 0, reconnectCount: 0,
        resyncCount: 0, lastSeq: i,
      });
      monitor._tick();
    }
    await monitor.close();

    const result = validateHealthGenerations(file);
    assert.equal(result.ok, true);
    assert.equal(result.files.length, 2);
    for (const generation of result.files) {
      const rows = (await fs.readFile(generation.file, 'utf8')).trim().split('\n').map(JSON.parse);
      assert.ok(rows.length > 0);
      assert.ok(rows.every(row => row.markets.binance_spot.state === 'running'));
    }
    assert.ok((await fs.stat(`${file}.manifest.json`)).size > 0);
  });

  it('keeps buffered memory bounded while retaining data quality', async () => {
    const { file } = await tempHealth();
    const monitor = new HealthMonitor(file, { rotateBytes: 1024 * 1024 });
    const heapBefore = process.memoryUsage().heapUsed;
    for (let i = 0; i < 2000; i++) monitor._tick();
    assert.ok(monitor._writer.getStats().bufferedBytes < 1024 * 1024);
    await monitor.close();
    global.gc?.();
    assert.ok(process.memoryUsage().heapUsed - heapBefore < 64 * 1024 * 1024);
    const rows = (await fs.readFile(file, 'utf8')).trim().split('\n').map(JSON.parse);
    assert.equal(rows.length, 2000);
    assert.ok(rows.every(row => Number.isSafeInteger(row.ts) && row.state === 'normal'));
  });
});
