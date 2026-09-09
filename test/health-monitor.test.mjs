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

describe('HealthMonitor setCompleteness merge + fail-visible state promotion', () => {
  /** Build a monitor over a throwaway path; getHealthSummary is pure-ish. */
  async function freshMonitor() {
    const { file } = await tempHealth();
    const monitor = new HealthMonitor(file, { rotateBytes: 1024 * 1024 });
    return { monitor, file };
  }

  it('running connectors + data_complete true ⇒ state normal', async () => {
    const { monitor, file } = await freshMonitor();
    monitor.updateConnector('binance_perp', { state: 'running', depthMsgCount: 1, tradeMsgCount: 1 });
    monitor.setCompleteness({
      expected_markets: ['binance_perp'],
      running_markets: ['binance_perp'],
      degraded_markets: {},
      data_complete: true,
      transitions: [],
    });
    const s = monitor.getHealthSummary();
    assert.equal(s.state, 'normal');
    assert.equal(s.data_complete, true);
    assert.equal(s.markets['binance_perp'].state, 'running');
    await monitor.close();
    await fs.rm(file, { force: true });
  });

  it('degraded market overrides a stale per-connector running state ⇒ warning', async () => {
    const { monitor, file } = await freshMonitor();
    // Connector stats still say running (last tick before isolation)…
    monitor.updateConnector('binance_perp', { state: 'running', depthMsgCount: 1, tradeMsgCount: 1 });
    // …but the aggregate view says it is isolated — degraded is authoritative.
    monitor.setCompleteness({
      expected_markets: ['binance_perp'],
      running_markets: [],
      degraded_markets: { binance_perp: 'initial sync failed: snapshot timeout' },
      data_complete: false,
      transitions: [],
    });
    const s = monitor.getHealthSummary();
    assert.equal(s.state, 'warning', 'degraded market must never present as normal');
    assert.equal(s.markets['binance_perp'].state, 'degraded');
    assert.equal(s.markets['binance_perp'].degraded_reason, 'initial sync failed: snapshot timeout');
    assert.equal(s.data_complete, false);
    assert.deepEqual(s.degraded_markets, { binance_perp: 'initial sync failed: snapshot timeout' });
    assert.deepEqual(s.running_markets, []);
    await monitor.close();
    await fs.rm(file, { force: true });
  });

  it('data_complete=false with NO degraded market (unknown/reconnecting) ⇒ warning, not normal', async () => {
    // Self-contradiction regression: required market is not running (still
    // unknown / mid-reconnect, no degraded reason recorded) — the report must
    // not say state:normal alongside data_complete:false.
    const { monitor, file } = await freshMonitor();
    monitor.setCompleteness({
      expected_markets: ['okx_perp'],
      running_markets: [],
      degraded_markets: {},
      data_complete: false,
      transitions: [],
    });
    const s = monitor.getHealthSummary();
    assert.equal(s.data_complete, false);
    assert.equal(s.state, 'warning', 'normal + data_complete=false is self-contradictory');
    await monitor.close();
    await fs.rm(file, { force: true });
  });

  it('data_complete not yet true (null) with completeness pushed ⇒ warning (fail-visible)', async () => {
    const { monitor, file } = await freshMonitor();
    monitor.setCompleteness({
      expected_markets: ['okx_perp'],
      running_markets: [],
      degraded_markets: {},
      data_complete: null,
      transitions: [],
    });
    const s = monitor.getHealthSummary();
    assert.equal(s.data_complete, null);
    assert.equal(s.state, 'warning', 'unknown completeness must not present as normal');
    await monitor.close();
    await fs.rm(file, { force: true });
  });

  it('no completeness view pushed yet ⇒ per-connector states decide (legacy path)', async () => {
    const { monitor, file } = await freshMonitor();
    monitor.updateConnector('binance_perp', { state: 'running', depthMsgCount: 1, tradeMsgCount: 1 });
    const s = monitor.getHealthSummary();
    assert.equal(s.state, 'normal');
    assert.equal(s.data_complete, null);
    await monitor.close();
    await fs.rm(file, { force: true });
  });

  it('optional-market degradation keeps data_complete=true but stays warning (visible)', async () => {
    const { monitor, file } = await freshMonitor();
    monitor.updateConnector('binance_perp', { state: 'running' });
    monitor.setCompleteness({
      expected_markets: ['binance_perp', 'kraken_spot'],
      running_markets: ['binance_perp'],
      degraded_markets: { kraken_spot: 'spot degraded (optional)' },
      data_complete: true,
      transitions: [],
    });
    const s = monitor.getHealthSummary();
    assert.equal(s.data_complete, true, 'optional degradation must not block completeness');
    assert.equal(s.state, 'warning', 'but an isolated market must still surface');
    await monitor.close();
    await fs.rm(file, { force: true });
  });
});
