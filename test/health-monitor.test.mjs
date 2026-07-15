// test/health-monitor.test.mjs — HealthMonitor unit tests
// C2 Phase 3a: new test file covering HealthMonitor constructor, state
// aggregation, per-market fields, JSONL output, and tick lifecycle.

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { HealthMonitor } from '../lib/health-monitor.mjs';

// ── Helpers ──────────────────────────────────────────────────────────────────

function tmpDir(label) {
  const dir = path.join(
    os.tmpdir(), 'btc-receiver-test', 'hm',
    `${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
  );
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

async function rmDir(dir) {
  try { await fsp.rm(dir, { recursive: true, force: true }); } catch {}
}

function makeStats(state, overrides = {}) {
  return {
    state,
    connectedAt: 1700000000000,
    lastDepthMsgAt: 1700000000000,
    lastTradeMsgAt: 1700000000000,
    depthMsgCount: 100,
    tradeMsgCount: 50,
    reconnectCount: 0,
    resyncCount: 0,
    lastSeq: 0,
    ...overrides,
  };
}

// ── Tests ────────────────────────────────────────────────────────────────────

describe('HealthMonitor constructor', () => {
  let dir;
  before(() => { dir = tmpDir('constructor'); });
  after(async () => { await rmDir(dir); });

  it('creates output directory and initializes without error', () => {
    const outputFile = path.join(dir, 'health.jsonl');
    const hm = new HealthMonitor(outputFile, { intervalMs: 1000 });
    assert.ok(hm);
    assert.strictEqual(hm._intervalMs, 1000);
    assert.ok(hm._writer, 'should have a BufferedWriter');
    assert.strictEqual(hm._connectorStats.size, 0);
    hm.close();
  });

  it('has default intervalMs of 1000 when not specified', () => {
    const outputFile = path.join(dir, 'health-default.jsonl');
    const hm = new HealthMonitor(outputFile);
    assert.strictEqual(hm._intervalMs, 1000);
    hm.close();
  });
});

describe('HealthMonitor state aggregation', () => {
  it('returns "normal" when all markets are running', () => {
    const hm = new HealthMonitor('/tmp/test-normal.jsonl');
    hm.updateConnector('binance_spot', makeStats('running'));
    hm.updateConnector('bybit_perp', makeStats('running'));
    const summary = hm.getHealthSummary();
    assert.strictEqual(summary.state, 'normal');
    hm.close();
  });

  it('returns "warning" when a market is not running but none are error/reconnecting', () => {
    const hm = new HealthMonitor('/tmp/test-warning.jsonl');
    hm.updateConnector('binance_spot', makeStats('running'));
    hm.updateConnector('bybit_perp', makeStats('connected')); // connected, not running
    const summary = hm.getHealthSummary();
    assert.strictEqual(summary.state, 'warning');
    hm.close();
  });

  it('returns "critical" when a market is in error state', () => {
    const hm = new HealthMonitor('/tmp/test-critical.jsonl');
    hm.updateConnector('binance_spot', makeStats('running'));
    hm.updateConnector('bybit_perp', makeStats('error'));
    const summary = hm.getHealthSummary();
    assert.strictEqual(summary.state, 'critical');
    hm.close();
  });

  it('returns "critical" when a market is in reconnecting state', () => {
    const hm = new HealthMonitor('/tmp/test-reconnecting.jsonl');
    hm.updateConnector('binance_spot', makeStats('running'));
    hm.updateConnector('bybit_perp', makeStats('reconnecting'));
    const summary = hm.getHealthSummary();
    assert.strictEqual(summary.state, 'critical');
    hm.close();
  });

  it('returns "normal" when there are no markets', () => {
    const hm = new HealthMonitor('/tmp/test-empty.jsonl');
    const summary = hm.getHealthSummary();
    assert.strictEqual(summary.state, 'normal');
    assert.deepStrictEqual(summary.markets, {});
    hm.close();
  });
});

describe('HealthMonitor per-market fields', () => {
  it('each market entry contains all required fields', () => {
    const hm = new HealthMonitor('/tmp/test-fields.jsonl');
    const stats = makeStats('running');
    hm.updateConnector('binance_spot', stats);
    const summary = hm.getHealthSummary();
    const market = summary.markets['binance_spot'];
    assert.ok(market, 'binance_spot should be present');

    const requiredFields = [
      'state', 'connectedAt', 'lastDepthMsgAt', 'lastTradeMsgAt',
      'depthMsgCount', 'tradeMsgCount', 'reconnectCount', 'resyncCount',
      'lastSeq',
    ];
    for (const field of requiredFields) {
      assert.ok(Object.hasOwn(market, field),
        `market entry should have field "${field}"`);
    }

    assert.strictEqual(market.state, 'running');
    assert.strictEqual(market.depthMsgCount, 100);
    assert.strictEqual(market.tradeMsgCount, 50);
    hm.close();
  });

  it('reflects updated stats after updateConnector call', () => {
    const hm = new HealthMonitor('/tmp/test-update.jsonl');
    hm.updateConnector('binance_spot', makeStats('running', { tradeMsgCount: 200 }));
    const summary1 = hm.getHealthSummary();
    assert.strictEqual(summary1.markets['binance_spot'].tradeMsgCount, 200);

    hm.updateConnector('binance_spot', makeStats('running', { tradeMsgCount: 300 }));
    const summary2 = hm.getHealthSummary();
    assert.strictEqual(summary2.markets['binance_spot'].tradeMsgCount, 300);
    hm.close();
  });
});

describe('HealthMonitor JSONL output', () => {
  let dir;
  before(() => { dir = tmpDir('jsonl-output'); });
  after(async () => { await rmDir(dir); });

  it('writes health.jsonl with valid JSON lines via _tick', async () => {
    const outputFile = path.join(dir, 'health.jsonl');
    const hm = new HealthMonitor(outputFile, { intervalMs: 1000 });

    // Use updateConnector then call _tick directly (skipping real interval)
    hm.updateConnector('binance_spot', makeStats('running'));
    hm._tick();

    // close() flushes the BufferedWriter
    await hm.close();

    // Read the file
    const content = fs.readFileSync(outputFile, 'utf-8').trim();
    assert.ok(content.length > 0, 'health.jsonl should have content');
    const lines = content.split('\n');
    assert.ok(lines.length >= 1, 'at least one line written');

    // Each line must be valid JSON
    for (const line of lines) {
      const parsed = JSON.parse(line);
      assert.ok(typeof parsed.ts === 'number');
      assert.ok(['normal', 'warning', 'critical'].includes(parsed.state));
      assert.ok(typeof parsed.markets === 'object');
    }
  });

  it('multiple _tick calls produce multiple JSON lines (append)', async () => {
    const outputFile = path.join(dir, 'health-multi.jsonl');
    const hm = new HealthMonitor(outputFile, { intervalMs: 1000 });
    hm.updateConnector('binance_spot', makeStats('running'));

    // Call _tick 3 times
    hm._tick();
    await new Promise(r => setTimeout(r, 150));
    hm._tick();
    await new Promise(r => setTimeout(r, 150));
    hm._tick();
    await new Promise(r => setTimeout(r, 200));
    hm.close();
    await new Promise(r => setTimeout(r, 100));

    const content = fs.readFileSync(outputFile, 'utf-8').trim();
    const lines = content.split('\n').filter(l => l.trim());
    assert.ok(lines.length >= 3,
      `expected at least 3 lines, got ${lines.length}`);

    // Verify each line is independently valid JSON
    for (const line of lines) {
      const parsed = JSON.parse(line);
      assert.ok(typeof parsed.ts === 'number');
    }
  });

  it('start/stop interval lifecycle does not throw', async () => {
    const outputFile = path.join(dir, 'health-lifecycle.jsonl');
    const hm = new HealthMonitor(outputFile, { intervalMs: 100 });
    hm.updateConnector('binance_spot', makeStats('running'));
    hm.start();
    await new Promise(r => setTimeout(r, 350)); // ~3-4 ticks
    hm.stop();
    assert.strictEqual(hm._timer, null, 'timer cleared after stop');
    hm.close();
  });
});

describe('HealthMonitor ts field', () => {
  it('ts is set to write time (Date.now)', () => {
    const hm = new HealthMonitor('/tmp/test-ts.jsonl');
    hm.updateConnector('binance_spot', makeStats('running'));
    const before = Date.now();
    const summary = hm.getHealthSummary();
    const after = Date.now();
    assert.ok(summary.ts >= before && summary.ts <= after,
      `ts ${summary.ts} should be between ${before} and ${after}`);
    hm.close();
  });
});
