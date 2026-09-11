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

  it('P2-4: completeness_transitions is written only when the transition list changes', async () => {
    // Regression: the full transition history (up to 50 entries) must not be
    // repeated on every health.jsonl row — only rows that follow an actual
    // state transition carry the list; unchanged ticks write [].
    const { monitor, file } = await freshMonitor();
    const t1 = { market: 'm1', from: 'unknown', to: 'running', tsMs: 1000 };
    const completeness = (transitions) => ({
      expected_markets: ['m1'], running_markets: ['m1'],
      degraded_markets: {}, data_complete: true, transitions,
    });

    monitor.setCompleteness(completeness([t1]));
    monitor._tick(); // transition t1: emit the history
    monitor._tick(); // unchanged: must NOT repeat the history
    monitor._tick(); // unchanged again

    const t2 = { market: 'm1', from: 'running', to: 'reconnecting', tsMs: 2000 };
    monitor.setCompleteness(completeness([t1, t2]));
    monitor._tick(); // new transition t2: emit again
    monitor._tick(); // unchanged
    await monitor.close();

    const rows = (await fs.readFile(file, 'utf8')).trim().split('\n').map(JSON.parse);
    assert.equal(rows.length, 5);
    assert.deepEqual(rows[0].completeness_transitions, [t1], 'first row carries the transition history');
    assert.deepEqual(rows[1].completeness_transitions, [], 'unchanged history must not repeat on every row');
    assert.deepEqual(rows[2].completeness_transitions, []);
    assert.deepEqual(rows[3].completeness_transitions, [t1, t2], 'a new transition reappears on the next row');
    assert.deepEqual(rows[4].completeness_transitions, []);
    await fs.rm(file, { force: true });
  });
});

describe('R-13: counted raw-DB drop is durable in health.jsonl', () => {
  // Regression: a shutdown drain that could not write queued raw events left
  // NO marker at all — the lost events were indistinguishable from a genuine
  // no-trade interval. The count must reach health.jsonl (and the drop report
  // file written by the caller).
  it('writes an extra final row carrying the drop when events were dropped', async () => {
    const { file } = await tempHealth();
    const monitor = new HealthMonitor(file, { rotateBytes: 1024 * 1024 });
    const report = {
      schema: 'receiver-raw-db-drop-report/v1',
      ts_ms: 1789000000000,
      dropped_events: 1234,
      reason: 'unable to open database file',
      queues: { raw: { dropped_events: 1234, flushed_events: 0, attempts: 3 } },
    };

    monitor.noteRawDbDroppedEvents(1234, report);
    monitor._tick();               // last periodic row
    await monitor.close();         // must append the drop-report row

    const rows = (await fs.readFile(file, 'utf8')).trim().split('\n').map(JSON.parse);
    assert.equal(rows.length, 2, 'one periodic row + one final drop-report row');
    assert.equal(rows[0].raw_db_dropped_events, 1234);
    assert.deepEqual(rows[0].raw_db_drop, {
      dropped_events: 1234,
      reason: 'unable to open database file',
      schema: 'receiver-raw-db-drop-report/v1',
      ts_ms: 1789000000000,
    });
    assert.equal(rows[1].raw_db_dropped_events, 1234, 'the final row keeps the counted drop');
    assert.equal(validateHealthGenerations(file).ok, true, 'manifest stays valid after the extra row');
    await fs.rm(file, { force: true });
    await fs.rm(`${file}.manifest.json`, { force: true });
  });

  it('adds no extra row and reports 0 drops on a clean shutdown', async () => {
    const { file } = await tempHealth();
    const monitor = new HealthMonitor(file, { rotateBytes: 1024 * 1024 });
    monitor._tick();
    await monitor.close();

    const rows = (await fs.readFile(file, 'utf8')).trim().split('\n').map(JSON.parse);
    assert.equal(rows.length, 1, 'clean shutdown keeps the previous row count');
    assert.equal(rows[0].raw_db_dropped_events, 0);
    assert.ok(!('raw_db_drop' in rows[0]), 'no drop object on a normal row');
    await fs.rm(file, { force: true });
    await fs.rm(`${file}.manifest.json`, { force: true });
  });

  it('ignores non-positive or non-finite drop counts', async () => {
    const { file } = await tempHealth();
    const monitor = new HealthMonitor(file, { rotateBytes: 1024 * 1024 });
    monitor.noteRawDbDroppedEvents(0, null);
    monitor.noteRawDbDroppedEvents(-5, null);
    monitor.noteRawDbDroppedEvents(Number.NaN, null);
    monitor._tick();
    await monitor.close();

    const rows = (await fs.readFile(file, 'utf8')).trim().split('\n').map(JSON.parse);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].raw_db_dropped_events, 0);
    await fs.rm(file, { force: true });
    await fs.rm(`${file}.manifest.json`, { force: true });
  });
});

describe('R14: pending-queue overflow is durable in health.jsonl', () => {
  // Regression: envelopes rejected at the pending-queue cap were dropped with no
  // counter at all, so the loss looked like a genuine no-trade interval.
  it('writes an extra final row carrying the counted overflow', async () => {
    const { file } = await tempHealth();
    const monitor = new HealthMonitor(file, { rotateBytes: 1024 * 1024 });

    monitor.noteRawDbPendingOverflow(812, {
      cap_events: 64, mode: 'count', raw: 700, canonical: 100, open_interest: 12,
      first_ts_ms: 1789000000000,
    });
    monitor._tick();
    await monitor.close();

    const rows = (await fs.readFile(file, 'utf8')).trim().split('\n').map(JSON.parse);
    assert.equal(rows.length, 2, 'one periodic row + one final overflow row');
    assert.equal(rows[0].raw_db_pending_overflow_events, 812);
    assert.deepEqual(rows[0].raw_db_pending_overflow, {
      dropped_events: 812, cap_events: 64, mode: 'count', raw: 700, canonical: 100,
      open_interest: 12, first_ts_ms: 1789000000000,
    });
    assert.equal(rows[1].raw_db_pending_overflow_events, 812, 'the final row keeps the count');
    assert.equal(validateHealthGenerations(file).ok, true, 'manifest stays valid after the extra row');
    await fs.rm(file, { force: true });
    await fs.rm(`${file}.manifest.json`, { force: true });
  });

  it('keeps the field at 0 and adds no object on a normal row', async () => {
    const { file } = await tempHealth();
    const monitor = new HealthMonitor(file, { rotateBytes: 1024 * 1024 });
    monitor._tick();
    await monitor.close();

    const rows = (await fs.readFile(file, 'utf8')).trim().split('\n').map(JSON.parse);
    assert.equal(rows.length, 1, 'an overflow-free shutdown keeps the previous row count');
    assert.equal(rows[0].raw_db_pending_overflow_events, 0);
    assert.ok(!('raw_db_pending_overflow' in rows[0]));
    await fs.rm(file, { force: true });
    await fs.rm(`${file}.manifest.json`, { force: true });
  });

  it('treats a drain loss and an overflow as separate, coexisting counts', async () => {
    const { file } = await tempHealth();
    const monitor = new HealthMonitor(file, { rotateBytes: 1024 * 1024 });

    monitor.noteRawDbDroppedEvents(40, { reason: 'unable to open database file' });
    monitor.noteRawDbPendingOverflow(7, { cap_events: 65536, mode: 'count', raw: 7 });
    monitor._tick();
    await monitor.close();

    const rows = (await fs.readFile(file, 'utf8')).trim().split('\n').map(JSON.parse);
    assert.equal(rows.length, 2, 'one final row covers both losses');
    assert.equal(rows[1].raw_db_dropped_events, 40);
    assert.equal(rows[1].raw_db_pending_overflow_events, 7);
    assert.equal(rows[1].raw_db_pending_overflow.canonical, 0);
    await fs.rm(file, { force: true });
    await fs.rm(`${file}.manifest.json`, { force: true });
  });

  it('ignores non-positive or non-finite overflow counts', async () => {
    const { file } = await tempHealth();
    const monitor = new HealthMonitor(file, { rotateBytes: 1024 * 1024 });
    monitor.noteRawDbPendingOverflow(0, null);
    monitor.noteRawDbPendingOverflow(-3, null);
    monitor.noteRawDbPendingOverflow(Number.NaN, null);
    monitor._tick();
    await monitor.close();

    const rows = (await fs.readFile(file, 'utf8')).trim().split('\n').map(JSON.parse);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].raw_db_pending_overflow_events, 0);
    await fs.rm(file, { force: true });
    await fs.rm(`${file}.manifest.json`, { force: true });
  });
});

describe('O-02: post-drain canonical drops are durable in health.jsonl', () => {
  // Regression: enqueueCanonicalFrames() returned on the `rawDbFailure` latch
  // before any counter, so canonical frames that arrived after the shutdown
  // drain had run were lost with no trace in health.jsonl or the drop report.
  it('writes an extra final row carrying the counted post-drain frames', async () => {
    const { file } = await tempHealth();
    const monitor = new HealthMonitor(file, { rotateBytes: 1024 * 1024 });

    monitor.noteRawDbPostDrainDroppedEvents(2, { first_ts_ms: 1789000000000, last_ts_ms: 1789000000500 });
    monitor._tick();
    await monitor.close();

    const rows = (await fs.readFile(file, 'utf8')).trim().split('\n').map(JSON.parse);
    assert.equal(rows.length, 2, 'one periodic row + one final post-drain row');
    assert.equal(rows[0].raw_db_post_drain_dropped_events, 2);
    assert.deepEqual(rows[0].raw_db_post_drain_drop, {
      dropped_events: 2, frames: 2, first_ts_ms: 1789000000000, last_ts_ms: 1789000000500,
    });
    assert.equal(rows[1].raw_db_post_drain_dropped_events, 2, 'the final row keeps the count');
    assert.equal(validateHealthGenerations(file).ok, true, 'manifest stays valid after the extra row');
    await fs.rm(file, { force: true });
    await fs.rm(`${file}.manifest.json`, { force: true });
  });

  it('keeps the field at 0, adds no object and no row when nothing arrived late', async () => {
    const { file } = await tempHealth();
    const monitor = new HealthMonitor(file, { rotateBytes: 1024 * 1024 });
    monitor.noteRawDbPostDrainDroppedEvents(0, null);
    monitor.noteRawDbPostDrainDroppedEvents(-3, null);
    monitor.noteRawDbPostDrainDroppedEvents(Number.NaN, null);
    monitor._tick();
    await monitor.close();

    const rows = (await fs.readFile(file, 'utf8')).trim().split('\n').map(JSON.parse);
    assert.equal(rows.length, 1, 'a clean shutdown keeps the previous row count');
    assert.equal(rows[0].raw_db_post_drain_dropped_events, 0);
    assert.ok(!('raw_db_post_drain_drop' in rows[0]));
    await fs.rm(file, { force: true });
    await fs.rm(`${file}.manifest.json`, { force: true });
  });

  it('keeps the three raw-DB loss fields independent on the same row', async () => {
    const { file } = await tempHealth();
    const monitor = new HealthMonitor(file, { rotateBytes: 1024 * 1024 });
    monitor.noteRawDbDroppedEvents(7, { reason: 'ENOSPC' });
    monitor.noteRawDbPendingOverflow(5, { cap_events: 64, mode: 'count', raw: 5 });
    monitor.noteRawDbPostDrainDroppedEvents(2, { first_ts_ms: 1, last_ts_ms: 2 });
    monitor._tick();
    await monitor.close();

    const rows = (await fs.readFile(file, 'utf8')).trim().split('\n').map(JSON.parse);
    assert.equal(rows.at(-1).raw_db_dropped_events, 7);
    assert.equal(rows.at(-1).raw_db_pending_overflow_events, 5);
    assert.equal(rows.at(-1).raw_db_post_drain_dropped_events, 2);
    assert.equal(rows.at(-1).raw_db_post_drain_drop.frames, 2);
    await fs.rm(file, { force: true });
    await fs.rm(`${file}.manifest.json`, { force: true });
  });
});
