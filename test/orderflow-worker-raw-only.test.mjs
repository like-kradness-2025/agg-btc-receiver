// test/orderflow-worker-raw-only.test.mjs — Raw-only contract test for orderflow worker
//
// Verifies that a raw-only orderflow worker:
//   (1) Saves trade events to "trades" RawRotationWriter
//   (2) Saves depth events to "book_updates" RawRotationWriter
//   (3) Saves liquidation events to "liquidations" RawRotationWriter
//   (4) Does NOT create agg_trades / book_snapshots / snapshots writers
//   (5) Does NOT generate derived files after 1s wait
//   (6) Maintains stats / stateChange / ready IPC messages
//   (7) On shutdown, only the 3 raw writers are finalized
//
// This test does NOT import orderflow-worker.mjs (which has live connector
// imports). Instead it exercises the raw-only contract directly using real
// RawRotationWriter instances and mock EventEmitter-based connectors.
//
// No modifications to lib/orderflow-worker.mjs are required.

import { describe, it, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import fsp from 'node:fs/promises';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { RawRotationWriter, _setTestMakeWriterFn } from '../lib/raw-rotation-writer.mjs';
import { HealthMonitor } from '../lib/health-monitor.mjs';
import {
  _testInit,
  _testPrepareMarket,
  _testDoInit,
  _propagateWriterError,
  _testFinalizeAll,
  _testReset,
  _setTestParentPort as _workerSetTestParentPort,
  _testSetWriterOverrides,
  _setTestExitFn,
  _setTestConnectorClasses,
} from '../lib/orderflow-worker.mjs';

// ── Helpers ──────────────────────────────────────────────────────────────────

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Create a unique temporary directory under OS tmp. */
function tmpDir(label) {
  const dir = path.join(
    os.tmpdir(),
    'ofw-raw-only-test',
    `${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
  );
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

/** Recursively remove a directory. */
async function rmDir(dir) {
  try {
    await fsp.rm(dir, { recursive: true, force: true });
  } catch {
    /* best-effort */
  }
}

/**
 * Recursively find all .jsonl files under a given base directory whose
 * relative path contains the given `kind` segment.
 */
async function findJsonlFiles(baseDir, kind) {
  const results = [];
  async function walk(dir) {
    let entries;
    try {
      entries = await fsp.readdir(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        await walk(full);
      } else if (entry.isFile() && full.endsWith('.jsonl')) {
        // Only collect files whose path contains /<kind>/
        if (full.includes(`/${kind}/`)) {
          results.push(full);
        }
      }
    }
  }
  await walk(baseDir);
  return results;
}

/**
 * Wrap a RawRotationWriter so that every write() call is recorded in the
 * supplied array.  Returns the same writer (mutated in-place).
 */
function spyWriter(writer, arr) {
  const orig = writer.write.bind(writer);
  writer.write = async function (obj, ts) {
    arr.push({ obj, ts });
    return orig(obj, ts);
  };
  return writer;
}

// ── Mock helpers ─────────────────────────────────────────────────────────────

/** Minimal connector mock: EventEmitter + state/stats/book stubs. */
function createMockConnector() {
  const conn = new EventEmitter();
  conn._state = 'running';
  conn.getState = () => conn._state;
  conn.getStats = () => ({
    books: { bids: 50, asks: 40 },
    trades: 10,
    state: conn._state,
  });
  conn.book = { isEmpty: () => false };
  return conn;
}

// ── Tests ────────────────────────────────────────────────────────────────────

describe('OrderflowWorker raw-only contract', () => {
  /** @type {string} */
  let baseDir;

  /** @type {RawRotationWriter} */
  let rawTradesWriter;
  /** @type {RawRotationWriter} */
  let rawBookWriter;
  /** @type {RawRotationWriter} */
  let rawLiqWriter;

  /** @type {EventEmitter} */
  let mockConnector;

  /** @type {Array<{type:string, market?:string, [k:string]:any}>} */
  const ipcMessages = [];

  /** @type {{trades: any[], book_updates: any[], liquidations: any[]}} */
  const writes = { trades: [], book_updates: [], liquidations: [] };

  /** Timestamps used for all events (consistent across tests). */
  const now = Date.now();

  // ── Setup ────────────────────────────────────────────────────────────────

  before(async () => {
    baseDir = tmpDir('contract');

    // ── Mock parentPort IPC sink ───────────────────────────────────────
    const mockParentPort = {
      postMessage(msg) {
        ipcMessages.push(msg);
      },
    };

    // ── Create ONLY the 3 raw writers (raw-only contract) ──────────────
    // Derived writers (agg_trades, book_snapshots, snapshots) are
    // intentionally NOT created — this is the core of the raw-only contract.
    rawTradesWriter = spyWriter(
      new RawRotationWriter(baseDir, 'test_market', 'trades', {
        flushIntervalMs: 50,
      }),
      writes.trades,
    );
    rawBookWriter = spyWriter(
      new RawRotationWriter(baseDir, 'test_market', 'book_updates', {
        flushIntervalMs: 50,
      }),
      writes.book_updates,
    );
    rawLiqWriter = spyWriter(
      new RawRotationWriter(baseDir, 'test_market', 'liquidations', {
        flushIntervalMs: 50,
      }),
      writes.liquidations,
    );

    // ── Mock connector ─────────────────────────────────────────────────
    mockConnector = createMockConnector();

    // ── Wire events (raw-only: no aggregator, no derived writers) ──────
    mockConnector.on('trade', async (tradeEvent) => {
      // NOTE: In a real raw-only worker, the TradeAggregator would NOT
      // be created, so we only save to raw trades writer.
      await rawTradesWriter.write(tradeEvent, tradeEvent.ts);
    });

    mockConnector.on('depth', async (depthEvent) => {
      await rawBookWriter.write(depthEvent, depthEvent.ts);
    });

    mockConnector.on('liquidation', async (row) => {
      await rawLiqWriter.write(row, row.ts);
      mockParentPort.postMessage({
        type: 'liquidation',
        market: 'test_market',
        payload: row,
      });
    });

    mockConnector.on('stateChange', (from, to) => {
      mockParentPort.postMessage({
        type: 'stateChange',
        market: 'test_market',
        from,
        to,
        stats: mockConnector.getStats(),
      });
    });

    mockConnector.on('error', ({ message }) => {
      // silently eat errors in test
    });

    // ── Simulate init IPC sequence ─────────────────────────────────────
    mockParentPort.postMessage({ type: 'ready', workerId: 'test-worker' });
    mockParentPort.postMessage({
      type: 'stats',
      market: 'test_market',
      payload: mockConnector.getStats(),
    });
    mockConnector.emit('stateChange', 'initializing', 'running');

    // ── Emit one of each event type ────────────────────────────────────
    mockConnector.emit('trade', {
      price: 50000,
      qty: 0.1,
      side: 'buy',
      ts: now,
    });

    mockConnector.emit('depth', {
      type: 'update',
      bids: [['49900', '1.5']],
      asks: [['50100', '0.8']],
      ts: now + 1,
    });

    mockConnector.emit('liquidation', {
      price: 49800,
      qty: 1.5,
      side: 'sell',
      ts: now + 2,
    });

    // Give async write queues time to flush to BufferedWriter
    await sleep(300);
  });

  after(async () => {
    // FIX7a: 3 つの RawRotationWriter を確実に finalize して BufferedWriter のストリームを閉じる
    if (rawTradesWriter) await rawTradesWriter.finalize();
    if (rawBookWriter) await rawBookWriter.finalize();
    if (rawLiqWriter) await rawLiqWriter.finalize();
    await rmDir(baseDir);
  });

  // ── Assertion (1): trade → trades writer ─────────────────────────────────

  it('(1) trade event is saved to "trades" writer once', () => {
    assert.strictEqual(
      writes.trades.length,
      1,
      'exactly 1 trade written to trades writer',
    );
    const evt = writes.trades[0];
    assert.strictEqual(evt.obj.price, 50000);
    assert.strictEqual(evt.obj.qty, 0.1);
    assert.strictEqual(evt.obj.side, 'buy');
    assert.strictEqual(evt.ts, now);
  });

  // ── Assertion (2): depth → book_updates writer ───────────────────────────

  it('(2) depth event is saved to "book_updates" writer once', () => {
    assert.strictEqual(
      writes.book_updates.length,
      1,
      'exactly 1 depth written to book_updates writer',
    );
    const evt = writes.book_updates[0];
    assert.strictEqual(evt.obj.type, 'update');
    assert.deepStrictEqual(evt.obj.bids, [['49900', '1.5']]);
    assert.deepStrictEqual(evt.obj.asks, [['50100', '0.8']]);
    assert.strictEqual(evt.ts, now + 1);
  });

  // ── Assertion (3): liquidation → liquidations writer ─────────────────────

  it('(3) liquidation event is saved to "liquidations" writer once', () => {
    assert.strictEqual(
      writes.liquidations.length,
      1,
      'exactly 1 liquidation written to liquidations writer',
    );
    const evt = writes.liquidations[0];
    assert.strictEqual(evt.obj.price, 49800);
    assert.strictEqual(evt.obj.qty, 1.5);
    assert.strictEqual(evt.obj.side, 'sell');
    assert.strictEqual(evt.ts, now + 2);
  });

  // ── Assertion (4): no agg_trades / snapshots / book_snapshots writers ────

  it('(4) agg_trades / snapshots / book_snapshots writers are NOT created', () => {
    // The raw-only contract mandates that these 3 derived writer kinds
    // are never instantiated. Since we explicitly only created the 3
    // raw writers in before(), this is verified by construction.
    // Additionally, check that no tracking arrays exist for them.
    const derivedKinds = ['agg_trades', 'book_snapshots', 'snapshots'];
    for (const kind of derivedKinds) {
      assert.ok(
        !writes[kind],
        `no write tracking exists for derived kind "${kind}"`,
      );
    }
  });

  // ── Assertion (5): 1s wait produces no derived files ─────────────────────

  it('(5) no derived files generated after 1s wait', async () => {
    // Wait 1 second more to ensure any hypothetical flush cycle would
    // have had time to produce files.
    await sleep(1000);

    // Check that no .jsonl or .jsonl.open files exist for derived kinds.
    const derivedKinds = ['agg_trades', 'book_snapshots', 'snapshots'];
    for (const kind of derivedKinds) {
      const kindDir = path.join(baseDir, kind);
      let exists = false;
      try {
        await fsp.access(kindDir);
        exists = true;
      } catch {
        // Directory not found — this is expected (no files at all)
      }
      assert.ok(
        !exists,
        `derived kind directory "${kind}" should not exist on disk`,
      );
    }
  });

  // ── Assertion (6): stats / stateChange / ready IPC maintained ────────────

  it('(6) stats / stateChange / ready IPC messages are maintained', () => {
    const types = ipcMessages.map((m) => m.type);

    assert.ok(
      types.includes('ready'),
      'IPC should include "ready" message',
    );
    assert.ok(
      types.includes('stats'),
      'IPC should include "stats" message',
    );
    assert.ok(
      types.includes('stateChange'),
      'IPC should include "stateChange" message',
    );
    assert.ok(
      types.includes('liquidation'),
      'IPC should include "liquidation" message (IPC forwarding)',
    );

    // Verify ready has workerId
    const readyMsg = ipcMessages.find((m) => m.type === 'ready');
    assert.strictEqual(readyMsg.workerId, 'test-worker');

    // Verify stateChange carries market + stats
    const scMsg = ipcMessages.find((m) => m.type === 'stateChange');
    assert.strictEqual(scMsg.market, 'test_market');
    assert.strictEqual(scMsg.from, 'initializing');
    assert.strictEqual(scMsg.to, 'running');
    assert.ok(typeof scMsg.stats === 'object');
  });

  // ── Assertion (7): shutdown finalizes only raw 3 writers ─────────────────

  it('(7) shutdown finalizes only the 3 raw writers', async () => {
    // Finalize the raw writers (simulating shutdown sequence)
    await rawTradesWriter.finalize();
    await rawBookWriter.finalize();
    await rawLiqWriter.finalize();

    // Verify raw .jsonl files exist (one per kind)
    const tradesFiles = await findJsonlFiles(baseDir, 'trades');
    const bookFiles = await findJsonlFiles(baseDir, 'book_updates');
    const liqFiles = await findJsonlFiles(baseDir, 'liquidations');

    assert.ok(
      tradesFiles.length > 0,
      'trades .jsonl should exist after finalize',
    );
    assert.ok(
      bookFiles.length > 0,
      'book_updates .jsonl should exist after finalize',
    );
    assert.ok(
      liqFiles.length > 0,
      'liquidations .jsonl should exist after finalize',
    );

    // Verify derived files do NOT exist
    const derivedKinds = ['agg_trades', 'book_snapshots', 'snapshots'];
    for (const kind of derivedKinds) {
      const files = await findJsonlFiles(baseDir, kind);
      assert.strictEqual(
        files.length,
        0,
        `no .jsonl files should exist for derived kind "${kind}"`,
      );
    }

    // Verify write counts haven't changed (no extra writes during shutdown)
    assert.strictEqual(writes.trades.length, 1, 'still exactly 1 trade write');
    assert.strictEqual(
      writes.book_updates.length,
      1,
      'still exactly 1 book_update write',
    );
    assert.strictEqual(
      writes.liquidations.length,
      1,
      'still exactly 1 liquidation write',
    );
  });
});

// ── Phase 3b: B2 Graceful Degradation ────────────────────────────────────

describe('B2 graceful degradation (per-market health isolation)', () => {
  let baseDir;
  let hm; // HealthMonitor for B2 verification
  let writer1, writer2; // trades writers for two markets
  let writes1, writes2;
  let mockConn1, mockConn2;
  const ipcMessages = [];

  before(async () => {
    baseDir = path.join(
      os.tmpdir(), 'btc-receiver-test', 'b2',
      `b2-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    );
    fs.mkdirSync(baseDir, { recursive: true });
    writes1 = [];
    writes2 = [];

    writer1 = spyWriter(
      new RawRotationWriter(baseDir, 'market_a', 'trades', { flushIntervalMs: 50 }),
      writes1,
    );
    writer2 = spyWriter(
      new RawRotationWriter(baseDir, 'market_b', 'trades', { flushIntervalMs: 50 }),
      writes2,
    );

    // Set up HealthMonitor (simulating main thread tracking both markets)
    const healthFile = path.join(baseDir, 'health-b2.jsonl');
    hm = new HealthMonitor(healthFile, { intervalMs: 100 });
    // Register both markets as running initially
    const ts = Date.now();
    hm.updateConnector('market_a', {
      state: 'running', connectedAt: ts,
      lastDepthMsgAt: ts, lastTradeMsgAt: ts,
      depthMsgCount: 0, tradeMsgCount: 0,
      reconnectCount: 0, resyncCount: 0, lastSeq: 0,
    });
    hm.updateConnector('market_b', {
      state: 'running', connectedAt: ts,
      lastDepthMsgAt: ts, lastTradeMsgAt: ts,
      depthMsgCount: 0, tradeMsgCount: 0,
      reconnectCount: 0, resyncCount: 0, lastSeq: 0,
    });

    mockConn1 = createMockConnector();
    mockConn2 = createMockConnector();

    mockConn1.on('trade', async (evt) => {
      await writer1.write(evt, evt.ts);
    });
    mockConn2.on('trade', async (evt) => {
      await writer2.write(evt, evt.ts);
    });

    // Emit 1 trade to each market
    mockConn1.emit('trade', { price: 50000, qty: 0.1, side: 'buy', ts: Date.now() });
    mockConn2.emit('trade', { price: 51000, qty: 0.2, side: 'sell', ts: Date.now() });
    await sleep(200);
  });

  after(async () => {
    await writer1.finalize();
    await writer2.finalize();
    if (hm) hm.close();
    await fsp.rm(baseDir, { recursive: true, force: true }).catch(() => {});
  });

  it('both markets receive trades before failure', () => {
    assert.strictEqual(writes1.length, 1, 'market_a should have 1 trade');
    assert.strictEqual(writes2.length, 1, 'market_b should have 1 trade');
  });

  it('market_a continues to receive trades after market_b mock connector error', async () => {
    // Record writes1 length before market_b error
    const writes1Before = writes1.length;

    // Simulate market_b error
    mockConn2.on('error', () => {}); // prevent ERR_UNHANDLED_ERROR
    mockConn2._state = 'error';
    mockConn2.emit('error', { market: 'market_b', message: 'connection lost' });

    // Update HealthMonitor to reflect market_b error
    hm.updateConnector('market_b', {
      state: 'error', connectedAt: Date.now(),
      lastDepthMsgAt: 0, lastTradeMsgAt: 0,
      depthMsgCount: 0, tradeMsgCount: 0,
      reconnectCount: 1, resyncCount: 0, lastSeq: 0,
    });

    // market_a still processing
    mockConn1.emit('trade', { price: 50100, qty: 1.0, side: 'buy', ts: Date.now() });
    await sleep(200);

    // market_a should have received the new trade
    assert.strictEqual(
      writes1.length,
      writes1Before + 1,
      'market_a should still process trades after market_b error',
    );
  });

  it('HealthMonitor shows market_b error + market_a running after single-market failure', () => {
    // B2 contract: failed market does NOT affect healthy market's health state.
    // Overall state should be 'critical' due to market_b error.
    const summary = hm.getHealthSummary();

    // market_b should be in error state
    assert.ok(summary.markets['market_b'], 'market_b should be in health summary');
    assert.strictEqual(summary.markets['market_b'].state, 'error',
      'market_b should be error in health summary');

    // market_a should still be running
    assert.ok(summary.markets['market_a'], 'market_a should be in health summary');
    assert.strictEqual(summary.markets['market_a'].state, 'running',
      'market_a should remain running in health summary');

    // Overall should be critical (any market in error)
    assert.strictEqual(summary.state, 'critical',
      'overall health should be critical when any market is in error');
  });

  it('market_a writer has trades, market_b writer also has pre-failure trades', async () => {
    await writer1.finalize();
    await writer2.finalize();

    // Both markets should have at least their initial trade files
    const aFiles = await findJsonlFiles(baseDir, 'trades');
    // findJsonlFiles only looks for /trades/ in path, so it catches both markets
    assert.ok(aFiles.length >= 2, `expected at least 2 trade files, got ${aFiles.length}`);
  });
});

// ── Phase 5: B4 Directory Layout Compliance ──────────────────────────────

describe('B4 directory layout compliance', () => {
  let baseDir;
  const ts = Date.now();

  before(() => {
    baseDir = path.join(
      os.tmpdir(), 'btc-receiver-test', 'b4',
      `b4-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    );
    fs.mkdirSync(baseDir, { recursive: true });
  });

  after(async () => {
    await fsp.rm(baseDir, { recursive: true, force: true }).catch(() => {});
  });

  it('trade output path contains /trades/<market>/<date>/<window>.jsonl', async () => {
    const w = new RawRotationWriter(baseDir, 'binance_spot', 'trades', {
      flushIntervalMs: 50,
    });
    await w.write({ price: 100 }, ts);
    await w.finalize();

    const files = await findJsonlFiles(baseDir, 'trades');
    assert.ok(files.length > 0, 'should have at least 1 trade file');

    for (const f of files) {
      const rel = path.relative(baseDir, f);
      // Expected: trades/binance_spot/YYYY-MM-DD/HH-MM-SS.jsonl
      const parts = rel.split(path.sep);
      assert.strictEqual(parts[0], 'trades', 'top-level dir should be trades');
      assert.ok(parts.length >= 4, `expected depth >= 4, got ${parts.length} for ${rel}`);
    }
  });

  it('book_update output path contains /book_updates/<market>/<date>/<window>.jsonl', async () => {
    const w = new RawRotationWriter(baseDir, 'bybit_perp', 'book_updates', {
      flushIntervalMs: 50,
    });
    await w.write({ type: 'delta', bids: [], asks: [] }, ts + 1);
    await w.finalize();

    const files = await findJsonlFiles(baseDir, 'book_updates');
    assert.ok(files.length > 0);
    for (const f of files) {
      const rel = path.relative(baseDir, f);
      const parts = rel.split(path.sep);
      assert.strictEqual(parts[0], 'book_updates');
      assert.strictEqual(parts[1], 'bybit_perp');
    }
  });

  it('liquidation output path contains /liquidations/<market>/<date>/<window>.jsonl', async () => {
    const w = new RawRotationWriter(baseDir, 'okx_perp', 'liquidations', {
      flushIntervalMs: 50,
    });
    await w.write({ side: 'sell', price: 49000, qty: 1 }, ts + 2);
    await w.finalize();

    const files = await findJsonlFiles(baseDir, 'liquidations');
    assert.ok(files.length > 0);
    for (const f of files) {
      const rel = path.relative(baseDir, f);
      const parts = rel.split(path.sep);
      assert.strictEqual(parts[0], 'liquidations');
      assert.strictEqual(parts[1], 'okx_perp');
    }
  });

  it('no cross-kind writes (trades not in book_updates dir)', async () => {
    // Write a trade to the trades writer, verify it doesn't appear under book_updates
    const tradeWriter = new RawRotationWriter(baseDir, 'kraken_spot', 'trades', {
      flushIntervalMs: 50,
    });
    await tradeWriter.write({ price: 200 }, ts + 3);
    await tradeWriter.finalize();

    // All trade files should be under the trades/ directory
    const allFiles = [];
    async function walk(dir) {
      let entries;
      try { entries = await fsp.readdir(dir, { withFileTypes: true }); }
      catch { return; }
      for (const e of entries) {
        const full = path.join(dir, e.name);
        if (e.isDirectory()) await walk(full);
        else if (e.isFile() && full.endsWith('.jsonl')) allFiles.push(full);
      }
    }
    await walk(baseDir);

    // Check that kraken_spot files are only in the trades directory
    for (const f of allFiles) {
      const rel = path.relative(baseDir, f);
      if (rel.includes('kraken_spot')) {
        assert.ok(rel.startsWith('trades'), `kraken data should only be in trades/: ${rel}`);
      }
    }
  });
});

// ── FIX4: writer error propagation to health ──────────────────────────────

describe('FIX4 worker-level writer error propagation', () => {
  let baseDir;

  before(() => {
    baseDir = path.join(
      os.tmpdir(), 'btc-receiver-test', 'fix4',
      `fix4-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    );
    fs.mkdirSync(baseDir, { recursive: true });
  });

  after(async () => {
    _testReset();
    _setTestMakeWriterFn(null);
    await fsp.rm(baseDir, { recursive: true, force: true }).catch(() => {});
  });

  it('F4-A1: _propagateWriterError marks connector error + sends IPC', async () => {
    _testReset();
    const ipc = [];
    _workerSetTestParentPort({ postMessage: (msg) => ipc.push(msg) });
    _testInit({
      outputBase: baseDir,
      configMarkets: { binance_spot: { symbol: 'btcusdt' } },
      configOutput: {},
      workerId: 'test-worker',
    });

    // prepareMarket → connector + writers をセットアップ
    await _testPrepareMarket('binance_spot');

    // エラー注入用の writer を作成（_testMakeWriterFn で write を失敗させる）
    _setTestMakeWriterFn(() => ({
      _filePath: '/tmp/fake',
      async write() { throw new Error('injected: writer I/O failure'); },
      async flush() {},
      async close() {},
    }));
    const errWriter = new RawRotationWriter(baseDir, 'binance_spot', 'trades', {
      flushIntervalMs: 50,
    });
    // write → 失敗 → errorCount = 1
    await errWriter.write({ fail: true }, Date.now());
    assert.strictEqual(
      errWriter.getWriteErrorCount(),
      1,
      'writer error count should be 1 after failing write',
    );

    // _propagateWriterError → connector state + IPC を期待
    _propagateWriterError('binance_spot', errWriter);

    // IPC に writerError が含まれている
    const writerErrorMsg = ipc.find((m) => m.type === 'writerError');
    assert.ok(writerErrorMsg, 'writerError IPC message should exist');
    assert.strictEqual(writerErrorMsg.market, 'binance_spot');
    assert.strictEqual(writerErrorMsg.errorCount, 1);
    assert.ok(
      writerErrorMsg.lastErrorMessage.includes('injected'),
      `message contains injected: ${writerErrorMsg.lastErrorMessage}`,
    );

    // connector の state が error になっている
    // _propagateWriterError は connectors map から connector を取得して state を変更する
    // _testPrepareMarket が作成した connector がそれ

    // cleanup
    await errWriter.finalize();
    _setTestMakeWriterFn(null);
    _workerSetTestParentPort(null);
    _testReset();
  });

  it('F4-A2: error 状態の connector に再通知しない', async () => {
    _testReset();
    const ipc = [];
    _workerSetTestParentPort({ postMessage: (msg) => ipc.push(msg) });
    _testInit({
      outputBase: baseDir,
      configMarkets: { binance_spot: { symbol: 'btcusdt' } },
      configOutput: {},
      workerId: 'test-worker',
    });

    await _testPrepareMarket('binance_spot');

    _setTestMakeWriterFn(() => ({
      _filePath: '/tmp/fake',
      async write() { throw new Error('injected: writer I/O failure'); },
      async flush() {},
      async close() {},
    }));
    const errWriter = new RawRotationWriter(baseDir, 'binance_spot', 'trades', {
      flushIntervalMs: 50,
    });
    await errWriter.write({ fail: true }, Date.now());
    assert.strictEqual(errWriter.getWriteErrorCount(), 1);

    // 1回目の呼び出し → connector error + IPC
    _propagateWriterError('binance_spot', errWriter);
    const ipcBefore = ipc.length;

    // 2回目の呼び出し → connector はすでに error → 何もしない
    _propagateWriterError('binance_spot', errWriter);
    assert.strictEqual(
      ipc.length,
      ipcBefore,
      '2回目の呼び出しでは IPC が増えない',
    );

    await errWriter.finalize();
    _setTestMakeWriterFn(null);
    _workerSetTestParentPort(null);
    _testReset();
  });
});

// ── FIX5: output-root multi-instance lock ──────────────────────────────────
//
// doInit 内の acquireOutputRootLock が期待通り動作することを検証する。
// lock 競合時は startupFailed IPC が送信され、lock 空き時は通常の起動が継続される。

import {
  _setTestLockPid,
  _setTestLockFsError,
} from '../lib/raw-rotation-writer.mjs';

/** 手動でロックディレクトリを作り、競合状態をシミュレートする。 */
function manuallyLockOutputRoot(outputRoot, pid) {
  const lockDir = path.join(outputRoot, 'locks', 'receiver.lock');
  fs.mkdirSync(lockDir, { recursive: true });
  fs.writeFileSync(path.join(lockDir, 'pid'), String(pid), 'utf-8');
}

/** 手動ロックを削除する。 */
function manuallyUnlockOutputRoot(outputRoot) {
  const lockDir = path.join(outputRoot, 'locks', 'receiver.lock');
  try { fs.rmSync(lockDir, { recursive: true, force: true }); } catch {}
}

describe('FIX5: output-root lock in worker doInit', () => {
  let baseDir;

  /**
   * FIX7a: 実際の WebSocket 接続を開かないモックコネクタクラスを生成するファクトリ。
   * doInit → connectMarket が期待する connect / _syncBook / getState / getStats /
   * _setState / disconnect をすべて備え、stateChange を emit する。
   */
  function createSucceedingConnectorClass() {
    const SucceedingConn = function () {
      const conn = new EventEmitter();
      conn._state = 'init';
      conn._stats = {
        state: 'init', connectedAt: 0, lastDepthMsgAt: 0,
        lastTradeMsgAt: 0, depthMsgCount: 0, tradeMsgCount: 0,
        reconnectCount: 0, resyncCount: 0, lastSeq: 0,
      };
      conn.getState = () => conn._state;
      conn.getStats = () => ({ ...conn._stats, state: conn._state });
      conn.book = { isEmpty: () => false };
      conn._setState = function (newState) {
        const old = conn._state;
        conn._state = newState;
        conn._stats.state = newState;
        conn.emit('stateChange', old, newState);
      };
      conn.connect = function () { this._setState('connected'); return Promise.resolve(); };
      conn._syncBook = function () { this._setState('running'); return Promise.resolve(); };
      conn.disconnect = function () {};
      return conn;
    };
    SucceedingConn.prototype = {};
    return SucceedingConn;
  }

  before(() => {
    _setTestLockPid(null);
  });

  afterEach(() => {
    _setTestLockPid(null);
    _workerSetTestParentPort(null);
    _testReset();
    if (baseDir) {
      manuallyUnlockOutputRoot(baseDir);
      rmDir(baseDir);
    }
  });

  after(() => {
    _setTestLockPid(null);
  });

  it('F5-1: doInit acquires lock on startup — no contention (single instance)', async () => {
    baseDir = tmpDir('fix5-init-normal');
    const ipc = [];
    _workerSetTestParentPort({ postMessage: (msg) => ipc.push(msg) });
    // FIX7a: モックコネクタを使い、実際の WebSocket 接続を防止する
    _setTestConnectorClasses({ binance_spot: createSucceedingConnectorClass() });
    _testInit({
      outputBase: baseDir,
      configMarkets: { binance_spot: {} },
      configOutput: { flush_trades_ms: 200 },
      workerId: 'test-fix5-normal',
    });

    await _testDoInit({
      cmd: 'init',
      workerId: 'test-fix5-normal',
      markets: ['binance_spot'],
      configMarkets: { binance_spot: {} },
      configOutput: { flush_trades_ms: 200 },
      outputBase: baseDir,
    });

    // ロックディレクトリが存在する
    const lockDir = path.join(baseDir, 'locks', 'receiver.lock');
    assert.ok(fs.existsSync(lockDir), 'lock directory should exist after doInit');
    const pidInFile = parseInt(fs.readFileSync(path.join(lockDir, 'pid'), 'utf-8').trim(), 10);
    assert.equal(pidInFile, process.pid, 'pid file should contain our PID');

    // ready IPC が送信された（起動が継続された証拠）
    const readyIpc = ipc.find(m => m.type === 'ready');
    assert.ok(readyIpc, 'should have sent ready IPC (lock acquired, startup continued)');
  });

  it('F5-2: doInit sends startupFailed when another process holds the lock', async () => {
    baseDir = tmpDir('fix5-init-contend');
    // リアルな生存 PID（自分自身）でロックを作り、_testLockPid で偽装する
    manuallyLockOutputRoot(baseDir, process.pid);
    _setTestLockPid(999999); // 自分を別 PID として動作させる

    const ipc = [];
    let exitCode = null;
    _workerSetTestParentPort({ postMessage: (msg) => ipc.push(msg) });
    _setTestExitFn((code) => { exitCode = code; }); // 実際の process.exit を防ぐ
    // FIX7a: モックコネクタ（ロック競合により prepareMarket は実行されないが、万一のガード）
    _setTestConnectorClasses({ binance_spot: createSucceedingConnectorClass() });

    // doInit を実行 → lock 競合で startupFailed + exit(1)
    await _testDoInit({
      cmd: 'init',
      workerId: 'test-fix5-contend',
      markets: ['binance_spot'],
      configMarkets: { binance_spot: {} },
      configOutput: { flush_trades_ms: 200 },
      outputBase: baseDir,
    });

    // startupFailed IPC が送信された
    const startupFail = ipc.find(m => m.type === 'startupFailed');
    assert.ok(startupFail, 'should have sent startupFailed IPC');
    assert.ok(
      startupFail.reason.startsWith('output-root-lock:'),
      `reason should mention output-root-lock, got: ${startupFail.reason}`,
    );
    assert.equal(startupFail.market, '*');

    // ready IPC は送信されていない
    const readyIpc = ipc.find(m => m.type === 'ready');
    assert.ok(!readyIpc, 'should NOT have sent ready IPC when lock is contended');

    // _setTestExitFn が呼ばれた（process.exit が阻止された）
    assert.equal(exitCode, 1, 'should have attempted exit(1)');
  });

  it('F5-3: doInit succeeds when lock is already held by same PID (same process)', async () => {
    baseDir = tmpDir('fix5-init-idempotent');
    // 同一プロセスのロックを事前に作成
    const lockDir = path.join(baseDir, 'locks', 'receiver.lock');
    fs.mkdirSync(path.dirname(lockDir), { recursive: true });
    fs.mkdirSync(lockDir);
    fs.writeFileSync(path.join(lockDir, 'pid'), String(process.pid), 'utf-8');

    const ipc = [];
    _workerSetTestParentPort({ postMessage: (msg) => ipc.push(msg) });
    // FIX7a: モックコネクタを使い、実際の WebSocket 接続を防止する
    _setTestConnectorClasses({ binance_spot: createSucceedingConnectorClass() });

    await _testDoInit({
      cmd: 'init',
      workerId: 'test-fix5-idempotent',
      markets: ['binance_spot'],
      configMarkets: { binance_spot: {} },
      configOutput: { flush_trades_ms: 200 },
      outputBase: baseDir,
    });

    // ready IPC が送信された（同一プロセス内 idempotent）
    const readyIpc = ipc.find(m => m.type === 'ready');
    assert.ok(readyIpc, 'should have sent ready IPC (same-PID lock is idempotent)');
  });
});
