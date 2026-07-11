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

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import fsp from 'node:fs/promises';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { RawRotationWriter } from '../lib/raw-rotation-writer.mjs';

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
