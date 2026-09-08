// test/market-status-file.test.mjs — Issue #9 (Done条件#8): the downstream
// market-status.json contract document (schema receiver-market-status/v1) and
// its atomic file writes, driven through the SAME state-transition functions
// the main thread uses (MarketStatusTracker.observeState / markDegraded /
// applyReady / observeStatsState + formatMarketStatusV1).
//
// Design: MarketStatusTracker (lib/market-status.mjs) is the SINGLE source of
// truth; the status file is a projection of its snapshot. There is no second
// tracker. A missing/stale (>15s) file must never be read as "complete"
// (fail-visible) — see docs/current/data-contract.md.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {
  MarketStatusTracker,
  defaultStatusFilePath,
  formatMarketStatusV1,
  writeMarketStatusFile,
} from '../lib/market-status.mjs';

/** All-enabled scope: binance_spot is the optional (never-blocking) member. */
const EXPECTED = ['binance_perp', 'coinbase_spot', 'binance_spot'];
const OPTIONAL = ['binance_spot'];

function freshTracker() {
  return new MarketStatusTracker({ expectedMarkets: EXPECTED, optionalMarkets: OPTIONAL });
}

/** Ready reports from worker B (binance_perp, coinbase_spot) and A (binance_spot). */
function readyReport(workerId, markets) {
  return {
    workerId,
    dataComplete: true,
    markets: markets.map((market) => ({ market, state: 'running', degradedReason: null })),
  };
}

/** File document for the current tracker state (main-thread equivalent). */
function docFor(tracker, processReady = true) {
  return formatMarketStatusV1(tracker.snapshot(), { processReady });
}

describe('formatMarketStatusV1 — receiver-market-status/v1 document', () => {
  it('exposes every contract key; expected = all enabled (optional subset repeated)', () => {
    const t = freshTracker();
    t.applyReady('B', readyReport('B', ['binance_perp', 'coinbase_spot']));
    t.applyReady('A', readyReport('A', ['binance_spot']));
    const doc = docFor(t);
    assert.equal(doc.schema, 'receiver-market-status/v1');
    assert.ok(Number.isSafeInteger(doc.ts_ms));
    assert.equal(doc.process_ready, true);
    assert.equal(doc.data_complete, true);
    assert.deepEqual(doc.expected_markets, EXPECTED);
    assert.deepEqual(doc.optional_markets, OPTIONAL);
    assert.deepEqual(doc.running_markets, EXPECTED);
    assert.deepEqual(doc.degraded_markets, {});
    // markets entries carry exactly the contract sub-keys.
    assert.deepEqual(Object.keys(doc.markets.binance_perp).sort(),
      ['degraded_reason', 'required', 'state', 'updated_at_ms']);
    assert.equal(doc.markets.binance_perp.state, 'running');
    assert.equal(doc.markets.binance_perp.required, true);
    assert.equal(doc.markets.binance_perp.degraded_reason, null);
    assert.equal(doc.markets.binance_spot.required, false);
  });

  it('required market degraded (markDegraded) ⇒ data_complete=false with the reason', () => {
    const t = freshTracker();
    t.applyReady('B', readyReport('B', ['binance_perp', 'coinbase_spot']));
    t.applyReady('A', readyReport('A', ['binance_spot']));
    t.markDegraded('binance_perp', 'initial sync failed: snapshot timeout');
    const doc = docFor(t);
    assert.equal(doc.data_complete, false);
    assert.deepEqual(doc.degraded_markets, { binance_perp: 'initial sync failed: snapshot timeout' });
    assert.deepEqual(doc.running_markets, ['coinbase_spot', 'binance_spot']);
    assert.equal(doc.markets.binance_perp.state, 'degraded');
    assert.equal(doc.markets.binance_perp.degraded_reason, 'initial sync failed: snapshot timeout');
  });

  it('required market that never reported stays unknown and incomplete (fail-visible)', () => {
    const t = freshTracker();
    // Only coinbase_spot reported; binance_perp never sent anything.
    t.applyReady('B', readyReport('B', ['coinbase_spot']));
    t.applyReady('A', readyReport('A', ['binance_spot']));
    const doc = docFor(t);
    assert.equal(doc.data_complete, false);
    assert.equal(doc.markets.binance_perp.state, 'unknown');
    assert.deepEqual(doc.degraded_markets, {}, 'unknown ≠ degraded: reason map stays empty');
    assert.deepEqual(doc.running_markets, ['coinbase_spot', 'binance_spot']);
  });

  it('OPTIONAL degradation never flips data_complete, but is still reported (P2 note)', () => {
    const t = freshTracker();
    t.applyReady('B', readyReport('B', ['binance_perp', 'coinbase_spot']));
    t.applyReady('A', readyReport('A', ['binance_spot']));
    t.markDegraded('binance_spot', 'optional feed down');
    const doc = docFor(t);
    assert.equal(doc.data_complete, true, 'optional market must not block completeness');
    // degraded_markets MAY include optional markets — fail-visible reporting.
    assert.deepEqual(doc.degraded_markets, { binance_spot: 'optional feed down' });
    assert.equal(doc.markets.binance_spot.state, 'degraded');
    assert.equal(doc.markets.binance_spot.required, false);
    assert.deepEqual(doc.running_markets, ['binance_perp', 'coinbase_spot']);
  });

  it('degraded is authoritative over stale stats; only an explicit event recovers', () => {
    const t = freshTracker();
    t.applyReady('B', readyReport('B', ['binance_perp', 'coinbase_spot']));
    t.applyReady('A', readyReport('A', ['binance_spot']));
    t.markDegraded('binance_perp', 'watchdog restart');
    // Periodic stats tick keeps asserting a stale 'running' — must NOT clear.
    t.observeStatsState('binance_perp', 'running');
    let doc = docFor(t);
    assert.equal(doc.data_complete, false);
    assert.equal(doc.markets.binance_perp.state, 'degraded', 'degraded overrides stale running');
    // Explicit stateChange → running recovers.
    t.observeState('binance_perp', 'running');
    doc = docFor(t);
    assert.equal(doc.data_complete, true);
    assert.equal(doc.markets.binance_perp.state, 'running');
    assert.deepEqual(doc.degraded_markets, {});
  });

  it('process_ready is a separate fact and does not gate the tracker verdict', () => {
    const t = freshTracker();
    t.applyReady('B', readyReport('B', ['binance_perp', 'coinbase_spot']));
    t.applyReady('A', readyReport('A', ['binance_spot']));
    const doc = docFor(t, false);
    assert.equal(doc.process_ready, false);
    assert.equal(doc.data_complete, true, 'data_complete is the market verdict; process_ready is separate');
  });
});

describe('market-status.json contract file writes', () => {
  it('defaults to a sibling of the receiver SQLite directory shared with downstream', () => {
    assert.equal(
      defaultStatusFilePath('/home/weed420/Tool/agg-btc-receiver/data/sqlite'),
      '/home/weed420/Tool/agg-btc-receiver/data/market-status.json',
    );
    assert.equal(
      defaultStatusFilePath('data/sqlite'),
      path.join(process.cwd(), 'data', 'market-status.json'),
    );
  });

  it('writes the current view atomically; required-market loss is persisted as data_complete=false', async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'agg-status-file-'));
    const file = path.join(dir, 'market-status.json');
    const t = freshTracker();
    t.applyReady('B', readyReport('B', ['binance_perp', 'coinbase_spot']));
    t.applyReady('A', readyReport('A', ['binance_spot']));
    await writeMarketStatusFile(file, docFor(t));
    let parsed = JSON.parse(await fs.readFile(file, 'utf8'));
    assert.equal(parsed.schema, 'receiver-market-status/v1');
    assert.equal(parsed.data_complete, true);

    // A market goes down → next write must persist the incomplete verdict
    // (this is the state-change immediate-write path in orderflow_monitor).
    t.markDegraded('coinbase_spot', 'worker B lost: exit code 1');
    await writeMarketStatusFile(file, docFor(t, false));
    parsed = JSON.parse(await fs.readFile(file, 'utf8'));
    assert.equal(parsed.data_complete, false);
    assert.equal(parsed.process_ready, false);
    assert.deepEqual(parsed.degraded_markets, { coinbase_spot: 'worker B lost: exit code 1' });
    assert.deepEqual(parsed.running_markets, ['binance_perp', 'binance_spot']);

    const leftovers = (await fs.readdir(dir)).filter((name) => name.includes('.tmp-'));
    assert.deepEqual(leftovers, [], 'tmp+rename must leave no residue after success');
    await fs.rm(dir, { recursive: true, force: true });
  });
});

describe('worker crash degradation regression (Qwen P1-2)', () => {
  it('worker exit/error degrades every market the worker owned via the tracker (data_complete=false)', async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'agg-status-crash-'));
    const file = path.join(dir, 'market-status.json');
    const t = freshTracker();
    t.applyReady('B', readyReport('B', ['binance_perp', 'coinbase_spot']));
    t.applyReady('A', readyReport('A', ['binance_spot']));
    assert.equal(docFor(t).data_complete, true);

    // orderflow_monitor degradeWorkerMarkets(): one markDegraded per market
    // assigned to the crashed worker, then force-publish.
    for (const market of ['binance_perp', 'coinbase_spot']) {
      t.markDegraded(market, 'worker B lost: error: boom');
    }
    const doc = docFor(t, false);
    assert.equal(doc.data_complete, false, 'crash must flip data_complete=false immediately');
    assert.deepEqual(doc.degraded_markets, {
      binance_perp: 'worker B lost: error: boom',
      coinbase_spot: 'worker B lost: error: boom',
    });
    assert.deepEqual(doc.running_markets, ['binance_spot'], 'surviving worker markets stay listed');
    assert.equal(doc.markets.binance_perp.state, 'degraded');
    await writeMarketStatusFile(file, doc);
    const parsed = JSON.parse(await fs.readFile(file, 'utf8'));
    assert.equal(parsed.data_complete, false);
    assert.equal(parsed.markets.binance_perp.degraded_reason, 'worker B lost: error: boom');
    await fs.rm(dir, { recursive: true, force: true });
  });

  it('crash of a worker owning ONLY optional markets stays data_complete=true but is reported', () => {
    // Worker A (binance_spot, optional) crashes: required markets are
    // untouched so completeness holds — the isolation must still surface.
    const t = freshTracker();
    t.applyReady('B', readyReport('B', ['binance_perp', 'coinbase_spot']));
    t.applyReady('A', readyReport('A', ['binance_spot']));
    t.markDegraded('binance_spot', 'worker A lost: exit code 1');
    const doc = docFor(t, false);
    assert.equal(doc.data_complete, true);
    assert.deepEqual(doc.degraded_markets, { binance_spot: 'worker A lost: exit code 1' });
    assert.deepEqual(doc.running_markets, ['binance_perp', 'coinbase_spot']);
  });
});
