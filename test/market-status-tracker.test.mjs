// test/market-status-tracker.test.mjs — Issue #16 state-model tests.
//
// Verifies the separation of worker "ready" from market "data_complete":
// a worker reports ready once every connect attempt finished, but its
// completeness is only true while every REQUIRED market is running with a
// synced book. This file exercises lib/market-status.mjs (shared tracker):
//
//   (a) initial-sync failure ⇒ market degraded ⇒ data_complete=false even
//       though the worker process is alive and "ready",
//   (b) recovery: a later stateChange to 'running' clears the degraded
//       reason, records the transition, and re-arms data_complete,
//   (c) worker-ready-but-incomplete: applyReady() from the 'ready' IPC
//       report with a degraded market leaves data_complete=false,
//   (d) optional markets never block completeness; unknown (non-required)
//       markets never make it false,
//   (e) transition history is bounded per market and ordered by time,
//   (f) snapshot exposes expected/running/degraded(+reasons)/data_complete
//       for health output and IPC aggregation.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MarketStatusTracker } from '../lib/market-status.mjs';

describe('MarketStatusTracker — worker ready vs data_complete separation', () => {
  it('starts unknown for every expected market; data_complete=false until running', () => {
    const t = new MarketStatusTracker({ expectedMarkets: ['binance_perp', 'okx_perp'] });
    const s = t.snapshot();
    assert.equal(s.data_complete, false);
    assert.deepEqual(s.expected_markets.sort(), ['binance_perp', 'okx_perp']);
    assert.deepEqual(s.running_markets, []);
    assert.deepEqual(s.degraded_markets, {});
    assert.equal(s.markets['binance_perp'].state, 'unknown');
    assert.equal(s.markets['binance_perp'].degraded_reason, null);
  });

  it('(a) initial sync failure marks the market degraded and breaks data_complete', () => {
    const t = new MarketStatusTracker({ expectedMarkets: ['binance_perp'] });
    const r = t.markDegraded('binance_perp', 'initial connect failed (ECONNREFUSED)');
    assert.equal(r.dataComplete, false);
    assert.equal(r.changed, false); // was already incomplete
    const s = t.snapshot();
    assert.equal(s.data_complete, false);
    assert.equal(s.markets['binance_perp'].state, 'degraded');
    assert.equal(s.markets['binance_perp'].degraded_reason, 'initial connect failed (ECONNREFUSED)');
    assert.deepEqual(s.degraded_markets, { binance_perp: 'initial connect failed (ECONNREFUSED)' });
    assert.deepEqual(s.running_markets, []);
  });

  it('all required markets running ⇒ data_complete=true; degrade later flips it false', () => {
    const t = new MarketStatusTracker({ expectedMarkets: ['binance_perp', 'bybit_perp'] });
    t.observeState('binance_perp', 'running');
    assert.equal(t.snapshot().data_complete, false, 'one of two required markets');
    const r2 = t.observeState('bybit_perp', 'running');
    assert.equal(r2.dataComplete, true);
    assert.equal(r2.changed, true);
    assert.deepEqual(t.snapshot().running_markets.sort(), ['binance_perp', 'bybit_perp']);
    assert.equal(t.snapshot().data_complete, true);

    // A mid-flight degradation is fail-visible: completeness goes false at once.
    const d = t.markDegraded('binance_perp', 'watchdog: depth socket generation expired');
    assert.equal(d.dataComplete, false);
    assert.equal(d.changed, true);
    assert.deepEqual(t.snapshot().running_markets, ['bybit_perp']);
  });

  it('(b) recovery via stateChange→running clears the reason and re-arms completeness', () => {
    const t = new MarketStatusTracker({ expectedMarkets: ['binance_perp'] });
    t.observeState('binance_perp', 'running');
    t.markDegraded('binance_perp', 'socket died');
    assert.equal(t.snapshot().data_complete, false);

    const r = t.observeState('binance_perp', 'running');
    assert.equal(r.recovered, true, 'degraded → running must be recorded as a recovery');
    assert.equal(r.dataComplete, true);
    assert.equal(r.changed, true);
    const s = t.snapshot();
    assert.equal(s.data_complete, true);
    assert.equal(s.markets['binance_perp'].degraded_reason, null);
    assert.equal(s.markets['binance_perp'].state, 'running');
    assert.deepEqual(s.degraded_markets, {});
  });

  it('(c) ready IPC report (applyReady) with a degraded market = ready but incomplete', () => {
    const t = new MarketStatusTracker({ expectedMarkets: ['binance_perp', 'okx_perp'] });
    // Worker sent 'ready' after both connect attempts finished; OKX failed.
    const snap = t.applyReady('w1', {
      dataComplete: false,
      markets: [
        { market: 'binance_perp', state: 'running', degradedReason: null },
        { market: 'okx_perp', state: 'degraded', degradedReason: 'initial sync failed: snapshot timeout' },
      ],
    });
    assert.equal(snap.data_complete, false, 'ready must not imply complete');
    assert.deepEqual(snap.running_markets, ['binance_perp']);
    assert.equal(snap.markets['okx_perp'].degraded_reason, 'initial sync failed: snapshot timeout');

    // Later the background retry succeeds: stateChange running ⇒ complete.
    const r = t.observeState('okx_perp', 'running');
    assert.equal(r.recovered, true);
    assert.equal(r.dataComplete, true);
  });

  it('(d) optional markets never block data_complete; unknown markets never make it false', () => {
    const t = new MarketStatusTracker({
      expectedMarkets: ['binance_perp', 'bybit_spot'],
      optionalMarkets: ['bybit_spot'],
    });
    t.observeState('binance_perp', 'running');
    t.markDegraded('bybit_spot', 'spot connector degraded');
    assert.equal(t.snapshot().data_complete, true, 'optional degradation must not block completeness');
    assert.equal(t.isRequired('bybit_spot'), false);
    assert.equal(t.isRequired('binance_perp'), true);

    // Defensive tracking of an unexpected market must not flip completeness.
    t.observeState('some_other_market', 'degraded');
    assert.equal(t.snapshot().data_complete, true);
  });

  it('(e) transition history is bounded per market and ordered by time', () => {
    const t = new MarketStatusTracker({ expectedMarkets: ['m1'] });
    for (let i = 0; i < 30; i++) {
      t.observeState('m1', i % 2 === 0 ? 'running' : 'reconnecting');
    }
    const s = t.snapshot();
    const tx = s.markets['m1'].last_transition;
    assert.ok(tx && tx.to === s.markets['m1'].state);
    // Aggregate list is bounded and time-ordered ascending.
    assert.ok(s.transitions.length <= 50);
    for (let i = 1; i < s.transitions.length; i++) {
      assert.ok(s.transitions[i - 1].tsMs <= s.transitions[i].tsMs);
    }
    // Per-market history in the aggregate is bounded (30 observes ⇒ 30
    // alternating transitions, tracker keeps the latest 20).
    const m1tx = s.transitions.filter((x) => x.market === 'm1');
    assert.equal(m1tx.length, 20, 'per-market history must be bounded (MAX_TRANSITIONS_PER_MARKET)');
    // The last kept transition is the terminal one (running→reconnecting).
    assert.equal(m1tx[m1tx.length - 1].to, 'reconnecting');
  });

  it('(f) snapshot shape exposes every health/IPC field with stable names', () => {
    const t = new MarketStatusTracker({ expectedMarkets: ['m1'], optionalMarkets: [] });
    t.markDegraded('m1', 'boom');
    const s = t.snapshot();
    for (const k of ['ts_ms', 'expected_markets', 'optional_markets', 'running_markets',
      'degraded_markets', 'data_complete', 'markets', 'transitions']) {
      assert.ok(k in s, `snapshot missing ${k}`);
    }
    assert.ok(s.markets['m1'].required === true);
    assert.equal(typeof s.markets['m1'].updated_at_ms, 'number');
    // repeated same-state observe is a no-op transition-wise
    t.observeState('m1', 'reconnecting');
    const r = t.observeState('m1', 'reconnecting');
    assert.equal(r.transition, null, 'same-state observe must not record a transition');
  });
});
