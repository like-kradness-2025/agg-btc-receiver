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

  it('(g) applyReady defaults a missing market state to unknown, not running (fail-visible)', () => {
    // A ready report that omits a market's state must NOT silently count that
    // market as running: the default is 'unknown' (data_complete stays false
    // until an explicit running report) — optimistic defaults hide outages.
    const t = new MarketStatusTracker({ expectedMarkets: ['m1', 'm2'] });
    const snap = t.applyReady('w1', {
      dataComplete: false,
      markets: [
        { market: 'm1', state: 'running', degradedReason: null },
        { market: 'm2', degradedReason: null }, // state omitted
      ],
    });
    assert.equal(snap.data_complete, false, 'missing state must not imply running');
    assert.equal(snap.markets['m2'].state, 'unknown');
    assert.deepEqual(snap.running_markets, ['m1']);
  });

  it('(h) stats-tick observations downgrade but never clear an active degradation', () => {
    const t = new MarketStatusTracker({ expectedMarkets: ['m1'] });
    t.markDegraded('m1', 'initial sync failed: snapshot timeout');

    // Periodic stats keep ticking a stale 'running' for the isolated market —
    // must NOT clear the degradation (false recovery via the 2s stats push).
    const r = t.observeStatsState('m1', 'running');
    assert.equal(r.recovered, false);
    assert.equal(r.dataComplete, false);
    assert.equal(r.changed, false);
    let s = t.snapshot();
    assert.equal(s.markets['m1'].state, 'degraded');
    assert.equal(s.markets['m1'].degraded_reason, 'initial sync failed: snapshot timeout');
    assert.ok('m1' in s.degraded_markets, 'stats running must not clear degraded_markets');

    // A stats downgrade (reconnecting) IS reflected while degradation persists.
    const d = t.observeStatsState('m1', 'reconnecting');
    assert.equal(d.dataComplete, false);
    s = t.snapshot();
    assert.equal(s.markets['m1'].state, 'reconnecting');
    assert.ok('m1' in s.degraded_markets, 'downgrade must keep the degraded flag');

    // Only an explicit event (stateChange → running) recovers the market.
    const rec = t.observeState('m1', 'running');
    assert.equal(rec.recovered, true);
    assert.equal(rec.dataComplete, true);
    s = t.snapshot();
    assert.equal(s.markets['m1'].degraded_reason, null);
    assert.deepEqual(s.degraded_markets, {});

    // A non-degraded market is freely updated by stats ticks.
    const t2 = new MarketStatusTracker({ expectedMarkets: ['m2'] });
    t2.observeStatsState('m2', 'running');
    assert.equal(t2.snapshot().data_complete, true);
    const down = t2.observeStatsState('m2', 'reconnecting');
    assert.equal(down.dataComplete, false, 'stats downgrade must flip data_complete');
  });

  it('(i) markDegraded(null) records the degraded→unknown transition on clear', () => {
    const t = new MarketStatusTracker({ expectedMarkets: ['m1'] });
    t.markDegraded('m1', 'watchdog restart');
    const r = t.markDegraded('m1', null);
    assert.ok(r.transition, 'clear-without-recovery must record a transition');
    assert.equal(r.transition.from, 'degraded');
    assert.equal(r.transition.to, 'unknown');
    const s = t.snapshot();
    assert.equal(s.markets['m1'].state, 'unknown');
    assert.equal(s.markets['m1'].degraded_reason, null);
    assert.equal(s.markets['m1'].last_transition.to, 'unknown');
    assert.equal(s.data_complete, false, 'cleared-but-never-running stays incomplete');
    // Clearing when nothing is degraded stays a no-op.
    const noop = t.markDegraded('m1', '');
    assert.equal(noop.transition, null);
    assert.equal(noop.changed, false);
  });

  it('(j) scope with no required markets (all optional) is vacuously complete', () => {
    const t = new MarketStatusTracker({
      expectedMarkets: ['opt1', 'opt2'],
      optionalMarkets: ['opt1', 'opt2'],
    });
    assert.equal(t.snapshot().data_complete, true,
      'zero required markets must not pin data_complete to false forever');
    // Optional degradations still never block completeness.
    t.markDegraded('opt1', 'spot feed degraded');
    assert.equal(t.snapshot().data_complete, true);
  });
});
