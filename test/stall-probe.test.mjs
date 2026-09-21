import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { StallProbe, createStallLog, STALL_PROBE_DEFAULTS } from '../lib/stall-probe.mjs';

// 時計を自分で進めるためのハーネス (実タイマーに依存しない)。
function harness(options = {}) {
  const clock = { ms: 1_000_000 };
  const records = [];
  const probe = new StallProbe({
    label: 'main',
    sampleMs: 250,
    lagThresholdMs: 1500,
    recoveryStreak: 2,
    now: () => clock.ms,
    onAnomaly: (r) => records.push(r),
    ...options,
  });
  // start() は実タイマーを立てるので、テストでは sample() を手で叩く。
  probe.start();
  probe.stop();
  const advance = (ms, samples = 1) => {
    for (let i = 0; i < samples; i += 1) {
      clock.ms += ms;
      probe.sample();
    }
  };
  return { probe, records, clock, advance };
}

test('a healthy loop never records anything', () => {
  const { probe, records, advance } = harness();
  for (let i = 0; i < 20; i += 1) advance(250);
  assert.equal(records.length, 0);
  assert.equal(probe.stats().events, 0);
  assert.equal(probe.stats().samples, 20);
});

test('a blocked loop is recorded once, with the measured lag', () => {
  const { probe, records, advance } = harness();
  advance(250);                    // 正常サンプル
  advance(19_300);                 // 19.3 秒ブロックして復帰
  assert.equal(records.length, 1);
  const rec = records[0];
  assert.equal(rec.kind, 'stall');
  assert.equal(rec.label, 'main');
  assert.ok(rec.max_lag_ms >= 19_000, `max_lag=${rec.max_lag_ms}`);
  assert.equal(rec.lag_threshold_ms, 1500);
  assert.equal(probe.stats().events, 1);
});

test('continued stalls do not emit repeated start records', () => {
  const { probe, records, advance } = harness();
  advance(5000, 3);
  assert.equal(records.filter((r) => r.kind === 'stall').length, 1);
  assert.equal(probe.stats().events, 1);
});

test('recovery is recorded after the loop is quiet again', () => {
  const { records, advance } = harness();
  advance(250);
  advance(19_300);
  advance(250);                    // quiet 1
  advance(250);                    // quiet 2 → 復帰
  const kinds = records.map((r) => r.kind);
  assert.deepEqual(kinds, ['stall', 'recovered']);
  const rec = records[1];
  assert.ok(rec.duration_ms >= 19_000, `duration=${rec.duration_ms}`);
  assert.equal(rec.stalled_since, records[0].stalled_since);
});

test('spans straddling the stall are attached, older ones are not', () => {
  const { probe, records, clock, advance } = harness();
  // ずっと前の処理 (窓の外)
  probe.span('old.op', clock.ms - 60_000, { endedAtMs: clock.ms - 59_000 });
  advance(250);
  // ブロック中に走っていた処理 (窓の中)
  const startedAt = clock.ms;
  probe.span('raw.append', startedAt, { endedAtMs: startedAt + 19_000, details: { events: 16_384 } });
  advance(19_000);
  const rec = records[0];
  const names = rec.spans.map((s) => s.name);
  assert.deepEqual(names, ['raw.append']);
  assert.equal(rec.spans[0].dur_ms, 19_000);
  assert.deepEqual(rec.spans[0].details, { events: 16_384 });
});

test('gauges are captured inside the window and bounded by the ring', () => {
  const { probe, records, clock, advance } = harness({ ringSize: 10 });
  for (let i = 0; i < 30; i += 1) probe.note('rawDbPending', i);
  advance(250);
  probe.note('rawDbPending', 999);
  advance(19_000);
  const rec = records[0];
  assert.ok(rec.gauges.rawDbPending.length > 0);
  assert.ok(rec.gauges.rawDbPending.length <= 12);
  assert.equal(rec.gauges.rawDbPending[rec.gauges.rawDbPending.length - 1][1], 999);
  assert.ok(probe.stats().spansHeld <= 10);
});

test('wrap measures async work, returns its value and rethrows failures', async () => {
  const { probe, records, clock, advance } = harness();
  const value = await probe.wrap('raw.append', async () => {
    clock.ms += 4_200;
    return 'ok';
  }, { events: 10 });
  assert.equal(value, 'ok');
  advance(250);
  advance(250);
  const rec = records[0];
  const span = rec.spans.find((s) => s.name === 'raw.append');
  assert.equal(span.dur_ms, 4_200);
  await assert.rejects(
    () => probe.wrap('raw.append', async () => { throw new Error('sqlite busy'); }),
    /sqlite busy/,
  );
  assert.ok(probe.stats().spansRecorded >= 2, 'failed work is still recorded as a span');
});

test('wrapSync measures synchronous work and rethrows failures', () => {
  const { probe, clock } = harness();
  const value = probe.wrapSync('pruneExpired', () => {
    clock.ms += 3_000;
    return 'pruned';
  });
  assert.equal(value, 'pruned');
  assert.throws(() => probe.wrapSync('pruneExpired', () => { throw new Error('boom'); }), /boom/);
  assert.equal(probe.stats().spansRecorded, 2, 'the throwing call recorded its span too');
});

test('lag samples in the record describe the stall window', () => {
  const { records, advance } = harness();
  advance(250);
  advance(19_300);
  const rec = records[0];
  assert.ok(rec.lag_samples.length >= 1, 'the window keeps pre-stall context');
  const [iso, firstLag] = rec.lag_samples[0];
  assert.match(iso, /^\d{4}-\d{2}-\d{2}T/);
  assert.ok(firstLag < 1_500, 'the first sample is the healthy context sample');
  const lastLag = rec.lag_samples[rec.lag_samples.length - 1][1];
  assert.ok(lastLag >= 19_000, `last lag=${lastLag}`);
  assert.ok(rec.window.from < rec.window.to);
});

test('createStallLog writes only when asked and rotates by size', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'stall-log-'));
  const file = path.join(dir, 'stall-events.jsonl');
  const log = createStallLog({ filePath: file, maxBytes: 200, generations: 3, fsModule: fs });
  assert.equal(fs.existsSync(file), false, 'no file until an anomaly is written');
  for (let i = 0; i < 10; i += 1) assert.equal(log.write({ i, kind: 'stall' }), true);
  assert.equal(fs.existsSync(file), true);
  const lines = fs.readFileSync(file, 'utf8').trim().split('\n').length;
  assert.ok(lines >= 1);
  assert.equal(fs.existsSync(`${file}.1`), true, 'rotation happened');
  // 書き込み不能先: 既存ファイルをディレクトリとして扱わせる (即 ENOTDIR、/proc のようなハングを避ける)
  const blocker = path.join(dir, 'not-a-dir');
  fs.writeFileSync(blocker, 'x');
  const broken = createStallLog({ filePath: path.join(blocker, 'x.jsonl'), fsModule: fs });
  assert.equal(broken.write({ kind: 'stall' }), false, 'a write failure is reported, never thrown');
  fs.rmSync(dir, { recursive: true, force: true });
});

test('defaults are stated and the probe requires a label', () => {
  assert.equal(STALL_PROBE_DEFAULTS.lagThresholdMs, 1500);
  assert.equal(STALL_PROBE_DEFAULTS.sampleMs, 250);
  assert.throws(() => new StallProbe({}), /label/);
});

test('start/stop is idempotent and stats expose the configuration', () => {
  const probe = new StallProbe({ label: 'worker:A', sampleMs: 250, lagThresholdMs: 1500 });
  assert.equal(probe.running, false);
  probe.start();
  assert.equal(probe.running, true);
  probe.start();
  probe.stop();
  probe.stop();
  assert.equal(probe.running, false);
  const stats = probe.stats();
  assert.equal(stats.lagThresholdMs, 1500);
  assert.equal(stats.sampleMs, 250);
  assert.equal(stats.inEvent, false);
});

test('minSpanMs keeps high-frequency paths out of the ring', () => {
  const { probe, records, clock, advance } = harness({ minSpanMs: 25 });
  for (let i = 0; i < 50; i += 1) probe.span('socket.trade', clock.ms, { endedAtMs: clock.ms + 1 });
  assert.equal(probe.stats().spansRecorded, 0, 'fast handlers are not recorded');
  const startedAt = clock.ms;
  probe.span('raw.append', startedAt, { endedAtMs: startedAt + 19_000 });
  advance(250);
  advance(19_000);
  const rec = records[0];
  assert.deepEqual(rec.spans.map((s) => s.name), ['raw.append']);
});

test('begin() marks a hot path and only keeps slow invocations', () => {
  const { probe, records, clock, advance } = harness({ minSpanMs: 25 });
  for (let i = 0; i < 10; i += 1) {
    const mark = probe.begin('ipc.flush', { pending: i });
    clock.ms += 1;
    mark.end();
  }
  assert.equal(probe.stats().spansRecorded, 0);
  const slow = probe.begin('ipc.flush');
  clock.ms += 19_000;
  slow.end({ pending: 99 });
  advance(250);
  advance(19_000);
  const span = records[0].spans.find((s) => s.name === 'ipc.flush');
  assert.equal(span.dur_ms, 19_000);
  assert.deepEqual(span.details, { pending: 99 });
});
