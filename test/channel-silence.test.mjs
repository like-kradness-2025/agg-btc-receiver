import test from 'node:test';
import assert from 'node:assert/strict';
import { channelSilenceReport, ageFrom } from '../lib/channel-silence.mjs';

test('上限ちょうどは停滞としない (境界は残す)', () => {
  const r = channelSilenceReport({ depthAgeMs: 300_000, tradeAgeMs: 299_999, limitMs: 300_000 });
  assert.deepEqual(r.stale, []);
  assert.equal(r.text(), 'channel_silence=ok');
});

test('上限を超えたチャンネルだけを挙げる', () => {
  const r = channelSilenceReport({ depthAgeMs: 300_001, tradeAgeMs: 1_000, limitMs: 300_000 });
  assert.deepEqual(r.staleChannels, ['depth']);
  assert.match(r.text(), /^channel_silence=depth:300001ms$/);
});

test('両方止まっていれば両方を挙げる', () => {
  const r = channelSilenceReport({ depthAgeMs: 400_000, tradeAgeMs: 500_000, limitMs: 300_000 });
  assert.deepEqual(r.staleChannels, ['depth', 'trades']);
  assert.deepEqual(r.ages, { depth_ms: 400_000, trades_ms: 500_000 });
});

test('未受信(null)は判定対象外', () => {
  const r = channelSilenceReport({ depthAgeMs: null, tradeAgeMs: 900_000, limitMs: 300_000 });
  assert.deepEqual(r.staleChannels, ['trades']);
  assert.deepEqual(r.ages, { trades_ms: 900_000 });
});

test('limitMs が不正なら例外 (設定ミスを黙認しない)', () => {
  assert.throws(() => channelSilenceReport({ limitMs: undefined }), TypeError);
});

test('ageFrom: 未受信(0)は null、受信済みは経過ms', () => {
  assert.equal(ageFrom(0, 1000), null);
  assert.equal(ageFrom(undefined, 1000), null);
  assert.equal(ageFrom(1000, 5000), 4000);
  assert.equal(ageFrom(9000, 5000), 0);
});
