import test from 'node:test';
import assert from 'node:assert/strict';
import { backoffDelayMs, shouldResetAttempts, STABILITY_MS } from '../lib/reconnect-backoff.mjs';

const noJitter = { random: () => 0 };

test('連続失敗で待ち時間が単調増加し、上限で頭打ちになる', () => {
  const seq = [1, 2, 3, 4, 5, 6, 7, 8].map((n) => backoffDelayMs(n, noJitter));
  assert.deepEqual(seq, [1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000], '1s→2s→4s…上限30s');
  assert.equal(backoffDelayMs(100, noJitter), 30000, '何回失敗しても上限を超えない');
});

test('jitter は上限の範囲内で加算される', () => {
  const d = backoffDelayMs(1, { random: () => 0.5, jitterMs: 1000 });
  assert.equal(d, 1500);
  const capped = backoffDelayMs(10, { random: () => 0.999, jitterMs: 1000 });
  assert.ok(capped <= 30000 + 1000, '上限 + jitter までは許容');
});

test('不正な入力でも既定値で動く (例外にしない)', () => {
  assert.equal(backoffDelayMs(undefined, noJitter), 1000);
  assert.equal(backoffDelayMs(0, noJitter), 1000);
  assert.equal(backoffDelayMs(-5, noJitter), 1000);
  assert.equal(backoffDelayMs(3, { baseMs: 0, maxMs: 0, random: () => 0 }), 4000, '不正な base/max は既定');
});

test('試行回数は「安定稼働」を確認するまで戻さない', () => {
  assert.equal(STABILITY_MS, 60000);
  assert.equal(shouldResetAttempts(0), false, '接続直後は戻さない');
  assert.equal(shouldResetAttempts(59999), false);
  assert.equal(shouldResetAttempts(60000), true, '60秒無事なら戻す');
  assert.equal(shouldResetAttempts(undefined), false);
  assert.equal(shouldResetAttempts(30000, { stableMs: 30000 }), true);
});
