import test from 'node:test';
import assert from 'node:assert/strict';
import { createSubscribeTracker } from '../lib/subscribe-tracker.mjs';

test('ack が期限内に来ない購読を未成立として挙げる (境界は超えたら)', () => {
  let t = 1000;
  const s = createSubscribeTracker({ now: () => t });
  s.requested('trades');
  t += 5000;
  assert.deepEqual(s.unacked(5000), [], '上限ちょうどは未成立としない');
  t += 1;
  assert.deepEqual(s.unacked(5000).map((u) => u.channel), ['trades']);
  s.acked('trades');
  assert.deepEqual(s.unacked(5000), []);
  assert.equal(s.text(5000), 'subs=ok');
});

test('失敗ackは unacked と failed の両方に出る (生存と誤認しない)', () => {
  let t = 0;
  const s = createSubscribeTracker({ now: () => t });
  s.requested('book');
  s.failed('book', 'invalid channel');
  t += 60000;
  assert.deepEqual(s.failedChannels(), [{ channel: 'book', reason: 'invalid channel' }]);
  assert.equal(s.text(30000), 'subs=book:failed');
});

test('再要求で失敗状態は消える', () => {
  let t = 0;
  const s = createSubscribeTracker({ now: () => t });
  s.requested('trades'); s.failed('trades', 'x');
  s.requested('trades');
  assert.deepEqual(s.failedChannels(), []);
  t += 40000;
  assert.equal(s.text(30000), 'subs=trades:no-ack');
});

test('複数チャンネルを重複なく要約する', () => {
  let t = 0;
  const s = createSubscribeTracker({ now: () => t });
  s.requested('a'); s.requested('b'); s.acked('b');
  s.requested('c'); s.failed('c', 'r');
  t += 45000;
  assert.equal(s.text(30000), 'subs=a:no-ack,c:failed');
  assert.equal(s.size(), 3);
});
