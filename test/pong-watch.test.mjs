import test from 'node:test';
import assert from 'node:assert/strict';
import { unansweredPings, isPongStarved, DEFAULT_PONG_SILENCE_MS } from '../lib/pong-watch.mjs';

// 実測値 (journal) をそのまま使う:
//   本物の停止  : pings=749 pongs=742 lastPong=34914ms ago  → 張り直す
//   誤爆した例  : pings=830 pongs=827 lastPong=2808ms ago   → 張り直さない
//   正常       : lastPong 0.3〜6.0秒                        → 張り直さない
const live = (over = {}) => ({ pingsSent: 830, pongsReceived: 827, pongAgeMs: 3000, ...over });

test('本物の停止 (lastPong 34.9秒) は途絶と判定する', () => {
  assert.equal(isPongStarved({ pingsSent: 749, pongsReceived: 742, pongAgeMs: 34914 }), true);
});

test('pong が返っている間は未応答が3回でも張り直さない (誤爆の再発防止)', () => {
  assert.equal(isPongStarved(live({ pongAgeMs: 2808 })), false, '実測の誤爆例: lastPong 2.8秒');
  assert.equal(isPongStarved(live({ pongAgeMs: 6022 })), false, '実測の最大正常値: 6.0秒');
  assert.equal(isPongStarved(live({ pongAgeMs: 10000 })), false, '10秒までは待つ');
  assert.equal(isPongStarved(live({ pongAgeMs: 15000 })), true, '15秒で張り直す');
});

test('既定のしきい値は15秒 (ping間隔5秒の3回分)', () => {
  assert.equal(DEFAULT_PONG_SILENCE_MS, 15000);
});

test('一度も pong が返っていない接続では判定しない', () => {
  assert.equal(isPongStarved({ pingsSent: 50, pongsReceived: 0, pongAgeMs: null }), false);
  assert.equal(isPongStarved({ pingsSent: 50, pongsReceived: 0, pongAgeMs: null }, { pongsEverReceived: true }), false,
    'pong 未受信なら経過時間が無いので判定しない');
});

test('ping を送っていない / 入力が欠けても例外にならない', () => {
  assert.equal(isPongStarved({ pingsSent: 0, pongsReceived: 1, pongAgeMs: 60000 }), false);
  assert.equal(isPongStarved(undefined), false);
  assert.equal(isPongStarved(null, {}), false);
  assert.equal(isPongStarved(live({ pongAgeMs: 'x' })), false);
});

test('しきい値は指定でき、不正な値は既定に戻る', () => {
  const l = live({ pongAgeMs: 8000 });
  assert.equal(isPongStarved(l, { maxAgeMs: 5000 }), true);
  assert.equal(isPongStarved(l, { maxAgeMs: 0 }), false);
});

test('unansweredPings はログ用の補助情報 (負にしない)', () => {
  assert.equal(unansweredPings({ pingsSent: 749, pongsReceived: 742 }), 7);
  assert.equal(unansweredPings({ pingsSent: 5, pongsReceived: 7 }), 0);
  assert.equal(unansweredPings({}), 0);
});
