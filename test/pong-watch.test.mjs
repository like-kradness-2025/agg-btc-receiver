import test from 'node:test';
import assert from 'node:assert/strict';
import { isPongStarved, DEFAULT_PONG_SILENCE_MS } from '../lib/pong-watch.mjs';

// 実測値 (journal) をそのまま使う:
//   本物の検知 2026-09-23 23:30:16: pings=1436 pongs=1432 lastPong=19976ms ago → 張り直す
//   誤爆 23:38:33        : pings=3 pongs=2 lastPong=307923ms ago (5分前の旧接続の pong) → 張り直さない
//   正常                : lastPong 2.8〜6.0秒
const atOpen = (pongs, ageMs) => ({ pongsAtConnectionOpen: pongs, connectionAgeMs: ageMs });

test('本物の途絶 (23:30 の実測: lastPong 20秒) は張り直す', () => {
  const live = { pingsSent: 1436, pongsReceived: 1432, pongAgeMs: 19976 };
  assert.equal(isPongStarved(live, { ...atOpen(1400, 60000) }), true);
});

test('新しい接続に旧接続の pong を当てない (23:38 の誤爆を再現しない)', () => {
  // 旧接続で2回 pong を受けており、いまの接続はまだ1回も受けていない
  const live = { pingsSent: 3, pongsReceived: 2, pongAgeMs: 307923 };
  assert.equal(isPongStarved(live, { ...atOpen(2, 20000) }), false);
});

test('開いた直後の接続は判定しない', () => {
  const live = { pingsSent: 1, pongsReceived: 1, pongAgeMs: 20000 };
  assert.equal(isPongStarved(live, { ...atOpen(0, 3000) }), false, '接続から3秒では判定しない');
  assert.equal(isPongStarved(live, { ...atOpen(0, 20000) }), true, '接続から20秒経っていれば判定する');
});

test('pong が返っている間は張り直さない (正常時の実測 2.8〜6.0秒)', () => {
  assert.equal(isPongStarved({ pingsSent: 830, pongsReceived: 827, pongAgeMs: 2808 }, atOpen(800, 600000)), false);
  assert.equal(isPongStarved({ pingsSent: 830, pongsReceived: 827, pongAgeMs: 6022 }, atOpen(800, 600000)), false);
  assert.equal(isPongStarved({ pingsSent: 830, pongsReceived: 827, pongAgeMs: 10000 }, atOpen(800, 600000)), false);
  assert.equal(isPongStarved({ pingsSent: 830, pongsReceived: 827, pongAgeMs: 15000 }, atOpen(800, 600000)), true);
});

test('既定のしきい値は15秒 (ping間隔5秒の3回分)', () => {
  assert.equal(DEFAULT_PONG_SILENCE_MS, 15000);
});

test('一度も pong が返っていない接続では判定しない (非対応venueで暴走しない)', () => {
  assert.equal(isPongStarved({ pingsSent: 50, pongsReceived: 0, pongAgeMs: null }, atOpen(0, 60000)), false);
});

test('ping を送っていない / 入力が欠けても例外にならない', () => {
  assert.equal(isPongStarved({ pingsSent: 0, pongsReceived: 1, pongAgeMs: 60000 }, atOpen(0, 60000)), false);
  assert.equal(isPongStarved(undefined), false);
  assert.equal(isPongStarved(null, {}), false);
  assert.equal(isPongStarved({ pingsSent: 5, pongsReceived: 5, pongAgeMs: 'x' }, atOpen(0, 60000)), false);
});
