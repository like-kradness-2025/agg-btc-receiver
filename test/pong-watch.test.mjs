import test from 'node:test';
import assert from 'node:assert/strict';
import { unansweredPings, isPongStarved } from '../lib/pong-watch.mjs';

// 実測 (24hの journal) に基づく境界:
//   Binance系: 切断時 pings=749 pongs=742 (未応答7) / 正常時 未応答0〜1
//   Bitfinex/Kraken: 未応答0〜1 (切断はサーバー主導)
test('未応答数は pings-pongs (負にはしない)', () => {
  assert.equal(unansweredPings({ pingsSent: 749, pongsReceived: 742 }), 7);
  assert.equal(unansweredPings({ pingsSent: 10, pongsReceived: 10 }), 0);
  assert.equal(unansweredPings({ pingsSent: 5, pongsReceived: 7 }), 0);
  assert.equal(unansweredPings({}), 0);
});

test('Binanceの実測値 (7回未応答) は途絶と判定する', () => {
  assert.equal(isPongStarved({ pingsSent: 749, pongsReceived: 742 }), true);
});

test('正常時の実測値 (0〜1回未応答) では判定しない', () => {
  assert.equal(isPongStarved({ pingsSent: 100, pongsReceived: 100 }), false);
  assert.equal(isPongStarved({ pingsSent: 101, pongsReceived: 100 }), false, '1回(5秒)では張り直さない');
  assert.equal(isPongStarved({ pingsSent: 103, pongsReceived: 100 }), true, '3回(15秒)で張り直す');
});

test('一度も pong が返っていない接続では判定しない (非対応venueの誤再接続を防ぐ)', () => {
  assert.equal(isPongStarved({ pingsSent: 50, pongsReceived: 0 }), false);
  assert.equal(isPongStarved({ pingsSent: 50, pongsReceived: 0 }, { pongsEverReceived: true }), true);
});

test('limit は指定できる (0以下の指定は既定3に戻す)', () => {
  const l = { pingsSent: 10, pongsReceived: 8 };
  assert.equal(isPongStarved(l, { limit: 2 }), true);
  assert.equal(isPongStarved(l, { limit: 3 }), false);
  assert.equal(isPongStarved(l, { limit: 0 }), false, '不正な limit は既定3で判定');
});

test('入力が欠けていても例外にならない', () => {
  assert.equal(isPongStarved(undefined), false);
  assert.equal(isPongStarved(null, {}), false);
});
