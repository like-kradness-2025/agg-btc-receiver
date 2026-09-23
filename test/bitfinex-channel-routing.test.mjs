import test from 'node:test';
import assert from 'node:assert/strict';
import { BitfinexConnector } from '../lib/bitfinex-connector.mjs';
import { BitstampConnector } from '../lib/bitstamp-connector.mjs';

// 2026-09-23 (Astra P1 / M2): 旧 chanId のフレームが約定経路へ流れる事故と、
// snapshot 前の差分を 'snapshot' と偽って出す分岐を固定する。
function makeBitfinex() {
  const c = new BitfinexConnector({ market: 'bitfinex_spot', wsUrl: 'wss://example.invalid' });
  const sent = [];
  const trades = [];
  const depths = [];
  c._ws = { send: (p) => sent.push(p), close: () => {} };
  c._emitTrade = (...args) => trades.push(args);
  c._emitDepth = (type, ...rest) => depths.push([type, ...rest]);
  return { c, sent, trades, depths };
}

test('trades chanId のフレームだけが約定になる', () => {
  const { c, trades } = makeBitfinex();
  c._onMessage({ event: 'subscribed', channel: 'trades', chanId: 111 });
  c._onMessage({ event: 'subscribed', channel: 'book', chanId: 222 });
  c._onMessage([111, 'te', [1, 2, 3, 4]]); // 未確定: 出さない
  assert.equal(trades.length, 0, "'te' は二重計上を避けるため出さない");
  c._onMessage([111, 'tu', [1, 2, 3, 4]]);
  assert.equal(trades.length, 1, 'trades chanId の tu は約定になる');
});

test('未知/旧 chanId のフレームは約定経路へ流れない (旧book混入の防止)', () => {
  const { c, trades, depths } = makeBitfinex();
  c._onMessage({ event: 'subscribed', channel: 'trades', chanId: 111 });
  c._onMessage({ event: 'subscribed', channel: 'book', chanId: 222 });
  // 再購読前の旧 book chanId (333) に trades 形式の配列が来ても約定にしない
  c._onMessage([333, 'tu', [1, 2, 3, 4]]);
  c._onMessage([333, [[1, 2, 3]]]);
  assert.equal(trades.length, 0, '未知 chanId は約定にしない');
  assert.equal(depths.length, 0, '未知 chanId は板にもしない');
  assert.equal(c._unknownChanFrames, 2, '無視した回数は数える (ログは初回のみ)');
});

test('_resubscribeAll は旧chanIdに解除を送り、記録を捨てて購読し直す', () => {
  const { c, sent } = makeBitfinex();
  c._onMessage({ event: 'subscribed', channel: 'trades', chanId: 111 });
  c._onMessage({ event: 'subscribed', channel: 'book', chanId: 222 });
  c._bookSnapshotReceived = true;
  sent.length = 0;
  assert.equal(c._resubscribeAll('test'), true);
  const unsubs = sent.filter((s) => s.includes('unsubscribe'));
  assert.equal(unsubs.length, 2, '両チャンネルに解除を送る');
  assert.ok(unsubs.some((s) => s.includes('111')) && unsubs.some((s) => s.includes('222')));
  assert.equal(sent.filter((s) => s.includes('"event":"subscribe"')).length, 2, '2チャンネル購読し直す');
  assert.equal(c._bookChanId, null);
  assert.equal(c._tradesChanId, null);
  assert.equal(c._bookSnapshotReceived, false, '板は snapshot 待ちに戻す');
});

test('snapshot 前の差分は板として出さず、再購読を1回だけ要求する', () => {
  const { c, depths } = makeBitfinex();
  let resub = 0;
  c._resubscribeAll = () => { resub += 1; return true; };
  c._onMessage({ event: 'subscribed', channel: 'book', chanId: 222 });
  c._bookSnapshotReceived = false;
  c._handleBook([222, [100, 1, 1]]);
  c._handleBook([222, [101, 1, 1]]);
  assert.equal(depths.length, 0, 'snapshot 前に板を出さない (誤った断面を配らない)');
  assert.equal(resub, 1, '再購読は1回に抑える (ループ防止)');
});

test('20061 は全ch再購読を起こす', () => {
  const { c } = makeBitfinex();
  c._onMessage({ event: 'subscribed', channel: 'book', chanId: 222 });
  c._bookSnapshotReceived = true;
  let resub = 0;
  c._resubscribeAll = (r) => { resub += 1; c._lastReason = r; return true; };
  c._onMessage({ event: 'info', code: 20061, msg: 'Maintenance ended' });
  assert.equal(resub, 1);
  assert.match(String(c._lastReason), /20061/);
});

test('bitstamp: bts:request_reconnect で自分から張り直す', () => {
  const c = new BitstampConnector({ market: 'bitstamp_spot', wsUrl: 'wss://example.invalid' });
  c._ws = { send: () => {}, close: () => {} };
  let n = 0;
  c._proactiveReconnect = () => { n += 1; };
  c._onMessage({ event: 'bts:request_reconnect' });
  assert.equal(n, 1);
  c._onMessage({ event: 'bts:heartbeat' });
  assert.equal(n, 1, 'ハートビートでは再接続しない');
});
