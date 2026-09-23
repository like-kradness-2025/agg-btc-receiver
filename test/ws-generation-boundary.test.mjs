import test from 'node:test';
import assert from 'node:assert/strict';
import { BaseConnector } from '../lib/base-connector.mjs';

// Astra P1③（世代境界・タイマー所有）のうち、モックWS無しで固定できる部分。
// ソケットイベント経由の pong/close の世代判定は、既存の base-connector.test.mjs の
// モックWSパターンを使う増分で追加する。
class TestConnector extends BaseConnector {
  constructor(opts = {}) { super(opts, { market: 'test_market', wsUrl: 'wss://example.invalid', restUrl: '' }); }
  subscribe() {}
  _getWebSocket() { return null; }
}

test('_clearTimers は ping とアプリ層ハートビートも解放する', () => {
  const c = new TestConnector();
  c.appHeartbeat = () => ({ intervalMs: 20000, payload: 'ping' });
  c._startPingTimer();
  assert.ok(c._pingTimer, 'ping タイマーが立っている');
  assert.ok(c._appHeartbeatTimer, 'ハートビートが立っている');
  c._clearTimers();
  assert.equal(c._pingTimer, null, 'ping タイマーが解放される');
  assert.equal(c._appHeartbeatTimer, null, 'ハートビートが解放される');
  assert.equal(c._pingTimerGen, null, '所有者の記録も消える');
});

test('ping タイマーは自分の世代を記録する', () => {
  const c = new TestConnector();
  c._wsGeneration = 7;
  c._startPingTimer();
  assert.equal(c._pingTimerGen, 7, '立った時点の世代を持つ');
  c._clearPingTimer();
  assert.equal(c._pingTimerGen, null);
});

test('disconnect は世代を進め、タイマーを残さない', () => {
  const c = new TestConnector();
  c._startPingTimer();
  const gen = c._wsGeneration;
  c.disconnect();
  assert.ok(c._wsGeneration > gen, '世代が進む');
  assert.equal(c._pingTimer, null, 'ping タイマーが残らない');
  assert.equal(c._staleTimer === undefined || c._staleTimer === null, true, 'stale タイマーも残らない');
});

test('_startReconnect: 同期中に失効した失敗では再接続を予約しない', async () => {
  const c = new TestConnector();
  let scheduled = 0;
  c._scheduleReconnect = () => { scheduled += 1; };
  c._resetBook = () => {};
  c._closeCurrentSocket = () => {};
  c.connect = async () => { c._wsGeneration += 1; };          // 接続で世代が進む
  c._syncBook = async () => { c._wsGeneration += 1; throw new Error('sync failed (stale)'); };
  await c._startReconnect();
  assert.equal(scheduled, 0, '失効した試行の失敗で新接続の再接続を予約してはいけない');
});

test('_startReconnect: 接続段階の失敗では再接続を予約する', async () => {
  const c = new TestConnector();
  c.on('error', () => {}); // リスナー無しの emit('error') は throw するため
  let scheduled = 0;
  c._scheduleReconnect = () => { scheduled += 1; };
  c._resetBook = () => {};
  c._closeCurrentSocket = () => {};
  c.connect = async () => { throw new Error('connect failed'); }; // syncGen は null のまま
  c._syncBook = async () => { throw new Error('never'); };
  await c._startReconnect();
  assert.equal(scheduled, 1, '接続そのものの失敗は再試行する');
});

test('_startReconnect: 同期中に失効した成功は成功扱いにしない', async () => {
  const c = new TestConnector();
  let scheduled = 0;
  c._scheduleReconnect = () => { scheduled += 1; };
  c._resetBook = () => {};
  c._closeCurrentSocket = () => {};
  c.connect = async () => { c._wsGeneration += 1; };
  c._syncBook = async () => { c._wsGeneration += 1; };          // 同期中に世代が進む
  await c._startReconnect();
  assert.equal(scheduled, 0);
  assert.notEqual(c._state, 'running', '失効した試行を成功（running）にしない');
});
