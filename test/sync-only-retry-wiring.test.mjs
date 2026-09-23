import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { BaseConnector } from '../lib/base-connector.mjs';

// 計画 B の配線 (2026-09-24): 同期失敗時に、ソケットが生存し購読も成立していれば
// WS を作り直さず REST 同期だけを再試行する。作り直すのは4条件のときだけ。
// 注意: 親クラスに _validateSnapshotLevels が実在するため明示的に stub する。
class MockWebSocket extends EventEmitter {
  constructor() { super(); this.readyState = 1; }
  close() { this.readyState = 3; }
  terminate() { this.readyState = 3; }
  removeAllListeners(ev) { return super.removeAllListeners(ev); }
  send() {}
}

class TestConnector extends BaseConnector {
  constructor() {
    super({}, { market: 'test_market', wsUrl: 'wss://example.invalid', restUrl: '' });
    this.book = { _lastSeq: 0 };
    this.applied = [];
    this.scheduled = 0;
    this.closed = 0;
    this._getWsImpl = async () => MockWebSocket;
    this._validateSnapshotLevels = () => true;
    this._validateSync = () => true;
    this._applyDiff = (m) => this.applied.push(m);
    this._fetchSnapshot = async () => ({ lastUpdateId: 1 });
  }
  subscribe() {}
  async _resetBook() {}
  _closeCurrentSocketStub() {}
  async connectStub() {}
}

function mk() {
  const c = new TestConnector();
  c.on('error', () => {});
  c._resetBook = () => {};
  c._closeCurrentSocket = () => { c.closed += 1; };
  c._scheduleReconnect = () => { c.scheduled += 1; };
  c.connect = async () => {}; // ソケットは呼び出し側の状態を保つ
  return c;
}

test('同期失敗 + ソケット生存 + 購読成立 → WS を作り直さず同期だけ再試行', async () => {
  const c = mk();
  c._ws = new MockWebSocket();       // 生存 (readyState=1)
  c._wsGeneration = 3;
  c._syncBook = async () => { throw new Error('init sync failed'); };
  await c._startReconnect();
  assert.equal(c.scheduled, 0, 'ソケットが生きているうちは作り直さない');
  assert.ok(c._syncRetryTimer, '同期だけ再試行のタイマーを立てる');
  assert.equal(c._state, 'syncing', 'stale 監視の対象状態に留まる');
  assert.equal(c._syncOnlyAttempt, 1, '再試行の予算を1つ使う');
  c._clearTimers();
});

test('ソケットが閉じていたら従来どおり再接続を予約する', async () => {
  const c = mk();
  c._ws = null;
  c._syncBook = async () => { throw new Error('init sync failed'); };
  await c._startReconnect();
  assert.equal(c.scheduled, 1, 'ソケットが無いときは作り直す');
  assert.ok(!c._syncRetryTimer, '同期再試行のタイマーは立てない');
  c._clearTimers();
});

test('購読が未成立なら作り直す (WS生存と購読成立は別)', async () => {
  const c = mk();
  c._ws = new MockWebSocket();
  c.subscribeFailed && c.subscribeFailed('trades', 'x');
  c._subsText = () => 'subs=trades:failed';
  c._syncBook = async () => { throw new Error('init sync failed'); };
  await c._startReconnect();
  assert.equal(c.scheduled, 1, '購読未成立なら WS を作り直す');
  c._clearTimers();
});

test('バッファが溢れていたら作り直す (順序を保証できない)', async () => {
  const c = mk();
  c._ws = new MockWebSocket();
  c._ringBufOverflow = true;
  c._syncBook = async () => { throw new Error('init sync failed'); };
  await c._startReconnect();
  assert.equal(c.scheduled, 1, '溢れているときは作り直す');
  c._clearTimers();
});

test('同期再試行の予算を使い切ったら作り直す', async () => {
  const c = mk();
  c._ws = new MockWebSocket();
  c._syncOnlyAttempt = 5; // 上限
  c._syncBook = async () => { throw new Error('init sync failed'); };
  await c._startReconnect();
  assert.equal(c.scheduled, 1, '上限到達なら既存の再接続 (バックオフ付き) に落ちる');
  c._clearTimers();
});
