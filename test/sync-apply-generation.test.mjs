import test from 'node:test';
import assert from 'node:assert/strict';
import { BaseConnector } from '../lib/base-connector.mjs';

// sol P1③ の残り (2026-09-24): バッファ適用中に世代が進んだら即中断し、
// 成功 (running) として確定しない。
class TestConnector extends BaseConnector {
  constructor() {
    super({}, { market: 'test_market', wsUrl: 'wss://example.invalid', restUrl: '' });
    this.book = { _lastSeq: 0 };
    this.applied = [];
    this._getWsImpl = async () => null;
    this._validateSnapshotLevels = () => true;
    this._validateSync = () => true;
    this._fetchSnapshot = async () => ({ lastUpdateId: 1 });
  }
  subscribe() {}
  _applyDiff(msg) { this.applied.push(msg); }
}

test('適用中に世代が進んだら即中断する (残りは破棄)', () => {
  const c = new TestConnector();
  c._ringBuf = ['m1', 'm2', 'm3'];
  c._wsGeneration = 10;
  c._applyDiff = (msg) => {
    c.applied.push(msg);
    if (msg === 'm1') c._wsGeneration = 11; // 適用中に失効
  };
  const ok = c._applyRingBuf({ lastUpdateId: 1 });
  assert.equal(ok, false, '失効を呼び出し側に伝える');
  assert.deepEqual(c.applied, ['m1'], '失効後は適用しない');
});

test('失効が無ければ全件を到着順に適用し true を返す', () => {
  const c = new TestConnector();
  c._ringBuf = ['m1', 'm2'];
  c._wsGeneration = 1;
  assert.equal(c._applyRingBuf({ lastUpdateId: 1 }), true);
  assert.deepEqual(c.applied, ['m1', 'm2']);
});

test('await 中に世代が進んだら running に戻さない (close と競合した同期を失効させる)', async () => {
  const c = new TestConnector();
  c.on('error', () => {});
  c._wsGeneration = 7;
  c._ringBuf = ['m1'];
  c._fetchSnapshot = async () => { c._wsGeneration = 8; return { lastUpdateId: 1 }; };
  await c._syncBook({ keepBuffer: true });
  assert.deepEqual(c.applied, [], '失効した同期ではバッファを適用しない');
  assert.notEqual(c._state, 'running', '古い世代の同期を成功扱いにしない');
});

test('失効が無ければ従来どおり running へ戻る', async () => {
  const c = new TestConnector();
  c._wsGeneration = 2;
  c._ringBuf = ['m1'];
  await c._syncBook({ keepBuffer: true });
  assert.deepEqual(c.applied, ['m1']);
  assert.equal(c._state, 'running');
});

test('同期成功の確定直前に失効していたら running に戻さない', async () => {
  const c = new TestConnector();
  c.on('error', () => {});
  c._ringBuf = ['m1', 'm2'];
  c._wsGeneration = 5;
  c._applyDiff = (msg) => { c.applied.push(msg); c._wsGeneration = 6; }; // 最初の適用で失効
  await c._syncBook({ keepBuffer: true });
  assert.deepEqual(c.applied, ['m1'], '適用は即中断');
  assert.notEqual(c._state, 'running', '失効した同期を成功扱いにしない');
});
