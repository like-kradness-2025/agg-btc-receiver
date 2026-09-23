import test from 'node:test';
import assert from 'node:assert/strict';
import { BaseConnector } from '../lib/base-connector.mjs';

// 計画 B / sol P0 の前提 (2026-09-24)。同期待ちのバッファは環状 (上限 65,536) なので:
//  - 同じソケットで同期を再試行するときは、待機中のフレームを捨てない (keepBuffer)
//  - 満杯の次の書き込みで古いフレームが消えるため、上書き前に溢れを記録する
//  - 溢れたバッファは「同期成功」と扱わない
// 注意: 親クラスに _validateSnapshotLevels が実在するため、テストでも明示的に stub する
//       (stub しないと 'invalid REST depth snapshot' で全attempt失敗する)。
class TestConnector extends BaseConnector {
  constructor() {
    super({}, { market: 'test_market', wsUrl: 'wss://example.invalid', restUrl: '' });
    this.book = { _lastSeq: 0 };
    this.applied = [];
    this._getWsImpl = async () => null;
  }
  subscribe() {}
  async _fetchSnapshot() { return { lastUpdateId: 1 }; }
  _validateSnapshotLevels() { return true; }
  _validateSync() { return true; }
  _applyDiff(msg) { this.applied.push(msg); }
}

test('keepBuffer: 待機中のフレームを消さず、同期成功時に到着順で適用する', async () => {
  const c = new TestConnector();
  c._ringBuf = ['m1', 'm2'];
  await c._syncBook({ keepBuffer: true });
  assert.deepEqual(c.applied, ['m1', 'm2'], '到着順に適用');
  assert.equal(c._state, 'running');
});

test('既定 (新規接続) は従来どおりバッファを消す', async () => {
  const c = new TestConnector();
  c._ringBuf = ['stale'];
  c._ringBufOverflow = true;
  await c._syncBook();
  assert.deepEqual(c.applied, [], '新規接続では前のバッファを持ち越さない');
  assert.equal(c._state, 'running');
});

test('溢れたバッファは同期成功として扱わない (呼び出し側が作り直す)', async () => {
  const c = new TestConnector();
  const msgs = [];
  c.on('error', (e) => msgs.push(e.message));
  c._ringBuf = ['m1'];
  c._ringBufOverflow = true;
  await assert.rejects(() => c._syncBook({ keepBuffer: true }), /init sync failed/);
  assert.ok(msgs.some((m) => /refusing to treat it as synchronized/.test(m)),
    '溢れを理由に拒否したことを記録する');
  assert.deepEqual(c.applied, [], '溢れたバッファを適用しない');
  assert.equal(c._state, 'error');
});

test('keepBuffer の再試行では溢れ状態を維持する (初期化は新世代だけ)', async () => {
  const c = new TestConnector();
  c.on('error', () => {});
  c._ringBufOverflow = true;
  await assert.rejects(() => c._syncBook({ keepBuffer: true }), /init sync failed/);
  assert.equal(c._ringBufOverflow, true, '同じソケットの再試行では溢れを消さない');
  const d = new TestConnector();
  d._ringBufOverflow = true;
  await d._syncBook();
  assert.equal(d._ringBufOverflow, false, '新規接続では溢れ状態も初期化する');
});

test('_bufferMsg は上書き前に溢れを記録する', () => {
  const c = new TestConnector();
  c._ringBuf = new Array(65536).fill('x');
  c._ringBufOverflow = false;
  c._bufferMsg({ a: 1 });
  assert.equal(c._ringBufOverflow, true, '満杯の次の書き込みでフラグを立てる');
  const d = new TestConnector();
  d._ringBuf = ['one'];
  d._ringBufOverflow = false;
  d._bufferMsg({ a: 1 });
  assert.equal(d._ringBufOverflow, false, '通常は立てない');
});
