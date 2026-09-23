import test from 'node:test';
import assert from 'node:assert/strict';
import {
  canRetrySyncOnSocket, syncRetryDelayMs, subscriptionStateFromText,
  SYNC_RETRY_MAX, SYNC_RETRY_MAX_MS,
} from '../lib/sync-retry.mjs';

// 計画 B / sol 監査: WS を作り直すのは4条件のみ。
const ok = (over = {}) => ({ socketOpen: true, subscriptionState: 'ok', bufferOverflow: false, retryCount: 0, ...over });

test('WS生存＋購読成立＋溢れなし＋上限未満なら同期だけ再試行する', () => {
  assert.deepEqual(canRetrySyncOnSocket(ok()), { retry: true, reason: 'ok' });
});

test('ソケットが閉じていたら作り直す', () => {
  assert.deepEqual(canRetrySyncOnSocket(ok({ socketOpen: false })), { retry: false, reason: 'socket-closed' });
});

test('バッファが溢れていたら作り直す (順序を保証できない)', () => {
  assert.deepEqual(canRetrySyncOnSocket(ok({ bufferOverflow: true })), { retry: false, reason: 'buffer-overflow' });
});

test('購読が失敗/未ackなら作り直す (WS生存と購読成立は別)', () => {
  assert.equal(canRetrySyncOnSocket(ok({ subscriptionState: 'failed' })).reason, 'subscription-not-established');
  assert.equal(canRetrySyncOnSocket(ok({ subscriptionState: 'unacked' })).reason, 'subscription-not-established');
});

test('購読の記録が無い venue は判断材料にしない (記録が無いことを理由に残す)', () => {
  assert.deepEqual(canRetrySyncOnSocket(ok({ subscriptionState: 'none-recorded' })),
    { retry: true, reason: 'ok-no-subscription-record' });
  assert.deepEqual(canRetrySyncOnSocket(ok({ subscriptionState: undefined })),
    { retry: true, reason: 'ok-no-subscription-record' });
});

test('再試行上限に達したら作り直す (上限は WS 再接続とは別)', () => {
  assert.equal(SYNC_RETRY_MAX, 5);
  assert.equal(canRetrySyncOnSocket(ok({ retryCount: 4 })).retry, true);
  assert.deepEqual(canRetrySyncOnSocket(ok({ retryCount: 5 })), { retry: false, reason: 'retry-cap' });
  assert.equal(canRetrySyncOnSocket(ok({ retryCount: 2, maxRetries: 2 })).reason, 'retry-cap');
});

test('条件の優先順位: ソケット死 > 溢れ > 購読 > 上限', () => {
  const all = { socketOpen: false, bufferOverflow: true, subscriptionState: 'failed', retryCount: 99 };
  assert.equal(canRetrySyncOnSocket(all).reason, 'socket-closed');
  assert.equal(canRetrySyncOnSocket({ ...all, socketOpen: true }).reason, 'buffer-overflow');
  assert.equal(canRetrySyncOnSocket({ ...all, socketOpen: true, bufferOverflow: false }).reason, 'subscription-not-established');
});

test('同期再試行の待機は 1→2→4…上限30秒', () => {
  const seq = [1, 2, 3, 4, 5, 6].map((n) => syncRetryDelayMs(n, { random: () => 0 }));
  assert.deepEqual(seq, [1000, 2000, 4000, 8000, 16000, 30000]);
  assert.equal(SYNC_RETRY_MAX_MS, 30000);
  assert.equal(syncRetryDelayMs(50, { random: () => 0 }), 30000, '上限を超えない');
  assert.equal(syncRetryDelayMs(undefined, { random: () => 0 }), 1000, '不正な入力は既定');
});

test('subscribe-tracker の1行表現を状態に変換する', () => {
  assert.equal(subscriptionStateFromText('subs=ok'), 'ok');
  assert.equal(subscriptionStateFromText('subs=book:failed'), 'failed');
  assert.equal(subscriptionStateFromText('subs=trades:no-ack'), 'unacked');
  assert.equal(subscriptionStateFromText(''), 'ok');
  assert.equal(subscriptionStateFromText('何か別の表現'), 'unknown');
});
