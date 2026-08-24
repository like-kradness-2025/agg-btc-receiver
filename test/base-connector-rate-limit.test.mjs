// test/base-connector-rate-limit.test.mjs — 429 backoff & seq-gap circuit breaker
//
// Verifies that:
// 1. A WebSocket "Unexpected server response: 429" error routes the next
//    reconnect onto the dedicated long backoff curve (>= 60s) instead of the
//    generic 30s-capped curve.
// 2. Consecutive 429s escalate (60s → 120s) and stay capped at 600s.
// 3. The generic MAX_RECONNECT_ATTEMPTS give-up sawtooth never engages while
//    rate-limited (_reconnectAttempt stays 0).
// 4. Repeated sequence gaps inside the window add an extra cooldown to the
//    next generic reconnect.
// 5. A successful open resets rate-limit state without erasing recent gaps.

import { describe, it, after } from 'node:test';
import assert from 'node:assert';
import { EventEmitter } from 'node:events';
import { BaseConnector } from '../lib/base-connector.mjs';

class MockWebSocket extends EventEmitter {
  constructor(url) {
    super();
    this.url = url;
    this.readyState = 0;
  }

  close(_code, _reason) {
    this.readyState = 3;
  }
}

const _cleanup = [];

function createTestConnector() {
  const conn = new BaseConnector(
    {},
    { market: 'rl_test', wsUrl: 'ws://localhost:9999', restUrl: 'http://localhost:9999' },
  );
  conn._setWebSocket(MockWebSocket);
  conn.subscribe = () => {};
  _cleanup.push(conn);
  return conn;
}

after(() => {
  for (const conn of _cleanup) conn._clearTimers();
  _cleanup.length = 0;
});

const KRAKEN_429 = 'kraken_spot: WebSocket error before open: Unexpected server response: 429';

function pendingDelayMs(conn) {
  // Node Timeout exposes its scheduled ms as _idleTimeout; null when unscheduled.
  return conn._reconnectTimer ? conn._reconnectTimer._idleTimeout : null;
}

describe('BaseConnector rate-limit backoff', () => {
  it('routes 429 errors onto the long backoff curve', () => {
    const conn = createTestConnector();
    conn._lastWsError = KRAKEN_429;
    conn._scheduleReconnect();
    const delay = pendingDelayMs(conn);
    assert.ok(delay !== null, 'a reconnect must be scheduled');
    assert.ok(delay >= 60000 && delay <= 61000, `first 429 retry should be ~60s, got ${delay}ms`);
    assert.strictEqual(conn._reconnectAttempt, 0, 'generic attempt counter must stay untouched');
  });

  it('escalates consecutive 429 retries and caps at 600s', () => {
    const conn = createTestConnector();
    conn._lastWsError = KRAKEN_429;
    conn._scheduleReconnect();
    const first = pendingDelayMs(conn);
    conn._clearTimers();

    conn._lastWsError = KRAKEN_429;
    conn._scheduleReconnect();
    const second = pendingDelayMs(conn);
    assert.ok(second >= 120000 && second <= 121000, `second 429 retry should be ~120s, got ${second}ms`);
    assert.ok(second > first);

    conn._rateLimitAttempt = 50;
    conn._clearTimers();
    conn._lastWsError = KRAKEN_429;
    conn._scheduleReconnect();
    const capped = pendingDelayMs(conn);
    assert.strictEqual(capped, 600000, `late 429 retries must cap at 600s, got ${capped}ms`);
  });

  it('never engages the 30-attempt give-up sawtooth while rate-limited', () => {
    const conn = createTestConnector();
    conn._reconnectAttempt = 29;
    conn._lastWsError = KRAKEN_429;
    let gaveUp = false;
    conn.on('error', (e) => {
      if (String(e.message).includes('reconnect failed after')) gaveUp = true;
    });
    conn._scheduleReconnect();
    assert.strictEqual(gaveUp, false, '429 path must bypass MAX_RECONNECT_ATTEMPTS');
    assert.strictEqual(conn.getState(), 'reconnecting');
    assert.strictEqual(conn._reconnectAttempt, 0);
  });

  it('keeps the generic curve for non-429 failures', () => {
    const conn = createTestConnector();
    conn._lastWsError = 'read ECONNRESET';
    conn._scheduleReconnect();
    const delay = pendingDelayMs(conn);
    assert.ok(delay >= 1000 && delay < 32000, `generic retry should stay under 32s, got ${delay}ms`);
  });

  it('adds a cooldown after repeated sequence gaps inside the window', () => {
    const conn = createTestConnector();
    conn.on('error', () => {}); // _handleSequenceGap emits 'error'
    for (let i = 0; i < 5; i++) {
      // Each iteration represents a new connection that reached a running
      // state and then hit one sequence gap. The production guard correctly
      // ignores duplicate gaps while already reconnecting.
      conn._state = 'running';
      conn._stats.state = 'running';
      conn._handleSequenceGap('test gap', {});
      if (i < 4) conn._clearTimers();
    }
    const delay = pendingDelayMs(conn);
    assert.ok(
      delay >= 120000 + 1000,
      `5 gaps inside 10 min must add ${'120'}s cooldown, got ${delay}ms`,
    );
    assert.ok(delay < 152001 + 2000);
  });

  it('does not trigger the breaker below the threshold', () => {
    const conn = createTestConnector();
    conn.on('error', () => {}); // _handleSequenceGap emits 'error'
    for (let i = 0; i < 4; i++) {
      conn._state = 'running';
      conn._stats.state = 'running';
      conn._handleSequenceGap('test gap', {});
      conn._clearTimers();
    }
    // Schedule once more from a running connection with the four recorded
    // gaps; this must use only the generic curve.
    conn._state = 'running';
    conn._stats.state = 'running';
    conn._scheduleReconnect();
    const delay = pendingDelayMs(conn);
    assert.ok(delay < 32000, `below threshold there must be no extra cooldown, got ${delay}ms`);
  });

  it('resets rate-limit state on successful open but keeps recent gap history', async () => {
    const conn = createTestConnector();
    conn._rateLimitAttempt = 7;
    conn._lastWsError = KRAKEN_429;
    const gapHistory = [Date.now(), Date.now()];
    conn._seqGapTimestamps = [...gapHistory];

    const connectPromise = conn.connect();
    await new Promise((resolve) => setImmediate(resolve));
    conn._ws.readyState = 1;
    conn._ws.emit('open');
    await assert.doesNotReject(connectPromise);

    assert.strictEqual(conn._rateLimitAttempt, 0);
    assert.strictEqual(conn._lastWsError, null);
    assert.deepStrictEqual(conn._seqGapTimestamps, gapHistory);
  });
});
