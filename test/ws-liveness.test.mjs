import test from 'node:test';
import assert from 'node:assert/strict';
import { createWsLiveness } from '../lib/ws-liveness.mjs';

test('Ping/Pong を数え、最後の Pong からの経過を返す', () => {
  let t = 1_000_000;
  const liveness = createWsLiveness({ now: () => t });
  assert.equal(liveness.pongAgeMs(), null, '最初は Pong 無し');
  liveness.onPingSent();
  assert.equal(liveness.pingsSent, 1);
  assert.equal(liveness.pongAgeMs(), null, 'Ping だけでは Pong の経過は出ない');
  t += 50;
  liveness.onPong();
  assert.equal(liveness.pongsReceived, 1);
  assert.equal(liveness.pongAgeMs(), 0);
  t += 4_500;
  assert.equal(liveness.pongAgeMs(), 4_500);
  assert.equal(liveness.text(), 'pings=1 pongs=1 lastPong=4500ms ago');
});

test('Pong が一度も来ない場合は none と表示する', () => {
  const liveness = createWsLiveness({ now: () => 0 });
  liveness.onPingSent();
  liveness.onPingSent();
  assert.equal(liveness.text(), 'pings=2 pongs=0 lastPong=none');
});

test('複数の Pong は最後のものだけを経過に使う', () => {
  let t = 0;
  const liveness = createWsLiveness({ now: () => t });
  liveness.onPong();
  t += 10_000;
  liveness.onPong();
  assert.equal(liveness.pongsReceived, 2);
  t += 1_000;
  assert.equal(liveness.pongAgeMs(), 1_000);
});
