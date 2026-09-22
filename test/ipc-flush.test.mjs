import test from 'node:test';
import assert from 'node:assert/strict';
import { flushEnvelopeQueue, frameUnits } from '../lib/ipc-flush.mjs';

function fakePort() {
  const sent = [];
  return { sent, postMessage: (message) => sent.push(message) };
}

test('queue を空にして送る (件数・順序・種別)', () => {
  const queue = [{ frame_text: 'ab' }, { frame_text: 'cde' }];
  const port = fakePort();
  const n = flushEnvelopeQueue({ queue, port, messageType: 'rawEvents', spanName: 's', probe: null });
  assert.equal(n, 2);
  assert.equal(queue.length, 0);
  assert.equal(port.sent.length, 1);
  assert.equal(port.sent[0].type, 'rawEvents');
  assert.deepEqual(port.sent[0].envelopes, [{ frame_text: 'ab' }, { frame_text: 'cde' }]);
});

test('probe の契約は { end() } (戻り値を関数として呼ばない)', () => {
  const calls = [];
  const ended = [];
  const probe = {
    begin: (name, details) => { calls.push([name, details]); return { end: () => ended.push(name) }; },
  };
  const port = fakePort();
  flushEnvelopeQueue({ queue: [{ frame_text: 'xy' }], port, messageType: 'rawEvents', spanName: 'worker.ipc_send_raw', probe });
  assert.deepEqual(calls, [['worker.ipc_send_raw', { envelopes: 1, frame_units: 2 }]]);
  assert.deepEqual(ended, ['worker.ipc_send_raw'], 'end() が呼ばれていない');
  assert.equal(port.sent.length, 1);
});

test('観測が壊れていても送信は成立する (begin が投げる / end が投げる)', () => {
  const port = fakePort();
  flushEnvelopeQueue({ queue: [{ frame_text: 'a' }], port, messageType: 'rawEvents', spanName: 's',
    probe: { begin: () => { throw new Error('observe broken'); } } });
  assert.equal(port.sent.length, 1);
  flushEnvelopeQueue({ queue: [{ frame_text: 'b' }], port, messageType: 'rawEvents', spanName: 's',
    probe: { begin: () => ({ end: () => { throw new Error('end broken'); } }) } });
  assert.equal(port.sent.length, 2);
});

test('送信の失敗は握り潰さず伝える (queue は既に空)', () => {
  const queue = [{ frame_text: 'a' }];
  const port = { postMessage: () => { throw new Error('ipc down'); } };
  assert.throws(() => flushEnvelopeQueue({ queue, port, messageType: 'rawEvents', spanName: 's', probe: null }), /ipc down/);
  assert.equal(queue.length, 0);
});

test('空 queue では送らない', () => {
  const port = fakePort();
  assert.equal(flushEnvelopeQueue({ queue: [], port, messageType: 'rawEvents', spanName: 's', probe: null }), 0);
  assert.equal(port.sent.length, 0);
});

test('frameUnits は文字列以外を無視する', () => {
  assert.equal(frameUnits([{ frame_text: 'abc' }, { frame_text: null }, {}, { frame_text: 'de' }]), 5);
});
