import test from 'node:test';
import assert from 'node:assert/strict';
import { OkxConnector } from '../lib/okx-connector.mjs';

// 2026-09-23: アプリ層 ping を送るようにしたところ OKX は生の 'pong' を返し、
// それが JSON 解析に渡って 20 秒ごとに parse error を出した。生フレームの前処理で
// 'ping'(応答を返す) と 'pong'(握って消費) の両方を扱うことを固定する。
function makeConnector() {
  const sent = [];
  const connector = new OkxConnector({ market: 'okx_perp', wsUrl: 'wss://example.invalid' });
  connector._ws = { send: (payload) => sent.push(payload) };
  return { connector, sent };
}

test("生の 'ping' は pong を返して消費する (既存動作)", () => {
  const { connector, sent } = makeConnector();
  assert.equal(connector._preprocessRaw(Buffer.from('ping')), true);
  assert.deepEqual(sent, ['pong']);
});

test("生の 'pong' は送信せずに消費する (20秒ごとのparse errorを防ぐ)", () => {
  const { connector, sent } = makeConnector();
  assert.equal(connector._preprocessRaw(Buffer.from('pong')), true);
  assert.deepEqual(sent, [], 'pong に対して何も送ってはいけない');
});

test('通常の JSON フレームは消費しない (本体の解析に渡す)', () => {
  const { connector } = makeConnector();
  assert.equal(connector._preprocessRaw(Buffer.from('{"event":"subscribe"}')), false);
});
