import test from 'node:test';
import assert from 'node:assert/strict';
import { BitfinexConnector } from '../lib/bitfinex-connector.mjs';
import { BinanceSpotConnector, BinancePerpConnector } from '../lib/binance-connector.mjs';

// 2026-09-23: 公式仕様に合わせた事象対応の回帰テスト。
// Bitfinex: 20051=再接続 / 20060=復旧保留(受信は継続) / 20061=保留解除
// Binance : serverShutdown を受けたら自分から張り直す
function makeConnector(Ctor, market) {
  const connector = new Ctor({ market, wsUrl: 'wss://example.invalid' });
  const log = { closed: 0, scheduled: 0, sent: [] };
  connector._ws = { close: () => { log.closed += 1; }, send: (p) => log.sent.push(p) };
  connector._scheduleReconnect = () => { log.scheduled += 1; };
  connector._setState = () => {};
  return { connector, log };
}

test('bitfinex: 20051 で自分から張り直す', () => {
  const { connector, log } = makeConnector(BitfinexConnector, 'bitfinex_spot');
  connector._onMessage({ event: 'info', code: 20051, msg: 'Stop/Restart Websocket Server' });
  assert.equal(log.closed, 1, '旧ソケットを閉じる');
  assert.equal(log.scheduled, 1, '再接続を予約する');
});

test('bitfinex: 20060 で復旧を保留し、20061 で解除する (受信は止めない)', () => {
  const { connector } = makeConnector(BitfinexConnector, 'bitfinex_spot');
  assert.equal(connector._recoveryPaused(), false, '初期は保留していない');
  connector._onMessage({ event: 'info', code: 20060, msg: 'Entering in Maintenance mode' });
  assert.equal(connector._recoveryPaused(), true, '保守中は保留');
  connector._onMessage({ event: 'info', code: 20061, msg: 'Maintenance ended' });
  assert.equal(connector._recoveryPaused(), false, '20061 で解除');
});

test('bitfinex: 20060 の後に何も来なくても時間切れで解除する (120秒 ± 余裕)', () => {
  const { connector } = makeConnector(BitfinexConnector, 'bitfinex_spot');
  connector._onMessage({ event: 'info', code: 20060 });
  assert.equal(connector._recoveryPaused(), true);
  connector._maintenanceUntilMs = Date.now() - 1; // 時間経過を模擬
  assert.equal(connector._recoveryPaused(), false, '時間切れで復旧を再開できる');
});

test('bitfinex: 未知の info/error コードでは何もしない (挙動を変えない)', () => {
  const { connector, log } = makeConnector(BitfinexConnector, 'bitfinex_spot');
  connector._onMessage({ event: 'info', code: 12345, msg: 'something' });
  connector._onMessage({ event: 'error', code: 10000, msg: 'unknown' });
  assert.equal(log.closed, 0);
  assert.equal(log.scheduled, 0);
  assert.equal(connector._recoveryPaused(), false);
});

test('binance: serverShutdown で自分から張り直す (spot / perp)', () => {
  for (const Ctor of [BinanceSpotConnector, BinancePerpConnector]) {
    const { connector, log } = makeConnector(Ctor, 'binance_spot');
    connector._onMessage({ e: 'serverShutdown' });
    assert.equal(log.closed, 1, `${Ctor.name}: 旧ソケットを閉じる`);
    assert.equal(log.scheduled, 1, `${Ctor.name}: 再接続を予約する`);
  }
});

test('binance: 通常の depthUpdate/trade では再接続しない', () => {
  const { connector, log } = makeConnector(BinanceSpotConnector, 'binance_spot');
  connector._handleDepth = () => {};
  connector._handleTrade = () => {};
  connector._onMessage({ e: 'depthUpdate', s: 'BTCUSDT', U: 1, u: 2, b: [], a: [] });
  connector._onMessage({ e: 'trade', s: 'BTCUSDT', p: '1', q: '1', T: 1, m: false });
  assert.equal(log.closed, 0);
  assert.equal(log.scheduled, 0);
});
