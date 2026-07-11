// test/burst-reducer/input-validator.test.mjs — InputValidator tests
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { createHash } from 'node:crypto';
import { validateAndParseTrades } from '../../lib/burst-reducer/input-validator.mjs';

describe('InputValidator', () => {
  it('valid trades pass validation', () => {
    const content = '{"market":"test","price":100,"qty":1,"side":"buy","ts":1000,"tradeId":"t1"}\n';
    const { trades, inputSha256 } = validateAndParseTrades(content, 0);
    assert.equal(trades.length, 1);
    assert.equal(trades[0].price, 100);
    assert.equal(typeof inputSha256, 'string');
    assert.equal(inputSha256.length, 64);
  });

  it('throws E001 on invalid JSON', () => {
    assert.throws(() => validateAndParseTrades('not json\n', 0), /E001/);
  });

  it('throws E002 on missing ts', () => {
    assert.throws(() => validateAndParseTrades('{"price":1,"qty":1,"side":"buy"}\n', 0), /E002/);
  });

  it('throws E002 on missing side', () => {
    assert.throws(() => validateAndParseTrades('{"price":1,"qty":1,"ts":1000}\n', 0), /E002/);
  });

  it('throws E003 on negative price', () => {
    assert.throws(() => validateAndParseTrades('{"price":-1,"qty":1,"side":"buy","ts":1000}\n', 0), /E003/);
  });

  it('throws E003 on zero qty', () => {
    assert.throws(() => validateAndParseTrades('{"price":1,"qty":0,"side":"buy","ts":1000}\n', 0), /E003/);
  });

  it('throws E003 on invalid side', () => {
    assert.throws(() => validateAndParseTrades('{"price":1,"qty":1,"side":"hold","ts":1000}\n', 0), /E003/);
  });

  it('§4.2: inversion counted, no throw on ts decrease (E004 now audit)', () => {
    const content = [
      '{"price":100,"qty":1,"side":"buy","ts":2000}',
      '{"price":100,"qty":1,"side":"buy","ts":1000}',
    ].join('\n');
    const { trades, reordered_input, timestamp_inversion_count } = validateAndParseTrades(content, 0);
    assert.equal(reordered_input, true);
    assert.equal(timestamp_inversion_count, 1);
    // After stable sort: ts ASC
    assert.ok(trades[0].ts <= trades[1].ts);
    assert.equal(trades[0].ts, 1000);
    assert.equal(trades[1].ts, 2000);
  });

  it('throws E005 on ts outside block', () => {
    assert.throws(() => validateAndParseTrades('{"price":1,"qty":1,"side":"buy","ts":99999}\n', 0), /E005/);
  });

  it('throws E005 on ts before block (negative)', () => {
    assert.throws(() => validateAndParseTrades('{"price":1,"qty":1,"side":"buy","ts":-1}\n', 0), /E005/);
  });

  it('§4.2: same-ts preserves original row order (no tradeId sort)', () => {
    const content = [
      '{"price":100,"qty":1,"side":"buy","ts":1000,"tradeId":"t3"}',
      '{"price":100,"qty":1,"side":"buy","ts":1000,"tradeId":"t1"}',
      '{"price":100,"qty":1,"side":"buy","ts":1000,"tradeId":"t2"}',
    ].join('\n');
    const { trades } = validateAndParseTrades(content, 0);
    // Original order: t3, t1, t2 — tradeId does NOT affect same-ts ordering
    assert.equal(trades[0].tradeId, 't3');
    assert.equal(trades[1].tradeId, 't1');
    assert.equal(trades[2].tradeId, 't2');
  });

  it('preserves extra fields from trade', () => {
    const content = '{"price":100,"qty":1,"side":"sell","ts":500,"market":"binance_spot","tradeId":"abc"}\n';
    const { trades } = validateAndParseTrades(content, 0);
    assert.equal(trades[0].side, 'sell');
    assert.equal(trades[0].market, 'binance_spot');
    assert.equal(trades[0].tradeId, 'abc');
  });

  // §4.2: 2ms reverse — simulation of the real-world trigger
  it('§4.2: 2ms reverse → sorted ASC, inversion count = 1', () => {
    const content = [
      '{"price":100,"qty":1,"side":"buy","ts":1002}',
      '{"price":101,"qty":2,"side":"buy","ts":1000}',
    ].join('\n');
    const { trades, reordered_input, timestamp_inversion_count } = validateAndParseTrades(content, 0);
    assert.equal(reordered_input, true);
    assert.equal(timestamp_inversion_count, 1);
    assert.equal(trades[0].ts, 1000);
    assert.equal(trades[1].ts, 1002);
    assert.equal(trades[0].price, 101);
    assert.equal(trades[1].price, 100);
  });

  // §4.2: multiple inversions in same block
  it('§4.2: multiple inversions counted correctly', () => {
    const content = [
      '{"price":100,"qty":1,"side":"buy","ts":3000}',
      '{"price":101,"qty":1,"side":"buy","ts":1000}',
      '{"price":102,"qty":1,"side":"buy","ts":2000}',
      '{"price":103,"qty":1,"side":"buy","ts":500}',
    ].join('\n');
    const { trades, reordered_input, timestamp_inversion_count } = validateAndParseTrades(content, 0);
    assert.equal(reordered_input, true);
    assert.equal(timestamp_inversion_count, 2);
    // After sort: [500, 1000, 2000, 3000]
    assert.equal(trades[0].ts, 500);
    assert.equal(trades[1].ts, 1000);
    assert.equal(trades[2].ts, 2000);
    assert.equal(trades[3].ts, 3000);
  });

  // §4.2: already sorted = no reorder
  it('§4.2: already sorted → reordered_input=false, count=0', () => {
    const content = [
      '{"price":100,"qty":1,"side":"buy","ts":1000}',
      '{"price":101,"qty":1,"side":"buy","ts":2000}',
      '{"price":102,"qty":1,"side":"buy","ts":3000}',
    ].join('\n');
    const { trades, reordered_input, timestamp_inversion_count } = validateAndParseTrades(content, 0);
    assert.equal(reordered_input, false);
    assert.equal(timestamp_inversion_count, 0);
    assert.equal(trades[0].ts, 1000);
    assert.equal(trades[1].ts, 2000);
    assert.equal(trades[2].ts, 3000);
  });

  // §4.2: inputSha256 reflects raw content, not sorted output
  it('§4.2: inputSha256 reflects raw content, not sorted output', () => {
    const rawContent = '{"price":100,"qty":1,"side":"buy","ts":2000}\n{"price":101,"qty":1,"side":"buy","ts":1000}\n';
    const { inputSha256 } = validateAndParseTrades(rawContent, 0);
    const expectedHash = createHash('sha256').update(rawContent).digest('hex');
    assert.equal(inputSha256, expectedHash);
  });

  // §4.2: E001/E005 still fail-closed
  it('§4.2: malformed JSON still throws E001', () => {
    assert.throws(() => validateAndParseTrades('not json\n', 0), /E001/);
  });

  it('§4.2: out-of-range ts still throws E005', () => {
    assert.throws(() => validateAndParseTrades('{"price":1,"qty":1,"side":"buy","ts":99999}\n', 0), /E005/);
  });
});
