// test/full-book.test.mjs — FullBook unit tests

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { FullBook } from '../lib/full-book.mjs';

describe('FullBook', () => {
  describe('constructor', () => {
    it('should initialize empty book', () => {
      const book = new FullBook('binance_spot');
      assert.strictEqual(book.market, 'binance_spot');
      assert.strictEqual(book.getBestBid(), null);
      assert.strictEqual(book.getBestAsk(), null);
      assert.strictEqual(book.getMid(), null);
      assert.strictEqual(book.getSpread(), null);
      assert.deepStrictEqual(book.getLevelCount(), { bids: 0, asks: 0 });
    });

    it('should respect maxLevels option', () => {
      const book = new FullBook('test', { maxLevels: 2 });
      assert.strictEqual(book._maxLevels, 2);
    });
  });

  describe('applySnapshot', () => {
    it('should populate bids and asks', () => {
      const book = new FullBook('test');
      book.applySnapshot(
        [['101.0', '1.0'], ['100.0', '2.0']],
        [['102.0', '0.5'], ['103.0', '1.5']],
      );
      assert.strictEqual(book.getBestBid(), '101.0');
      assert.strictEqual(book.getBestAsk(), '102.0');
      assert.strictEqual(book.getMid(), 101.5);
      assert.strictEqual(book.getSpread(), 1.0);
    });

    it('should clear previous data on snapshot', () => {
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], [['101', '1']]);
      book.applySnapshot([['99', '1']], [['102', '1']]);
      assert.strictEqual(book.getBestBid(), '99');
      assert.strictEqual(book.getBestAsk(), '102');
    });

    it('should respect maxLevels', () => {
      const book = new FullBook('test', { maxLevels: 2 });
      book.applySnapshot(
        [['101', '1'], ['100', '2'], ['99', '3']],
        [['102', '1'], ['103', '2'], ['104', '3']],
      );
      assert.strictEqual(book.bids.size, 2);
      assert.strictEqual(book.asks.size, 2);
    });

    it('should record seq', () => {
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], [['101', '1']], 12345);
      assert.strictEqual(book._lastSeq, 12345);
    });
  });

  describe('applyDiff', () => {
    it('should add new level', () => {
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], [['102', '1']]);
      book.applyDiff('bid', '101', '2');
      assert.strictEqual(book.getBestBid(), '101');
    });

    it('should delete level when qty=0', () => {
      const book = new FullBook('test');
      book.applySnapshot([['100', '1'], ['99', '2']], [['101', '1']]);
      book.applyDiff('bid', '100', '0');
      assert.strictEqual(book.getBestBid(), '99');
      assert.strictEqual(book.bids.has('100'), false);
    });

    it('should handle qty=0.0 and empty string', () => {
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], [['101', '1']]);
      book.applyDiff('bid', '100', '0.0');
      assert.strictEqual(book.getBestBid(), null);
      book.applyDiff('ask', '101', '');
      assert.strictEqual(book.getBestAsk(), null);
    });

    it('should delete level with Binance zero formats (0.00000000 spot, 0.000 perp)', () => {
      const book = new FullBook('test');
      book.applySnapshot(
        [['100.00', '1.5'], ['99.00', '2.0']],
        [['101.00', '0.8'], ['102.00', '1.2']],
      );
      // Spot format: 0.00000000
      book.applyDiff('bid', '100.00', '0.00000000');
      assert.strictEqual(book.getBestBid(), '99.00');
      assert.strictEqual(book.bids.has('100.00'), false);
      // Perp format: 0.000
      book.applyDiff('ask', '101.00', '0.000');
      assert.strictEqual(book.getBestAsk(), '102.00');
      assert.strictEqual(book.asks.has('101.00'), false);
    });

    it('should update existing level qty', () => {
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], [['101', '1']]);
      book.applyDiff('bid', '100', '5');
      assert.strictEqual(book.getBestBid(), '100');
      assert.strictEqual(book.bids.get('100'), '5');
    });

    it('should update batch best prices without scanning on every level', () => {
      const book = new FullBook('test');
      book.applySnapshot([['100', '1'], ['99', '1']], [['101', '1'], ['102', '1']]);

      book.applyDiffs('bid', [['98', '1'], ['103', '1']]);
      book.applyDiffs('ask', [['104', '1'], ['100.5', '1']]);

      assert.strictEqual(book.getBestBid(), '103');
      assert.strictEqual(book.getBestAsk(), '100.5');
    });

    it('should recalculate correctly when a batch deletes the best level', () => {
      const book = new FullBook('test');
      book.applySnapshot([['102', '1'], ['101', '1']], [['103', '1'], ['104', '1']]);

      book.applyDiffs('bid', [['102', '0'], ['100', '1']]);
      book.applyDiffs('ask', [['103', ''], ['105', '1']]);

      assert.strictEqual(book.getBestBid(), '101');
      assert.strictEqual(book.getBestAsk(), '104');
    });
  });

  describe('imbalance and depth queries', () => {
    it('should compute imbalance', () => {
      const book = new FullBook('test');
      book.applySnapshot(
        [['100.0', '10'], ['99.0', '5']],
        [['101.0', '5'], ['102.0', '5']],
      );
      const imb = book.getImbalance(1.0);
      assert.strictEqual(typeof imb, 'number');
      // bids have more qty near best => positive imbalance
      assert.ok(imb > 0);
    });

    it('should return null imbalance when empty', () => {
      const book = new FullBook('test');
      assert.strictEqual(book.getImbalance(), null);
    });

    it('should return correct level counts', () => {
      const book = new FullBook('test');
      book.applySnapshot(
        [['100', '1'], ['99', '2'], ['98', '3']],
        [['101', '1'], ['102', '2']],
      );
      assert.deepStrictEqual(book.getLevelCount(), { bids: 3, asks: 2 });
    });
  });

  describe('toSnapshot', () => {
    it('should return sorted snapshot', () => {
      const book = new FullBook('test');
      book.applySnapshot(
        [['100', '2'], ['101', '1']],
        [['102', '1'], ['103', '2']],
        999
      );
      const snap = book.toSnapshot(123456);
      assert.strictEqual(snap.market, 'test');
      assert.strictEqual(snap.ts, 123456);
      assert.strictEqual(snap.seq, 999);
      // bids should be sorted desc (highest first)
      assert.strictEqual(snap.bids[0][0], '101');
      assert.strictEqual(snap.bids[1][0], '100');
      // asks should be sorted asc (lowest first)
      assert.strictEqual(snap.asks[0][0], '102');
      assert.strictEqual(snap.asks[1][0], '103');
    });
  });

  describe('getTop', () => {
    it('should return top N levels', () => {
      const book = new FullBook('test');
      book.applySnapshot(
        [['100', '1'], ['99', '2'], ['98', '3']],
        [['101', '1'], ['102', '2']],
      );
      const top = book.getTop(2);
      assert.strictEqual(top.bids.length, 2);
      assert.strictEqual(top.bids[0][0], '100');
      assert.strictEqual(top.asks.length, 2);
    });
  });

  describe('clear', () => {
    it('should reset all state', () => {
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], [['101', '1']], 42);
      book.clear();
      assert.strictEqual(book.getBestBid(), null);
      assert.strictEqual(book.getBestAsk(), null);
      assert.strictEqual(book.bids.size, 0);
      assert.strictEqual(book.asks.size, 0);
      assert.strictEqual(book._lastSeq, null);
    });
  });

  describe('isEmpty', () => {
    it('should return true for fresh book', () => {
      const book = new FullBook('test');
      assert.strictEqual(book.isEmpty(), true);
    });

    it('should return false after snapshot with data', () => {
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], [['101', '1']]);
      assert.strictEqual(book.isEmpty(), false);
    });

    it('should return true after clear', () => {
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], [['101', '1']]);
      book.clear();
      assert.strictEqual(book.isEmpty(), true);
    });

    it('should return false when only one side has data', () => {
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], []);
      assert.strictEqual(book.isEmpty(), false);
    });
  });
});
