// test/binance-sync.test.mjs — Binance init sync logic unit tests

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { BinancePerpConnector, BinanceSpotConnector } from '../lib/binance-connector.mjs';
import { BinanceSpotUsdcConnector } from '../lib/binance-usdc-connector.mjs';

/**
 * Create a connector with mocked ringBuf for testing sync logic.
 */
function createSyncConnector() {
  const conn = new BinanceSpotConnector({});
  conn._ws = null;
  return conn;
}

function createPerpSyncConnector() {
  const conn = new BinancePerpConnector({});
  conn._ws = null;
  return conn;
}

function createUsdcSyncConnector() {
  const conn = new BinanceSpotUsdcConnector({});
  conn._ws = null;
  return conn;
}

describe('Binance sync logic', () => {
  describe('_validateSync', () => {
    it('should pass when U <= lastUpdateId + 1 <= u', () => {
      const conn = createSyncConnector();
      // lastUpdateId=105, so lastUpdateId+1=106
      // For each diff: U <= 106 <= u must hold
      conn._ringBuf = [
        { U: 100, u: 110 },  // 100 <= 106 <= 110 ✓
        { U: 106, u: 115 },  // 106 <= 106 <= 115 ✓
        { U: 111, u: 120 },  // 111 <= 106? NO — this test only checks if ANY diff fails
      ];
      const snapshot = { lastUpdateId: 105 };
      // Diff 3: U=111 > lastUpdateId+1=106 → fail
      // Actually all must pass. Let's make all pass:
      conn._ringBuf = [
        { U: 100, u: 110 },  // 100 <= 106 <= 110 ✓
        { U: 106, u: 106 },  // 106 <= 106 <= 106 ✓
      ];
      assert.ok(conn._validateSync(snapshot));
    });

    it('should fail when U > lastUpdateId + 1', () => {
      const conn = createSyncConnector();
      conn._ringBuf = [
        { U: 200, u: 205 },
      ];
      const snapshot = { lastUpdateId: 100 };
      // U=200 > lastUpdateId+1=101 → fail
      assert.ok(!conn._validateSync(snapshot));
    });

    it('should fail when lastUpdateId + 1 > all buffered u (snapshot ahead)', () => {
      const conn = createSyncConnector();
      conn._ringBuf = [
        { U: 100, u: 102 },
      ];
      const snapshot = { lastUpdateId: 103 };
      // lastUpdateId+1=104 > u=102 → no bridge exists, so retry sync.
      assert.ok(!conn._validateSync(snapshot));
    });

    it('should fail with empty ring buffer during initial sync', () => {
      const conn = createSyncConnector();
      conn._ringBuf = [];
      const snapshot = { lastUpdateId: 100 };
      assert.ok(!conn._validateSync(snapshot));
    });

    it('should pass with stale old diff before first valid diff', () => {
      const conn = createSyncConnector();
      // lastUpdateId=100, so lastUpdateId+1=101
      // msg[0]: u=100 ≤ 100 → stale, discarded
      // msg[1]: U=101 ≤ 101 ≤ 105 → first valid
      conn._ringBuf = [
        { U: 90, u: 100 },
        { U: 101, u: 105 },
      ];
      assert.ok(conn._validateSync({ lastUpdateId: 100 }));
    });

    it('should fail when all diffs are stale (snapshot ahead of buffered diffs)', () => {
      const conn = createSyncConnector();
      // All buffered diffs have u <= lastUpdateId → snapshot is ahead of all of them
      conn._ringBuf = [
        { U: 80, u: 90 },
        { U: 91, u: 95 },
      ];
      assert.ok(!conn._validateSync({ lastUpdateId: 100 }));
    });

    it('should fail when first remaining diff has U > lastUpdateId + 1 (gap)', () => {
      const conn = createSyncConnector();
      conn._ringBuf = [
        { U: 103, u: 105 },
      ];
      // lastUpdateId=100, lastUpdateId+1=101, U=103 > 101 → gap
      assert.ok(!conn._validateSync({ lastUpdateId: 100 }));
    });

    it('should pass when partial overlap diff satisfies U <= lastUpdateId+1 <= u', () => {
      const conn = createSyncConnector();
      // lastUpdateId=100, lastUpdateId+1=101
      // msg[0]: U=95 ≤ 101 ≤ 102 → first valid (diff starts before snapshot but covers 101)
      conn._ringBuf = [
        { U: 95, u: 102 },
      ];
      assert.ok(conn._validateSync({ lastUpdateId: 100 }));
    });
  });

  describe('_applyRingBuf', () => {
    it('should apply snapshot and skip diffs with u <= lastUpdateId', () => {
      const conn = createSyncConnector();
      const snapshot = {
        lastUpdateId: 100,
        bids: [['100.0', '1.0']],
        asks: [['101.0', '2.0']],
      };
      conn._ringBuf = [
        { U: 98, u: 99, E: 1700000000000, b: [['100.0', '0']], a: [] },  // u <= 100 → skip
        { U: 100, u: 102, E: 1700000000001, b: [['100.0', '3.0']], a: [] }, // u > 100 → apply
      ];

      conn._applyRingBuf(snapshot);
      // After snapshot: bid 100.0 qty=1.0
      // After diff with u=102: bid 100.0 qty=3.0
      assert.strictEqual(conn.book.bids.get('100.0'), '3.0');
      assert.strictEqual(conn.book.asks.get('101.0'), '2.0');
    });

    it('should handle empty bids/asks in snapshot', () => {
      const conn = createSyncConnector();
      const snapshot = {
        lastUpdateId: 50,
        bids: [],
        asks: [],
      };
      conn._ringBuf = [
        { U: 50, u: 51, E: 1700000000000, b: [['100.0', '1.0']], a: [['101.0', '2.0']] },
      ];
      conn._applyRingBuf(snapshot);
      assert.strictEqual(conn.book.getBestBid(), '100.0');
      assert.strictEqual(conn.book.getBestAsk(), '101.0');
    });

    it('should skip stale diffs and apply from first valid onwards', () => {
      const conn = createSyncConnector();
      const snapshot = {
        lastUpdateId: 100,
        bids: [['100.0', '1.0']],
        asks: [['101.0', '2.0']],
      };
      conn._ringBuf = [
        { U: 90, u: 99, E: 1700000000000, b: [['100.0', '0']], a: [] },    // u=99 ≤ 100 → stale, discard
        { U: 100, u: 100, E: 1700000000001, b: [['100.0', '0']], a: [] },   // u=100 ≤ 100 → stale, discard
        { U: 101, u: 103, E: 1700000000002, b: [['100.0', '5.0']], a: [] },  // first valid
      ];
      conn._applyRingBuf(snapshot);
      // Stale diffs must NOT have been applied; only the valid diff should apply
      assert.strictEqual(conn.book.bids.get('100.0'), '5.0');
      assert.strictEqual(conn.book.asks.get('101.0'), '2.0');
    });

    it('should reject a sequence gap after the first bridge diff', () => {
      const conn = createSyncConnector();
      const snapshot = { lastUpdateId: 100, bids: [], asks: [] };
      const emitted = [];
      conn.on('depth', (event) => emitted.push(event));
      conn._ringBuf = [
        { U: 101, u: 101, E: 1700000000000, b: [['100.0', '1.0']], a: [] },
        { U: 103, u: 103, E: 1700000000001, b: [['100.0', '3.0']], a: [] },
      ];

      assert.throws(() => conn._applyRingBuf(snapshot), /buffered depth gap/);
      assert.deepStrictEqual(emitted, []);
      assert.strictEqual(conn.book._lastSeq, null);
      assert.strictEqual(conn.book.bids.has('100.0'), false);
    });

    it('should reject an invalid buffered timestamp before applying it', () => {
      const conn = createSyncConnector();
      conn._ringBuf = [
        { U: 101, u: 101, E: NaN, b: [['100.0', '1.0']], a: [] },
      ];

      assert.throws(() => conn._applyRingBuf({ lastUpdateId: 100, bids: [], asks: [] }), /invalid timestamp/);
      assert.strictEqual(conn.book._lastSeq, null);
      assert.strictEqual(conn.book.bids.has('100.0'), false);
    });

    it('should reject missing or malformed REST depth arrays before applying', () => {
      const conn = createSyncConnector();
      assert.throws(() => conn._applyRingBuf({ lastUpdateId: 100 }), /invalid depth levels/);
      assert.throws(() => conn._applyRingBuf({
        lastUpdateId: 100,
        bids: [['not-a-price', '1']],
        asks: [],
      }), /invalid depth levels/);
      assert.strictEqual(conn.book.isEmpty(), true);
    });
  });

  describe('perpetual _applyRingBuf', () => {
    it('should require pu continuity after the first bridge diff', () => {
      const conn = createPerpSyncConnector();
      const snapshot = { lastUpdateId: 100, bids: [], asks: [] };
      conn._ringBuf = [
        { U: 95, u: 101, pu: 99, E: 1700000000000, b: [['100.0', '1.0']], a: [] },
        { U: 103, u: 103, pu: 102, E: 1700000000001, b: [['100.0', '3.0']], a: [] },
      ];

      assert.throws(() => conn._applyRingBuf(snapshot), /buffered depth gap/);
      assert.strictEqual(conn.book._lastSeq, null);
      assert.strictEqual(conn.book.bids.has('100.0'), false);
    });
  });

  describe('full sync flow (no network)', () => {
    it('should wait for a delayed WS event to bridge the REST snapshot', async () => {
      const conn = createSyncConnector();
      let fetchCount = 0;
      conn._fetchSnapshot = async () => {
        fetchCount++;
        return { lastUpdateId: 100, bids: [], asks: [] };
      };
      setTimeout(() => conn._bufferMsg({
        U: 101, u: 102, E: 1700000000000, b: [['100', '1']], a: [],
      }), 25);

      await conn._syncBook();

      assert.strictEqual(conn.getState(), 'running');
      assert.strictEqual(fetchCount, 1);
      assert.strictEqual(conn.book._lastSeq, 102);
    });

    it('should preserve a late event across a failed bridge attempt', async () => {
      const conn = createSyncConnector();
      conn._syncBridgeWaitMs = 20;
      let fetchCount = 0;
      conn._fetchSnapshot = async () => {
        fetchCount++;
        return { lastUpdateId: 100, bids: [], asks: [] };
      };
      setTimeout(() => conn._bufferMsg({
        U: 101, u: 102, E: 1700000000000, b: [['100', '1']], a: [],
      }), 30);

      await conn._syncBook();

      assert.strictEqual(conn.getState(), 'running');
      assert.strictEqual(fetchCount, 2);
      assert.strictEqual(conn.book._lastSeq, 102);
    });

    it('should preserve delayed perp events while retrying the bridge', async () => {
      const conn = createPerpSyncConnector();
      conn._syncBridgeWaitMs = 20;
      let fetchCount = 0;
      conn._fetchSnapshot = async () => {
        fetchCount++;
        return { lastUpdateId: 100, bids: [], asks: [] };
      };
      setTimeout(() => conn._bufferMsg({
        U: 95, u: 101, pu: 99, E: 1700000000000, b: [['100', '1']], a: [],
      }), 30);

      await conn._syncBook();

      assert.strictEqual(conn.getState(), 'running');
      assert.strictEqual(fetchCount, 2);
      assert.strictEqual(conn.book._lastSeq, 101);
    });

    it('should preserve delayed USDC events while retrying the bridge', async () => {
      const conn = createUsdcSyncConnector();
      conn._syncBridgeWaitMs = 20;
      let fetchCount = 0;
      conn._fetchSnapshot = async () => {
        fetchCount++;
        return { lastUpdateId: 100, bids: [], asks: [] };
      };
      setTimeout(() => conn._bufferMsg({
        U: 101, u: 102, E: 1700000000000, b: [['100', '1']], a: [],
      }), 30);

      await conn._syncBook();

      assert.strictEqual(conn.getState(), 'running');
      assert.strictEqual(fetchCount, 2);
      assert.strictEqual(conn.book._lastSeq, 102);
    });

    it('should not emit partial replay state before a failed retry attempt', async () => {
      const conn = createSyncConnector();
      conn._syncBridgeWaitMs = 0;
      conn.on('error', () => {});
      const emitted = [];
      conn.on('depth', (event) => emitted.push(event));
      let fetchCount = 0;
      conn._fetchSnapshot = async () => {
        fetchCount++;
        if (fetchCount === 1) {
          conn._ringBuf = [
            { U: 101, u: 101, E: 1700000000000, b: [['100', '1']], a: [] },
            { U: 103, u: 103, E: 1700000000001, b: [['100', '3']], a: [] },
          ];
          return { lastUpdateId: 100, bids: [], asks: [] };
        }
        conn._ringBuf.push({
          U: 104, u: 104, E: 1700000000002, b: [['100', '4']], a: [],
        });
        return { lastUpdateId: 103, bids: [], asks: [] };
      };

      await conn._syncBook();

      assert.strictEqual(fetchCount, 2);
      assert.deepStrictEqual(emitted.map(({ type, seq }) => [type, seq]), [
        ['snapshot', 103], ['update', 104], ['snapshot', 104],
      ]);
    });

    it('should share one in-flight sync promise between concurrent callers', async () => {
      const conn = createSyncConnector();
      conn._syncBridgeWaitMs = 50;
      let fetchCount = 0;
      conn._fetchSnapshot = async () => {
        fetchCount++;
        return { lastUpdateId: 100, bids: [], asks: [] };
      };
      const first = conn._syncBook();
      const second = conn._syncBook();
      setTimeout(() => conn._bufferMsg({
        U: 101, u: 102, E: 1700000000000, b: [['100', '1']], a: [],
      }), 10);

      assert.strictEqual(first, second);
      await Promise.all([first, second]);
      assert.strictEqual(fetchCount, 1);
      assert.strictEqual(conn.getState(), 'running');
      assert.strictEqual(conn.book._lastSeq, 102);
    });

    it('should reject a regressing REST snapshot across retries', async () => {
      const conn = createSyncConnector();
      conn._syncBridgeWaitMs = 0;
      conn.on('error', () => {});
      const emitted = [];
      conn.on('depth', (event) => emitted.push(event));
      let fetchCount = 0;
      conn._fetchSnapshot = async () => {
        fetchCount++;
        if (fetchCount === 1) return { lastUpdateId: 100, bids: [], asks: [] };
        conn._ringBuf = [
          { U: 100, u: 101, E: 1700000000000, b: [['100', '1']], a: [] },
        ];
        return { lastUpdateId: 99, bids: [], asks: [] };
      };

      await assert.rejects(() => conn._syncBook(), /init sync failed/);
      assert.strictEqual(fetchCount, 3);
      assert.deepStrictEqual(emitted, []);
    });

    it('should attempt _fetchSnapshot and handle failure gracefully', async () => {
      const conn = createSyncConnector();
      conn._fetchSnapshot = async () => { throw new Error('network error'); };
      conn._applyRingBuf = () => {};
      // Suppress expected error events
      conn.on('error', () => {});

      await assert.rejects(
        () => conn._syncBook(),
        /init sync failed/
      );
      // After 3 retries, should end up in 'error' state
      assert.strictEqual(conn.getState(), 'error');
    });

    it('should succeed on first snapshot attempt', async () => {
      const conn = createSyncConnector();
      conn._fetchSnapshot = async () => ({ lastUpdateId: 100, bids: [['100', '1']], asks: [['101', '1']] });
      conn._ringBuf = [];
      conn._validateSync = () => true;
      conn._applyRingBuf = (snap) => {
        assert.strictEqual(snap.lastUpdateId, 100);
      };

      await conn._syncBook();
      assert.strictEqual(conn.getState(), 'running');
    });
  });

  describe('_applyDiff', () => {
    it('should apply diff event to book', () => {
      const conn = createSyncConnector();
      conn.book.applySnapshot([['100.0', '1.0']], [['101.0', '1.0']], 0);

      const msg = {
        U: 1, u: 1,
        b: [['100.0', '5.0'], ['99.0', '2.0']],
        a: [['101.0', '0']],
      };
      conn._applyDiff(msg);
      assert.strictEqual(conn.book.bids.get('100.0'), '5.0');
      assert.strictEqual(conn.book.asks.has('101.0'), false);
      assert.strictEqual(conn.book.bids.get('99.0'), '2.0');
    });
  });
});
