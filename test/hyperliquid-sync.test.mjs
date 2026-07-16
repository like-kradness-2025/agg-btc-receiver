// test/hyperliquid-sync.test.mjs — Hyperliquid sync failure handling

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { HyperliquidConnector } from '../lib/hyperliquid-connector.mjs';

describe('HyperliquidConnector sync', () => {
  it('should transition to error when the first l2Book never arrives', async () => {
    const conn = new HyperliquidConnector({});
    conn._ws = null;

    // Force the wait path to fail immediately instead of sleeping for 15s.
    conn._waitForWsSnapshot = async () => {
      const err = new Error('ws l2Book timeout');
      err.code = 'WS_SNAPSHOT_TIMEOUT';
      throw err;
    };

    const errors = [];
    conn.on('error', (ev) => errors.push(ev));

    await assert.rejects(
      () => conn._syncBook(),
      /l2Book not received/
    );

    assert.strictEqual(conn.getState(), 'error');
    assert.ok(errors.some((ev) => String(ev.message).includes('l2Book not received')));
  });
});
