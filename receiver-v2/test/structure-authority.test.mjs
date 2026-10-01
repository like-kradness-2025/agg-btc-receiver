import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { openBook } from '../src/book/state.mjs';
import { openDurability } from '../src/durability.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';

test('a process that runs as a named run takes over a board whose recorded owner has no run', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'authority-'));
  try {
    const dbPath = join(dir, 'state.sqlite');

    // A board this build wrote down with no run at all. That is a recorded owner, not an unanswered
    // question (§2.3 reserves "unestablished" for the first accept and for rows that predate the column),
    // so handing it to a named run has to go through the explicit takeover - which is what the wiring is
    // for, and the reason a receiver must not sit refusing for ever because nobody is allowed to ask.
    const seeding = openDurability({ path: dbPath, runId: 'run-0' });
    const board = openBook({ market: 'kraken_spot', stream: 'trades', durability: seeding });
    board.accept('kraken_spot:1', { generation: 1, runId: null, firstSeq: 1 });
    seeding.close();

    const store = openDurability({ path: dbPath, runId: 'run-1' });
    const sockets = [];
    const stops = [];
    const diagnostics = [];
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-1',
      venue: 'kraken',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
      },
      durability: store,
      webSocketImpl: function fakeSocket(url) {
        const socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
        sockets.push(socket);
        return socket;
      },
      rawWriter: () => true,
      spoolDir: null,
      onStop: (stop) => stops.push(stop),
      onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
    });

    structure.start();
    assert.equal(sockets.length, 1, 'the board is handed over, so reception starts');
    assert.equal(structure.book.appliedBoundary.runId, 'run-1', 'and the run that is running owns it');
    assert.equal(structure.book.appliedBoundary.connectionId, 'run-1:kraken:kraken_spot:1');
    assert.deepEqual(stops, [], 'nothing had to be stopped');
    assert.equal(
      diagnostics.filter((diagnostic) => String(diagnostic.reason).includes('did not accept')).length,
      0,
      'and nothing was refused',
    );
    // The owner it replaced had no run name, but it is an owner all the same: the retirement is written down
    // against the empty name, which no run can carry, so it cannot walk back in later (C11).
    assert.deepEqual(
      store.db.prepare('SELECT run_id FROM retired_run').all().map((row) => row.run_id),
      [''],
      'the run-less owner was retired, under the name no run can carry',
    );
    assert.deepEqual(structure.book.retiredRuns(), [null], 'and it is reported as the identity it was accepted as');
    store.close();

    // Reopened, the board belongs to the named run: the owner without a name is not handed the board back,
    // whatever number it quotes and whoever authorises it.
    const reopened = openDurability({ path: dbPath, runId: 'run-2' });
    const later = openBook({ market: 'kraken_spot', stream: 'trades', durability: reopened });
    const returned = later.accept('kraken_spot:2', { generation: 2, runId: null, firstSeq: 1, takeover: true });
    assert.equal(returned.accepted, false, 'the replaced owner does not come back');
    assert.match(returned.reason, /already replaced/);
    assert.equal(later.appliedBoundary.runId, 'run-1', 'and the board is still the named run\'s');
    reopened.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
