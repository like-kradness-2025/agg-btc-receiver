/**
 * What a change operation must do when another one is already running.
 *
 * The caller's hooks are synchronous functions it hands in, so they can call back into the structure
 * while it is in the middle of a frame. The store owns one execution right, and every public change
 * operation takes it: a call that arrives while another one is in progress is refused as a normal
 * answer - never as a store failure, which reception would turn into a stop - and nothing it asked for
 * is written. What the module that *is* the operation does continues on its own private route.
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';
import { openOrganizer } from '../src/organize/watermark.mjs';
import { withInjectableWrites, LEDGER_CONFIRM_WRITE } from '../test-support/failing-store.mjs';
import { internalsOf } from '../src/internal/wiring.mjs';

/** The parts of a structure, for a test that drives one of them directly: the wiring's private side. */
const partsOf = (structure) => internalsOf(structure);

const envelope = (seq, { connectionId = 'conn-1', generation = 1, payload, meta = { first_seq: 1 } } = {}) =>
  makeEnvelope({
    market: 'kraken_spot',
    stream: 'trades',
    connectionId,
    runId: 'run-1',
    venue: 'kraken',
    generation,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000 + seq,
    raw: payload ?? `{"seq":${seq},"size":${seq}}`,
    ...(meta ? { meta } : {}),
  });

function build(
  store,
  { written = [], rawWriter = null, spoolDir = null, onRawWrite = null, onAck = () => {}, onStop = () => {} } = {},
) {
  const holder = {};
  const structure = createStructure({
    market: 'kraken_spot',
    stream: 'trades',
    runId: 'run-1',
    venue: 'kraken',
    adapter: {
      url: 'ws://venue.test/ws',
      stream: 'trades',
      parse: () => ({ kind: 'data' }),
      changesFor: (frame) => {
        const { seq, size } = JSON.parse(frame.raw.toString('utf8'));
        return [{ side: 'bid', price: 100 + seq, size }];
      },
    },
    durability: store,
    spoolDir,
    webSocketImpl: function fakeSocket() {
      return { url: '', onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
    },
    // A test that needs the raw to refuse (so a frame is spilled) or to watch the writer itself
    // replaces it; the default writes every frame down once and takes it.
    rawWriter:
      rawWriter ??
      ((frame) => {
        written.push(`${frame.connection_id}:${frame.receive_seq}`);
        if (onRawWrite) onRawWrite(frame, holder.structure);
        return true;
      }),
    onAck,
    onGap: () => {},
    onStop,
    onDiagnostic: () => {},
  });
  holder.structure = structure;
  return structure;
}

async function withStore(fn) {
  const dir = await mkdtemp(join(tmpdir(), 'reentry-'));
  try {
    return await fn(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test('a frame handed in from inside another frame s write is refused, and its declared origin is not written', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const written = [];
    const refusals = [];
    let driven = false;
    const structure = build(store, {
      written,
      onRawWrite: (frame, subject) => {
        if (driven) return;
        driven = true;
        // A different sequence declaring a start, handed in while the outer frame is being written. It is
        // exactly the frame a start declaration must not cross, and it arrives from inside the operation
        // that owns this connection.
        refusals.push(subject.feed(envelope(5, { meta: { first_seq: 5 } })));
      },
    });
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    const first = structure.feed(envelope(1));

    assert.equal(first.applied, true, 'the frame that was being processed completed');
    assert.equal(refusals.length, 1, 'the second frame was handed in from inside the write');
    assert.equal(refusals[0].accepted, false, 'and it was refused');
    assert.equal(refusals[0].code, 'REENTRANT_OPERATION', 'as a change operation already in progress');
    assert.equal(structure.stats.stopped, false, 'a refusal is not a stop');
    assert.deepEqual(written, ['conn-1:1'], 'the refused frame never reached the raw');
    assert.equal(internalsOf(store).db.prepare('SELECT first_seq FROM applied_boundary').get().first_seq, 1, 'the origin is the accepted one');
    assert.equal(internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM book_level').get().n, 1);
    store.close();
  });
});

test('a frame handed in from inside the ledger s own write is refused, and the outer frame completes', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const injectable = withInjectableWrites(store);
    const holder = {};
    const refusals = [];
    let armed = true;
    injectable.onWrite((sql) => {
      if (!armed || !LEDGER_CONFIRM_WRITE.test(sql)) return;
      armed = false;
      // Inside the confirmation, which is inside the frame's transaction: a second frame handed in here
      // used to open a second BEGIN, whose failure reception turned into a permanent stop.
      refusals.push(holder.structure.feed(envelope(2)));
    });
    const structure = build(injectable.durability, {});
    holder.structure = structure;
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    const first = structure.feed(envelope(1));

    assert.equal(first.durable, true, 'the frame that was being processed is durable');
    assert.equal(first.applied, true, 'and it reached the board');
    assert.equal(refusals.length, 1, 'the second frame was handed in from inside the confirmation');
    assert.equal(refusals[0].code, 'REENTRANT_OPERATION');
    assert.equal(structure.stats.stopped, false, 'no second transaction was attempted, so nothing stopped');
    assert.equal(internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM book_level').get().n, 1, 'the refused frame left nothing behind');
    store.close();
  });
});

test('a file is held by one handle: a second open is refused, and nothing is written', async () => {
  await withStore(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const first = openDurability({ path, runId: 'run-1' });
    first.beginRun();

    const attempts = [];
    for (const [label, name] of [
      ['the same name', path],
      ['a symlink to it', join(dir, 'link.sqlite')],
      ['a relative name', `./${path.split('/').pop()}`],
    ]) {
      if (label === 'a symlink to it') {
        const { symlinkSync } = await import('node:fs');
        symlinkSync(path, name);
      }
      const cwd = process.cwd();
      process.chdir(dir);
      try {
        openDurability({ path: name, runId: 'run-2' });
        attempts.push({ label, opened: true });
      } catch (error) {
        attempts.push({ label, opened: false, code: error.code, message: error.message });
      } finally {
        process.chdir(cwd);
      }
    }

    assert.equal(attempts.length, 3);
    for (const attempt of attempts) {
      assert.equal(attempt.opened, false, `${attempt.label} is refused`);
      assert.equal(attempt.code, 'REENTRANT_OPERATION');
    }
    // Nothing of a second open was written, and the run that was live is still the live run.
    assert.equal(
      internalsOf(first).db.prepare('SELECT state FROM run_marker WHERE run_id = ?').get('run-1').state,
      'running',
      'the store was not opened a second time',
    );
    assert.equal(internalsOf(first).db.prepare("SELECT COUNT(*) AS n FROM run_marker WHERE run_id = 'run-2'").get().n, 0);

    // And the file is free again once the handle is closed.
    first.close();
    const again = openDurability({ path, runId: 'run-3' });
    again.beginRun();
    assert.equal(internalsOf(again).db.prepare('SELECT COUNT(*) AS n FROM run_marker').get().n, 2);
    again.close();
  });
});
test('reads are allowed from inside a frame s processing, and change nothing', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const seen = [];
    let driven = false;
    const structure = build(store, {
      onRawWrite: (frame, subject) => {
        if (driven) return;
        driven = true;
        seen.push({
          boundary: subject.book.appliedBoundary,
          gaps: subject.book.openGaps().length,
          owed: subject.ledger.size(),
          rows: subject.book.board.rows().length,
          depth: subject.book.board.depth,
          running: subject.book.isRunning,
        });
      },
    });
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    assert.equal(structure.feed(envelope(1)).applied, true);

    assert.equal(seen.length, 1);
    // The reads returned what they are for, not a refusal: a refusal carries the code, and these do not.
    assert.deepEqual(
      seen[0].boundary,
      { connectionId: 'conn-1', generation: 1, upToSeq: null, runId: 'run-1', firstSeq: 1 },
      'the position is readable from inside the frame',
    );
    assert.equal(seen[0].gaps, 0);
    assert.equal(seen[0].owed, 1, 'the intent for this very frame is already written down, and reading it is allowed');
    assert.equal(seen[0].rows, 0, 'the board has not applied it yet: the write comes first');
    assert.equal(seen[0].running, false);

    // And the board a caller can reach cannot change the book: the levels are readable, nothing else.
    assert.equal(structure.book.board.apply, undefined);
    assert.equal(structure.book.board.restore, undefined);
    assert.equal(typeof structure.book.board.size, 'function');
    assert.equal(structure.book.board.size('bid', 101), 1);
    store.close();
  });
});

test('the execution right is released, so the next frame is processed normally', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const written = [];
    const refusals = [];
    let driven = false;
    const structure = build(store, {
      written,
      onRawWrite: (frame, subject) => {
        if (driven) return;
        driven = true;
        refusals.push(subject.accept('conn-2', { runId: 'run-1', generation: 2, firstSeq: 1 }));
      },
    });
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    assert.equal(structure.feed(envelope(1)).applied, true);
    assert.equal(refusals[0].code, 'REENTRANT_OPERATION');

    // The next frame of the same connection is processed as though nothing had happened.
    const next = structure.feed(envelope(2));
    assert.equal(next.applied, true, 'the right was released with the refused call');
    assert.equal(structure.stats.owed, 0);
    assert.deepEqual(written, ['conn-1:1', 'conn-1:2']);
    assert.equal(internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM book_level').get().n, 2);
    store.close();
  });
});

test('a change attempted on any public surface from inside a frame is refused', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const seen = {};
    let driven = false;
    const structure = build(store, {
      onRawWrite: (frame, subject) => {
        if (driven) return;
        driven = true;
        // Every public change operation of every module, attempted from inside the frame that is being
        // processed. Each one is refused with the same code: the right belongs to the frame, not to the
        // caller, and a caller's hook is a caller.
        seen.ledgerRecord = partsOf(subject).ledger.record(envelope(9), 'a test entry', 'owed');
        seen.ledgerConfirm = partsOf(subject).ledger.confirm(envelope(1));
        seen.ledgerRelease = partsOf(subject).ledger.release({ connectionId: 'conn-1', firstSeq: 1, upToSeq: 1 });
        seen.ledgerDrop = partsOf(subject).ledger.drop('conn-1', 1);
        seen.ledgerSkip = partsOf(subject).ledger.skip('conn-1', 1, 'a test decision');
        seen.bookApply = partsOf(subject).book.apply({ envelope: envelope(2), changes: [{ side: 'bid', price: 102, size: 2 }] });
        seen.bookAccept = partsOf(subject).book.accept('conn-9', { runId: 'run-1', generation: 9, firstSeq: 1 });
        seen.bookBeginSync = partsOf(subject).book.beginSync();
        seen.bookProveBoundary = partsOf(subject).book.proveBoundary();
        seen.organizerAccept = partsOf(subject).organizer.accept('conn-9', { firstSeq: 1, runId: 'run-1', generation: 9 });
        seen.organizerNote = partsOf(subject).organizer.note(envelope(9));
        seen.storeBeginRun = store.beginRun();
        seen.structureAccept = subject.accept('conn-2', { runId: 'run-1', generation: 2, firstSeq: 1 });
        seen.structureResume = subject.resume();
        seen.structureRedeliver = subject.redeliverPending();
        seen.structureStart = subject.start();
      },
    });
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    const first = structure.feed(envelope(1));

    assert.equal(first.applied, true, 'the frame that was being processed completed');
    for (const [name, answer] of Object.entries(seen)) {
      assert.equal(answer?.code, 'REENTRANT_OPERATION', `${name} was refused`);
    }
    assert.equal(seen.structureStart.started, false, 'and starting reception is refused as a start');
    assert.equal(structure.stats.stopped, false, 'none of them stopped reception');
    assert.equal(internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM delivery_ledger').get().n, 0, 'and none of them wrote');
    assert.equal(internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM book_level').get().n, 1);
    assert.equal(structure.book.appliedBoundary.upToSeq, 1, 'the ceiling the frame moved is the only one');
    store.close();
  });
});

test('opening a module from inside a frame is refused before anything is written', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const injectable = withInjectableWrites(store);
    const attempts = [];
    let armed = true;
    injectable.onWrite((sql) => {
      if (!armed || !LEDGER_CONFIRM_WRITE.test(sql)) return;
      armed = false;
      // A module opened from inside the frame's own transaction. Its initialisation writes - and the nested
      // BEGIN that failed and was read as a store failure - must never be attempted.
      try {
        const other = openOrganizer({ market: 'other', stream: 'trades', durability: injectable.durability, writeRaw: () => true });
        attempts.push({ opened: true, other });
      } catch (error) {
        attempts.push({ opened: false, code: error.code });
      }
    });
    const structure = build(injectable.durability, {});
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    structure.feed(envelope(1));

    assert.equal(attempts.length, 1, 'the module was opened from inside the frame');
    assert.equal(attempts[0].opened, false, 'and refused');
    assert.equal(attempts[0].code, 'REENTRANT_OPERATION', 'with our own code, not a SQL error');
    // The refusal came before any write of its own: the other board has no tables and no rows.
    assert.equal(
      internalsOf(store).db.prepare("SELECT COUNT(*) AS n FROM sqlite_master WHERE name LIKE 'organized_watermark%' AND name != 'organized_watermark'").get().n,
      0,
      'nothing of the refused module was written',
    );
    assert.equal(internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM organized_watermark').get().n, 1, 'and this board has its own row');
    // Opening it outside the frame works: the refusal was about the moment, not the store.
    const opened = openOrganizer({ market: 'other', stream: 'trades', durability: store, writeRaw: () => true });
    opened.accept('other:1', { firstSeq: 1, runId: 'run-1', generation: 1 });
    assert.equal(opened.openGaps().length, 0);
    store.close();
  });
});

test('the private routes are not reachable from what a caller is handed', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const structure = build(store, {});
    assert.equal(structure.internal, undefined, 'the structure keeps its unguarded names to itself');
    assert.equal(structure.book.internal, undefined);
    assert.equal(structure.ledger.internal, undefined);
    assert.equal(store.endChange, undefined, 'and nothing handed out can release the right a frame is holding');
    assert.equal(store.beginChange, undefined);
    assert.equal(store.guard, undefined, 'and the way to take the right is not handed out either');
    assert.equal(typeof store.inChange, 'function', 'while the right can be observed, never moved');
    const { internalsOf } = await import('../src/internal/wiring.mjs');
    assert.equal(typeof internalsOf(store).guard, 'function', 'the wiring can still take it');
    store.close();
  });
});

test('opening the store from inside a frame is refused, and the live run is untouched', async () => {
  await withStore(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const store = openDurability({ path, runId: 'run-1' });
    store.beginRun();
    const injectable = withInjectableWrites(store);
    const attempts = [];
    let armed = true;
    injectable.onWrite((sql) => {
      if (!armed || !LEDGER_CONFIRM_WRITE.test(sql)) return;
      armed = false;
      try {
        openDurability({ path, runId: 'run-2' });
        attempts.push({ opened: true });
      } catch (error) {
        attempts.push({ opened: false, code: error.code, message: error.message });
      }
    });
    const structure = build(injectable.durability, {});
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    structure.feed(envelope(1));

    assert.equal(attempts.length, 1);
    assert.equal(attempts[0].opened, false, 'the store was not opened from inside the frame');
    assert.equal(attempts[0].code, 'REENTRANT_OPERATION', 'it is refused by us, not by SQLite');
    assert.match(attempts[0].message, /already held by another handle/, 'with our own refusal, not a storage error');
    assert.notEqual(attempts[0].message, 'database is locked', 'and not as a lock contention');
    // Nothing of the second open was written: the live run is still the live run.
    assert.equal(
      internalsOf(store).db.prepare('SELECT state FROM run_marker WHERE run_id = ?').get('run-1').state,
      'running',
      'the run that was live is not invalidated',
    );
    assert.equal(internalsOf(store).db.prepare("SELECT COUNT(*) AS n FROM run_marker WHERE run_id = 'run-2'").get().n, 0);
    store.close();
  });
});

test('a database SQLite cannot name as a file is refused', async () => {
  await withStore(async (dir) => {
    const { existsSync } = await import('node:fs');
    for (const name of [':memory:', 'file::memory:', 'file:shared-mem?mode=memory&cache=shared', 'file:memdb-name?vfs=memdb']) {
      assert.throws(
        () => openDurability({ path: name, runId: 'run-1' }),
        (error) => error.code === 'REENTRANT_OPERATION' && /no file to be held/.test(error.message),
        `${name} is refused`,
      );
    }
    assert.equal(existsSync(join(dir, ':memory:')), false, 'and none of them was created on disk');
    // A file is still a file, even when its name looks like a URI option.
    const odd = openDurability({ path: join(dir, 'a?x.sqlite'), runId: 'run-1' });
    odd.close();
    const oddMemoryName = openDurability({ path: join(dir, 'mode=memory.sqlite'), runId: 'run-1' });
    oddMemoryName.close();
  });
});
test('a hard link created while the store is live is refused before anything is written', async () => {
  await withStore(async (dir) => {
    const { linkSync } = await import('node:fs');
    const path = join(dir, 'state.sqlite');
    const store = openDurability({ path, runId: 'run-1' });
    store.beginRun();
    const injectable = withInjectableWrites(store);
    const attempts = [];
    let armed = true;
    injectable.onWrite((sql) => {
      if (!armed || !LEDGER_CONFIRM_WRITE.test(sql)) return;
      armed = false;
      // The link is made after this handle opened, so the name is new to the registry while the file is not.
      const linked = join(dir, 'made-later.sqlite');
      linkSync(path, linked);
      try {
        openDurability({ path: linked, runId: 'run-2' });
        attempts.push({ opened: true });
      } catch (error) {
        attempts.push({ opened: false, code: error.code, message: error.message });
      }
    });
    const structure = build(injectable.durability, {});
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    const first = structure.feed(envelope(1));

    assert.equal(attempts.length, 1);
    assert.equal(attempts[0].opened, false, 'the same inode is the same store, under its new name too');
    assert.equal(attempts[0].code, 'REENTRANT_OPERATION', 'refused by us');
    assert.equal(
      /SQL|locked|I\/O/i.test(String(attempts[0].message)),
      false,
      'and not as a SQL or storage failure, which is what a refusal after the initialisation would look like',
    );
    assert.equal(first.applied, true, 'the frame that was being processed completed');
    assert.equal(
      internalsOf(store).db.prepare('SELECT state FROM run_marker WHERE run_id = ?').get('run-1').state,
      'running',
      'the live run is untouched',
    );
    store.close();
  });
});


test('closing twice does not free a file another handle has taken', async () => {
  await withStore(async (dir) => {
    const path = join(dir, 'state.sqlite');
    // A handle whose close does not complain about being closed again: with this, "closing is once" is the
    // only thing that can keep the second call from freeing a file another handle has taken.
    const { DatabaseSync } = await import('node:sqlite');
    class LenientClose extends DatabaseSync {
      close() {
        try {
          return super.close();
        } catch {
          return undefined;
        }
      }
    }
    const first = openDurability({ path, runId: 'run-1', Database: LenientClose });
    first.close();
    const second = openDurability({ path, runId: 'run-2', Database: LenientClose });

    first.close(); // already closed: this must change nothing

    assert.throws(
      () => openDurability({ path, runId: 'run-3' }),
      (error) => error.code === 'REENTRANT_OPERATION',
      'the handle that holds the file still holds it',
    );
    second.close();
    const third = openDurability({ path, runId: 'run-3' });
    third.close();
  });
});

test('a relative URI is refused only where it means the same file', async () => {
  await withStore(async (dir) => {
    const { mkdirSync } = await import('node:fs');
    const elsewhere = join(dir, 'elsewhere');
    mkdirSync(elsewhere);
    const cwd = process.cwd();
    process.chdir(dir);
    const here = openDurability({ path: 'file:state.sqlite', runId: 'run-1' });
    here.beginRun();
    assert.throws(
      () => openDurability({ path: 'file:state.sqlite', runId: 'run-2' }),
      (error) => error.code === 'REENTRANT_OPERATION',
      'the same URI in the same place is the held file',
    );
    try {
      process.chdir(elsewhere);
      const there = openDurability({ path: 'file:state.sqlite', runId: 'run-3' });
      there.close();
      assert.equal(internalsOf(here).db.prepare('SELECT COUNT(*) AS n FROM run_marker').get().n, 1, 'the held file is untouched');
    } finally {
      process.chdir(cwd);
      here.close();
    }
  });
});

test('a relative name is refused only where it means the same file', async () => {
  await withStore(async (dir) => {
    const { mkdirSync } = await import('node:fs');
    const elsewhere = join(dir, 'elsewhere');
    mkdirSync(elsewhere);
    const cwd = process.cwd();
    process.chdir(dir);
    const here = openDurability({ path: 'state.sqlite', runId: 'run-1' });
    here.beginRun();
    try {
      process.chdir(elsewhere);
      // The same relative name, a different directory, a different file: this is not the held one.
      const there = openDurability({ path: 'state.sqlite', runId: 'run-2' });
      there.close();
      assert.equal(internalsOf(here).db.prepare('SELECT COUNT(*) AS n FROM run_marker').get().n, 1, 'the held file is untouched');
    } finally {
      process.chdir(cwd);
      here.close();
    }
  });
});

test('a close that fails leaves the file held, and closing can be tried again', async () => {
  await withStore(async (dir) => {
    const path = join(dir, 'state.sqlite');
    let refuseOnce = true;
    const { DatabaseSync } = await import('node:sqlite');
    class FlakyClose extends DatabaseSync {
      close() {
        if (refuseOnce) {
          refuseOnce = false;
          throw new Error('close refused');
        }
        return super.close();
      }
    }
    const first = openDurability({ path, runId: 'run-1', Database: FlakyClose });
    assert.throws(() => first.close(), /close refused/);
    assert.throws(
      () => openDurability({ path, runId: 'run-2' }),
      (error) => error.code === 'REENTRANT_OPERATION',
      'the file is not freed by a close that failed',
    );
    first.close();
    const again = openDurability({ path, runId: 'run-3' });
    again.close();
  });
});

test('a store whose file cannot be identified is refused, not identified by its name', async () => {
  await withStore(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const { DatabaseSync } = await import('node:sqlite');
    class Unidentifiable extends DatabaseSync {
      prepare(sql) {
        if (/database_list/.test(sql)) {
          // What SQLite would report if the file it opened could not be looked at afterwards.
          return { all: () => [{ name: 'main', file: join(dir, 'nowhere.sqlite') }] };
        }
        return super.prepare(sql);
      }
    }
    assert.throws(
      () => openDurability({ path, runId: 'run-1', Database: Unidentifiable }),
      /could not be identified/,
    );
    // The file is not left held by the attempt that failed.
    const after = openDurability({ path, runId: 'run-2' });
    after.close();
  });
});

test('a re-open from inside the store s own initialisation is refused', async () => {
  await withStore(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const attempts = [];
    let armed = true;
    const store = openDurability({
      path,
      runId: 'run-1',
      nowMs: () => {
        if (armed) {
          armed = false;
          try {
            openDurability({ path, runId: 'run-2' });
            attempts.push({ opened: true });
          } catch (error) {
            attempts.push({ opened: false, code: error.code });
          }
        }
        return Date.now();
      },
    });

    assert.equal(attempts.length, 1, 'the clock was called while the store was being opened');
    assert.equal(attempts[0].opened, false, 'the file was already held when the clock was called');
    assert.equal(attempts[0].code, 'REENTRANT_OPERATION');
    store.beginRun();
    assert.equal(internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM run_marker').get().n, 1, 'only this run is written');
    store.close();
  });
});

test('the private routes are not handed over with the module, in an argument or on the object', async () => {
  const { internalsOf } = await import('../src/internal/wiring.mjs');
  await withStore(async (dir) => {
    const { openOrganizer } = await import('../src/organize/watermark.mjs');
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const bag = {};
    const organizer = openOrganizer({
      market: 'kraken_spot',
      stream: 'trades',
      durability: store,
      writeRaw: () => true,
      // the shape an earlier version accepted: a caller's object to fill with the unguarded routes
      internals: bag,
    });
    assert.deepEqual(Object.keys(bag), [], 'no argument is filled with the private routes');
    const internal = internalsOf(organizer);
    for (const [name, value] of Object.entries(organizer)) {
      assert.notEqual(value, internal.note, `${name} is not the unguarded route`);
      assert.notEqual(value, internal.accept, `${name} is not the unguarded route`);
    }
    assert.equal(typeof internal.note, 'function', 'the wiring can still obtain them');
    assert.throws(() => internalsOf({}), /not opened by the wiring/);
    store.close();
  });
});

test('the recovery a new structure runs holds the store while it runs', async () => {
  await withStore(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const store = openDurability({ path, runId: 'run-1' });
    const first = build(store, {});
    first.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    // A frame the raw may not hold: an intent, which is what a crash between the raw and its confirmation
    // leaves behind, and what the next structure has to offer back through the organizer.
    assert.equal(partsOf(first).ledger.record(envelope(1), 'about to be written', 'intent').recorded, true);
    // The next structure takes its own store, as a restarted process does: a store belongs to one
    // structure at a time, and a closed one is not taken again.
    store.close();
    const nextStore = openDurability({ path, runId: 'run-2' });

    const { openOrganizer } = await import('../src/organize/watermark.mjs');
    const other = openOrganizer({
      market: 'kraken_spot',
      stream: 'trades',
      durability: nextStore,
      writeRaw: () => true,
    });
    const observed = [];
    build(nextStore, {
      onRawWrite: () => {
        observed.push({ inChange: nextStore.inChange() });
        // A public change operation, driven from inside the recovery: exactly what the construction-time
        // recovery not holding the store would let through.
        observed.push({ refusal: other.note(envelope(99), {})?.code ?? null });
      },
    });

    assert.ok(observed.length >= 2, 'the recovery re-offered the intent through the raw writer');
    assert.equal(observed[0].inChange, true, 'the recovery holds the store while it runs');
    assert.equal(observed[1].refusal, 'REENTRANT_OPERATION', 'so no change operation can start from inside it');
    nextStore.close();
  });
});

test('an open that fails frees the file only if its handle really closed', async () => {
  // Not `withStore`: the second half of this test leaves its file held on purpose, and removing the
  // directory would free the file's inode - a later store in this process can be handed that inode and
  // then be refused by a handle that is already gone, because the store's identity is the file itself,
  // not its name. The directory is kept until the process ends, when every handle is being torn down.
  const dir = await mkdtemp(join(tmpdir(), 'reentry-held-'));
  const { rmSync } = await import('node:fs');
  process.once('exit', () => rmSync(dir, { recursive: true, force: true }));
  const { DatabaseSync } = await import('node:sqlite');
  const path = join(dir, 'state.sqlite');
  let refusals = 1;
  class FlakyClose extends DatabaseSync {
    close() {
      if (refusals-- > 0) throw new Error('close refused');
      return super.close();
    }
  }
  const failingClock = () => {
    throw new Error('the clock failed');
  };
  // The close refuses once: the retry closes it, so the file is free again.
  assert.throws(
    () => openDurability({ path, runId: 'run-1', Database: FlakyClose, nowMs: failingClock }),
    /the clock failed/,
  );
  const afterwards = openDurability({ path, runId: 'run-2' });
  afterwards.close();

  // The close never succeeds: the file stays held rather than admitting a handle over a live connection.
  class NeverCloses extends DatabaseSync {
    close() {
      throw new Error('never closes');
    }
  }
  assert.throws(
    () => openDurability({ path, runId: 'run-3', Database: NeverCloses, nowMs: failingClock }),
    /the clock failed/,
  );
  assert.throws(
    () => openDurability({ path, runId: 'run-4' }),
    (error) => error.code === 'REENTRANT_OPERATION',
    'the file is still held',
  );
});

test('a module takes the right and the transactions from the wiring, never from what it is handed', async () => {
  await withStore(async (dir) => {
    const { bindInternals, internalsOf } = await import('../src/internal/wiring.mjs');
    const { openOrganizer } = await import('../src/organize/watermark.mjs');
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const captured = [];
    const replaceable = {
      ...store,
      guard: (name, fn, ...rest) => {
        captured.push([name, fn]);
        return internalsOf(store).guard(name, fn, ...rest);
      },
      inTransaction: (fn) => {
        captured.push(['inTransaction', fn]);
        return internalsOf(store).inTransaction(fn);
      },
      inChange: () => {
        captured.push(['inChange']);
        return false;
      },
    };
    bindInternals(replaceable, internalsOf(store));
    const organizer = openOrganizer({
      market: 'kraken_spot',
      stream: 'trades',
      durability: replaceable,
      writeRaw: () => true,
    });

    assert.deepEqual(
      captured,
      [],
      'nothing the caller can replace was handed a route or a write of the module s own',
    );
    const accepted = organizer.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    assert.equal(accepted.connectionId, 'conn-1', 'and the module still works, taking the right from the wiring');
    store.close();
  });
});

test('a lying observation cannot let a module open while the store is held', async () => {
  await withStore(async (dir) => {
    const { bindInternals, internalsOf } = await import('../src/internal/wiring.mjs');
    const { openOrganizer } = await import('../src/organize/watermark.mjs');
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    const injectable = withInjectableWrites(store);
    const attempts = [];
    let armed = true;
    injectable.onWrite((sql) => {
      if (!armed || !LEDGER_CONFIRM_WRITE.test(sql)) return;
      armed = false;
      const lying = { ...store, inChange: () => false, inTransaction: (fn) => fn() };
      bindInternals(lying, internalsOf(store));
      try {
        openOrganizer({ market: 'kraken_spot', stream: 'trades', durability: lying, writeRaw: () => true });
        attempts.push({ opened: true });
      } catch (error) {
        attempts.push({ opened: false, code: error.code });
      }
    });
    const structure = build(injectable.durability, {});
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    structure.feed(envelope(1));

    assert.equal(attempts.length, 1);
    assert.equal(attempts[0].opened, false, 'the module refuses while the store is held, whatever it is told');
    assert.equal(attempts[0].code, 'REENTRANT_OPERATION');
    store.close();
  });
});

test('the store a caller is handed offers no transaction of its own', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    // A caller's transaction was a change operation; it is not handed out at all now, so there is nothing to
    // refuse and nothing to nest. The wiring keeps the one it needs for an operation it already owns.
    assert.equal(store.inTransaction, undefined, 'no transaction helper is handed out');
    assert.equal(store.db, undefined, 'and no database handle either');
    assert.equal(typeof internalsOf(store).inTransaction, 'function', 'the wiring keeps the transaction');
    assert.equal(typeof internalsOf(store).db, 'object', 'and the handle it runs statements through');
    store.close();
  });
});
test('opening a module holds the store while its initialisation runs', async () => {
  await withStore(async (dir) => {
    const { openOrganizer } = await import('../src/organize/watermark.mjs');
    const { openBook } = await import('../src/book/state.mjs');
    // A store that predates the ownership columns: opening the book then migrates it, and the migration is
    // what calls the caller's clock - the hook a re-entrant call can be driven from.
    const { DatabaseSync } = await import('node:sqlite');
    const path = join(dir, 'state.sqlite');
    const legacy = new DatabaseSync(path);
    legacy.exec(`
      CREATE TABLE applied_boundary (
        market TEXT NOT NULL, stream TEXT NOT NULL, connection_id TEXT, generation INTEGER,
        up_to_receive_seq INTEGER, updated_at_ms INTEGER, PRIMARY KEY (market, stream)
      );
      INSERT INTO applied_boundary (market, stream, connection_id, generation, up_to_receive_seq, updated_at_ms)
      VALUES ('kraken_spot', 'book', 'kraken_spot:1', 5, 7, 1);
    `);
    legacy.close();

    const store = openDurability({ path, runId: 'run-1' });
    const organizer = openOrganizer({
      market: 'kraken_spot',
      stream: 'trades',
      durability: store,
      writeRaw: () => true,
    });

    const seen = [];
    openBook({
      market: 'kraken_spot',
      stream: 'book',
      durability: store,
      nowMs: () => {
        if (seen.length === 0) {
          seen.push({ inChange: store.inChange() });
          const note = organizer.note(envelope(1), {});
          seen.push({ accepted: note?.accepted, code: note?.code ?? null });
        }
        return Date.now();
      },
    });

    assert.equal(seen.length, 2, 'the initialisation called the clock, and the clock drove a re-entry');
    assert.equal(seen[0].inChange, true, 'the initialisation holds the store');
    assert.equal(seen[1].accepted, false, 'so a public operation from inside it did not run');
    assert.equal(seen[1].code, 'REENTRANT_OPERATION', 'and it was refused by us');
    store.close();
  });
});

test('a frame that arrives on the socket is one operation, so a hook inside it cannot start another', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    const written = [];
    const observations = [];
    let socket = null;
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-1',
      venue: 'kraken',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: (frame) => {
          const { seq, size } = JSON.parse(frame.raw.toString('utf8'));
          return [{ side: 'bid', price: 100 + seq, size }];
        },
      },
      durability: store,
      webSocketImpl: function fakeSocket(url) {
        socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
        return socket;
      },
      rawWriter: (frame) => {
        written.push(frame.receive_seq);
        observations.push({ inChange: store.inChange() });
        observations.push({ resumed: structure.resume() });
        return true;
      },
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
      onDiagnostic: () => {},
    });

    structure.start();
    socket.onopen?.();
    // A frame the raw may not hold: an intent, which a resume offers back through the organizer.
    partsOf(structure).ledger.record(envelope(1), 'about to be written', 'intent');
    socket.onmessage?.({ data: JSON.stringify({ seq: 1, size: 1 }) });

    assert.equal(observations.length >= 2, true, 'the socket frame reached the raw writer');
    assert.equal(observations[0].inChange, true, 'the frame holds the store while it is processed');
    assert.equal(observations[1].resumed.refused, true, 'so a public operation from inside it is refused');
    assert.equal(observations[1].resumed.code, 'REENTRANT_OPERATION', 'with our own code');
    assert.deepEqual(written, [1], 'and the frame was written to the raw once, not twice');
    store.close();
  });
});

test('every public operation of the structure is refused from inside a frame', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    const written = [];
    const attempts = [];
    let armed = true;
    const structure = build(store, {
      written,
      onRawWrite: () => {
        if (!armed) return;
        armed = false;
        // Every window the structure offers a caller, driven from inside the frame that is being written.
        attempts.push(structure.feed(envelope(2)));
        attempts.push(structure.accept('conn-9', { runId: 'run-1', generation: 1, firstSeq: 1 }));
        attempts.push(structure.resume());
        attempts.push(structure.redeliverPending());
        attempts.push(structure.start());
        attempts.push(structure.stop());
      },
    });
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    const first = structure.feed(envelope(1));
    const second = structure.feed(envelope(2));

    assert.equal(attempts.length, 6, 'all six were attempted from inside the frame');
    for (const [index, result] of attempts.entries()) {
      assert.equal(result?.code, 'REENTRANT_OPERATION', `attempt ${index} came back as our refusal, not a throw`);
    }
    assert.equal(first.applied, true, 'the frame that was being processed completed');
    assert.equal(second.applied, true, 'and so did the next one');
    assert.deepEqual(written, ['conn-1:1', 'conn-1:2'], 'each was written to the raw exactly once');
    store.close();
  });
});

test('the construction of a structure holds the store while its parts are opened', async () => {
  await withStore(async (dir) => {
    // A store that predates the ownership columns: opening the book migrates it, and the migration calls the
    // clock - the hook a re-entrant call can be driven from, during the structure's own construction.
    const { DatabaseSync } = await import('node:sqlite');
    const path = join(dir, 'state.sqlite');
    const legacy = new DatabaseSync(path);
    legacy.exec(`
      CREATE TABLE applied_boundary (
        market TEXT NOT NULL, stream TEXT NOT NULL, connection_id TEXT, generation INTEGER,
        up_to_receive_seq INTEGER, updated_at_ms INTEGER, PRIMARY KEY (market, stream)
      );
      INSERT INTO applied_boundary (market, stream, connection_id, generation, up_to_receive_seq, updated_at_ms)
      VALUES ('kraken_spot', 'book', 'kraken_spot:1', 5, 7, 1);
    `);
    legacy.close();

    const store = openDurability({ path, runId: 'run-1' });
    const seen = [];
    let armed = true;
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-1',
      venue: 'kraken',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: () => [],
      },
      durability: store,
      webSocketImpl: function fakeSocket(url) {
        return { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
      },
      rawWriter: () => true,
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
      onDiagnostic: () => {},
      nowMs: () => {
        if (armed) {
          armed = false;
          seen.push({ inChange: store.inChange() });
          // The structure is still being built, so the re-entry is driven at the store's own surface.
          try {
            // A store operation refuses by returning its refusal, and the caller's transaction by throwing;
            // both shapes are read here.
            const result = store.beginRun();
            seen.push({ refused: result?.code ?? null });
          } catch (error) {
            seen.push({ refused: error.code });
          }
        }
        return Date.now();
      },
    });

    assert.equal(armed, false, 'the clock was called while the structure was being built');
    assert.equal(seen[0].inChange, true, 'the construction holds the store while its parts are opened');
    assert.equal(seen[1].refused, 'REENTRANT_OPERATION', 'so a public operation from inside it is refused');
    structure.stop();
    store.close();
  });
});

test('the connection a caller is handed cannot be started or stopped at all', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const structure = build(store, {});
    // Reception is started and stopped through the structure's own windows; a caller holding the connection
    // can read it and has no way to replace it.
    assert.equal(structure.connection.start, undefined, 'no way to start reception from the handle');
    assert.equal(structure.connection.stop, undefined, 'and none to stop it');
    assert.equal(typeof structure.connection.generation, 'number', 'while its reads are there');
    assert.equal(typeof structure.connection.state, 'string');
    assert.equal(typeof partsOf(structure).connection.start, 'function', 'the wiring keeps the real one');
    store.close();
  });
});
test('an arrival during a frame waits its turn instead of being refused or dropped', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    const written = [];
    const acks = [];
    let socket = null;
    let duringWrite = null;
    let numberedWhileWriting = null;
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-1',
      venue: 'kraken',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: (frame) => [{ side: 'bid', price: 100, size: JSON.parse(frame.raw.toString('utf8')).size }],
      },
      durability: store,
      webSocketImpl: function fakeSocket(url) {
        socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
        return socket;
      },
      rawWriter: (frame) => {
        written.push(frame.receive_seq);
        if (written.length === 1) {
          // Two more frames arrive while this one is being written.
          socket.onmessage?.({ data: JSON.stringify({ seq: 2, size: 2 }) });
          socket.onmessage?.({ data: JSON.stringify({ seq: 3, size: 3 }) });
          duringWrite = [...written];
          // The arrivals are not parsed or numbered until their turn: the connection still reports the one
          // frame that is being written.
          numberedWhileWriting = structure.connection.receiveSeq;
        }
        return true;
      },
      onAck: (ack) => acks.push(ack.upToSeq),
      onGap: () => {},
      onStop: () => {},
      onDiagnostic: () => {},
    });

    structure.start();
    socket.onmessage?.({ data: JSON.stringify({ seq: 1, size: 1 }) });

    assert.deepEqual(duringWrite, [1], 'nothing was written for the frames that arrived during the write');
    assert.equal(numberedWhileWriting, 1, 'and they were not parsed or numbered until their turn came');
    assert.deepEqual(written, [1, 2, 3], 'and then they ran in order, once each, before the frame returned');
    assert.deepEqual(acks, [1, 2, 3], 'with the acknowledgements in the same order');

    socket.onmessage?.({ data: JSON.stringify({ seq: 4, size: 4 }) });
    assert.deepEqual(written, [1, 2, 3, 4], 'and an arrival with nothing running is processed at once');
    store.close();
  });
});

test('a wait-list that fills up stops reception and says what was dropped', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    const written = [];
    const stops = [];
    let socket = null;
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-1',
      venue: 'kraken',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: (frame) => [{ side: 'bid', price: 100, size: JSON.parse(frame.raw.toString('utf8')).size }],
      },
      durability: store,
      maxWaitingEvents: 1,
      webSocketImpl: function fakeSocket(url) {
        socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
        return socket;
      },
      rawWriter: (frame) => {
        written.push(frame.receive_seq);
        if (written.length === 1) {
          socket.onmessage?.({ data: JSON.stringify({ seq: 2, size: 2 }) });
          socket.onmessage?.({ data: JSON.stringify({ seq: 3, size: 3 }) });
        }
        return true;
      },
      onAck: () => {},
      onGap: () => {},
      onStop: (stop) => stops.push(stop),
      onDiagnostic: () => {},
    });

    structure.start();
    socket.onmessage?.({ data: JSON.stringify({ seq: 1, size: 1 }) });

    assert.equal(stops.length, 1, 'the structure stopped instead of dropping an arrival in silence');
    assert.match(String(stops[0].reason), /wait-list/, 'and it says which wait-list filled up');
    assert.match(String(stops[0].reason), /message/, 'and which arrival it was');
    store.close();
  });
});

test('a timer that fires during a frame waits for it, and an abandoned socket is not heard', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    const written = [];
    const diagnostics = [];
    const timers = [];
    const sockets = [];
    let fired = false;
    let duringWrite = null;
    let silenceTimer = null;
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-1',
      venue: 'kraken',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: (frame) => [{ side: 'bid', price: 100, size: JSON.parse(frame.raw.toString('utf8')).size }],
      },
      durability: store,
      silenceDeadlineMs: 1_000,
      setTimer: (fn, ms) => {
        const timer = { fn, ms, unref() {} };
        timers.push(timer);
        return timer;
      },
      clearTimer: (timer) => {
        timer.cleared = true;
      },
      webSocketImpl: function fakeSocket(url) {
        const socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
        sockets.push(socket);
        return socket;
      },
      rawWriter: (frame) => {
        written.push(frame.receive_seq);
        if (!fired) {
          fired = true;
          const silence = timers.find((timer) => timer.ms === 1_000 && !timer.cleared);
          silence.cleared = true;
          silenceTimer = silence;
          silence.fn(); // the silence deadline passes while this frame is being written
          duringWrite = diagnostics.length;
        }
        return true;
      },
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
      onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
    });

    structure.start();
    sockets[0].onopen?.();
    sockets[0].onmessage?.({ data: JSON.stringify({ seq: 1, size: 1 }) });

    assert.equal(duringWrite, 0, 'the timer s work did not run in the middle of the frame');
    assert.equal(
      diagnostics.some((diagnostic) => /silence deadline passed/.test(String(diagnostic.reason))),
      true,
      'and it ran once the frame was done',
    );
    const reconnect = timers.find((timer) => !timer.cleared && timer.ms > 0);
    assert.ok(reconnect, 'the silence scheduled a replacement socket');
    reconnect.cleared = true;
    reconnect.fn();
    assert.equal(sockets.length, 2, 'the silence replaced the socket, as it does');
    const writesSoFar = written.length;
    sockets[0].onmessage?.({ data: JSON.stringify({ seq: 2, size: 2 }) });
    assert.equal(written.length, writesSoFar, 'a frame from the abandoned socket is not heard');
    // And its own deadline, fired after the replacement, is not acted on: the silence belonged to a
    // connection that is no longer receiving, so it neither reports nor replaces anything.
    const socketsBefore = sockets.length;
    const diagnosticsBefore = diagnostics.length;
    silenceTimer.fn();
    assert.equal(sockets.length, socketsBefore, 'an abandoned socket s deadline replaces no socket');
    assert.equal(diagnostics.length, diagnosticsBefore, 'and it reports nothing about the new connection');
    store.close();
  });
});

test('the views a caller is handed cannot change what the structure is doing', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    const written = [];
    const structure = build(store, { written, spoolDir: join(dir, 'spool') });
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    structure.feed(envelope(1));

    // No change operation is reachable from what a caller is handed.
    for (const [name, value] of [
      ['book.apply', structure.book.apply],
      ['book.accept', structure.book.accept],
      ['book.beginSync', structure.book.beginSync],
      ['organizer.accept', structure.organizer.accept],
      ['organizer.note', structure.organizer.note],
      ['ledger.record', structure.ledger.record],
      ['ledger.confirm', structure.ledger.confirm],
      ['ledger.release', structure.ledger.release],
      ['ledger.drop', structure.ledger.drop],
      ['ledger.skip', structure.ledger.skip],
      ['connection.start', structure.connection.start],
      ['connection.stop', structure.connection.stop],
    ]) {
      assert.equal(value, undefined, `${name} is not handed out`);
    }

    // What a read returns is a copy: changing it does not change the structure.
    const boundary = structure.book.appliedBoundary;
    boundary.upToSeq = 999;
    assert.equal(structure.book.appliedBoundary.upToSeq, 1, 'the structure still reports its own position');

    const rows = structure.book.board.rows();
    assert.ok(rows.length > 0, 'the board has the level the frame carried');
    const level = rows[0];
    const size = structure.book.board.size(level.side, level.price);
    level.size = 0;
    assert.equal(structure.book.board.size(level.side, level.price), size, 'and its own board');

    const gaps = structure.book.openGaps();
    gaps.push({ side: 'bid', price: 1, size: 1 });
    assert.equal(structure.book.openGaps().length, 0, 'a gap pushed onto a copy is not a gap');

    // And it keeps working: the acceptance it already has, and the next frame.
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    const second = structure.feed(envelope(2));
    assert.equal(second.applied, true, 'the next frame is processed normally');
    assert.deepEqual(written, ['conn-1:1', 'conn-1:2'], 'and reaches the raw once each');

    // The spool is handed over as reads only: appending, advancing the cursor, syncing and closing are
    // the structure's own operations. A cursor a caller could advance deletes the segments behind the
    // position it names, and with them frames nobody has delivered yet.
    for (const name of ['append', 'advance', 'close', 'sync']) {
      assert.equal(structure.spool[name], undefined, `spool.${name} is a change operation and is not handed out`);
    }
    assert.equal(typeof structure.spool.drain, 'function', 'while what is waiting stays readable');

    // Replacing a name on a view is a change to the caller's own copy, not to the structure: the frame
    // that follows goes through the parts themselves, not through the view.
    structure.book.openGaps = () => [{ side: 'bid', price: 0, size: 0 }];
    structure.organizer.currentAck = () => null;
    structure.ledger.pending = () => [];
    const third = structure.feed(envelope(3));
    assert.equal(third.applied, true, 'a replaced view name is not the structure s route');
    assert.equal(structure.stats.gaps, 0, 'and the structure does not read its gaps through it');
    assert.equal(structure.book.board.size('bid', 103), 3, 'the frame reached the board');
    assert.deepEqual(written, ['conn-1:1', 'conn-1:2', 'conn-1:3'], 'and the raw was asked once for it');
    store.close();
  });
});

test('the spool a caller is handed holds no way to take a frame out of it', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    // A name that describes its own identity, because what the spool holds is read back as a frame.
    const connectionId = 'run-1:kraken:kraken_spot:1';
    const structure = build(store, {
      spoolDir: join(dir, 'spool'),
      rawWriter: () => false, // the raw refuses, so the frame is spilled rather than written
    });
    structure.accept(connectionId, { runId: 'run-1', generation: 1, firstSeq: 1 });
    const spilled = structure.feed(envelope(1, { connectionId }));
    assert.equal(spilled.spooled, true, 'the raw refused and the spool took the frame');
    assert.equal(structure.stats.spooledFrames, 1);

    // The view carries the reads...
    for (const name of ['bytes', 'segments', 'cursor', 'isOverBound', 'failed']) {
      assert.notEqual(structure.spool[name], undefined, `spool.${name} stays readable`);
    }
    assert.equal(structure.spool.bytes > 0, true, 'and the frame is in it');

    // ...and none of the change operations the spool itself has. `advance` is the one that drops
    // undelivered frames: it deletes the segments behind the position it names.
    for (const name of ['append', 'advance', 'close', 'sync']) {
      assert.equal(structure.spool[name], undefined, `spool.${name} is a change operation and is not handed out`);
    }

    // What a read returns is a copy: editing it changes nothing.
    const segments = structure.spool.segments;
    segments.push({ name: 'segment-9999999999.spool', index: 9_999_999_999, bytes: 0 });
    assert.equal(structure.spool.segments.length, 1, 'a segment pushed onto a copy is not a segment');
    const cursor = structure.spool.cursor;
    cursor.segment = 9_999_999_999;
    assert.equal(structure.spool.cursor.segment, null, 'and a cursor edited on a copy is not the cursor');

    // The reviewer s sequence: advance the public spool past everything waiting. The view holds no such
    // operation, so the frame nobody has delivered is still there to read.
    const waiting = () => [...structure.spool.drain()].filter((entry) => entry && typeof entry === 'object');
    assert.equal(waiting().length, 1, 'the spilled frame is readable through the view');
    structure.spool.advance?.({ segment: 9_999_999_999, offset: 0 });
    assert.equal(waiting().length, 1, 'and nothing reachable from the view advances past it');

    // The wiring keeps the real spool: the change operations live on the internal side only.
    assert.equal(typeof partsOf(structure).spool.advance, 'function', 'the structure still owns the real spool');
    store.close();
  });
});

test('a name a caller replaces on the structure is not the route its own re-delivery takes', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    const written = [];
    const structure = build(store, { written });
    structure.accept('conn-1', { runId: 'run-1', generation: 1 });
    // A frame the raw takes and the board cannot anchor yet: exactly the state an accept offers again.
    const held = structure.feed(envelope(1, { meta: null }));
    assert.equal(held.durable, true, 'the raw took the frame');
    assert.equal(held.applied, false, 'and the board has nothing to anchor it to yet');
    assert.equal(structure.stats.owed, 1, 'so it waits as an owed frame');

    // Every public change operation is replaced with a function that would leave the frame undelivered.
    const calls = [];
    const original = {};
    for (const name of ['feed', 'accept', 'redeliverPending', 'resume', 'start', 'stop', 'close']) {
      original[name] = structure[name];
      structure[name] = () => {
        calls.push(name);
        return { replaced: name };
      };
    }

    // The accept that completes the origin offers the owed frame again - internally, through the
    // closed-over route, not through the name a caller replaced.
    const accepted = original.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    assert.equal(accepted.accepted, true, 'the completion went through the real route');
    assert.deepEqual(calls, [], 'no replaced name was the structure s route');
    assert.equal(structure.book.appliedBoundary.upToSeq, 1, 'the re-delivered frame reached the board');
    assert.equal(structure.stats.owed, 0, 'and stopped being owed');
    const next = original.feed(envelope(2));
    assert.equal(next.applied, true, 'the next frame is processed normally');
    assert.deepEqual(calls, [], 'still through the structure s own route');
    assert.deepEqual(written, ['conn-1:1', 'conn-1:2'], 'the raw was asked once per frame');

    // And the replacement is the caller's own function, which only the caller calls.
    assert.deepEqual(structure.feed(envelope(3)), { replaced: 'feed' }, 'a direct call reaches the replacement');
    assert.deepEqual(calls, ['feed'], 'which the structure s own operations never did');
    store.close();
  });
});

test('the subscription map a caller reads is a copy, so editing it changes nothing', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    const sockets = [];
    const handed = [];
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-1',
      venue: 'kraken',
      durability: store,
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: (raw) => JSON.parse(raw),
      },
      webSocketImpl: function fakeSocket(url) {
        const socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
        sockets.push(socket);
        return socket;
      },
      rawWriter: () => true,
      onSubscriptions: (info) => handed.push(info),
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
      onDiagnostic: () => {},
    });

    structure.start();
    sockets[0].onopen?.();
    const deliver = (body) => sockets[0].onmessage?.({ data: JSON.stringify(body) });

    // One acknowledged subscription carries nested data, so the copy is tested at depth too.
    deliver({ kind: 'subscription', key: 'a', ok: true, detail: { code: 'ok' } });
    assert.equal(structure.connection.subscriptionState, 'acknowledged', 'a is acknowledged');

    const read = structure.connection.subscriptions;
    read.get('a').state = 'failed';
    read.get('a').detail.code = 'edited';
    assert.equal(structure.connection.subscriptions.get('a').state, 'acknowledged', 'the connection still has a acknowledged');
    assert.equal(structure.connection.subscriptions.get('a').detail.code, 'ok', 'and its nested data is its own');

    // The reviewer s sequence: edit what was handed over, then let b succeed. The state the connection
    // computes must come from its own records, not from the copy a caller holds.
    deliver({ kind: 'subscription', key: 'b', ok: true });
    assert.equal(structure.connection.subscriptionState, 'acknowledged', 'b s success does not read a as failed');
    assert.equal(structure.connection.subscriptions.get('a').state, 'acknowledged', 'and a is still acknowledged');

    // The map a caller's hook is handed follows the same rule, at depth too.
    const fromHook = handed.at(-1).subscriptions;
    fromHook.get('b').state = 'failed';
    fromHook.get('a').detail.code = 'edited';
    deliver({ kind: 'subscription', key: 'c', ok: true });
    assert.equal(structure.connection.subscriptions.get('b').state, 'acknowledged', 'a hook that edits its copy changes nothing');
    assert.equal(structure.connection.subscriptions.get('a').detail.code, 'ok', 'nor anything nested in it');
    assert.equal(structure.connection.subscriptionState, 'acknowledged', 'nor the state the connection computes');
    structure.stop();
    store.close();
  });
});

test('the structure creates its store from a path, holds the file, and closes it', async () => {
  await withStore(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-1',
      venue: 'kraken',
      path,
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: () => [],
      },
      webSocketImpl: function fakeSocket(url) {
        return { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
      },
      rawWriter: () => true,
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
      onDiagnostic: () => {},
    });

    assert.throws(
      () => openDurability({ path, runId: 'run-2' }),
      (error) => error.code === 'REENTRANT_OPERATION',
      'the file belongs to the structure while it is open',
    );
    structure.close();
    // Free again, and the store the structure wrote is the one that was there.
    const reopened = openDurability({ path, runId: 'run-3' });
    reopened.beginRun();
    assert.equal(
      internalsOf(reopened).db.prepare('SELECT COUNT(*) AS n FROM run_marker').get().n >= 1,
      true,
      'the store it wrote survived the close',
    );
    reopened.close();
  });
});

test('a store serves one structure per board', async () => {
  await withStore(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const store = openDurability({ path, runId: 'run-1' });
    store.beginRun();
    const first = build(store, {});
    // The same board again - and a wrapper that names the same store - is refused: a second structure
    // would share the board's position and outlive the first close.
    assert.throws(
      () => build(store, {}),
      (error) => error.code === 'REENTRANT_OPERATION',
      'the board belongs to the structure that has it',
    );
    assert.throws(
      () => build(withInjectableWrites(store).durability, {}),
      (error) => error.code === 'REENTRANT_OPERATION',
      'and a wrapper of the store is the same store',
    );
    // A different board is a separate page of the same store: one market's boards share one store, and
    // each board is taken on its own.
    const other = createStructure({
      market: 'kraken_spot',
      stream: 'book',
      runId: 'run-1',
      venue: 'kraken',
      adapter: { url: 'ws://venue.test/ws', stream: 'book', parse: () => ({ kind: 'data' }) },
      durability: store,
      spoolDir: null,
      webSocketImpl: function unused() {
        throw new Error('this test opens no socket');
      },
      rawWriter: () => true,
    });
    other.close();
    // Closing one board's structure gives that board back - not the store, and not another board.
    assert.throws(
      () => build(store, {}),
      (error) => error.code === 'REENTRANT_OPERATION',
      'the first board is still held after the other board closes',
    );

    // The first structure keeps working while the other board was there.
    assert.equal(
      first.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 }).accepted,
      true,
      'the structure that owns its board keeps working',
    );

    // Closing gives the board back and leaves the store open - a store a caller handed in is the
    // caller's to end - so the same board can be taken again from the same store.
    first.close();
    const again = build(store, {});
    again.close();

    // Given a path instead, the structure opens the file itself: a second owner of that file is refused,
    // and its own close is what frees the file for the construction that follows (a restart reopens it).
    store.close();
    const owned = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-1',
      venue: 'kraken',
      path,
      adapter: { url: 'ws://venue.test/ws', stream: 'trades', parse: () => ({ kind: 'data' }) },
      spoolDir: null,
      webSocketImpl: function unused() {
        throw new Error('this test opens no socket');
      },
      rawWriter: () => true,
    });
    assert.throws(
      () =>
        createStructure({
          market: 'kraken_spot',
          stream: 'trades',
          runId: 'run-1',
          venue: 'kraken',
          path,
          adapter: { url: 'ws://venue.test/ws', stream: 'trades', parse: () => ({ kind: 'data' }) },
          spoolDir: null,
          webSocketImpl: function unused() {
            throw new Error('this test opens no socket');
          },
          rawWriter: () => true,
        }),
      (error) => error.code === 'REENTRANT_OPERATION',
      'the file the structure opened is held',
    );
    owned.close();
    const nextStore = openDurability({ path, runId: 'run-2' });
    const next = build(nextStore, {});
    next.close();
    nextStore.close();
  });
});

test('a structure that fails to construct leaves no store behind', async () => {
  await withStore(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const base = {
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-1',
      venue: 'kraken',
      path,
      webSocketImpl: function unused() {
        throw new Error('this test opens no socket');
      },
    };
    const adapter = { url: 'ws://venue.test/ws', stream: 'trades', parse: () => ({ kind: 'data' }) };

    // The check that can refuse the configuration runs before anything is opened: no file appears. A
    // raw writer is optional now, but a value that is neither absent nor a function is still refused.
    assert.throws(
      () => createStructure({ ...base, adapter, rawWriter: 'not-a-function' }),
      /raw writer/,
      'the configuration cannot construct',
    );
    assert.equal(existsSync(path), false, 'and nothing was opened for it');

    // A failure after the store is open takes the store down with it: this adapter cannot be built into a
    // connection, and the construction that follows is the proof that the file came free again.
    assert.throws(
      () =>
        createStructure({
          ...base,
          adapter: { url: 'ws://venue.test/ws', stream: 'trades' },
          rawWriter: () => true,
        }),
      /adapter that can parse/,
      'the construction fails after the store is open',
    );
    const structure = createStructure({ ...base, adapter, rawWriter: () => true });
    structure.close();

    // A store the caller handed in is the caller's: the failed attempt releases its claim and leaves the
    // store open, so the retry with a working configuration takes the same store - and the caller closes
    // it, as the caller also opened it.
    const store = openDurability({ path: join(dir, 'other.sqlite'), runId: 'run-1' });
    store.beginRun();
    assert.throws(
      () =>
        createStructure({
          ...base,
          path: null,
          durability: store,
          adapter: { url: 'ws://venue.test/ws', stream: 'trades' },
          rawWriter: () => true,
        }),
      /adapter that can parse/,
    );
    const retry = build(store, {});
    retry.close();
    store.close();
  });
});

test('nothing a caller supplies is handed the store, a part, or a private route', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    const seen = [];
    const record = (label, ...args) => seen.push({ label, args });
    // The hooks a caller supplies: what they receive is data, never the store, a part, or a route. The
    // connection's own observations are among them, because the socket is driven here too. The raw
    // writer takes the frame (`true`), so reception carries on and the socket really opens.
    const hooks = [
      ['rawWriter', (...args) => { record('rawWriter', ...args); return true; }],
      ['onAck', (...args) => record('onAck', ...args)],
      ['onGap', (...args) => record('onGap', ...args)],
      ['onStop', (...args) => record('onStop', ...args)],
      ['onDiagnostic', (...args) => record('onDiagnostic', ...args)],
      ['onState', (...args) => record('onState', ...args)],
      ['onSubscriptions', (...args) => record('onSubscriptions', ...args)],
    ];
    const sockets = [];
    const connectionId = 'run-1:kraken:kraken_spot:1';
    const second = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-1',
      venue: 'kraken',
      durability: store,
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        // A subscription on the socket reaches the observation hooks; a plain frame is data.
        parse: (raw) => {
          const parsed = JSON.parse(raw);
          return parsed.kind ? parsed : { kind: 'data' };
        },
        changesFor: () => [],
      },
      webSocketImpl: function fakeSocket(url) {
        const socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
        sockets.push(socket);
        return socket;
      },
      rawWriter: hooks[0][1],
      onAck: hooks[1][1],
      onGap: hooks[2][1],
      onStop: hooks[3][1],
      onDiagnostic: hooks[4][1],
      onState: hooks[5][1],
      onSubscriptions: hooks[6][1],
    });
    // The connection is named before reception starts, so the announcement start() makes is the
    // connection the board already holds - and the socket it opens is driven here.
    second.accept(connectionId, { runId: 'run-1', generation: 1, firstSeq: 1 });
    second.feed(envelope(1, { connectionId }));
    second.start();
    sockets[0].onopen?.();
    sockets[0].onmessage?.({ data: JSON.stringify({ kind: 'subscription', key: 'a', ok: true, detail: { code: 'ok' } }) });

    assert.ok(seen.length > 0, 'the hooks were called');

    // Nothing reachable from an argument - at any depth, not only the top level - is the store, a part,
    // a view, a private route, or a function: a nested object carries whatever it closes over.
    const internals = [internalsOf(second), internalsOf(store)];
    const forbidden = new Set([
      store,
      second,
      second.book,
      second.organizer,
      second.ledger,
      second.connection,
      second.spool,
      ...internals.flatMap((part) => Object.values(part)),
    ]);
    const reachable = new Set();
    const functions = [];
    const visit = (value, path) => {
      if (value === null || (typeof value !== 'object' && typeof value !== 'function')) return;
      if (reachable.has(value)) return;
      reachable.add(value);
      assert.equal(forbidden.has(value), false, `${path} reaches the store, a part, or a private route`);
      if (typeof value === 'function') {
        functions.push({ path, fn: value });
        return; // a function's own properties are the caller's business, not this check's
      }
      if (value instanceof Map) {
        for (const [key, nested] of value) {
          visit(key, `${path}.key`);
          visit(nested, `${path}[${String(key)}]`);
        }
        return;
      }
      if (value instanceof Set) {
        for (const nested of value) visit(nested, `${path}[]`);
        return;
      }
      if (Buffer.isBuffer(value)) return; // the frame's own bytes
      for (const key of Object.keys(value)) visit(value[key], `${path}.${key}`);
    };
    for (const { label, args } of seen) {
      for (const [index, argument] of args.entries()) visit(argument, `${label}[${index}]`);
    }

    // A function inside an argument is a capability the caller can call: calling it must run no internal
    // operation. Both halves matter - the state a call changed, and the contract that, together with the
    // walk above, none is handed over at all.
    const before = JSON.stringify({ stats: second.stats, boundary: second.book.appliedBoundary });
    for (const { path, fn } of functions) {
      try {
        fn();
      } catch {
        // A function that wants its own arguments before it does anything has performed nothing here.
      }
      assert.equal(
        JSON.stringify({ stats: second.stats, boundary: second.book.appliedBoundary }),
        before,
        `calling ${path} ran an internal operation`,
      );
    }
    assert.deepEqual(functions.map(({ path }) => path), [], 'no function is reachable from what a caller is handed');
    // The checks run while the structure is still alive (its stats are read above), and ended after them.
    second.close();
    store.close();
  });
});

test('a frame that waited on the replaced socket is dropped, not stamped as the new generation', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    const written = [];
    const timers = [];
    const sockets = [];
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-1',
      venue: 'kraken',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: (frame) => [{ side: 'bid', price: 100, size: JSON.parse(frame.raw.toString('utf8')).size }],
      },
      durability: store,
      silenceDeadlineMs: 1_000,
      setTimer: (fn, ms) => {
        const timer = { fn, ms, unref() {} };
        timers.push(timer);
        return timer;
      },
      clearTimer: (timer) => {
        timer.cleared = true;
      },
      webSocketImpl: function fakeSocket(url) {
        const socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
        sockets.push(socket);
        return socket;
      },
      rawWriter: (frame) => {
        written.push(`${frame.generation}:${frame.receive_seq}`);
        if (written.length === 1) {
          // The silence deadline passes while this frame is being written, and then B arrives on the same
          // socket - both wait their turn, and the silence replaces the socket before B runs. B belongs to
          // the old connection and must not be stamped as the new generation's first frame.
          const silence = timers.find((timer) => timer.ms === 1_000 && !timer.cleared);
          silence.cleared = true;
          silence.fn();
          sockets[0].onmessage?.({ data: JSON.stringify({ seq: 2, size: 2 }) });
        }
        return true;
      },
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
      onDiagnostic: () => {},
    });

    structure.start();
    sockets[0].onopen?.();
    sockets[0].onmessage?.({ data: JSON.stringify({ seq: 1, size: 1 }) });

    assert.equal(structure.connection.generation, 2, 'the silence replaced the socket');
    assert.deepEqual(written, ['1:1'], 'the frame that waited from the abandoned socket never reached the raw');
    assert.equal(structure.connection.receiveSeq, 0, 'nothing was numbered under the new generation');
    assert.equal(structure.book.appliedBoundary.upToSeq, null, 'and nothing was applied under it');
    const closeOutcome = store.close();
    assert.equal(closeOutcome, undefined, 'the store closed (a refusal comes back as a value, not a throw)');
  });
});

test('an arrival kept when a hook threw runs before the next arrival, not after it', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    const written = [];
    const sockets = [];
    let fired = false;
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-1',
      venue: 'kraken',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: (frame) => [{ side: 'bid', price: 100, size: JSON.parse(frame.raw.toString('utf8')).size }],
      },
      durability: store,
      webSocketImpl: function fakeSocket(url) {
        const socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
        sockets.push(socket);
        return socket;
      },
      rawWriter: (frame) => {
        written.push(JSON.parse(frame.raw.toString('utf8')).seq);
        return true;
      },
      onState: ({ state }) => {
        if (state === 'subscribing' && !fired) {
          fired = true;
          // B arrives from inside the open processing, and then this hook throws. The exception is the
          // caller's to see, and B was accepted: it keeps its place ahead of whatever arrives next.
          sockets[0].onmessage?.({ data: JSON.stringify({ seq: 2, size: 2 }) });
          throw new Error('hook failure');
        }
      },
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
      onDiagnostic: () => {},
    });

    structure.start();
    assert.throws(() => sockets[0].onopen?.(), /hook failure/, 'the exception reached the caller');
    assert.deepEqual(written, [], 'the arrival that was kept did not run in the middle of the failure');
    sockets[0].onmessage?.({ data: JSON.stringify({ seq: 3, size: 3 }) });

    assert.deepEqual(written, [2, 3], 'the kept arrival ran first, and the later one after it');
    assert.equal(structure.stats.stopped, false, 'nothing here is a stop');
    store.close();
  });
});

test('a wait-list that overflows closes the socket and stops outside the frame, once', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    const written = [];
    const stops = [];
    const sockets = [];
    let inHook = false;
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-1',
      venue: 'kraken',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: (frame) => [{ side: 'bid', price: 100, size: JSON.parse(frame.raw.toString('utf8')).size }],
      },
      durability: store,
      maxWaitingEvents: 1,
      webSocketImpl: function fakeSocket(url) {
        const socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() { this.closes = (this.closes ?? 0) + 1; } };
        sockets.push(socket);
        return socket;
      },
      rawWriter: (frame) => {
        written.push(frame.receive_seq);
        if (written.length === 1) {
          inHook = true;
          // One slot is the whole wait-list: B fills it, C overflows it.
          sockets[0].onmessage?.({ data: JSON.stringify({ seq: 2, size: 2 }) });
          sockets[0].onmessage?.({ data: JSON.stringify({ seq: 3, size: 3 }) });
          inHook = false;
        }
        return true;
      },
      onAck: () => {},
      onGap: () => {},
      onStop: (stop) => stops.push({ ...stop, duringHook: inHook, closes: sockets[0].closes ?? 0 }),
      onDiagnostic: () => {},
    });

    structure.start();
    sockets[0].onopen?.();
    sockets[0].onmessage?.({ data: JSON.stringify({ seq: 1, size: 1 }) });

    assert.equal(stops.length, 1, 'the structure stopped exactly once');
    assert.match(String(stops[0].reason), /wait-list/, 'and it says which wait-list filled up');
    assert.match(String(stops[0].reason), /message/, 'and which arrival it was');
    assert.equal(stops[0].duringHook, false, 'the stop was carried out after the frame, not from inside a hook');
    assert.equal(stops[0].closes, 1, 'and the socket was closed by it');
    assert.equal(structure.connection.state, 'stopped', 'reception really is stopped, not only recorded as stopped');
    assert.equal(structure.stats.stopped, true);
    assert.deepEqual(written, [1, 2], 'the arrival that fit the wait-list ran; the overflowed one was the dropped one');
    store.close();
  });
});

test('the stop a full wait-list owes runs inside the gate, so its own hook is refused', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    const written = [];
    const sockets = [];
    let attempted;
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-1',
      venue: 'kraken',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: (frame) => [{ side: 'bid', price: 100, size: JSON.parse(frame.raw.toString('utf8')).size }],
      },
      durability: store,
      maxWaitingEvents: 1,
      webSocketImpl: function fakeSocket(url) {
        const socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() { this.closes = (this.closes ?? 0) + 1; } };
        sockets.push(socket);
        return socket;
      },
      rawWriter: (frame) => {
        written.push(frame.receive_seq);
        if (written.length === 1) {
          // One slot is the whole wait-list: B fills it, C overflows it, and the stop it owes is carried
          // out when the frame has ended.
          sockets[0].onmessage?.({ data: JSON.stringify({ seq: 2, size: 2 }) });
          sockets[0].onmessage?.({ data: JSON.stringify({ seq: 3, size: 3 }) });
        }
        return true;
      },
      onAck: () => {},
      onGap: () => {},
      onStop: () => {
        // The stop's own hook is a caller's hook like any other: a change operation attempted from it runs
        // while the stop is being carried out, inside the executor, and is refused there.
        attempted = structure.accept('run-1:kraken:kraken_spot:2', { runId: 'run-1', generation: 2 });
      },
      onDiagnostic: () => {},
    });

    structure.start();
    sockets[0].onopen?.();
    sockets[0].onmessage?.({ data: JSON.stringify({ seq: 1, size: 1 }) });

    assert.equal(structure.stats.stopped, true, 'the wait-list overflow stopped reception');
    assert.ok(attempted, 'the stop hook ran');
    assert.equal(attempted.accepted, false, 'and the connection it tried to admit was refused');
    assert.equal(attempted.code, 'REENTRANT_OPERATION', 'as a re-entrant change operation');
    assert.equal(
      structure.book.appliedBoundary.generation,
      1,
      'so the generation it tried to admit never reached the board',
    );
    structure.close();
    store.close();
  });
});

test('a frame that arrived while a failed task waits is still kept, and runs in its turn', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    const written = [];
    const sockets = [];
    let fired = false;
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-1',
      venue: 'kraken',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: (frame) => [{ side: 'bid', price: 100, size: JSON.parse(frame.raw.toString('utf8')).size }],
      },
      durability: store,
      webSocketImpl: function fakeSocket(url) {
        const socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
        sockets.push(socket);
        return socket;
      },
      rawWriter: (frame) => {
        written.push(JSON.parse(frame.raw.toString('utf8')).size);
        return true;
      },
      onState: ({ state }) => {
        if (state === 'subscribing' && !fired) {
          fired = true;
          // A failure event arrives from inside the open processing, and then this hook throws: the event
          // is kept for its turn, ahead of whatever arrives next.
          sockets[0].onerror?.(new Error('socket failure'));
          throw new Error('open failure');
        }
      },
      onFailure: () => {
        throw new Error('the kept failure');
      },
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
      onDiagnostic: () => {},
    });

    structure.start();
    assert.throws(() => sockets[0].onopen?.(), /open failure/, 'the exception reached the caller');
    assert.deepEqual(written, [], 'nothing ran while the failure was in the air');
    assert.throws(
      () => sockets[0].onmessage?.({ data: JSON.stringify({ seq: 3, size: 3 }) }),
      /the kept failure/,
      'the kept failure event runs first, and its exception reaches this caller',
    );
    assert.deepEqual(written, [], 'and the frame that arrived in the same turn is kept, not lost');
    sockets[0].onmessage?.({ data: JSON.stringify({ seq: 4, size: 4 }) });
    assert.deepEqual(written, [3, 4], 'the kept frame ran in its turn, before the later one');
    store.close();
  });
});

test('a closed structure writes nothing, and its close is once', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    const written = [];
    const first = build(store, { written });
    first.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    first.close();

    // The board is free again, and the structure that closed is not a writer any more: the same frame,
    // fed into it, is refused and never reaches the raw.
    const second = build(store, { written });
    second.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    assert.equal(second.feed(envelope(1)).applied, true, 'the new structure took the board');
    assert.deepEqual(first.feed(envelope(1)), { accepted: false, reason: 'this structure is closed' });
    assert.deepEqual(first.accept('conn-9', { runId: 'run-1', generation: 9 }), {
      accepted: false,
      reason: 'this structure is closed',
    });
    assert.deepEqual(first.start(), { started: false, reason: 'this structure is closed' });
    assert.deepEqual(first.resume(), { delivered: 0, refused: true, reason: 'this structure is closed' });
    assert.deepEqual(first.redeliverPending(), { applied: 0, refused: true, reason: 'this structure is closed' });
    assert.deepEqual(written, ['conn-1:1'], 'nothing the closed structure was asked to do reached the raw');
    assert.equal(second.stats.applied, 1, 'and the board it no longer holds is untouched by it');

    // A close that runs again is a no-op: the board given back by the first close now belongs to the
    // second structure, and neither a repeated close nor a release that does not hold the claim may free it.
    first.close();
    assert.throws(
      () => build(store, {}),
      (error) => error.code === 'REENTRANT_OPERATION',
      'the second structure kept the board through the first one s repeated close',
    );
    internalsOf(store).releaseStructureOwner({ market: 'kraken_spot', stream: 'trades' }, {});
    assert.throws(
      () => build(store, {}),
      (error) => error.code === 'REENTRANT_OPERATION',
      'and through a release that is not the claim s own',
    );
    second.close();
    store.close();
  });
});
