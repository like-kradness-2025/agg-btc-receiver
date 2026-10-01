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

function build(store, { written = [], onRawWrite = null, onAck = () => {}, onStop = () => {} } = {}) {
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
    webSocketImpl: function fakeSocket() {
      return { url: '', onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
    },
    rawWriter: (frame) => {
      written.push(`${frame.connection_id}:${frame.receive_seq}`);
      if (onRawWrite) onRawWrite(frame, holder.structure);
      return true;
    },
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
    assert.equal(store.db.prepare('SELECT first_seq FROM applied_boundary').get().first_seq, 1, 'the origin is the accepted one');
    assert.equal(store.db.prepare('SELECT COUNT(*) AS n FROM book_level').get().n, 1);
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
    assert.equal(store.db.prepare('SELECT COUNT(*) AS n FROM book_level').get().n, 1, 'the refused frame left nothing behind');
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
      first.db.prepare('SELECT state FROM run_marker WHERE run_id = ?').get('run-1').state,
      'running',
      'the store was not opened a second time',
    );
    assert.equal(first.db.prepare("SELECT COUNT(*) AS n FROM run_marker WHERE run_id = 'run-2'").get().n, 0);

    // And the file is free again once the handle is closed.
    first.close();
    const again = openDurability({ path, runId: 'run-3' });
    again.beginRun();
    assert.equal(again.db.prepare('SELECT COUNT(*) AS n FROM run_marker').get().n, 2);
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
    assert.equal(store.db.prepare('SELECT COUNT(*) AS n FROM book_level').get().n, 2);
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
    assert.equal(store.db.prepare('SELECT COUNT(*) AS n FROM delivery_ledger').get().n, 0, 'and none of them wrote');
    assert.equal(store.db.prepare('SELECT COUNT(*) AS n FROM book_level').get().n, 1);
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
      store.db.prepare("SELECT COUNT(*) AS n FROM sqlite_master WHERE name LIKE 'organized_watermark%' AND name != 'organized_watermark'").get().n,
      0,
      'nothing of the refused module was written',
    );
    assert.equal(store.db.prepare('SELECT COUNT(*) AS n FROM organized_watermark').get().n, 1, 'and this board has its own row');
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
      store.db.prepare('SELECT state FROM run_marker WHERE run_id = ?').get('run-1').state,
      'running',
      'the run that was live is not invalidated',
    );
    assert.equal(store.db.prepare("SELECT COUNT(*) AS n FROM run_marker WHERE run_id = 'run-2'").get().n, 0);
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
      store.db.prepare('SELECT state FROM run_marker WHERE run_id = ?').get('run-1').state,
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
      assert.equal(here.db.prepare('SELECT COUNT(*) AS n FROM run_marker').get().n, 1, 'the held file is untouched');
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
      assert.equal(here.db.prepare('SELECT COUNT(*) AS n FROM run_marker').get().n, 1, 'the held file is untouched');
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
    assert.equal(store.db.prepare('SELECT COUNT(*) AS n FROM run_marker').get().n, 1, 'only this run is written');
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
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const injectable = withInjectableWrites(store);
    const first = build(store, {});
    first.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    // A frame the raw may not hold: an intent, which is what a crash between the raw and its confirmation
    // leaves behind, and what the next structure has to offer back through the organizer.
    assert.equal(partsOf(first).ledger.record(envelope(1), 'about to be written', 'intent').recorded, true);

    const { openOrganizer } = await import('../src/organize/watermark.mjs');
    const other = openOrganizer({
      market: 'kraken_spot',
      stream: 'trades',
      durability: store,
      writeRaw: () => true,
    });
    const observed = [];
    build(store, {
      onRawWrite: () => {
        observed.push({ inChange: store.inChange() });
        // A public change operation, driven from inside the recovery: exactly what the construction-time
        // recovery not holding the store would let through.
        observed.push({ refusal: other.note(envelope(99), {})?.code ?? null });
      },
    });

    assert.ok(observed.length >= 2, 'the recovery re-offered the intent through the raw writer');
    assert.equal(observed[0].inChange, true, 'the recovery holds the store while it runs');
    assert.equal(observed[1].refusal, 'REENTRANT_OPERATION', 'so no change operation can start from inside it');
    store.close();
  });
});

test('an open that fails frees the file only if its handle really closed', async () => {
  await withStore(async (dir) => {
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

test('a caller s transaction cannot begin while a change operation is being processed', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    const injectable = withInjectableWrites(store);
    const attempts = [];
    const stops = [];
    let armed = true;
    let structure = null;
    injectable.onWrite((sql) => {
      if (!armed || !LEDGER_CONFIRM_WRITE.test(sql)) return;
      armed = false;
      try {
        store.inTransaction(() => structure.feed(envelope(2)));
        attempts.push({ refused: false });
      } catch (error) {
        attempts.push({ refused: true, code: error.code });
      }
    });
    structure = build(injectable.durability, { written: [], onStop: () => stops.push(1) });
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    const first = structure.feed(envelope(1));

    assert.equal(attempts.length, 1, 'the transaction was attempted from inside the frame s write');
    assert.equal(attempts[0].refused, true, 'and refused');
    assert.equal(attempts[0].code, 'REENTRANT_OPERATION', 'by us, not by SQLite');
    assert.equal(stops.length, 0, 'so no nested BEGIN was attempted and nothing stopped');
    assert.equal(first.durable, true, 'the frame that was being processed completed');
    assert.equal(first.applied, true, 'and reached the board');
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
    store.close();
  });
});

test('the views a caller is handed cannot change what the structure is doing', async () => {
  await withStore(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    store.beginRun();
    const written = [];
    const structure = build(store, { written });
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
      reopened.db.prepare('SELECT COUNT(*) AS n FROM run_marker').get().n >= 1,
      true,
      'the store it wrote survived the close',
    );
    reopened.close();
  });
});
