/**
 * Real-module proof for ruling ②: an uncompleted organizer→book handoff survives crashes.
 *
 * Unlike a schema replica, this drives createOrganizeProcess/createBookProcess against real SQLite files.
 * The book link can be cut at the same boundaries a process death cuts: after organize commits, after the
 * book commits but before its acknowledgement is accepted, and during the organizer's ACK transaction.
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { CHANGES_FORMAT } from '../src/changes.mjs';
import { IPC_VERSION, makeMessage } from '../src/ipc-message.mjs';
import { createBookProcess } from '../src/book/main.mjs';
import { createOrganizeProcess } from '../src/organize/main.mjs';
import { openOrganizeStore } from '../src/organize/store.mjs';

const MARKET = 'kraken_spot';
const STREAM = 'trades';
const RUN1 = 'run-1';
const RUN2 = 'run-2';
const CID = `${RUN1}:kraken:${MARKET}:1`;
const CRASHABLE_ORGANIZER = fileURLToPath(new URL('../test-support/organize-crash-role.mjs', import.meta.url));

function frame(seq) {
  return makeEnvelope({
    market: MARKET,
    stream: STREAM,
    connectionId: CID,
    runId: RUN1,
    venue: 'kraken',
    generation: 1,
    receiveSeq: seq,
    recvTsMs: 1_000 + seq,
    recvMonoNs: seq,
    raw: Buffer.from(`frame-${seq}`),
    meta: {
      first_seq: 1,
      changes_format: CHANGES_FORMAT,
      changes: { replace: false, changes: [{ side: 'bid', price: 100 + seq, size: seq }] },
    },
  });
}

function acceptRequest() {
  return makeMessage({
    version: IPC_VERSION,
    type: 'accept',
    role_instance: 'ingest-1',
    request_id: 'ingest-1:accept:run-1:kraken:kraken_spot:1:1',
    run_id: RUN1,
    market: MARKET,
    stream: STREAM,
    connection_id: CID,
    generation: 1,
    payload: { first_seq: 1, takeover: false },
  });
}

function makeWorld(dir, { failCommit = { armed: false } } = {}) {
  const organizerPath = join(dir, 'organize.sqlite');
  const bookPath = join(dir, 'book.sqlite');
  let organizer = null;
  let organizeStore = null;
  let book = null;
  let deliverToBook = false;
  const ackFailures = [];
  const bookLink = {
    sent: [],
    received: [],
    sendControl(message) {
      this.sent.push(message);
      if (organizer === null) return false;
      try {
        organizer.handleControl(message, bookLink);
        return true;
      } catch (error) {
        ackFailures.push(error);
        return false;
      }
    },
    sendEnvelope(envelope) {
      this.received.push(envelope);
      if (!deliverToBook || book === null) return true;
      book.handleEnvelope(envelope, bookLink);
      return true;
    },
    close() {},
  };
  const ingestLink = {
    controls: [],
    sendControl(message) {
      this.controls.push(message);
      return true;
    },
    sendEnvelope() {
      return true;
    },
    close() {},
  };

  function openOrganizer(runId, Database = DatabaseSync) {
    organizeStore = openOrganizeStore({
      path: organizerPath,
      runId,
      Database,
      nowMs: () => 10_000,
    });
    organizer = createOrganizeProcess({
      market: MARKET,
      stream: STREAM,
      runId,
      roleInstance: `organize-${runId}`,
      store: organizeStore,
      rawWriter: () => true,
      nowMs: () => 10_000,
    });
    return organizer;
  }

  function openBook(runId) {
    book = createBookProcess({
      market: MARKET,
      stream: STREAM,
      runId,
      roleInstance: `book-${runId}`,
      storePath: bookPath,
      organizeChannel: bookLink,
    });
    book.announceHello();
    return book;
  }

  function accept() {
    book.handleControl(acceptRequest(), bookLink);
  }

  function submit(seq) {
    return organizer.handleEnvelope(frame(seq), ingestLink);
  }

  function closeOrganizer() {
    organizer?.close();
    organizeStore?.close();
    organizer = null;
    organizeStore = null;
  }

  function closeBook() {
    book?.close();
    book = null;
  }

  return {
    organizerPath,
    bookPath,
    bookLink,
    ingestLink,
    ackFailures,
    get organizer() {
      return organizer;
    },
    openOrganizer,
    openBook,
    accept,
    submit,
    closeOrganizer,
    closeBook,
    setDeliverToBook(value) {
      deliverToBook = value;
    },
    currentBook: () => book,
  };
}

async function withDir(fn) {
  const dir = mkdtempSync(join(tmpdir(), 'pending-boundary-recovery-'));
  try {
    return await fn(dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

function seedUnappliedFrame(world) {
  world.openOrganizer(RUN1);
  world.openBook(RUN1);
  world.accept();
  world.setDeliverToBook(false); // the organizer commits; the peer dies before applying the sent frame
  world.submit(1);
  const status = world.organizer.recoveryStatus();
  assert.equal(status.resolved, false);
  assert.equal(status.owedCount, 1);
  assert.equal(world.organizer.watermarkRows()[0].upToSeq, 1);
  assert.deepEqual(world.organizer.ledgerEntries({ state: 'owed' }).map((entry) => entry.receiveSeq), [1]);
}

function nextChildMessage(child, expectedKind, { onOther = () => {}, timeoutMs = 5_000, stderr = () => '' } = {}) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const cleanup = () => {
      clearTimeout(timer);
      child.removeListener('message', onMessage);
      child.removeListener('exit', onExit);
    };
    const finish = (error, value) => {
      if (settled) return;
      settled = true;
      cleanup();
      if (error) reject(error);
      else resolve(value);
    };
    const onMessage = (message) => {
      try {
        onOther(message);
      } catch (error) {
        finish(error);
        return;
      }
      if (message?.kind === expectedKind) finish(null, message);
      else if (message?.kind === 'fatal') finish(new Error(message.reason));
    };
    const onExit = (code, signal) =>
      finish(new Error(`crash helper exited before ${expectedKind} (code=${code}, signal=${signal}); stderr=${stderr()}`));
    const timer = setTimeout(
      () => finish(new Error(`timed out waiting for ${expectedKind}; stderr=${stderr()}`)),
      timeoutMs,
    );
    child.on('message', onMessage);
    child.once('exit', onExit);
  });
}

async function exerciseKilledOrganizer({ applyBeforeCrash }) {
  await withDir(async (dir) => {
    const world = makeWorld(dir);
    let child = null;
    let exitPromise = null;
    let stderr = '';
    try {
      world.openBook(RUN1);
      world.accept(); // the book owner's accept is durable before organize can hand a frame to it
      child = fork(CRASHABLE_ORGANIZER, [world.organizerPath], {
        stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
      });
      child.stderr.on('data', (chunk) => {
        stderr += chunk.toString('utf8');
      });
      exitPromise = new Promise((resolve) => child.once('exit', (code, signal) => resolve({ code, signal })));
      await nextChildMessage(child, 'ready', { stderr: () => stderr });

      const seededPromise = nextChildMessage(child, 'seeded', {
        stderr: () => stderr,
        onOther: (message) => {
          if (message?.kind !== 'envelope' || !applyBeforeCrash) return;
          const applied = world.currentBook().handleEnvelope(message.envelope, {
            sendControl: () => false, // persist the book commit, but lose its ACK with the organizer still alive
            close() {},
          });
          assert.equal(applied.applied, true, 'the independent book process committed the envelope');
        },
      });
      child.send({ kind: 'seed' });
      const seeded = await seededPromise;
      assert.equal(seeded.status.owedCount, 1, 'the organizer committed an owed ledger row before the crash');
      assert.equal(seeded.watermarks[0].upToSeq, 1, 'the contiguous watermark committed with the ledger');
      if (applyBeforeCrash) {
        assert.equal(world.currentBook().appliedBoundary.upToSeq, 1, 'the book committed before organizer death');
      }

      child.kill('SIGKILL');
      const exit = await exitPromise;
      assert.equal(exit.signal, 'SIGKILL', 'the process really died, rather than closing its store normally');
      child = null;

      world.closeBook();
      world.openOrganizer(RUN2);
      world.openBook(RUN2);
      world.setDeliverToBook(true);
      world.organizer.resumeFromBoundary(world.currentBook().appliedBoundary);
      assert.equal(world.organizer.deliverOwed().delivered, 1);

      assert.equal(world.organizer.recoveryStatus().resolved, true);
      assert.deepEqual(world.organizer.ledgerEntries({ state: 'owed' }), []);
      assert.equal(world.currentBook().appliedBoundary.upToSeq, 1);
      assert.equal(world.currentBook().board.size('bid', 101), 1, 'replay never applies the frame twice');
    } finally {
      if (child !== null && child.exitCode === null && child.signalCode === null) {
        child.kill('SIGKILL');
        await exitPromise;
      }
      world.closeOrganizer();
      world.closeBook();
    }
  });
}

test('SIGKILL after organizer commit but before book apply recovers from the real role stores', async () => {
  await exerciseKilledOrganizer({ applyBeforeCrash: false });
});

test('SIGKILL after book commit but before applied_ack recovers by replaying a duplicate', async () => {
  await exerciseKilledOrganizer({ applyBeforeCrash: true });
});

test('a crash after the organizer commit replays the owed frame and clears it only after book ACK', () => {
  withDir((dir) => {
    const world = makeWorld(dir);
    try {
      seedUnappliedFrame(world);
      world.closeOrganizer();
      world.closeBook();

      world.openOrganizer(RUN2);
      world.openBook(RUN2);
      world.setDeliverToBook(true);
      world.organizer.resumeFromBoundary(world.currentBook().appliedBoundary);
      assert.equal(world.organizer.deliverOwed().delivered, 1);

      assert.equal(world.organizer.recoveryStatus().resolved, true);
      assert.equal(world.organizer.ledgerEntries({ state: 'owed' }).length, 0);
      assert.equal(world.organizer.watermarkRows()[0].upToSeq, 1);
      assert.equal(world.currentBook().board.size('bid', 101), 1);
    } finally {
      world.closeOrganizer();
      world.closeBook();
    }
  });
});

test('a crash after book commit but before organizer ACK replays as a duplicate and clears the ledger', () => {
  withDir((dir) => {
    const world = makeWorld(dir);
    try {
      seedUnappliedFrame(world);
      // The book's DB commit survives, but this channel cannot deliver its applied_ack to organize.
      world.currentBook().handleEnvelope(frame(1), { sendControl: () => false, close() {} });
      assert.equal(world.currentBook().appliedBoundary.upToSeq, 1);
      world.closeOrganizer();
      world.closeBook();

      world.openOrganizer(RUN2);
      world.openBook(RUN2);
      world.setDeliverToBook(true);
      world.organizer.resumeFromBoundary(world.currentBook().appliedBoundary);
      assert.equal(world.organizer.deliverOwed().delivered, 1);

      assert.equal(world.organizer.recoveryStatus().resolved, true);
      assert.equal(world.currentBook().appliedBoundary.upToSeq, 1);
      assert.equal(world.currentBook().board.size('bid', 101), 1, 'the duplicate did not apply the frame twice');
    } finally {
      world.closeOrganizer();
      world.closeBook();
    }
  });
});

test('a failed applied-ACK transaction leaves the boundary and ledger intact for restart recovery', () => {
  withDir((dir) => {
    const failCommit = { armed: false };
    const world = makeWorld(dir, { failCommit });
    class FailOneCommit extends DatabaseSync {
      exec(sql) {
        if (failCommit.armed && sql.trim().toUpperCase() === 'COMMIT') {
          failCommit.armed = false;
          throw new Error('injected crash before ACK commit');
        }
        return super.exec(sql);
      }
    }

    try {
      seedUnappliedFrame(world);
      world.closeOrganizer();
      world.closeBook();

      world.openOrganizer(RUN2, FailOneCommit);
      world.openBook(RUN2);
      world.setDeliverToBook(true);
      world.organizer.resumeFromBoundary(world.currentBook().appliedBoundary);
      failCommit.armed = true;
      world.organizer.deliverOwed();

      assert.match(world.ackFailures[0]?.message ?? '', /injected crash before ACK commit/);
      assert.equal(world.organizer.recoveryStatus().resolved, false);
      assert.deepEqual(world.organizer.ledgerEntries({ state: 'owed' }).map((entry) => entry.receiveSeq), [1]);
      assert.equal(world.currentBook().appliedBoundary.upToSeq, 1, 'the book commit is independent and durable');
      world.closeOrganizer();
      world.closeBook();

      world.openOrganizer('run-3');
      world.openBook('run-3');
      world.setDeliverToBook(true);
      world.organizer.resumeFromBoundary(world.currentBook().appliedBoundary);
      world.organizer.deliverOwed();

      assert.equal(world.organizer.recoveryStatus().resolved, true);
      assert.deepEqual(world.organizer.ledgerEntries({ state: 'owed' }), []);
      assert.equal(world.currentBook().board.size('bid', 101), 1);
    } finally {
      world.closeOrganizer();
      world.closeBook();
    }
  });
});

test('opening a legacy organizer store drops the redundant boundary table without losing the watermark or owed frame', () => {
  withDir((dir) => {
    const world = makeWorld(dir);
    try {
      seedUnappliedFrame(world);
      world.closeOrganizer();
      world.closeBook();

      const old = new DatabaseSync(world.organizerPath);
      old.exec(`CREATE TABLE pending_boundary (
        market TEXT NOT NULL,
        stream TEXT NOT NULL,
        boundary_seq INTEGER NOT NULL,
        boundary_ts_ms INTEGER NOT NULL,
        run_id TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL,
        state TEXT NOT NULL DEFAULT 'pending',
        PRIMARY KEY (market, stream)
      )`);
      old.prepare(`INSERT INTO pending_boundary
        (market, stream, boundary_seq, boundary_ts_ms, run_id, created_at_ms, state)
        VALUES (?, ?, ?, ?, ?, ?, 'pending')`).run(MARKET, STREAM, 1, 1_001, RUN1, 1_001);
      old.close();

      world.openOrganizer('run-3');
      const db = new DatabaseSync(world.organizerPath);
      try {
        assert.equal(
          db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'pending_boundary'").get(),
          undefined,
          'the old table is removed during store migration',
        );
        assert.equal(
          db.prepare('SELECT up_to_receive_seq FROM organized_watermark WHERE market = ? AND stream = ?').get(MARKET, STREAM)
            .up_to_receive_seq,
          1,
          'the durable watermark survives',
        );
        assert.equal(
          db.prepare("SELECT COUNT(*) AS n FROM delivery_ledger WHERE market = ? AND stream = ? AND state = 'owed'")
            .get(MARKET, STREAM).n,
          1,
          'the ledger retains the exact frame required for recovery',
        );
      } finally {
        db.close();
      }
    } finally {
      world.closeOrganizer();
      world.closeBook();
    }
  });
});

test('the ledger keeps an out-of-order frame visible after an earlier applied ACK releases its prefix', () => {
  withDir((dir) => {
    const world = makeWorld(dir);
    try {
      world.openOrganizer(RUN1);
      world.openBook(RUN1);
      world.accept();
      world.setDeliverToBook(false);
      world.submit(1);
      world.submit(3); // sequence 2 is missing, so the contiguous watermark stays at 1
      assert.equal(world.organizer.watermarkRows()[0].upToSeq, 1);
      assert.deepEqual(world.organizer.ledgerEntries({ state: 'owed' }).map((entry) => entry.receiveSeq), [1, 3]);

      world.currentBook().handleEnvelope(frame(1), world.bookLink);
      assert.equal(world.organizer.recoveryStatus().resolved, false, 'sequence 3 is still owed in the ledger');
      assert.deepEqual(world.organizer.ledgerEntries({ state: 'owed' }).map((entry) => entry.receiveSeq), [3]);
    } finally {
      world.closeOrganizer();
      world.closeBook();
    }
  });
});
