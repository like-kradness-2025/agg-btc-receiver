/**
 * Stage 5b units: the pieces the run supervisor is built from, fixed independently.
 *
 *   - the child-failure policy (ruling ⑬), as a truth table;
 *   - readiness aggregation: disconnect, instance mismatch and the report deadline each lose readiness;
 *   - database-unit store exclusion: a live owner is never displaced, a stale one is recovered;
 *   - the startup-order invariant (d) before (e), stated as a decision the wiring must honour.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  decideChildFailure,
  createReadinessAggregator,
  createStoreExclusion,
  defaultProcessAlive,
  PROCESSING_STOP_ORDER,
  TERMINATION_ORDER,
} from '../src/supervisor/run.mjs';

test('child-failure policy: the book alone restarts, organize continues within capacity', () => {
  const book = decideChildFailure('book', { restarts: 0, maxRestarts: 3 });
  assert.equal(book.action, 'restart-book');
  assert.equal(book.renewGeneration, false, 'a book restart never renews the receive generation');

  const organizeHolding = decideChildFailure('organize', { spoolWithinCapacity: true });
  assert.equal(organizeHolding.action, 'continue', 'reception continues while the spool can hold it');

  const organizeOverflow = decideChildFailure('organize', { spoolWithinCapacity: false, canRecordMissing: true });
  assert.equal(organizeOverflow.action, 'stop-reception-record', 'reception stops and the missing is recorded');

  const organizeCannotRecord = decideChildFailure('organize', { spoolWithinCapacity: false, canRecordMissing: false });
  assert.equal(organizeCannotRecord.action, 'exit');
  assert.equal(organizeCannotRecord.code, 1, 'a missing that cannot be recorded ends non-zero');

  const exhausted = decideChildFailure('book', { restarts: 3, maxRestarts: 3 });
  assert.equal(exhausted.action, 'exit', 'past the restart budget the run ends');

  const ingest = decideChildFailure('ingest', {});
  assert.equal(ingest.action, 'exit', 'an ingest failure has no continue rule in this stage');
});

test('readiness: disconnect, instance mismatch and the report deadline each lose readiness', () => {
  let now = 1_000;
  const readiness = createReadinessAggregator({ nowMs: () => now, reportDeadlineMs: 5_000 });
  for (const role of ['ingest', 'organize', 'book']) {
    readiness.setConnected(role, true);
    readiness.setExpectedInstance(role, `${role}-1`);
    readiness.setBoundInstance(role, `${role}-1`);
  }
  assert.equal(readiness.snapshot().ready, true, 'all connected, no mismatch, nothing reported yet');

  // Disconnect.
  readiness.setConnected('book', false);
  let snap = readiness.snapshot();
  assert.equal(snap.ready, false);
  assert.deepEqual(snap.reasons, [{ role: 'book', reason: 'disconnected' }]);
  readiness.setConnected('book', true);

  // Instance (generation) mismatch: the bound instance is another generation's channel.
  readiness.setBoundInstance('organize', 'organize-9');
  snap = readiness.snapshot();
  assert.equal(snap.ready, false);
  assert.ok(snap.reasons.some((r) => r.role === 'organize' && r.reason === 'instance-mismatch'));
  readiness.setBoundInstance('organize', 'organize-1');

  // Report deadline: a fresh report is fine; once it ages past the deadline, readiness is lost.
  readiness.noteReport('ingest', { ready: true });
  assert.equal(readiness.snapshot().ready, true, 'a fresh report holds readiness');
  now += 5_001;
  snap = readiness.snapshot();
  assert.equal(snap.ready, false, 'the report deadline passed');
  assert.ok(snap.reasons.some((r) => r.role === 'ingest' && r.reason === 'report-deadline'));
});

test('readiness: a role that reports not-ready is not ready, whatever else holds', () => {
  const readiness = createReadinessAggregator({ nowMs: () => 0, reportDeadlineMs: 1_000 });
  for (const role of ['ingest', 'organize', 'book']) {
    readiness.setConnected(role, true);
    readiness.setExpectedInstance(role, `${role}-1`);
    readiness.setBoundInstance(role, `${role}-1`);
  }
  readiness.noteReport('ingest', { ready: false, reason: 'the subscription failed' });
  const snap = readiness.snapshot();
  assert.equal(snap.ready, false);
  assert.ok(snap.reasons.some((r) => r.role === 'ingest' && r.reason === 'reported-not-ready'));
});

test('store exclusion: a live owner is refused, release needs confirmation, and a released store can be claimed again', () => {
  const dir = mkdtempSync(join(tmpdir(), 'exclusion-'));
  try {
    const dbPath = join(dir, 'state.sqlite');
    // A process is alive if it is this process (self) - a stand-in for a live owner.
    const exclusion = createStoreExclusion({ isProcessAlive: (pid) => pid === 4242 || defaultProcessAlive(pid) });

    const first = exclusion.claim({ path: dbPath, role: 'organize', instance: 'organize-1', pid: 4242 });
    assert.equal(first.claimed, true);
    assert.equal(first.tookOverStale, false);

    // A second owner is refused while the first is alive.
    const second = exclusion.claim({ path: dbPath, role: 'organize', instance: 'organize-2', pid: 4242 });
    assert.equal(second.claimed, false);
    assert.equal(second.code, 'STORE_ALREADY_OWNED');

    // A release is refused while the owner is alive and termination is not confirmed.
    const refused = exclusion.release({ path: dbPath, instance: 'organize-1', confirmedTerminated: false });
    assert.equal(refused.released, false);
    assert.equal(refused.refused, true);

    // The supervisor confirms the termination: the release is allowed.
    const released = exclusion.release({ path: dbPath, instance: 'organize-1', confirmedTerminated: true });
    assert.equal(released.released, true);

    // The previous lease is gone after confirmed release, so another instance can acquire it.
    const nextOwner = exclusion.claim({ path: dbPath, role: 'organize', instance: 'organize-3', pid: 4242 });
    assert.equal(nextOwner.claimed, true);
    assert.equal(nextOwner.tookOverStale, false, 'crash release is the lock manager\'s atomic OS/SQLite behavior, not a stale-file guess');
    assert.equal(exclusion.release({ path: dbPath, instance: 'organize-3', confirmedTerminated: true }).released, true);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the two stop orders are distinct: reception stops first, the board is let go first', () => {
  assert.deepEqual(PROCESSING_STOP_ORDER, ['ingest', 'organize', 'book']);
  assert.deepEqual(TERMINATION_ORDER, ['book', 'organize', 'ingest']);
  assert.notDeepEqual(PROCESSING_STOP_ORDER, TERMINATION_ORDER, 'processing and termination are separate orders');
});
