import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

import { makeEnvelope } from '../src/envelope.mjs';
import { IPC_VERSION, makeMessage } from '../src/ipc-message.mjs';
import { createOrganizeProcess } from '../src/organize/main.mjs';
import { openOrganizeStore } from '../src/organize/store.mjs';
import * as organizeStoreModule from '../src/organize/store.mjs';

const RUN = 'run-finalize-1';
const MARKET = 'kraken_spot';
const STREAM = 'trades';
const CONNECTION = `${RUN}:kraken:${MARKET}:1`;
const CONNECTION_2 = `${RUN}:kraken:${MARKET}:2`;

function memoryChannel() {
  return {
    sendControl() {
      return true;
    },
    sendEnvelope() {
      return true;
    },
  };
}

function acceptedMessage(connectionId = CONNECTION) {
  return makeMessage({
    version: IPC_VERSION,
    type: 'accepted',
    role_instance: 'book-1',
    request_id: `accept:${connectionId}`,
    run_id: RUN,
    market: MARKET,
    stream: STREAM,
    connection_id: connectionId,
    generation: 1,
    payload: { accepted: true, first_seq: 1 },
  });
}

function envelope(seq, connectionId = CONNECTION) {
  return makeEnvelope({
    market: MARKET,
    stream: STREAM,
    connectionId,
    runId: RUN,
    venue: 'kraken',
    generation: 1,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000_000 + seq,
    raw: `{"seq":${seq}}`,
  });
}

function tailSealed({ spoolEmpty, tails = [{ connectionId: CONNECTION, lastReceivedSeq: 1 }] }) {
  return makeMessage({
    version: IPC_VERSION,
    type: 'tail_sealed',
    role_instance: 'ingest-1',
    run_id: RUN,
    payload: {
      tails,
      spool_empty: spoolEmpty,
    },
  });
}

const FINALIZE_REQUEST = {
  finalizeRequestId: 'finalize-1',
  barrierId: 'barrier-1',
  tails: [{ connectionId: CONNECTION, lastReceivedSeq: 1 }],
  bookStop: { requestId: 'book-stop-1', roleInstance: 'book-1', stopped: true },
};

test('a prepared finalization commits its request proof only after organize reaches all sealed tails', () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-finalize-'));
  const storePath = join(dir, 'organize.sqlite');
  let organizer;
  let db;
  try {
    organizer = createOrganizeProcess({ frameBatchMs: 0, frameBatchMax: 1, 
      market: MARKET,
      stream: STREAM,
      runId: RUN,
      roleInstance: 'organize-1',
      storePath,
      rawWriter: () => true,
    });
    const channel = memoryChannel();
    organizer.handleControl(acceptedMessage(), channel);
    organizer.handleEnvelope(envelope(1), channel);
    organizer.handleControl(tailSealed({ spoolEmpty: false }), channel);

    assert.equal(organizer.allAcked, false, 'the outstanding spool prevents completion');
    assert.equal(organizer.prepareFinalize(FINALIZE_REQUEST).prepared, true);
    assert.equal(organizer.finalize(FINALIZE_REQUEST).completed, false);
    assert.equal(organizer.runMarkerState(), 'running', 'an unproven or premature finish stays running');

    organizer.handleControl(tailSealed({ spoolEmpty: true }), channel);
    assert.equal(organizer.allAcked, true);
    assert.equal(organizer.finalize().completed, false, 'all-ACK alone cannot write an unproved completion');
    assert.equal(organizer.runMarkerState(), 'running');
    assert.equal(organizer.finalize(FINALIZE_REQUEST).completed, true);
    organizer.close();
    organizer = null;

    db = new DatabaseSync(storePath);
    const marker = db
      .prepare(
        `SELECT state, at_ms, finalize_request_id, finalize_request_payload, finalize_receipt_json
           FROM run_marker WHERE run_id = ?`,
      )
      .get(RUN);
    assert.equal(marker.state, 'complete');
    assert.equal(marker.finalize_request_id, FINALIZE_REQUEST.finalizeRequestId);
    assert.ok(marker.finalize_request_payload, 'the immutable prepared payload remains with the run');
    assert.ok(marker.finalize_receipt_json, 'the completion receipt is committed with the complete marker');
    const receipt = JSON.parse(marker.finalize_receipt_json);
    assert.equal(receipt.runId, RUN);
    assert.equal(receipt.finalizeRequestId, FINALIZE_REQUEST.finalizeRequestId);
    assert.equal(receipt.barrierId, FINALIZE_REQUEST.barrierId);
    assert.deepEqual(receipt.tails, FINALIZE_REQUEST.tails);
    assert.deepEqual(receipt.bookStop, FINALIZE_REQUEST.bookStop);
    assert.equal(receipt.completedAtMs, marker.at_ms);
  } finally {
    db?.close();
    organizer?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a later sealed all-ACK tail cannot authorize an older prepared request', () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-finalize-stale-barrier-'));
  const storePath = join(dir, 'organize.sqlite');
  let organizer;
  let db;
  try {
    organizer = createOrganizeProcess({ frameBatchMs: 0, frameBatchMax: 1, 
      market: MARKET,
      stream: STREAM,
      runId: RUN,
      roleInstance: 'organize-1',
      storePath,
      rawWriter: () => true,
    });
    const channel = memoryChannel();
    organizer.handleControl(acceptedMessage(), channel);
    organizer.handleEnvelope(envelope(1), channel);
    organizer.handleControl(tailSealed({ spoolEmpty: true, tails: FINALIZE_REQUEST.tails }), channel);
    assert.equal(organizer.prepareFinalize(FINALIZE_REQUEST).prepared, true);

    organizer.handleEnvelope(envelope(2), channel);
    organizer.handleControl(
      tailSealed({ spoolEmpty: true, tails: [{ connectionId: CONNECTION, lastReceivedSeq: 2 }] }),
      channel,
    );
    assert.equal(organizer.allAcked, true, 'the later tail has reached its durable ceiling');

    const result = organizer.finalize(FINALIZE_REQUEST);
    assert.notEqual(result.completed, true, 'a later seal cannot authorize the older prepared barrier');
    assert.equal(organizer.runMarkerState(), 'running');
    organizer.close();
    organizer = null;

    db = new DatabaseSync(storePath);
    const marker = db
      .prepare('SELECT state, finalize_receipt_json FROM run_marker WHERE run_id = ?')
      .get(RUN);
    assert.equal(marker.state, 'running');
    assert.equal(marker.finalize_receipt_json, null);
  } finally {
    db?.close();
    organizer?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('retrying a committed finalize request with the same payload returns the identical durable receipt', () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-finalize-retry-'));
  let organizer;
  try {
    organizer = createOrganizeProcess({ frameBatchMs: 0, frameBatchMax: 1, 
      market: MARKET,
      stream: STREAM,
      runId: RUN,
      roleInstance: 'organize-1',
      storePath: join(dir, 'organize.sqlite'),
      rawWriter: () => true,
    });
    const channel = memoryChannel();
    organizer.handleControl(acceptedMessage(), channel);
    organizer.handleEnvelope(envelope(1), channel);
    organizer.handleControl(acceptedMessage(CONNECTION_2), channel);
    organizer.handleEnvelope(envelope(1, CONNECTION_2), channel);
    const request = {
      ...FINALIZE_REQUEST,
      tails: [
        { connectionId: CONNECTION_2, lastReceivedSeq: 1 },
        { connectionId: CONNECTION, lastReceivedSeq: 1 },
      ],
    };
    organizer.handleControl(tailSealed({ spoolEmpty: true, tails: request.tails }), channel);
    assert.equal(organizer.allAcked, true);
    assert.equal(organizer.prepareFinalize(request).prepared, true);

    const committed = organizer.finalize(request);
    const sameCanonicalPayload = {
      bookStop: { stopped: true, roleInstance: 'book-1', requestId: 'book-stop-1' },
      tails: [
        { lastReceivedSeq: 1, connectionId: CONNECTION },
        { lastReceivedSeq: 1, connectionId: CONNECTION_2 },
      ],
      barrierId: 'barrier-1',
      finalizeRequestId: 'finalize-1',
    };
    const retry = organizer.finalize(sameCanonicalPayload);
    assert.deepEqual(retry, committed);
  } finally {
    organizer?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the same finalize request id with a different payload is refused without changing the prepared request', () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-finalize-conflict-'));
  const storePath = join(dir, 'organize.sqlite');
  let organizer;
  let db;
  try {
    organizer = createOrganizeProcess({ frameBatchMs: 0, frameBatchMax: 1, 
      market: MARKET,
      stream: STREAM,
      runId: RUN,
      roleInstance: 'organize-1',
      storePath,
      rawWriter: () => true,
    });
    const channel = memoryChannel();
    organizer.handleControl(acceptedMessage(), channel);
    organizer.handleEnvelope(envelope(1), channel);
    organizer.handleControl(tailSealed({ spoolEmpty: true }), channel);
    assert.equal(organizer.allAcked, true);
    assert.equal(organizer.prepareFinalize(FINALIZE_REQUEST).prepared, true);

    const changedPayload = { ...FINALIZE_REQUEST, barrierId: 'barrier-other' };
    const conflict = organizer.prepareFinalize(changedPayload);
    assert.equal(conflict.refused, true);
    assert.equal(conflict.prepared, false);
    const committed = organizer.finalize(FINALIZE_REQUEST);
    assert.equal(committed.completed, true, 'the original prepared request remains usable');
    const completedConflict = organizer.finalize(changedPayload);
    assert.equal(completedConflict.refused, true, 'a conflicting replay cannot rewrite a completed receipt');
    assert.equal(organizer.finalize(FINALIZE_REQUEST).receipt.completedAtMs, committed.receipt.completedAtMs);
    organizer.close();
    organizer = null;

    db = new DatabaseSync(storePath);
    const marker = db
      .prepare('SELECT state, finalize_request_id, finalize_request_payload, finalize_receipt_json FROM run_marker WHERE run_id = ?')
      .get(RUN);
    assert.equal(marker.state, 'complete');
    assert.equal(marker.finalize_request_id, FINALIZE_REQUEST.finalizeRequestId);
    assert.equal(JSON.parse(marker.finalize_request_payload).barrierId, FINALIZE_REQUEST.barrierId);
    assert.equal(JSON.parse(marker.finalize_receipt_json).barrierId, FINALIZE_REQUEST.barrierId);
  } finally {
    db?.close();
    organizer?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a failed completion COMMIT leaves the run uncompleted and without a receipt', () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-finalize-commit-'));
  const storePath = join(dir, 'organize.sqlite');
  const failure = { armed: false };
  class FailOneCommit extends DatabaseSync {
    exec(sql) {
      if (failure.armed && sql.trim().toUpperCase() === 'COMMIT') {
        failure.armed = false;
        throw new Error('injected finalize commit failure');
      }
      return super.exec(sql);
    }
  }
  let store;
  let organizer;
  let db;
  try {
    store = openOrganizeStore({ path: storePath, runId: RUN, Database: FailOneCommit, nowMs: () => 1_000 });
    organizer = createOrganizeProcess({ frameBatchMs: 0, frameBatchMax: 1, 
      market: MARKET,
      stream: STREAM,
      runId: RUN,
      roleInstance: 'organize-1',
      store,
      markRunning: true,
      rawWriter: () => true,
      nowMs: () => 1_000,
    });
    const channel = memoryChannel();
    organizer.handleControl(acceptedMessage(), channel);
    organizer.handleEnvelope(envelope(1), channel);
    organizer.handleControl(tailSealed({ spoolEmpty: true }), channel);
    assert.equal(organizer.prepareFinalize(FINALIZE_REQUEST).prepared, true);

    failure.armed = true;
    assert.throws(() => organizer.finalize(FINALIZE_REQUEST), /injected finalize commit failure/);
    organizer.close();
    organizer = null;
    store.close();
    store = null;

    db = new DatabaseSync(storePath);
    const marker = db
      .prepare('SELECT state, finalize_receipt_json FROM run_marker WHERE run_id = ?')
      .get(RUN);
    assert.equal(marker.state, 'running');
    assert.equal(marker.finalize_receipt_json, null);
  } finally {
    db?.close();
    organizer?.close();
    store?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the marker probe does not create a database when the requested file is absent', () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-finalize-probe-missing-'));
  const storePath = join(dir, 'does-not-exist.sqlite');
  try {
    assert.equal(existsSync(storePath), false);
    const readback = organizeStoreModule.probeRunMarker({
      path: storePath,
      runId: RUN,
      request: FINALIZE_REQUEST,
    });

    assert.equal(existsSync(storePath), false, 'a read-only probe must not create the database file');
    assert.equal(readback.status, 'error');
    assert.equal(readback.fresh, false);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the fresh run-marker probe reads committed WAL proof without changing SQLite state', () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-finalize-probe-'));
  const storePath = join(dir, 'organize.sqlite');
  let organizer;
  try {
    organizer = createOrganizeProcess({ frameBatchMs: 0, frameBatchMax: 1, 
      market: MARKET,
      stream: STREAM,
      runId: RUN,
      roleInstance: 'organize-1',
      storePath,
      rawWriter: () => true,
    });
    const channel = memoryChannel();
    organizer.handleControl(acceptedMessage(), channel);
    organizer.handleEnvelope(envelope(1), channel);
    organizer.handleControl(tailSealed({ spoolEmpty: true }), channel);
    assert.equal(organizer.prepareFinalize(FINALIZE_REQUEST).prepared, true);

    const inspect = () => {
      const reader = new DatabaseSync(storePath, { readOnly: true });
      try {
        return {
          tables: reader.prepare("SELECT name, sql FROM sqlite_schema WHERE type = 'table' ORDER BY name").all(),
          marker: reader
            .prepare(
              `SELECT run_id, state, at_ms, finalize_request_id, finalize_request_payload, finalize_receipt_json
                 FROM run_marker WHERE run_id = ?`,
            )
            .get(RUN),
        };
      } finally {
        reader.close();
      }
    };

    const beforePrepareRead = inspect();
    const preparedReadback = organizeStoreModule.probeRunMarker({
      path: storePath,
      runId: RUN,
      request: FINALIZE_REQUEST,
    });
    assert.equal(preparedReadback.status, 'prepared');
    assert.equal(preparedReadback.fresh, true);
    assert.equal(preparedReadback.requestId, FINALIZE_REQUEST.finalizeRequestId);
    assert.equal(inspect().marker.state, beforePrepareRead.marker.state, 'the probe does not update the run marker');
    assert.deepEqual(inspect().tables, beforePrepareRead.tables, 'the probe does not create or migrate tables');

    const committed = organizer.finalize(FINALIZE_REQUEST);
    assert.equal(committed.completed, true);
    const beforeCompleteRead = inspect();
    const completeReadback = organizeStoreModule.probeRunMarker({
      path: storePath,
      runId: RUN,
      request: FINALIZE_REQUEST,
    });
    assert.equal(completeReadback.status, 'complete', 'a new read-only connection sees the committed WAL receipt');
    assert.equal(completeReadback.fresh, true);
    assert.deepEqual(completeReadback.receipt, committed.receipt);
    assert.deepEqual(inspect(), beforeCompleteRead, 'the probe leaves the complete marker and schema unchanged');
  } finally {
    organizer?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the outcome classifier confirms only a matching fresh complete receipt', () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-finalize-classifier-complete-'));
  const storePath = join(dir, 'organize.sqlite');
  let organizer;
  try {
    organizer = createOrganizeProcess({ frameBatchMs: 0, frameBatchMax: 1, 
      market: MARKET,
      stream: STREAM,
      runId: RUN,
      roleInstance: 'organize-1',
      storePath,
      rawWriter: () => true,
    });
    const channel = memoryChannel();
    organizer.handleControl(acceptedMessage(), channel);
    organizer.handleEnvelope(envelope(1), channel);
    organizer.handleControl(tailSealed({ spoolEmpty: true }), channel);
    assert.equal(organizer.prepareFinalize(FINALIZE_REQUEST).prepared, true);
    assert.equal(organizer.finalize(FINALIZE_REQUEST).completed, true);

    const readback = organizeStoreModule.probeRunMarker({
      path: storePath,
      runId: RUN,
      request: FINALIZE_REQUEST,
    });
    assert.equal(
      organizeStoreModule.classifyFinalizationOutcome({ runId: RUN, request: FINALIZE_REQUEST, readback }),
      'COMPLETE_CONFIRMED',
    );
  } finally {
    organizer?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the probe preserves and labels a legacy complete marker as proofless', () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-finalize-probe-legacy-'));
  const storePath = join(dir, 'organize.sqlite');
  let db;
  try {
    db = new DatabaseSync(storePath);
    db.exec(`
      CREATE TABLE run_marker (
        run_id TEXT NOT NULL PRIMARY KEY,
        state TEXT NOT NULL,
        at_ms INTEGER NOT NULL
      );
      INSERT INTO run_marker (run_id, state, at_ms) VALUES ('legacy-run', 'complete', 42);
    `);
    db.close();
    db = null;

    const inspect = () => {
      const reader = new DatabaseSync(storePath, { readOnly: true });
      try {
        return {
          columns: reader.prepare('PRAGMA table_info(run_marker)').all(),
          marker: reader.prepare('SELECT run_id, state, at_ms FROM run_marker WHERE run_id = ?').get('legacy-run'),
        };
      } finally {
        reader.close();
      }
    };
    const before = inspect();
    const readback = organizeStoreModule.probeRunMarker({
      path: storePath,
      runId: 'legacy-run',
      request: FINALIZE_REQUEST,
    });

    assert.equal(readback.status, 'legacy');
    assert.equal(readback.fresh, true);
    assert.equal(readback.state, 'complete');
    assert.deepEqual(inspect(), before, 'legacy proofless state and schema remain untouched');
  } finally {
    db?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a proof-capable schema reports an absent requested run without creating a marker', () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-finalize-probe-absent-'));
  const storePath = join(dir, 'organize.sqlite');
  let organizer;
  try {
    organizer = createOrganizeProcess({ frameBatchMs: 0, frameBatchMax: 1, 
      market: MARKET,
      stream: STREAM,
      runId: RUN,
      roleInstance: 'organize-1',
      storePath,
    });
    const inspect = () => {
      const reader = new DatabaseSync(storePath, { readOnly: true });
      try {
        return {
          tables: reader.prepare("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name").all(),
          markers: reader.prepare('SELECT run_id, state, at_ms FROM run_marker ORDER BY run_id').all(),
        };
      } finally {
        reader.close();
      }
    };
    const before = inspect();
    const absentRunId = 'run-with-no-marker';
    const readback = organizeStoreModule.probeRunMarker({
      path: storePath,
      runId: absentRunId,
      request: { ...FINALIZE_REQUEST, finalizeRequestId: 'absent:request' },
    });

    assert.equal(readback.status, 'absent');
    assert.equal(readback.fresh, true);
    assert.equal(readback.runId, absentRunId);
    assert.equal(readback.schema, 'proof-capable');
    assert.equal(
      organizeStoreModule.classifyFinalizationOutcome({
        runId: absentRunId,
        request: { ...FINALIZE_REQUEST, finalizeRequestId: 'absent:request' },
        readback,
      }),
      'UNKNOWN',
      'an absent marker is not proof of no-complete while the writer may still commit',
    );
    assert.equal(
      organizeStoreModule.classifyFinalizationOutcome({
        runId: absentRunId,
        request: { ...FINALIZE_REQUEST, finalizeRequestId: 'absent:request' },
        readback,
        writerFenced: true,
      }),
      'NO_COMPLETE_CONFIRMED',
    );
    assert.deepEqual(inspect(), before, 'probing an absent run does not create its marker or alter schema');
  } finally {
    organizer?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a fresh unprepared marker proves no-complete only after the writer fence', () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-finalize-unprepared-'));
  const storePath = join(dir, 'organize.sqlite');
  let organizer;
  try {
    organizer = createOrganizeProcess({ frameBatchMs: 0, frameBatchMax: 1, 
      market: MARKET,
      stream: STREAM,
      runId: RUN,
      roleInstance: 'organize-1',
      storePath,
    });
    const readback = organizeStoreModule.probeRunMarker({ path: storePath, runId: RUN });

    assert.equal(readback.status, 'unprepared');
    assert.equal(readback.fresh, true);
    assert.equal(readback.state, 'running');
    assert.equal(
      organizeStoreModule.classifyFinalizationOutcome({ runId: RUN, readback }),
      'UNKNOWN',
      'a running writer can still prepare and commit after this read',
    );
    assert.equal(
      organizeStoreModule.classifyFinalizationOutcome({ runId: RUN, readback, writerFenced: true }),
      'NO_COMPLETE_CONFIRMED',
    );
  } finally {
    organizer?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('NO_COMPLETE_CONFIRMED requires both an explicit writer fence and a fresh prepared readback', () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-finalize-classifier-pending-'));
  const storePath = join(dir, 'organize.sqlite');
  let organizer;
  try {
    organizer = createOrganizeProcess({ frameBatchMs: 0, frameBatchMax: 1, 
      market: MARKET,
      stream: STREAM,
      runId: RUN,
      roleInstance: 'organize-1',
      storePath,
      rawWriter: () => true,
    });
    const channel = memoryChannel();
    organizer.handleControl(acceptedMessage(), channel);
    organizer.handleEnvelope(envelope(1), channel);
    organizer.handleControl(tailSealed({ spoolEmpty: true }), channel);
    assert.equal(organizer.prepareFinalize(FINALIZE_REQUEST).prepared, true);

    const readback = organizeStoreModule.probeRunMarker({
      path: storePath,
      runId: RUN,
      request: FINALIZE_REQUEST,
    });
    assert.equal(readback.status, 'prepared');
    assert.equal(
      organizeStoreModule.classifyFinalizationOutcome({ runId: RUN, request: FINALIZE_REQUEST, readback }),
      'UNKNOWN',
      'fresh absence of a receipt is not enough while the writer might still commit',
    );
    assert.equal(
      organizeStoreModule.classifyFinalizationOutcome({
        runId: RUN,
        request: FINALIZE_REQUEST,
        readback,
        writerFenced: true,
      }),
      'NO_COMPLETE_CONFIRMED',
    );
    assert.equal(
      organizeStoreModule.classifyFinalizationOutcome({
        runId: RUN,
        request: FINALIZE_REQUEST,
        readback: { ...readback, fresh: false },
        writerFenced: true,
      }),
      'UNKNOWN',
      'a stale snapshot cannot prove no-complete even after a fence',
    );
  } finally {
    organizer?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('errors, stale reads, legacy markers and mismatched completion proofs remain UNKNOWN', () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-finalize-classifier-unknown-'));
  const storePath = join(dir, 'organize.sqlite');
  let organizer;
  try {
    organizer = createOrganizeProcess({ frameBatchMs: 0, frameBatchMax: 1, 
      market: MARKET,
      stream: STREAM,
      runId: RUN,
      roleInstance: 'organize-1',
      storePath,
      rawWriter: () => true,
    });
    const channel = memoryChannel();
    organizer.handleControl(acceptedMessage(), channel);
    organizer.handleEnvelope(envelope(1), channel);
    organizer.handleControl(tailSealed({ spoolEmpty: true }), channel);
    assert.equal(organizer.prepareFinalize(FINALIZE_REQUEST).prepared, true);
    assert.equal(organizer.finalize(FINALIZE_REQUEST).completed, true);

    const readback = organizeStoreModule.probeRunMarker({
      path: storePath,
      runId: RUN,
      request: FINALIZE_REQUEST,
    });
    assert.equal(readback.status, 'complete');
    const uncertainReadbacks = [
      ['error', { ...readback, status: 'error', fresh: false }],
      ['stale snapshot', { ...readback, fresh: false }],
      ['marker mismatch', { ...readback, status: 'mismatch' }],
      ['request mismatch', { ...readback, requestId: 'other-request' }],
      ['payload mismatch', { ...readback, payloadJson: 'different-payload' }],
      ['receipt mismatch', { ...readback, receipt: { ...readback.receipt, barrierId: 'different-barrier' } }],
      ['legacy proofless complete marker', { status: 'legacy', fresh: true, runId: RUN, state: 'complete' }],
    ];

    for (const [label, uncertainReadback] of uncertainReadbacks) {
      assert.equal(
        organizeStoreModule.classifyFinalizationOutcome({
          runId: RUN,
          request: FINALIZE_REQUEST,
          readback: uncertainReadback,
          writerFenced: true,
        }),
        'UNKNOWN',
        label,
      );
    }
  } finally {
    organizer?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('opening a legacy three-column run marker preserves its state without inventing finalization proof', () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-finalize-legacy-'));
  const storePath = join(dir, 'organize.sqlite');
  let db;
  let store;
  try {
    db = new DatabaseSync(storePath);
    db.exec(`
      CREATE TABLE run_marker (
        run_id TEXT NOT NULL PRIMARY KEY,
        state TEXT NOT NULL,
        at_ms INTEGER NOT NULL
      );
      INSERT INTO run_marker (run_id, state, at_ms) VALUES ('legacy-run', 'complete', 42);
    `);
    db.close();
    db = null;

    store = openOrganizeStore({ path: storePath, runId: 'legacy-run' });
    assert.throws(() => store.beginRun(), /cannot be started again/);
    store.close();
    store = null;

    db = new DatabaseSync(storePath);
    const columns = new Set(db.prepare('PRAGMA table_info(run_marker)').all().map((row) => row.name));
    assert.ok(columns.has('finalize_request_id'));
    assert.ok(columns.has('finalize_request_payload'));
    assert.ok(columns.has('finalize_receipt_json'));
    const marker = db
      .prepare(
        `SELECT state, at_ms, finalize_request_id, finalize_request_payload, finalize_receipt_json
           FROM run_marker WHERE run_id = 'legacy-run'`,
      )
      .get();
    assert.equal(marker.state, 'complete');
    assert.equal(marker.at_ms, 42);
    assert.equal(marker.finalize_request_id, null);
    assert.equal(marker.finalize_request_payload, null);
    assert.equal(marker.finalize_receipt_json, null);
    assert.equal(
      db.prepare("SELECT COUNT(*) AS count FROM sqlite_master WHERE type = 'table' AND name LIKE '%receipt%'").get().count,
      0,
      'finalization proof stays in run_marker rather than a new receipt table',
    );
  } finally {
    db?.close();
    store?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});


