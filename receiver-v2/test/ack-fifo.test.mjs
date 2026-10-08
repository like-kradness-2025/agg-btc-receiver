/**
 * The release FIFO: how far an acknowledgement may move the spool's cursor when the frames waiting
 * in the spool do not all belong to one connection.
 *
 * The spool retains a frame until the durable acknowledgement that covers it. With a hand-over the
 * spool holds frames of two connections at once, and an acknowledgement names only its own: the
 * walk over the spool cannot decide on its own which positions became contiguous. This module keeps
 * the physical order of every unacknowledged record, raises a per-identity ceiling as
 * acknowledgements arrive, and releases from the head only - so no acknowledgement can carry the
 * cursor over a record the spool has not actually confirmed.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { ackIdentityOf, createAckFifo, rebuildAckFifo } from '../src/ack-fifo.mjs';
import { createSpool } from '../src/spool.mjs';

const envelope = (seq, connectionId = 'conn-1') =>
  makeEnvelope({
    market: 'kraken_spot',
    stream: 'trades',
    connectionId,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000 + seq,
    raw: `{"seq":${seq}}`,
  });

const entry = (identity, seq, offset) => ({ identity, seq, position: { segment: 1, offset } });

test('the cursor passes a record only when the record itself is covered', () => {
  const fifo = createAckFifo();
  fifo.record(entry('conn-A', 1, 10));
  fifo.record(entry('conn-A', 2, 20));
  fifo.record(entry('conn-B', 1, 30));
  fifo.record(entry('conn-A', 3, 40));

  // B's acknowledgement alone releases nothing: the head is A's first record.
  assert.deepEqual(fifo.noteAck({ identity: 'conn-B', upToSeq: 1 }), { released: 0, position: null });

  // A's ceiling covers its first two, which frees B's first as well - all three are contiguous.
  const across = fifo.noteAck({ identity: 'conn-A', upToSeq: 2 });
  assert.equal(across.released, 3);
  assert.deepEqual(across.position, { segment: 1, offset: 30 }, 'the position is the end of the last released record');

  // The remaining record is released when its own ceiling covers it.
  const rest = fifo.noteAck({ identity: 'conn-A', upToSeq: 3 });
  assert.equal(rest.released, 1);
  assert.deepEqual(rest.position, { segment: 1, offset: 40 });
  assert.equal(fifo.size, 0);
  assert.equal(fifo.head, null);
});

test('a repeated or backwards acknowledgement releases nothing new', () => {
  const fifo = createAckFifo();
  fifo.record(entry('conn-A', 1, 10));
  fifo.record(entry('conn-A', 2, 20));
  assert.equal(fifo.noteAck({ identity: 'conn-A', upToSeq: 1 }).released, 1);
  assert.deepEqual(fifo.noteAck({ identity: 'conn-A', upToSeq: 1 }), { released: 0, position: null });
  assert.deepEqual(fifo.noteAck({ identity: 'conn-A', upToSeq: 0 }), { released: 0, position: null });
  assert.equal(
    fifo.noteAck({ identity: 'conn-A', upToSeq: 2 }).released,
    1,
    'the ceiling is monotonic, so a later advance still releases what follows',
  );
});

test('a jump acknowledgement releases the whole contiguous run it covers', () => {
  const fifo = createAckFifo();
  for (let seq = 1; seq <= 5; seq += 1) fifo.record(entry('conn-A', seq, seq * 10));
  const outcome = fifo.noteAck({ identity: 'conn-A', upToSeq: 5 });
  assert.equal(outcome.released, 5);
  assert.deepEqual(outcome.position, { segment: 1, offset: 50 });
});

test('an acknowledgement for a connection with nothing recorded releases nothing', () => {
  const fifo = createAckFifo();
  fifo.record(entry('conn-A', 1, 10));
  assert.deepEqual(fifo.noteAck({ identity: 'conn-B', upToSeq: 5 }), { released: 0, position: null });
  assert.deepEqual(fifo.noteAck({ identity: 'conn-A', upToSeq: 1 }), {
    released: 1,
    position: { segment: 1, offset: 10 },
  });
});

test('released entries do not accumulate without bound', () => {
  const fifo = createAckFifo();
  for (let seq = 1; seq <= 5000; seq += 1) fifo.record(entry('conn-A', seq, seq));
  assert.equal(fifo.size, 5000);
  for (let seq = 1; seq <= 5000; seq += 1) fifo.noteAck({ identity: 'conn-A', upToSeq: seq });
  assert.equal(fifo.size, 0);
  assert.equal(fifo.head, null);
  // ...and it still works after the compaction.
  fifo.record(entry('conn-A', 5001, 5001));
  assert.equal(fifo.noteAck({ identity: 'conn-A', upToSeq: 5001 }).released, 1);
});

test('a rebuilt FIFO keeps the spool walk order and releases a hand-over correctly', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'ack-fifo-'));
  try {
    const spool = createSpool({ dir });
    spool.append(envelope(1, 'conn-A'));
    spool.append(envelope(2, 'conn-A'));
    spool.append(envelope(1, 'conn-B'));
    spool.sync();

    const fifo = rebuildAckFifo([...spool.drainRecords()]);
    assert.equal(fifo.size, 3, 'the walk rebuilt every unacknowledged record');

    const identityB = ackIdentityOf(envelope(1, 'conn-B'));
    assert.deepEqual(
      fifo.noteAck({ identity: identityB, upToSeq: 1 }),
      { released: 0, position: null },
      "B's frames wait behind A's",
    );

    const identityA = ackIdentityOf(envelope(1, 'conn-A'));
    const outcome = fifo.noteAck({ identity: identityA, upToSeq: 2 });
    assert.equal(outcome.released, 3, 'A1, A2 and then B1 were contiguous once A was covered');

    // The released position is the end of the last record; advancing to it releases the segment
    // because it is still the end of it.
    assert.equal(outcome.position.segment, 1);
    spool.advance(outcome.position);
    assert.deepEqual(spool.segments, [], 'the fully covered segment is released');
    assert.deepEqual([...spool.drainRecords()], [], 'and nothing comes back');
    spool.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('a ceiling is retired with the last record it held, and stays cheap across reconnects', () => {
  const fifo = createAckFifo();
  for (let generation = 1; generation <= 100; generation += 1) {
    fifo.record(entry(`conn-${generation}`, 1, generation));
    assert.equal(fifo.noteAck({ identity: `conn-${generation}`, upToSeq: 1 }).released, 1);
  }
  assert.equal(fifo.size, 0);
  assert.equal(fifo.identities, 0, 'a retired identity is not kept');

  // A stale acknowledgement for a retired identity is a no-op, not a leak...
  assert.deepEqual(fifo.noteAck({ identity: 'conn-50', upToSeq: 1 }), { released: 0, position: null });
  assert.equal(fifo.identities, 0);

  // ...and the identity works again the moment a record waits under it.
  fifo.record(entry('conn-50', 2, 500));
  assert.equal(fifo.noteAck({ identity: 'conn-50', upToSeq: 2 }).released, 1);
  assert.equal(fifo.identities, 0, 'released once more, retired once more');
});

test('the frame identity names the delivery, not the position in the stream', () => {
  assert.equal(
    ackIdentityOf(envelope(1, 'conn-A')),
    ackIdentityOf(envelope(2, 'conn-A')),
    'the scan position does not change the identity',
  );
  assert.notEqual(
    ackIdentityOf(envelope(1, 'conn-A')),
    ackIdentityOf(envelope(1, 'conn-B')),
    'another connection is another identity',
  );
});
