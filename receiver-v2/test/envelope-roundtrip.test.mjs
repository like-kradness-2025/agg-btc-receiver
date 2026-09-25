import { test } from 'node:test';
import assert from 'node:assert/strict';

import { decodeEnvelope, encodeEnvelope, makeEnvelope } from '../src/envelope.mjs';

/**
 * C1, the part the first attempt got wrong: the identity must come back OUT of the frame. The
 * previous test read the encoded header, which proved the write side and said nothing about the
 * read side - and the read side was dropping the run, the venue and the generation back to null.
 */
const parts = {
  market: 'kraken_spot',
  venue: 'kraken',
  stream: 'book',
  runId: 'run-7',
  generation: 4,
  receiveSeq: 12,
  recvTsMs: 1_792_000_000_012,
  recvMonoNs: 5_000_000_012,
  raw: '{"a":1}',
};

test('a frame round-trips the whole identity, not just the connection id', () => {
  const sent = makeEnvelope(parts);
  const received = decodeEnvelope(encodeEnvelope(sent));

  assert.equal(received.connection_id, 'run-7:kraken:kraken_spot:4');
  assert.equal(received.run_id, 'run-7', 'the run survives the hop');
  assert.equal(received.venue, 'kraken', 'and so does the venue');
  assert.equal(received.generation, 4, 'and the generation is not quietly reset to null');
  assert.equal(received.receive_seq, 12);
  assert.equal(received.recv_ts_ms, parts.recvTsMs);
  assert.deepEqual(received.raw, Buffer.from(parts.raw));
});

test('an envelope without a run comes back without one, and says so with nulls', () => {
  const sent = makeEnvelope({ ...parts, runId: null, venue: null, generation: null, connectionId: 'conn-1' });
  const received = decodeEnvelope(encodeEnvelope(sent));
  assert.equal(received.connection_id, 'conn-1');
  assert.equal(received.run_id, null);
  assert.equal(received.generation, null, 'absent is represented as null rather than as a missing key');
});

test('a header whose connection id contradicts its own identity is refused', () => {
  const frame = encodeEnvelope(makeEnvelope(parts));
  const headerLength = frame.readUInt32BE(0);
  const header = JSON.parse(frame.subarray(4, 4 + headerLength).toString('utf8'));
  header.connection_id = 'run-other:kraken:kraken_spot:4';
  const headerBytes = Buffer.from(JSON.stringify(header), 'utf8');
  const length = Buffer.alloc(4);
  length.writeUInt32BE(headerBytes.length, 0);
  const tampered = Buffer.concat([length, headerBytes, frame.subarray(4 + headerLength)]);

  assert.throws(
    () => decodeEnvelope(tampered),
    /does not describe/,
    'a frame that names a different connection than its own parts describe is not accepted',
  );
});

test('the generation is what decides a newer connection, and it is carried rather than inferred', () => {
  const older = decodeEnvelope(encodeEnvelope(makeEnvelope({ ...parts, generation: 4 })));
  const newer = decodeEnvelope(encodeEnvelope(makeEnvelope({ ...parts, generation: 5 })));
  assert.ok(newer.generation > older.generation, 'the numbers came off the wire, not from a counter here');
});
