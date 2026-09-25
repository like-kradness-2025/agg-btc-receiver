import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  FRAME_HEADER_BYTES,
  encodeEnvelope,
  makeEnvelope,
} from '../src/envelope.mjs';

/**
 * C1 / C2: the envelope carries the identity of the connection it came from, and that identity
 * survives the wire. This is checked on the encoded bytes rather than through the decoder, because
 * the wire is where the generation used to disappear.
 */
function headerOf(envelope) {
  const frame = encodeEnvelope(envelope);
  const length = frame.readUInt32BE(0);
  assert.ok(length > 0, 'the encoded frame announces its header length');
  return JSON.parse(frame.subarray(FRAME_HEADER_BYTES, FRAME_HEADER_BYTES + length).toString('utf8'));
}

const parts = {
  market: 'kraken_spot',
  venue: 'kraken',
  stream: 'book',
  runId: 'run-2026-09-25T21:00:00Z',
  generation: 3,
  receiveSeq: 41,
  recvTsMs: 1_792_000_000_041,
  recvMonoNs: 9_000_000_041,
  raw: '{"hello":"world"}',
};

test('a connection is named once, from the run, the venue and the generation', () => {
  const envelope = makeEnvelope(parts);
  assert.equal(
    envelope.connection_id,
    'run-2026-09-25T21:00:00Z:kraken:kraken_spot:3',
    'the id includes the run, so a restarted process cannot collide with a dead connection',
  );
  assert.equal(envelope.run_id, parts.runId);
  assert.equal(envelope.venue, 'kraken');
  assert.equal(envelope.generation, 3);
});

test('the identity travels on the wire, not just in the object that was built', () => {
  const header = headerOf(makeEnvelope(parts));
  assert.equal(header.connection_id, 'run-2026-09-25T21:00:00Z:kraken:kraken_spot:3');
  assert.equal(header.run_id, parts.runId, 'the run survives the hop that used to drop the generation');
  assert.equal(header.venue, 'kraken');
  assert.equal(header.generation, 3);
});

test('a caller that already holds a full id keeps it', () => {
  const envelope = makeEnvelope({ ...parts, connectionId: 'legacy:conn-1' });
  assert.equal(envelope.connection_id, 'legacy:conn-1');
});

test('without an id and without the parts, the envelope is refused rather than guessed', () => {
  assert.throws(
    () => makeEnvelope({ ...parts, runId: null, venue: null, generation: null }),
    /connectionId/,
    'no name means no envelope: an unnamed connection cannot be deduplicated or superseded',
  );
  assert.throws(
    () => makeEnvelope({ ...parts, generation: '3' }),
    /generation must be an integer/,
  );
});

test('an unnamed-but-complete identity is still written down as nulls, not omitted', () => {
  const envelope = makeEnvelope({ ...parts, runId: null, venue: null, generation: null, connectionId: 'conn-1' });
  const header = headerOf(envelope);
  for (const field of ['run_id', 'venue', 'generation']) {
    assert.ok(field in header, `${field} is present on the wire even when unknown`);
  }
  assert.equal(header.generation, null);
});
