#!/usr/bin/env node
// Child used only by crash-recovery tests: commit one owed frame, then remain killable.
import process from 'node:process';

import { makeEnvelope } from '../src/envelope.mjs';
import { CHANGES_FORMAT } from '../src/changes.mjs';
import { IPC_VERSION, makeMessage } from '../src/ipc-message.mjs';
import { createOrganizeProcess } from '../src/organize/main.mjs';

const [storePath] = process.argv.slice(2);
const MARKET = 'kraken_spot';
const STREAM = 'trades';
const RUN = 'run-1';
const CONNECTION = `${RUN}:kraken:${MARKET}:1`;

const channelToParent = {
  sendControl(message) {
    process.send?.({ kind: 'control', message });
    return true;
  },
  sendEnvelope(envelope) {
    process.send?.({ kind: 'envelope', envelope });
    return true;
  },
  close() {},
};
const ingest = {
  sendControl(message) {
    process.send?.({ kind: 'control', message });
    return true;
  },
  sendEnvelope() {
    return true;
  },
  close() {},
};

const organizer = createOrganizeProcess({
  market: MARKET,
  stream: STREAM,
  runId: RUN,
  roleInstance: 'organize-crash-test',
  storePath,
  markRunning: false,
});
organizer.handleControl(
  makeMessage({ version: IPC_VERSION, type: 'hello', role_instance: 'book-1', run_id: RUN, payload: { role: 'book' } }),
  channelToParent,
);
organizer.handleControl(
  makeMessage({
    version: IPC_VERSION,
    type: 'accepted',
    role_instance: 'book-1',
    request_id: 'book-accept-1',
    run_id: RUN,
    market: MARKET,
    stream: STREAM,
    connection_id: CONNECTION,
    generation: 1,
    payload: { accepted: true, first_seq: 1, takeover: false },
  }),
  channelToParent,
);

function frame() {
  return makeEnvelope({
    market: MARKET,
    stream: STREAM,
    connectionId: CONNECTION,
    runId: RUN,
    venue: 'kraken',
    generation: 1,
    receiveSeq: 1,
    recvTsMs: 1_001,
    recvMonoNs: 1,
    raw: Buffer.from('frame-1'),
    meta: {
      first_seq: 1,
      changes_format: CHANGES_FORMAT,
      changes: { replace: false, changes: [{ side: 'bid', price: 101, size: 1 }] },
    },
  });
}

process.on('message', async (message) => {
  if (message?.kind !== 'seed') return;
  try {
    const result = organizer.handleEnvelope(frame(), ingest);
    // The owed delivery is scheduled (Set 5): let its pass run before reporting, so the envelope
    // that reaches the parent through the channel shim arrives ahead of this report, exactly as a
    // synchronous hand-over would have.
    await new Promise((resolve) => setTimeout(resolve, 50));
    process.send?.({
      kind: 'seeded',
      result,
      status: organizer.recoveryStatus(),
      watermarks: organizer.watermarkRows(),
      ledger: organizer.ledgerEntries(),
    });
  } catch (error) {
    process.send?.({ kind: 'fatal', reason: error.message });
  }
});

process.send?.({ kind: 'ready' });
setInterval(() => {}, 60_000);
