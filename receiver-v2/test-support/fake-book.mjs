/**
 * A fake book peer, for driving the organize process over the real IPC transport.
 *
 * This is a test support module, not product code. Organize listens; this peer connects, announces
 * itself as the book with `hello`, records the invalidation requests organize sends, and answers them
 * with `invalidated` - or, for the alternative condition, stays silent while a test confirms the book
 * ended. It can also report an applied boundary.
 */

import { connect } from '../src/ipc.mjs';
import { IPC_VERSION, makeMessage } from '../src/ipc-message.mjs';

export async function startFakeBook(
  path,
  { roleInstance = 'book-1', market = 'kraken_spot', stream = 'trades', runId = 'run-1', batchFrames = 1 } = {},
) {
  const state = { controls: [], invalidations: [], envelopes: [], error: null };
  let channel;
  channel = await connect(path, {
    batchFrames,
    onControl: (message) => handle(message),
    onEnvelope: (envelope) => state.envelopes.push(envelope),
    onError: (error) => {
      state.error = error;
    },
  });
  channel.sendControl(
    makeMessage({ version: IPC_VERSION, type: 'hello', role_instance: roleInstance, run_id: runId, payload: { role: 'book' } }),
  );

  function handle(message) {
    state.controls.push(message);
    if (message.type === 'invalidate') state.invalidations.push(message);
  }

  return {
    state,
    roleInstance,
    get channel() {
      return channel;
    },
    sendInvalidated({ requestId, connectionId, generation = 1 }) {
      return channel.sendControl(
        makeMessage({
          version: IPC_VERSION,
          type: 'invalidated',
          role_instance: roleInstance,
          request_id: requestId,
          run_id: runId,
          connection_id: connectionId,
          generation,
          payload: {},
        }),
      );
    },
    /**
     * The book's authorization of a connection, relayed by the supervisor in the real topology. This
     * peer sends it directly to organize (which is the whole point of the adoption: organize reflects
     * what the book decided).
     */
    sendAccepted({ requestId, connectionId, generation = 1, firstSeq = 1, takeover = false, accepted = true, reason = '' }) {
      return channel.sendControl(
        makeMessage({
          version: IPC_VERSION,
          type: 'accepted',
          role_instance: roleInstance,
          request_id: requestId,
          run_id: runId,
          market,
          stream,
          connection_id: connectionId,
          generation,
          payload: { accepted, reason, first_seq: firstSeq, takeover },
        }),
      );
    },
    sendAppliedAck({ connectionId, generation = 1, upToSeq }) {
      return channel.sendControl(
        makeMessage({
          version: IPC_VERSION,
          type: 'applied_ack',
          role_instance: roleInstance,
          run_id: runId,
          market,
          stream,
          connection_id: connectionId,
          generation,
          payload: { up_to_seq: upToSeq },
        }),
      );
    },
    close() {
      try {
        channel.close();
      } catch {
        // the socket may already be gone
      }
    },
  };
}
