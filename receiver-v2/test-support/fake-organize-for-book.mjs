/**
 * A fake organize peer, for driving the book process over the real IPC transport.
 *
 * This is a test support module, not product code: it lives in `test-support/` so no test-only route
 * is opened inside `src/`. The book connects to organize (the same direction as ingest); this peer
 * listens on a unix socket with the real `src/ipc.mjs`, announces itself as organize with `hello`, and
 * speaks the stage-1 vocabulary - it issues the `accept`, sends frames, asks for an invalidation, and
 * records the answers the book sends back (`accepted`, `invalidated`, `applied_ack`, `stopped`).
 */

import { listen } from '../src/ipc.mjs';
import { IPC_VERSION, makeMessage } from '../src/ipc-message.mjs';

export async function startFakeOrganizeForBook(
  path,
  { roleInstance = 'organize-1', market = 'kraken_spot', stream = 'trades', runId = 'run-1', batchFrames = 1 } = {},
) {
  const state = {
    controls: [],
    accepted: [],
    invalidated: [],
    appliedAcks: [],
    stopped: null,
    error: null,
  };
  let channel = null;

  const server = await listen(path, {
    batchFrames,
    onChannel: (opened) => {
      channel = opened;
    },
    onControl: (message) => handle(message),
    onError: (error) => {
      state.error = error;
    },
  });

  function handle(message) {
    state.controls.push(message);
    if (message.type === 'accepted') state.accepted.push(message);
    if (message.type === 'invalidated') state.invalidated.push(message);
    if (message.type === 'applied_ack') state.appliedAcks.push(message);
    if (message.type === 'stopped') state.stopped = message;
  }

  function reply(message) {
    channel?.sendControl(message);
    return message;
  }

  return {
    server,
    state,
    market,
    stream,
    runId,
    get channel() {
      return channel;
    },
    reply,
    sendHello() {
      return reply(
        makeMessage({ version: IPC_VERSION, type: 'hello', role_instance: roleInstance, run_id: runId, payload: { role: 'organize' } }),
      );
    },
    /** Ask the book to accept a connection. The book's verdict comes back as `accepted`. */
    sendAccept({ connectionId, generation = 1, firstSeq = 1, takeover = false, requestId = null, runId: runIdOverride = null } = {}) {
      return reply(
        makeMessage({
          version: IPC_VERSION,
          type: 'accept',
          role_instance: roleInstance,
          request_id: requestId ?? `${roleInstance}:accept:${connectionId}:${generation}`,
          run_id: runIdOverride ?? runId,
          connection_id: connectionId,
          generation,
          payload: { first_seq: firstSeq, takeover },
        }),
      );
    },
    /** Hand one frame to the book. The board's answer comes back as an `applied_ack`. */
    sendFrame(envelope) {
      return channel?.sendEnvelope(envelope);
    },
    /** Ask the book to stop serving and persist a loss. The answer comes back as `invalidated`. */
    sendInvalidate({ requestId, connectionId, generation = 1, from, to, revision = 1, reason = 'a range must be declared missing' }) {
      return reply(
        makeMessage({
          version: IPC_VERSION,
          type: 'invalidate',
          role_instance: roleInstance,
          request_id: requestId,
          run_id: runId,
          connection_id: connectionId,
          generation,
          payload: { from, to, revision, reason },
        }),
      );
    },
    sendStop(requestId = 'stop-1') {
      return reply(makeMessage({ version: IPC_VERSION, type: 'stop', role_instance: roleInstance, request_id: requestId }));
    },
    close() {
      return server.close();
    },
  };
}
