/**
 * A fake ingest peer, for driving the organize process over the real IPC transport.
 *
 * This is a test support module, not product code: it lives in `test-support/` so no test-only route is
 * opened inside `src/`. Organize listens; this peer connects, announces itself as ingest with `hello`,
 * and speaks the stage-1 vocabulary - it issues the `accept`, sends frames, seals the final tails after
 * reception stops, and records the acknowledgements and readiness organize sends back.
 */

import { connect } from '../src/ipc.mjs';
import { IPC_VERSION, makeMessage } from '../src/ipc-message.mjs';

export async function startFakeIngest(
  path,
  { roleInstance = 'ingest-1', market = 'kraken_spot', stream = 'trades', runId = 'run-1', batchFrames = 1 } = {},
) {
  const state = {
    controls: [],
    accepted: [],
    durableAcks: [],
    drained: [],
    stopped: null,
    readiness: [],
    error: null,
  };
  let channel;
  channel = await connect(path, {
    batchFrames,
    onControl: (message) => handle(message),
    onError: (error) => {
      state.error = error;
    },
  });
  channel.sendControl(
    makeMessage({ version: IPC_VERSION, type: 'hello', role_instance: roleInstance, run_id: runId, payload: { role: 'ingest' } }),
  );

  function handle(message) {
    state.controls.push(message);
    if (message.type === 'accepted') state.accepted.push(message);
    if (message.type === 'durable_ack') state.durableAcks.push(message);
    if (message.type === 'drained') state.drained.push(message);
    if (message.type === 'readiness') state.readiness.push(message);
    if (message.type === 'stopped') state.stopped = message;
  }

  return {
    state,
    roleInstance,
    get channel() {
      return channel;
    },
    sendAccept({ connectionId, generation = 1, firstSeq = 1, takeover = false, requestId = null } = {}) {
      return channel.sendControl(
        makeMessage({
          version: IPC_VERSION,
          type: 'accept',
          role_instance: roleInstance,
          request_id: requestId ?? `${roleInstance}:accept:${connectionId}:${generation}`,
          run_id: runId,
          connection_id: connectionId,
          generation,
          payload: { first_seq: firstSeq, takeover },
        }),
      );
    },
    sendFrame(envelope) {
      return channel.sendEnvelope(envelope);
    },
    sendTailSealed({ tails, spoolEmpty = true }) {
      return channel.sendControl(
        makeMessage({
          version: IPC_VERSION,
          type: 'tail_sealed',
          role_instance: roleInstance,
          run_id: runId,
          payload: { tails, spool_empty: spoolEmpty },
        }),
      );
    },
    sendStop(requestId = 'stop-1') {
      return channel.sendControl(
        makeMessage({ version: IPC_VERSION, type: 'stop', role_instance: roleInstance, request_id: requestId }),
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
