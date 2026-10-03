/**
 * A fake organize peer, for driving the ingest process over the real IPC transport.
 *
 * This is a test support module, not product code: it lives in `test-support/` so no test-only route
 * is opened inside `src/`. It listens on a unix socket with the real `src/ipc.mjs`, records what the
 * ingest process says, and answers on the stage-1 vocabulary.
 *
 * It implements just enough of the accept rules for stage 2's C2 checks:
 *   - a new run for a connection is admitted only with an explicit `takeover`;
 *   - a connection that already has an owner refuses an `accept` whose generation is not strictly
 *     greater (an old instance); the recorded generation moves only on a real advance.
 * Its `accepted` carries the request it answers so the ingest side can match it.
 */

import { listen } from '../src/ipc.mjs';

export const IPC_VERSION = 1;

/**
 * Start the fake peer on `path`.
 *
 * `replies` are collected so a test can assert what was actually written on the wire. Envelopes the
 * ingest process sends are recorded in `envelopes` (and optionally acknowledged by the test).
 */
export async function startFakeOrganize(path, { roleInstance = 'organize-1', market = 'kraken_spot', stream = 'trades', runId = 'run-1', batchFrames = 1 } = {}) {
  const state = {
    accepts: [],
    controls: [],
    envelopes: [],
    replies: [],
    // connectionId -> { generation, runId, roleInstance }
    acceptedByConnection: new Map(),
  };
  let channel = null;

  const server = await listen(path, {
    batchFrames,
    onChannel: (opened) => {
      channel = opened;
    },
    onEnvelope: (envelope) => state.envelopes.push(envelope),
    onControl: (message) => handle(message),
  });

  function handle(message) {
    state.controls.push(message);
    if (message.type === 'accept') return handleAccept(message);
    return undefined;
  }

  function handleAccept(message) {
    state.accepts.push(message);
    // Ownership is a fact about the board, not the connection: a new run names its connection
    // differently (run, venue, market, generation - C2), so two runs on one board never share a key.
    const key = `${message.market ?? ''}:${message.stream ?? ''}`;
    const previous = state.acceptedByConnection.get(key);
    const takeover = message.payload?.takeover === true;
    let accepted = true;
    let reason = '';
    if (previous) {
      if (previous.runId === message.run_id) {
        if (message.generation <= previous.generation) {
          accepted = false;
          reason = 'the board is already owned by a newer generation (old instance)';
        }
      } else if (!takeover) {
        accepted = false;
        reason = 'a different run needs an explicit takeover';
      }
    }
    if (accepted) {
      state.acceptedByConnection.set(key, {
        generation: message.generation,
        runId: message.run_id,
        roleInstance: message.role_instance,
        connectionId: message.connection_id,
      });
    }
    reply({
      version: IPC_VERSION,
      type: 'accepted',
      role_instance: roleInstance,
      request_id: message.request_id,
      connection_id: message.connection_id,
      generation: message.generation,
      payload: { accepted, reason, takeover },
    });
    return { accepted, reason };
  }

  /** Write one control message to the ingest process (on its most recent channel). */
  function reply(message) {
    state.replies.push(message);
    channel?.sendControl(message);
    return message;
  }

  return {
    server,
    state,
    /** The channel the ingest process connected on, once it has. */
    get channel() {
      return channel;
    },
    reply,
    sendDurableAck({ connectionId, generation, upToSeq, capacity = 'ok' }) {
      return reply({
        version: IPC_VERSION,
        type: 'durable_ack',
        role_instance: roleInstance,
        run_id: runId,
        market,
        stream,
        connection_id: connectionId,
        generation,
        payload: { up_to_seq: upToSeq, capacity },
      });
    },
    sendReadiness({ capacity, ready = undefined }) {
      return reply({
        version: IPC_VERSION,
        type: 'readiness',
        role_instance: roleInstance,
        payload: { ...(capacity === undefined ? {} : { capacity }), ...(ready === undefined ? {} : { ready }) },
      });
    },
    sendResend({ connectionId, generation }) {
      return reply({
        version: IPC_VERSION,
        type: 'resend',
        role_instance: roleInstance,
        request_id: `${roleInstance}:resend:${generation}`,
        run_id: runId,
        market,
        stream,
        connection_id: connectionId,
        generation,
        payload: {},
      });
    },
    sendHello({ role, instance }) {
      return reply({
        version: IPC_VERSION,
        type: 'hello',
        role_instance: instance,
        run_id: runId,
        payload: { role },
      });
    },
    close() {
      return server.close();
    },
  };
}
