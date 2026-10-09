/**
 * The supervisor's socket and router (three-process design, docs/fix-plan-sets.md §5.8, ruling ① -
 * Astra).
 *
 * The topology handed to the supervisor: it owns the single socket. Ingest, organize and the book
 * connect to it and announce themselves with `hello`; every message they send is delivered to the
 * role it is addressed to. Nothing here listens as organize (that hub was rejected - an organize
 * failure must not become an ingest/book communication failure), and no role connects directly to
 * another.
 *
 * What the router validates, and what it refuses to do:
 *
 *   - the sender's identity: a channel may carry no message until it has announced a known role, and
 *     a new instance of a role replaces the old channel (a restart is a fact about that role);
 *   - the destination: each (sender role, message type) has exactly one destination, and a message
 *     with no legal destination, or whose destination is not connected, is refused - never invented;
 *   - the connection state of the accept path: the book's answer is matched against an acceptance
 *     this supervisor is actually holding, and a stray or stale answer is refused.
 *
 * What it deliberately does not do: it never authorizes an accept, never judges durability, never
 * touches the board, and never fabricates an `accepted`. It reads only the routing information - the
 * sender role, the message type and the request identity - and passes the business payload through
 * untouched.
 *
 * The relay is bounded and non-durable: it holds no store. When a destination cannot take a message
 * the router refuses it and tells the sender its capacity is full, so the owning process keeps the
 * frame (spool/resend) rather than treating the send as an acknowledgement.
 */

import net from 'node:net';
import { existsSync, unlinkSync } from 'node:fs';

import { createChannel } from '../ipc.mjs';
import { IPC_VERSION } from '../ipc-message.mjs';

/** How long a probe waits for a live socket to answer before the path is called a stale leftover. */
const SOCKET_PROBE_TIMEOUT_MS = 250;

/** The roles that may announce themselves, and the frame route each one has. */
export const ROLE_ROUTES = Object.freeze({
  ingest: Object.freeze({ accept: 'book', tail_sealed: 'organize', resend: 'organize' }),
  organize: Object.freeze({
    durable_ack: 'ingest',
    drained: 'ingest',
    accepted: 'ingest',
    invalidate: 'book',
    resend: 'ingest',
  }),
  book: Object.freeze({ invalidated: 'organize', applied_ack: 'organize', resend: 'organize' }),
});

/** The frame route: ingest's frames are made durable by organize; organize's owed frames go to the book. */
export const ENVELOPE_ROUTES = Object.freeze({ ingest: 'organize', organize: 'book' });

const KNOWN_ROLES = new Set(Object.keys(ROLE_ROUTES));

/**
 * Build the router. Callbacks are observations only - none of them may change the verdict, and none
 * of them is handed a way to send as another role.
 */
export function createRouter({ onDiagnostic = () => {}, onRefusal = () => {}, onRouted = () => {}, onObserved = () => {} } = {}) {
  const bindings = new Map(); // channel -> { role, instance }
  const byRole = new Map(); // role -> channel
  const pendingAccepts = new Map(); // request_id -> { message, generation, connectionId }
  // Set 1 (observability): every refusal is counted by reason, the stored sample is bounded, and a
  // burst is reported as one rate-limited diagnostic rather than one line per refusal. What the
  // window leaves unreported is flushed when the router closes.
  const REFUSAL_SAMPLE_LIMIT = 100;
  const REFUSAL_REPORT_MIN_MS = 1000;
  const refusals = [];
  const refusalTally = new Map(); // reason -> count
  // Set 5: who has been told "full" and has not been told the room came back, and which destination
  // refused them. A sender that only ever hears "full" waits for a signal that never comes - and
  // waiting for its own next message cannot be the trigger, because it has none to send. The
  // destination's own drain is the event that ends the wait.
  const fullSignalled = new Map(); // sender channel -> the destination role that refused
  let refusalTotal = 0;
  let refusalBurst = 0;
  let lastRefusalReason = null;
  let refusalReportedAtMs = 0;
  let routedCount = 0;

  function diagnostic(reason, extra = {}) {
    try {
      onDiagnostic({ reason, ...extra });
    } catch {
      /* a diagnostic is best-effort by contract */
    }
  }

  function refuse(reason, extra = {}, tally = null) {
    const refusal = { reason, ...extra };
    refusalTotal += 1;
    refusalBurst += 1;
    lastRefusalReason = reason;
    // The tally groups by a fixed key when the message embeds a caller-supplied value (tally); the
    // value itself stays in the bounded sample only, so input streams cannot grow the tally's keys.
    const key = tally ?? reason;
    refusalTally.set(key, (refusalTally.get(key) ?? 0) + 1);
    refusals.push(refusal);
    if (refusals.length > REFUSAL_SAMPLE_LIMIT) refusals.shift();
    try {
      onRefusal(refusal);
    } catch {
      /* an observation that throws is not a fact about the refusal */
    }
    const nowMs = Date.now();
    if (nowMs - refusalReportedAtMs >= REFUSAL_REPORT_MIN_MS) {
      refusalReportedAtMs = nowMs;
      reportRefusalBurst('');
    }
    return { routed: false, refused: true, reason };
  }

  /** Report - and reset - whatever the reporting window is holding back. */
  function reportRefusalBurst(suffix) {
    if (refusalBurst === 0) return;
    const count = refusalBurst;
    refusalBurst = 0;
    diagnostic(`the relay refused ${count} message${count === 1 ? '' : 's'}${suffix}: ${lastRefusalReason}`, {
      refused: count,
    });
  }

  function noteRouted(to, kind) {
    routedCount += 1;
    try {
      onRouted({ to, kind });
    } catch {
      /* best-effort */
    }
  }

  /** The role a channel has announced, or null. */
  function roleOf(channel) {
    return bindings.get(channel)?.role ?? null;
  }

  function instanceOf(channel) {
    return bindings.get(channel)?.instance ?? null;
  }

  /** Bind a channel to a role, replacing - and closing - an older instance of the same role. */
  function bind(channel, role, instance) {
    const previous = byRole.get(role);
    if (previous && previous !== channel) {
      bindings.delete(previous);
      try {
        previous.close?.();
      } catch {
        /* the old socket may already be gone */
      }
    }
    bindings.set(channel, { role, instance });
    byRole.set(role, channel);
    return { role, instance };
  }

  /**
   * Tell a sender its relay is over the bound. This is a capacity signal, never an acknowledgement:
   * the owner keeps the frame and resends it when capacity returns.
   */
  function signalFull(channel, destRole) {
    if (!channel || typeof channel.sendControl !== 'function') return;
    fullSignalled.set(channel, destRole ?? null);
    try {
      channel.sendControl({
        version: IPC_VERSION,
        type: 'readiness',
        role_instance: 'supervisor',
        payload: { role: 'supervisor', capacity: 'full', ready: true },
      });
    } catch {
      /* a signal that cannot be sent is not a fact about the message */
    }
  }

  /**
   * The other half of the full signal: a message from this sender has been taken again, so the room
   * is back. Without it a sender that was told "full" would keep its frames held until some other
   * event - the soak's stuck drain was exactly that wait.
   */
  function signalRoom(channel) {
    if (!channel || typeof channel.sendControl !== 'function') return false;
    try {
      return (
        channel.sendControl({
          version: IPC_VERSION,
          type: 'readiness',
          role_instance: 'supervisor',
          payload: { role: 'supervisor', capacity: 'ok', ready: true },
        }) === true
      );
    } catch {
      return false; // the wait is kept, so a later drain or success can try again
    }
  }

  function forwardControl(destRole, message, fromChannel) {
    const target = byRole.get(destRole);
    if (!target) return refuse(`no ${destRole} is connected to the supervisor`, { type: message?.type });
    let ok = false;
    try {
      ok = target.sendControl(message);
    } catch (error) {
      diagnostic(`the ${destRole} relay threw: ${error.message}`, { type: message?.type });
      ok = false;
    }
    if (ok !== true) {
      signalFull(fromChannel, destRole);
      return refuse(`the ${destRole} relay could not take the message`, { type: message?.type });
    }
    if (fullSignalled.get(fromChannel) === destRole && signalRoom(fromChannel)) {
      fullSignalled.delete(fromChannel); // a notice that could not be sent keeps the wait alive
    }
    noteRouted(destRole, 'control');
    return { routed: true, to: destRole };
  }

  function forwardEnvelope(destRole, envelope, fromChannel) {
    const target = byRole.get(destRole);
    if (!target) return refuse(`no ${destRole} is connected to the supervisor`, frameIdentity(envelope));
    let ok = false;
    try {
      ok = target.sendEnvelope(envelope);
    } catch (error) {
      diagnostic(`the ${destRole} relay threw: ${error.message}`, { kind: 'envelope' });
      ok = false;
    }
    if (ok !== true) {
      signalFull(fromChannel, destRole);
      return refuse(`the ${destRole} relay could not take the frame`, frameIdentity(envelope));
    }
    if (fullSignalled.get(fromChannel) === destRole && signalRoom(fromChannel)) {
      fullSignalled.delete(fromChannel); // a notice that could not be sent keeps the wait alive
    }
    noteRouted(destRole, 'envelope');
    return { routed: true, to: destRole };
  }

  // ---------------------------------------------------------------------------------------------
  // The accept path: ingest issues, the book authorizes, organize adopts, ingest is let in.
  // ---------------------------------------------------------------------------------------------

  function routeAccept(message, channel) {
    if (typeof message.request_id !== 'string' || message.request_id.length === 0) {
      return refuse('an accept must carry a request id');
    }
    if (typeof message.connection_id !== 'string' || message.connection_id.length === 0) {
      return refuse('an accept must name a connection');
    }
    pendingAccepts.set(message.request_id, {
      message,
      generation: message.generation ?? null,
      connectionId: message.connection_id,
    });
    return forwardControl('book', message, channel);
  }

  function routeAccepted(message, channel, senderRole) {
    if (senderRole === 'organize') {
      // organize's answer is the adoption confirmation: it is what lets ingest open the socket. Only
      // a request this supervisor is holding may settle, so a stray confirmation is refused.
      const held = pendingAccepts.get(message.request_id);
      if (!held) {
        return refuse('an adoption confirmation arrived for a request the supervisor is not holding', {
          request_id: message.request_id,
        });
      }
      pendingAccepts.delete(message.request_id);
      return forwardControl('ingest', message, channel);
    }
    if (senderRole !== 'book') {
      return refuse(`a ${senderRole} may not answer an accept`, { type: message.type });
    }
    const held = pendingAccepts.get(message.request_id);
    if (!held) {
      return refuse('an authorization arrived for a request the supervisor is not holding', {
        request_id: message.request_id,
      });
    }
    if (message.payload?.accepted === true) {
      // The book authorized. The adopt is organize's, and it is the ONLY thing that may open ingest's
      // socket - the book's answer alone is deliberately not forwarded to ingest. The pending request
      // stays held until organize confirms adoption.
      return forwardControl('organize', message, channel);
    }
    if (message.payload?.accepted === false) {
      pendingAccepts.delete(message.request_id);
      return forwardControl('ingest', message, channel);
    }
    return refuse('an authorization must state accepted true or false', { request_id: message.request_id });
  }

  // ---------------------------------------------------------------------------------------------
  // Entry points. Each channel's callbacks land here; the channel identity is the only context.
  // ---------------------------------------------------------------------------------------------

  function handleControl(message, channel) {
    const type = message?.type;
    if (type === 'hello') {
      const role = message.payload?.role;
      if (!KNOWN_ROLES.has(role)) {
        return refuse(
          `a hello announced an unknown role ${JSON.stringify(role)}`,
          { role },
          'a hello announced an unknown role',
        );
      }
      bind(channel, role, message.role_instance);
      return { bound: role, instance: message.role_instance };
    }
    const senderRole = roleOf(channel);
    if (senderRole === null) {
      return refuse('a message arrived on a channel that has not announced a role', { type });
    }
    // readiness and error are observed by the supervisor itself; they are not relayed to a peer.
    if (type === 'readiness' || type === 'error') {
      // Stage 5c: hand the observation to the supervisor, which feeds a role's readiness report into
      // its aggregator (the report deadline is what lets a live-but-silent role lose readiness).
      try {
        onObserved({ type, from: senderRole, message });
      } catch {
        /* an observation that throws is not a fact about the message */
      }
      return { observed: type, from: senderRole };
    }
    if (type === 'accept') {
      if (senderRole !== 'ingest') return refuse(`a ${senderRole} may not issue an accept`);
      return routeAccept(message, channel);
    }
    if (type === 'accepted') {
      return routeAccepted(message, channel, senderRole);
    }
    const dest = ROLE_ROUTES[senderRole]?.[type];
    if (!dest) {
      return refuse(`a ${type} from ${senderRole} has no destination`, { type }, `a ${senderRole} message has no destination`);
    }
    return forwardControl(dest, message, channel);
  }

  /** The identity of the frame a refusal is about, for the record and the diagnostic. */
  function frameIdentity(envelope) {
    return {
      kind: 'envelope',
      market: envelope?.market ?? null,
      stream: envelope?.stream ?? null,
      connection_id: envelope?.connection_id ?? null,
      receive_seq: envelope?.receive_seq ?? null,
    };
  }

  function handleEnvelope(envelope, channel) {
    const senderRole = roleOf(channel);
    if (senderRole === null) {
      return refuse('a frame arrived on a channel that has not announced a role', frameIdentity(envelope));
    }
    const dest = ENVELOPE_ROUTES[senderRole];
    if (!dest) return refuse(`a ${senderRole} process has no route for frames`, frameIdentity(envelope));
    return forwardEnvelope(dest, envelope, channel);
  }

  function handleError(error, channel) {
    const role = roleOf(channel) ?? 'unknown';
    diagnostic(`the ${role} channel failed: ${error?.message ?? error}`, { role });
    return { error: true, role };
  }

  /** Record a freshly connected channel. Its role is learned from the hello that follows. */
  function attach(channel) {
    bindings.set(channel, { role: null, instance: null });
    return channel;
  }

  function detach(channel) {
    const binding = bindings.get(channel);
    bindings.delete(channel);
    fullSignalled.delete(channel);
    if (binding?.role && byRole.get(binding.role) === channel) byRole.delete(binding.role);
  }

  /**
   * A peer's link drained: it can take messages again. Every sender waiting on that role's capacity
   * hears the room is back - without this, the wait could only end through the sender's own next
   * message, and a sender that was told "full" has none to send.
   */
  function handlePeerDrain(channel) {
    const role = bindings.get(channel)?.role ?? null;
    let woken = 0;
    // The drained peer may be a destination (its waiters can be woken) or a waiting sender itself:
    // a notice this sender could not hear is retried now that its link is readable again. Without
    // this, a refused notice would wait for the destination's next drain, which may never come.
    if (fullSignalled.has(channel) && signalRoom(channel)) {
      fullSignalled.delete(channel);
      woken += 1;
    }
    if (role === null) return { woken };
    for (const [sender, destRole] of [...fullSignalled]) {
      if (destRole === role && signalRoom(sender)) {
        fullSignalled.delete(sender); // a notice that could not be sent is tried again later
        woken += 1;
      }
    }
    return { woken };
  }

  let closeServer = () => {};
  let socketPath = null;
  let closed = false;
  const router = {
    handleControl,
    handleEnvelope,
    handleError,
    handlePeerDrain,
    attach,
    detach,
    roleOf,
    instanceOf,
    channels: () => new Map(byRole),
    instances: () => new Map([...byRole].map(([role, channel]) => [role, bindings.get(channel)?.instance ?? null])),
    get pendingAcceptCount() {
      return pendingAccepts.size;
    },
    get routedCount() {
      return routedCount;
    },
    refusals: () => refusals.slice(),
    refusalSummary: () => ({ total: refusalTotal, byReason: Object.fromEntries(refusalTally) }),
    attachServer(server, path) {
      socketPath = path;
      closeServer = () => {
        try {
          server.close();
        } catch {
          /* the server may already be closed */
        }
        if (socketPath !== null) {
          try { unlinkSync(socketPath); } catch { /* already absent */ }
        }
      };
    },
    close() {
      if (closed) return;
      reportRefusalBurst(' more');
      closed = true;
      closeServer();
      for (const channel of bindings.keys()) {
        try {
          channel.close?.();
        } catch {
          /* the socket may already be gone */
        }
      }
      bindings.clear();
      byRole.clear();
    },
    get stats() {
      return {
        roles: [...byRole.keys()].sort(),
        routed: routedCount,
        refusals: refusalTotal,
        refusalsByReason: Object.fromEntries(refusalTally),
        pendingAccepts: pendingAccepts.size,
      };
    },
  };
  return router;
}

/** Open the supervisor's socket: the single rendezvous the three role processes connect to. */
/**
 * Whether a socket path has nobody listening on it (a leftover from a hard stop) rather than a live
 * listener. Only two answers mean dead: the path is gone (`ENOENT`), or the connection was refused
 * (`ECONNREFUSED`) - the kernel saying no process holds it. Anything else (a permission problem, a
 * listener that accepts but does not answer, an error we do not recognise) is treated as alive, so
 * the cleanup can never remove a socket that might still be somebody's.
 */
async function socketIsDead(path) {
  try {
    if (!existsSync(path)) return false;
  } catch {
    return false;
  }
  return await new Promise((resolve) => {
    const socket = net.connect(path);
    const settle = (dead) => {
      socket.removeAllListeners();
      socket.destroy();
      resolve(dead);
    };
    socket.once('connect', () => settle(false));
    socket.once('error', (error) => settle(error?.code === 'ENOENT' || error?.code === 'ECONNREFUSED'));
    socket.setTimeout(SOCKET_PROBE_TIMEOUT_MS, () => settle(false));
  });
}

export async function openRouter({ listenPath, channelOptions = {}, ...hooks } = {}) {
  if (!listenPath) throw new TypeError('the supervisor router needs a socket path to listen on');
  // A run that died without a graceful close (a SIGKILL, a power loss) leaves its socket file behind,
  // and binding to an existing path fails with EADDRINUSE - which would make a restart impossible
  // until a human removed the file. A stale socket is removed so a restart is possible; a socket
  // somebody is still listening on is left alone, because removing a live entrance's socket would
  // break that entrance to no purpose (a second start of the same entrance is a deployment mistake,
  // and it is the bind below - not this cleanup - that has to fail for it).
  if (await socketIsDead(listenPath)) {
    try {
      unlinkSync(listenPath);
    } catch {
      // Raced with something else removing it; the bind below is the authority either way.
    }
  }
  const router = createRouter(hooks);
  const server = net.createServer((socket) => {
    const channel = createChannel(socket, {
      ...channelOptions,
      onControl: (message) => router.handleControl(message, channel),
      onEnvelope: (envelope) => router.handleEnvelope(envelope, channel),
      onError: (error) => {
        router.handleError(error, channel);
        router.detach(channel);
      },
      onDrain: () => router.handlePeerDrain(channel),
    });
    socket.on('close', () => router.detach(channel));
    router.attach(channel);
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(listenPath, () => {
      server.removeListener('error', reject);
      resolve();
    });
  });
  router.attachServer(server, listenPath);
  router.path = listenPath;
  return router;
}
