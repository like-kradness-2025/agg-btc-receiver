/**
 * Kraken: the venue-specific half of reception, transplanted from the connector that runs today.
 *
 * Everything here is fact taken from the working implementation rather than from documentation: the
 * subscribe payloads are the ones this system already sends, the ignored events are the ones it
 * already ignores, and the book framing (a channel name inside an array, `as`/`bs` for a snapshot,
 * `a`/`b` for an update, `c` for the checksum) is how the frames actually arrive.
 *
 * Two things worth writing down because they are easy to get wrong:
 *  - book frames carry no exchange timestamp, so there is nothing to put in source_event_ts_ms. The
 *    raw is stamped at the socket boundary by reception, and the venue's own time is simply absent
 *    rather than invented.
 *  - the depth is subscribed at 1000, so a level leaving the subscribed range shows up as a size of
 *    zero rather than a removal message. Nothing here assumes a removal that never comes.
 *
 * `changesFor` is what turns a venue payload into board changes. It is deliberately the only place
 * that knows how this venue expresses a level, so the book stays venue-agnostic.
 */

const IGNORED_EVENTS = new Set(['systemStatus', 'heartbeat', 'pong', 'ping']);

/** A Kraken array frame names its channel inside the array; the payload objects follow it. */
function channelOf(frame) {
  const index = frame.findIndex(
    (value, position) => position > 0 && typeof value === 'string' && value.startsWith('book-'),
  );
  if (index < 0) return { channel: null, payloads: [] };
  return {
    channel: frame[index],
    payloads: frame.slice(1, index).filter((part) => part && typeof part === 'object' && !Array.isArray(part)),
  };
}

const isTradeFrame = (frame) => {
  const index = frame.findIndex((value, position) => position > 0 && typeof value === 'string');
  return index > 0 && frame.slice(1, index).some((part) => part && typeof part === 'object' && (part.length || part.price));
};

/** [[price, size, ts], ...] -> board changes. A size of zero is a level leaving the book. */
function levelsToChanges(levels, side) {
  if (!Array.isArray(levels)) return [];
  return levels
    .map((level) => {
      if (!Array.isArray(level) || level.length < 2) return null;
      const price = Number(level[0]);
      const size = Number(level[1]);
      if (!Number.isFinite(price) || !Number.isFinite(size)) return null;
      return { side, price, size };
    })
    .filter(Boolean);
}

export function createKrakenAdapter({ market = 'kraken_spot', symbol, bookDepth = 1000, url = null } = {}) {
  if (!symbol) throw new TypeError('the Kraken adapter needs a symbol/pair');
  const stream = 'trades';
  return {
    url: url ?? 'wss://ws.kraken.com',
    stream,

    subscribeMessages: () => [
      JSON.stringify({ event: 'subscribe', pair: [symbol], subscription: { name: 'book', depth: bookDepth } }),
      JSON.stringify({ event: 'subscribe', pair: [symbol], subscription: { name: 'trade' } }),
    ],

    // Kraken sends its own heartbeats; there is nothing for us to send, and inventing a ping the
    // venue does not expect would be a change in behaviour rather than a transplant.
    heartbeatMessage: () => null,

    /**
     * Classify one frame. The kinds are the ones reception understands: data, heartbeat,
     * subscription, shutdown. Anything unrecognised returns null and is counted by reception rather
     * than guessed at.
     */
    parse(raw) {
      const text = typeof raw === 'string' ? raw : raw?.toString?.('utf8') ?? '';
      let data;
      try {
        data = JSON.parse(text);
      } catch (error) {
        return null; // not JSON: reception records an unparsable frame
      }

      if (!Array.isArray(data)) {
        if (data.event === 'subscriptionStatus') {
          const ok = data.status === 'subscribed';
          const key = `${data.subscription?.name ?? 'unknown'}:${(data.pair ?? []).join(',') || '*'}`;
          return { kind: 'subscription', key, ok, detail: data.errorMessage ?? data.status ?? '' };
        }
        if (data.event === 'error') {
          // An error frame is reported as a failed subscription rather than dropped: it is the venue
          // saying something went wrong, and that has to leave a trace.
          return {
            kind: 'subscription',
            key: 'error',
            ok: false,
            detail: data.errorMessage ?? JSON.stringify(data),
          };
        }
        if (IGNORED_EVENTS.has(data.event)) return { kind: 'heartbeat', answered: data.event === 'pong' };
        const hasBook = Boolean(data.as || data.bs || data.a || data.b);
        if (hasBook) return { kind: 'data' };
        return null;
      }

      const { channel } = channelOf(data);
      if (channel) return { kind: 'data' };
      if (isTradeFrame(data)) return { kind: 'data' };
      return null;
    },

    /**
     * The board changes a frame carries. Snapshots and updates both produce level changes, and a
     * trade frame produces none: trades do not move this book, which is how the current system
     * behaves and therefore how this one must.
     */
    changesFor(envelope) {
      const raw = envelope?.raw;
      const text = Buffer.isBuffer(raw) ? raw.toString('utf8') : String(raw ?? '');
      let data;
      try {
        data = JSON.parse(text);
      } catch {
        return [];
      }
      const payloads = Array.isArray(data) ? channelOf(data).payloads : [data];
      const changes = [];
      for (const payload of payloads) {
        if (!payload || typeof payload !== 'object') continue;
        changes.push(...levelsToChanges(payload.b ?? payload.bs, 'bid'));
        changes.push(...levelsToChanges(payload.a ?? payload.as, 'ask'));
      }
      return changes;
    },

    // Kraken book frames carry a checksum rather than a sequence number, so there is no venue
    // sequence to record. Saying so is better than filling the field with an invented value.
    venueSeqOf: () => null,
  };
}
