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
 * The boundary proof is Kraken's published book checksum, and it is not a number that goes up. It is a
 * CRC32 over the top ten levels, formatted the venue's way, and it only means anything if the levels
 * are held with the exact precision they arrived in - so the adapter keeps its own mirror of the top of
 * the book, built from the frames' own strings, and answers `connects` before the book commits
 * anything. The numeric rows the book can hand the rule are not enough: "0.10000000" and 0.1 format to
 * different checksum inputs, which is the whole reason this mirror exists.
 *
 * `changesFor` is what turns a venue payload into board changes. It is deliberately the only place
 * that knows how this venue expresses a level, so the book stays venue-agnostic - and it is not the
 * place the mirror is kept, because a mirror that moved while a frame was still being judged could not
 * answer honestly.
 */

const IGNORED_EVENTS = new Set(['systemStatus', 'heartbeat', 'pong', 'ping']);

/**
 * The CRC32 Kraken's rule ends in, over the zlib polynomial (0xEDB88320), reflected, init and final
 * xor all-ones - the same function `zlib.crc32` gives. The table is built once per process.
 */
const CRC32_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n += 1) {
    let c = n;
    for (let k = 0; k < 8; k += 1) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[n] = c >>> 0;
  }
  return table;
})();

function crc32(text) {
  let crc = 0xffffffff;
  for (let i = 0; i < text.length; i += 1) crc = (crc >>> 8) ^ CRC32_TABLE[(crc ^ text.charCodeAt(i)) & 0xff];
  return (crc ^ 0xffffffff) >>> 0;
}

/** The rule's own formatting, kept verbatim: drop the `.`, then drop leading zeros. Trailing zeros stay. */
const plain = (value) => value.replace(/\./g, '').replace(/^0+/, '');

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

  // The precision-preserving mirror of the top of the book, held in the venue's own strings. It is
  // keyed by numeric price so an update can find and remove the level it names, but the strings it
  // stores are the ones that arrived, because the checksum is sensitive to their formatting. It is
  // truncated to the subscribed depth after every frame, exactly as the venue's own book is.
  const mirror = { bids: new Map(), asks: new Map() };
  let mirrorSeeded = false;
  let mirrorConnectionId = null;

  /**
   * The book half of one frame, as the rule has to see it: the levels with their exact strings, the
   * checksum the frame carries, and whether it is a snapshot (a whole book) or an update. `null` means
   * the frame is not a book frame at all - a trade, say - and there is nothing here for the proof to
   * judge or for the mirror to hold.
   */
  function bookFrameOf(envelope) {
    const raw = envelope?.raw;
    const text = Buffer.isBuffer(raw) ? raw.toString('utf8') : String(raw ?? '');
    let data;
    try {
      data = JSON.parse(text);
    } catch {
      return null;
    }
    const payloads = Array.isArray(data)
      ? channelOf(data).payloads
      : data && typeof data === 'object' && (data.as || data.bs || data.a || data.b)
        ? [data]
        : [];
    if (payloads.length === 0) return null;

    let checksum = null;
    let snapshot = false;
    let book = false;
    const levels = [];
    for (const payload of payloads) {
      if (!payload || typeof payload !== 'object') continue;
      if (payload.c != null && checksum === null) checksum = String(payload.c);
      if (payload.as || payload.bs || payload.a || payload.b) book = true;
      if (payload.as !== undefined || payload.bs !== undefined) snapshot = true;
      for (const [side, list] of [
        ['bid', payload.b ?? payload.bs],
        ['ask', payload.a ?? payload.as],
      ]) {
        if (!Array.isArray(list)) continue;
        for (const level of list) {
          if (!Array.isArray(level) || level.length < 2) continue;
          const price = String(level[0]);
          const qty = String(level[1]);
          if (price === '' || qty === '') continue;
          levels.push({ side, price, qty });
        }
      }
    }
    if (!book) return null;
    return { checksum, snapshot, levels };
  }

  /** Keep only the depth the subscription carries, from the good end: dearest bids, cheapest asks. */
  function keepTop(map, byPrice) {
    if (map.size <= bookDepth) return;
    const ordered = [...map.keys()].sort(byPrice);
    for (const key of ordered.slice(bookDepth)) map.delete(key);
  }

  /** Apply a frame's levels to a book (the mirror, or a copy of it), a zero qty meaning a removal. */
  function applyFrameTo(book, frame) {
    if (frame.snapshot) {
      book.bids.clear();
      book.asks.clear();
    }
    for (const { side, price, qty } of frame.levels) {
      const key = Number(price);
      if (!Number.isFinite(key)) continue;
      const map = side === 'bid' ? book.bids : book.asks;
      if (Number(qty) === 0) map.delete(key);
      else map.set(key, { price, qty });
    }
    keepTop(book.bids, (a, b) => b - a);
    keepTop(book.asks, (a, b) => a - b);
  }

  /** The mirror as the frame would leave it, without moving the mirror itself. */
  function candidateMirror(frame) {
    const candidate = { bids: new Map(mirror.bids), asks: new Map(mirror.asks) };
    applyFrameTo(candidate, frame);
    return candidate;
  }

  /**
   * The exact string Kraken's rule hashes: the top ten asks, cheapest first, then the top ten bids,
   * dearest first, each level's price and qty stripped the venue's way and simply appended.
   */
  function checksumInput(book) {
    const side = (map, name) =>
      [...map.values()]
        .sort((a, b) => (name === 'ask' ? Number(a.price) - Number(b.price) : Number(b.price) - Number(a.price)))
        .slice(0, 10)
        .map((level) => plain(level.price) + plain(level.qty))
        .join('');
    return side(book.asks, 'ask') + side(book.bids, 'bid');
  }

  return {
    url: url ?? 'wss://ws.kraken.com',
    stream,

    // The proof this venue can give: a CRC32 over the top of the book, not a sequence. Declared here
    // so the book resolves it once and never has to interpret the frames to find out.
    boundary: 'checksum',

    // C3: Kraken answers every subscribe with a `subscriptionStatus` event, so establishment is explicit.
    // The expected set is the two subscriptions this adapter sends; their keys are the ones `parse()`
    // builds from the same name and pair, so an acknowledgement is matched to the request it answers.
    ackMode: 'explicit',
    expectedSubscriptions: () => [`book:${symbol}`, `trade:${symbol}`],

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

    /**
     * Kraken's published book checksum, applied before anything is committed.
     *
     * The frame's own levels are read from `current.raw` and applied to a copy of the mirror; the copy
     * is formatted and hashed the venue's way, and the result is compared with the checksum the frame
     * carries. Only a match integrates the frame into the mirror - a frame that is refused leaves the
     * mirror exactly where it was, which is the only way the next frame's checksum can still be judged.
     *
     * A frame that is not a book frame at all (a trade) has nothing to prove and is let through
     * without touching the mirror. A book frame that carries no checksum at all is refused: with no
     * way to judge it, believing it would be claiming a proof that was never made - fail-closed.
     */
    connects({ previous = null, current = null } = {}) {
      const frame = bookFrameOf(current);
      if (frame === null) return true; // not a book frame: nothing to prove, nothing the mirror holds

      // A different connection is a different book: what the mirror holds describes a range that has
      // ended, and the next frame of the new connection is judged against the new book, not its own.
      const connectionId = current?.connection_id ?? null;
      if (
        mirrorSeeded &&
        connectionId !== null &&
        mirrorConnectionId !== null &&
        connectionId !== mirrorConnectionId
      ) {
        mirror.bids.clear();
        mirror.asks.clear();
        mirrorSeeded = false;
      }

      // No seeding from a `previous` frame: the rule now judges the first book frame of a range too, and
      // that frame is judged against the mirror the frame itself produces - which is exactly what a
      // snapshot proves. A mirror carried over from a frame the rule never saw would be a memory the
      // book does not know the rule depends on, and it would make the first frame's own proof meaningless.
      if (frame.checksum === null) return false; // fail-closed: no checksum, no proof

      const candidate = candidateMirror(frame);
      if (frame.checksum !== String(crc32(checksumInput(candidate)))) return false;

      applyFrameTo(mirror, frame);
      mirrorSeeded = true;
      mirrorConnectionId = connectionId ?? mirrorConnectionId;
      return true;
    },

    // Kraken book frames carry a checksum rather than a sequence number, so there is no venue
    // sequence to record. Saying so is better than filling the field with an invented value.
    venueSeqOf: () => null,
  };
}
