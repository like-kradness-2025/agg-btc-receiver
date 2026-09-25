/**
 * Bitfinex: the venue-specific half of reception, written against the official websocket docs.
 *
 * What the documentation settles, quoted from docs.bitfinex.com/docs/ws-general:
 *  - a subscription is answered with {"event":"subscribed","chanId":CHANNEL_ID}, and "chanId is a
 *    numeric channel identifier that the developer can use to distinguish between updates";
 *  - an unsubscribe is answered with "unsubscribed"; 10301 means "Already subscribed" and 10401
 *    "Not subscribed", which are states rather than failures;
 *  - info messages carry the connection's state as numeric codes, with an explicit instruction:
 *    "Only rely on 'CODE' for 'info' events". 20051 is "Stop/Restart Websocket Server (please
 *    reconnect)", 20060 is "Entering in Maintenance mode. Please pause any activity", and 20061 is
 *    "Maintenance ended. You can resume normal activity. It is advised to unsubscribe/subscribe
 *    again all channels."
 *  - a ping may be sent to test the connection; the server answers with a pong.
 *
 * The codes are compared as numbers for exactly the reason the docs give: the text is documentation
 * for humans and can be reworded, the code is the protocol.
 *
 * Book updates arrive as [chanId, [[price, count, amount], ...]] where a positive amount is a bid, a
 * negative amount is an ask, and a count of zero means the level is gone. That is the venue's way of
 * expressing a removal, and it is translated to the size-zero change the book understands.
 */

const INFO_RECONNECT = 20051;
const INFO_MAINTENANCE_START = 20060;
const INFO_MAINTENANCE_END = 20061;
const ALREADY_SUBSCRIBED = 10301; // documented as a state, not a failure

export function createBitfinexAdapter({
  market = 'bitfinex_spot',
  symbol = 'tBTCUSD',
  bookPrecision = 'P0',
  bookFrequency = 'F0',
  bookLength = '25',
  url = 'wss://api-pub.bitfinex.com/ws/2',
} = {}) {
  const stream = 'trades';
  return {
    url,
    stream,

    subscribeMessages: () => [
      JSON.stringify({
        event: 'subscribe',
        channel: 'book',
        symbol,
        prec: bookPrecision,
        freq: bookFrequency,
        len: bookLength,
      }),
      JSON.stringify({ event: 'subscribe', channel: 'trades', symbol }),
    ],

    // The docs allow a ping to test the connection, and the server answers with a pong.
    heartbeatMessage: () => JSON.stringify({ event: 'ping' }),

    parse(raw) {
      const text = typeof raw === 'string' ? raw : raw?.toString?.('utf8') ?? '';
      let data;
      try {
        data = JSON.parse(text);
      } catch {
        return null;
      }

      if (!Array.isArray(data)) {
        if (data.event === 'subscribed') {
          return { kind: 'subscription', key: `${data.channel}:${data.symbol ?? '*'}:${data.chanId}`, ok: true };
        }
        if (data.event === 'unsubscribed') {
          return { kind: 'subscription', key: `${data.chanId}`, ok: false, detail: 'unsubscribed' };
        }
        if (data.event === 'error') {
          // 10301 means the subscription already exists, which is the state we wanted anyway.
          const already = Number(data.code) === ALREADY_SUBSCRIBED;
          return {
            kind: 'subscription',
            key: data.chanId ? String(data.chanId) : 'error',
            ok: already,
            detail: `${data.code ?? ''} ${data.msg ?? ''}`.trim(),
          };
        }
        if (data.event === 'info') {
          const code = Number(data.code);
          if (code === INFO_RECONNECT) {
            return { kind: 'shutdown', detail: `info ${code}` };
          }
          if (code === INFO_MAINTENANCE_START) {
            // Pause, do not reconnect: the docs say to resume after 20061 arrives.
            return { kind: 'maintenance', detail: `info ${code}`, resume: false };
          }
          if (code === INFO_MAINTENANCE_END) {
            return { kind: 'maintenance', detail: `info ${code}`, resume: true };
          }
          return { kind: 'heartbeat', answered: false };
        }
        if (data.event === 'pong') return { kind: 'heartbeat', answered: true };
        return null;
      }

      // Array frames are addressed by channel id: [chanId, "hb"], [chanId, [...levels]], [chanId,
      // "tu", [...]] and so on.
      const [, body] = data;
      if (body === 'hb') return { kind: 'heartbeat', answered: false };
      if (typeof body === 'string') {
        // te/tu are trade frames, and trades do not move this book.
        if (body === 'te' || body === 'tu') return { kind: 'data', trade: true };
        return null;
      }
      if (Array.isArray(body)) {
        if (body.length > 0 && Array.isArray(body[0])) return { kind: 'data', book: true };
        if (body.length > 0 && typeof body[0] === 'number' && data.length >= 3 && typeof data[2] === 'string') {
          return { kind: 'data', trade: true }; // [chanId, [id, mts, amount, price], "te"]
        }
      }
      return null;
    },

    /**
     * Board changes from a book frame. Positive amount is a bid, negative is an ask, and a count of
     * zero is the venue saying the level is gone - expressed here as the size-zero change the book
     * already understands.
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
      if (!Array.isArray(data)) return [];
      const body = data[1];
      if (!Array.isArray(body)) return [];

      // A snapshot is a list of levels, each of them a list; an update is one level, which is a list
      // of numbers. The difference is what sits at position zero, not how deep the nesting goes.
      const levels = Array.isArray(body[0]) ? body : [body];
      const changes = [];
      for (const level of levels) {
        if (!Array.isArray(level) || level.length < 3) continue;
        const price = Number(level[0]);
        const count = Number(level[1]);
        const amount = Number(level[2]);
        if (!Number.isFinite(price) || !Number.isFinite(amount)) continue;
        changes.push({
          side: amount > 0 ? 'bid' : 'ask',
          price,
          size: count === 0 ? 0 : Math.abs(amount),
        });
      }
      return changes;
    },

    venueSeqOf: () => null, // Bitfinex book frames carry no sequence number
  };
}
