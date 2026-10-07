/**
 * A tiny, real WebSocket server that speaks Kraken's frames (test-support only).
 *
 * The process entrance runs the *production* path: the role children resolve Node's global
 * `WebSocket` and the built-in venue adapter, and no module seam is offered through the config (a
 * test-only key in the product config is exactly what the design forbids). So an entry-level test
 * that wants a run to actually receive needs a venue the global `WebSocket` can really connect to: a
 * real RFC 6455 server. This implements just enough of the protocol - the handshake, server-to-client
 * text frames, and masked client frames read far enough to notice a `subscribe` - and then answers
 * like Kraken: a `subscriptionStatus` per subscription, a book snapshot, and updates.
 *
 * Nothing here is imported by product code.
 */

import net from 'node:net';
import { createHash } from 'node:crypto';

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const PAIR = 'XBT/USD';

function acceptFor(key) {
  return createHash('sha1')
    .update(`${key}${GUID}`)
    .digest('base64');
}

/** Frame one server-to-client text message. No masking (only clients mask). */
function textFrame(text) {
  const payload = Buffer.from(text, 'utf8');
  const length = payload.length;
  let header;
  if (length < 126) {
    header = Buffer.from([0x81, length]);
  } else if (length < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x81;
    header[1] = 126;
    header.writeUInt16BE(length, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x81;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(length), 2);
  }
  return Buffer.concat([header, payload]);
}

/** Read one masked client frame from `buffer`; returns {opcode, payload, rest} or null if incomplete. */
function readFrame(buffer) {
  if (buffer.length < 2) return null;
  const first = buffer[0];
  const opcode = first & 0x0f;
  const masked = (buffer[1] & 0x80) !== 0;
  let length = buffer[1] & 0x7f;
  let offset = 2;
  if (length === 126) {
    if (buffer.length < 4) return null;
    length = buffer.readUInt16BE(2);
    offset = 4;
  } else if (length === 127) {
    if (buffer.length < 10) return null;
    length = Number(buffer.readBigUInt64BE(2));
    offset = 10;
  }
  const maskLength = masked ? 4 : 0;
  if (buffer.length < offset + maskLength + length) return null;
  const mask = masked ? buffer.subarray(offset, offset + 4) : null;
  const start = offset + maskLength;
  const payload = Buffer.from(buffer.subarray(start, start + length));
  if (mask) for (let i = 0; i < payload.length; i += 1) payload[i] ^= mask[i % 4];
  return { opcode, payload, rest: buffer.subarray(start + length) };
}

const bookFrame = (seq) =>
  seq === 1
    ? JSON.stringify([1234, { bs: [['100.0', '1.0', '1.0']], as: [['101.0', '1.0', '1.0']], c: '1' }, 'book-1000', PAIR])
    : JSON.stringify([1234, { b: [[`${100 + seq}.0`, '1.0', '1.0']], a: [] }, 'book-1000', PAIR]);

export async function startVenueServer({ subscriptionStatus = 'subscribed', errorMessage = 'denied' } = {}) {
  let socket = null;
  const server = net.createServer((connection) => {
    socket = connection;
    let handshaken = false;
    let buffer = Buffer.alloc(0);
    connection.on('error', () => {});
    connection.on('data', (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      if (!handshaken) {
        const end = buffer.indexOf('\r\n\r\n');
        if (end < 0) return;
        const headers = buffer.subarray(0, end).toString('utf8');
        const key = /Sec-WebSocket-Key:\s*(.+)\r?\n/i.exec(headers)?.[1]?.trim();
        buffer = buffer.subarray(end + 4);
        handshaken = true;
        connection.write(
          'HTTP/1.1 101 Switching Protocols\r\n' +
            'Upgrade: websocket\r\n' +
            'Connection: Upgrade\r\n' +
            `Sec-WebSocket-Accept: ${acceptFor(key)}\r\n\r\n`,
        );
      }
      for (;;) {
        const frame = readFrame(buffer);
        if (frame === null) break;
        buffer = frame.rest;
        if (frame.opcode === 0x8) {
          connection.end();
          return;
        }
        if (frame.opcode === 0x1) {
          let message;
          try {
            message = JSON.parse(frame.payload.toString('utf8'));
          } catch {
            continue;
          }
          if (message?.method === 'subscribe') {
            const channel = message.params?.channel;
            const pair = message.params?.symbol?.[0] ?? PAIR;
            connection.write(
              textFrame(
                JSON.stringify({
                  method: 'subscribe',
                  result: {
                    channel,
                    symbol: [pair],
                    success: subscriptionStatus === 'subscribed',
                    ...(subscriptionStatus === 'subscribed' ? {} : { error: errorMessage }),
                  },
                }),
              ),
            );
          } else if (message?.event === 'subscribe') {
            const name = message.subscription?.name;
            const pair = message.pair?.[0] ?? PAIR;
            connection.write(
              textFrame(
                JSON.stringify({
                  event: 'subscriptionStatus',
                  status: subscriptionStatus,
                  pair: [pair],
                  subscription: { name },
                  ...(subscriptionStatus === 'subscribed' ? {} : { errorMessage }),
                }),
              ),
            );
          }
        }
      }
    });
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  return {
    port,
    get connected() {
      return socket !== null && !socket.destroyed;
    },
    sendFrame(seq) {
      socket.write(textFrame(bookFrame(seq)));
    },
    close() {
      server.close();
      try {
        socket?.destroy();
      } catch {
        /* already gone */
      }
    },
  };
}
