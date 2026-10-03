/**
 * A fake WebSocket for the real-process through test (stage 5c).
 *
 * The role CLI resolves the websocket implementation exactly as `bin/receiver.mjs` does - Node's
 * global `WebSocket` - unless a module is named. This seam is what lets the through test feed frames
 * to a *real* child process without a real venue: the module speaks raw line-framed TCP to the test's
 * fake venue server, one message per line. It is deliberately not part of the product path.
 */

import net from 'node:net';

export default function FakeWebSocket(url) {
  const target = new URL(url);
  const socket = net.createConnection({ host: target.hostname, port: Number(target.port) });
  const self = { url, onopen: null, onmessage: null, onclose: null, onerror: null };
  let buffer = '';

  self.send = (message) => {
    try {
      socket.write(`${String(message)}\n`);
    } catch {
      /* the socket may already be gone */
    }
  };
  self.close = () => {
    try {
      socket.end();
    } catch {
      /* the socket may already be gone */
    }
  };

  socket.on('connect', () => self.onopen?.());
  socket.on('data', (chunk) => {
    buffer += chunk.toString('utf8');
    let index;
    while ((index = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, index);
      buffer = buffer.slice(index + 1);
      if (line.length > 0) self.onmessage?.({ data: line });
    }
  });
  socket.on('close', () => self.onclose?.());
  socket.on('error', (error) => self.onerror?.(error));

  return self;
}
