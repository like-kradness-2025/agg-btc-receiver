/**
 * A fake Bybit venue for run-level tests (test-support only).
 *
 * Line-framed TCP, one JSON message per line - the protocol `test-support/fake-websocket.mjs`
 * speaks. It answers each subscribe request with the topic's own verdict (the documented success
 * ack, or the venue's failure ack naming the topic), answers pings with a pong, and can push book
 * frames on demand. `ackDelayMs` delays every ack so a test can let startup admission complete
 * before the verdicts land (the order a real venue produces).
 *
 * Nothing here is imported by product code.
 */

import net from 'node:net';

export async function startBybitVenueServer({ refuseTopics = [], ackDelayMs = 0, symbol = 'BTCUSDT', bookDepth = 1000 } = {}) {
  let socket = null;
  let connections = 0;
  const server = net.createServer((connection) => {
    socket = connection;
    connections += 1;
    connection.on('error', () => {});
    connection.on('data', (chunk) => {
      for (const line of chunk.toString('utf8').split('\n')) {
        if (line.length === 0) continue;
        let message;
        try {
          message = JSON.parse(line);
        } catch {
          continue;
        }
        if (message.op === 'subscribe') {
          const topic = message.req_id;
          const respond = () => {
            const refused = refuseTopics.includes(topic);
            connection.write(
              `${JSON.stringify(
                refused
                  ? { success: false, ret_msg: `error:handler not found,topic:${topic}`, req_id: topic, op: 'subscribe' }
                  : { success: true, ret_msg: '', req_id: topic, op: 'subscribe' },
              )}\n`,
            );
          };
          if (ackDelayMs > 0) setTimeout(respond, ackDelayMs);
          else respond();
        } else if (message.op === 'ping') {
          connection.write(
            `${JSON.stringify({ success: true, ret_msg: 'pong', conn_id: 'fake-conn', req_id: '', op: 'ping' })}\n`,
          );
        }
      }
    });
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  const bookTopic = `orderbook.${bookDepth}.${symbol}`;
  const send = (frame) => socket.write(`${JSON.stringify(frame)}\n`);
  return {
    port,
    get connected() {
      return socket !== null && !socket.destroyed;
    },
    get connections() {
      return connections;
    },
    sendSnapshot(u = 100) {
      send({
        topic: bookTopic,
        type: 'snapshot',
        ts: 1,
        cts: 1,
        data: { s: symbol, b: [['100.0', '1.0']], a: [['101.0', '1.0']], u, seq: 1 },
      });
    },
    sendDelta(u = 101) {
      send({ topic: bookTopic, type: 'delta', ts: 1, data: { s: symbol, b: [['100.0', '2.0']], a: [], u, seq: 2 } });
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
