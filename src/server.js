'use strict';

/**
 * WebSocket fan-out server — one instance of a horizontally-scalable fleet.
 *
 * Architecture:
 *
 *   publisher ──▶ Redis pub/sub ──▶ every instance ──▶ local subscribers only
 *
 * Each instance holds its own client connections and subscribes to ONE Redis
 * pattern channel. A published event reaches every instance once, and each
 * instance fans it out only to its own sockets that subscribed to that logical
 * channel. Adding capacity = adding instances behind the load balancer; no
 * instance ever needs to know about another instance's sockets.
 *
 * Client protocol (JSON over WS):
 *   → {"action":"subscribe","channel":"product:42"}
 *   → {"action":"unsubscribe","channel":"product:42"}
 *   ← {"type":"subscribed","channel":"product:42"}
 *   ← {"type":"event","channel":"product:42","data":{...},"publishedAt":1699999999999}
 */

const http = require('http');
const { WebSocketServer } = require('ws');
const Redis = require('ioredis');

const config = require('./config');
const { Hub } = require('./hub');
const { Metrics } = require('./metrics');

const hub = new Hub(config.backpressure && {
  maxBufferedBytes: config.backpressure.maxBufferedBytes,
  dropLimit: config.backpressure.dropLimit,
  maxSubscriptionsPerConnection: config.limits.maxSubscriptionsPerConnection,
});
const metrics = new Metrics({ instanceId: config.instanceId });

// ── Redis: one connection for subscribing (a subscriber conn is exclusive) ──
const sub = new Redis({ ...config.redis, retryStrategy: (t) => Math.min(t * 100, 3000) });

sub.on('error', (err) => console.error(JSON.stringify({ level: 'error', msg: 'redis error', error: err.message })));

// All logical channels ride on one Redis channel; the envelope carries the
// logical channel name. One psubscribe would also work — a single channel
// keeps the Redis side O(1) regardless of how many logical channels exist.
const REDIS_FANOUT_CHANNEL = config.channelPrefix + 'all';

sub.subscribe(REDIS_FANOUT_CHANNEL).then(() => {
  log('info', `subscribed to ${REDIS_FANOUT_CHANNEL}`);
});

sub.on('message', (_redisChannel, raw) => {
  let envelope;
  try {
    envelope = JSON.parse(raw);
  } catch {
    metrics.inc('malformedEnvelopes');
    return;
  }
  const { channel } = envelope;
  if (!channel) return;

  // Serialize once, deliver many.
  const payload = JSON.stringify({
    type: 'event',
    channel,
    data: envelope.data,
    publishedAt: envelope.publishedAt,
    relayedBy: config.instanceId,
    relayedAt: Date.now(),
  });

  const { delivered, dropped } = hub.broadcast(channel, payload);
  metrics.add('delivered', delivered);
  metrics.add('dropped', dropped);
});

// ── HTTP server: health + metrics + WS upgrade ─────────────────────────────
const server = http.createServer((req, res) => {
  if (req.url === '/healthz') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, instance: config.instanceId }));
    return;
  }
  if (req.url === '/metrics') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ...metrics.snapshot(), hub: hub.snapshot() }, null, 2));
    return;
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({
  server,
  maxPayload: config.limits.maxPayloadBytes,
  // perMessageDeflate off: compression costs CPU per-frame and hurts p99 at
  // high connection counts; our payloads are small JSON.
  perMessageDeflate: false,
});

wss.on('connection', (socket) => {
  metrics.inc('connectionsTotal');
  hub.addClient(socket);
  socket.isAlive = true;

  socket.on('pong', () => { socket.isAlive = true; });

  socket.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      socket.send(JSON.stringify({ type: 'error', error: 'invalid JSON' }));
      return;
    }

    if (msg.action === 'subscribe' && typeof msg.channel === 'string') {
      const result = hub.subscribe(socket, msg.channel);
      socket.send(JSON.stringify(result.ok
        ? { type: 'subscribed', channel: msg.channel }
        : { type: 'error', error: result.error }));
    } else if (msg.action === 'unsubscribe' && typeof msg.channel === 'string') {
      hub.unsubscribe(socket, msg.channel);
      socket.send(JSON.stringify({ type: 'unsubscribed', channel: msg.channel }));
    } else if (msg.action === 'ping') {
      socket.send(JSON.stringify({ type: 'pong', t: msg.t ?? null }));
    } else {
      socket.send(JSON.stringify({ type: 'error', error: 'unknown action' }));
    }
  });

  socket.on('close', () => hub.removeClient(socket));
  socket.on('error', () => hub.removeClient(socket));
});

// ── Heartbeat: reap dead connections (mobile clients vanish without FIN) ───
const heartbeat = setInterval(() => {
  for (const socket of wss.clients) {
    if (socket.isAlive === false) {
      metrics.inc('reapedConnections');
      socket.terminate();
      continue;
    }
    socket.isAlive = false;
    socket.ping();
  }
}, config.heartbeat.intervalMs);

// ── Graceful shutdown ───────────────────────────────────────────────────────
function shutdown(signal) {
  log('info', `${signal} received — draining`);
  clearInterval(heartbeat);
  for (const socket of wss.clients) {
    socket.close(1001, 'server shutting down');
  }
  wss.close(() => {
    server.close(() => {
      sub.quit().finally(() => process.exit(0));
    });
  });
  // Hard exit if draining hangs
  setTimeout(() => process.exit(1), 10_000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

function log(level, msg) {
  console.log(JSON.stringify({ level, instance: config.instanceId, msg, t: new Date().toISOString() }));
}

server.listen(config.port, () => {
  log('info', `listening on :${config.port}`);
});
