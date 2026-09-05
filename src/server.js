'use strict';

/**
 * WebSocket fan-out server — one instance of a horizontally-scalable fleet.
 *
 * Architecture:
 *
 *   publisher ──▶ Redis pub/sub ──▶ every instance ──▶ local subscribers only
 *
 * Each instance holds its own client connections and subscribes to ONE Redis
 * literal Redis channel. Connected subscribers fan events out to their own
 * sockets subscribed to that logical channel. Disconnected instances miss
 * events; adding processes does not by itself prove a capacity gain.
 * Instances do not know about another instance's sockets.
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
const { Readiness } = require('./readiness');
const { validMessage, validEnvelope, TokenBucket } = require('./protocol');

const hub = new Hub(config.backpressure && {
  maxBufferedBytes: config.backpressure.maxBufferedBytes,
  dropLimit: config.backpressure.dropLimit,
  maxSubscriptionsPerConnection: config.limits.maxSubscriptionsPerConnection,
});
const metrics = new Metrics({ instanceId: config.instanceId });

// ── Redis: one connection for subscribing (a subscriber conn is exclusive) ──
const sub = new Redis({ ...config.redis, retryStrategy: (t) => Math.min(t * 100, 3000),
  autoResubscribe: false, autoResendUnfulfilledCommands: false, enableOfflineQueue: false,
  maxRetriesPerRequest: 0, commandTimeout: config.readiness.timeoutMs });

sub.on('error', () => { metrics.inc('redisErrors'); log('error', 'redis error', { event: 'redis-error' }); });
sub.on('reconnecting', () => metrics.inc('redisReconnects'));

// All logical channels ride on one Redis channel; the envelope carries the
// logical channel name. One psubscribe would also work — a single channel
// keeps the Redis side O(1) regardless of how many logical channels exist.
const REDIS_FANOUT_CHANNEL = config.channelPrefix + 'all';

const readiness = new Readiness(sub, REDIS_FANOUT_CHANNEL, {
  intervalMs: config.readiness.intervalMs,
  onFailure: () => metrics.inc('redisCheckFailures'),
  onTransition: (state) => {
    metrics.inc('readinessTransitions');
    if (state.ready) metrics.inc('redisSubscriptionAcks');
    log('info', state.ready ? `subscribed to ${REDIS_FANOUT_CHANNEL}` : 'readiness changed', { event: 'readiness', ...state });
  },
});

sub.on('message', (_redisChannel, raw) => {
  let envelope, payload;
  try {
    if (Buffer.byteLength(raw) > config.limits.maxEnvelopeBytes) throw new Error('oversized envelope');
    envelope = JSON.parse(raw);
    if (!validEnvelope(envelope)) throw new Error('invalid envelope');
    // Serialize inside the protected boundary too: deeply nested valid JSON
    // can parse successfully but exceed the serializer's call stack.
    payload = JSON.stringify({
      type: 'event', channel: envelope.channel, data: envelope.data,
      publishedAt: envelope.publishedAt, relayedBy: config.instanceId, relayedAt: Date.now(),
    });
  } catch {
    metrics.inc('malformedEnvelopes');
    return;
  }
  const { channel } = envelope;
  metrics.inc('envelopesReceived');

  // Serialize once above, deliver many.
  const { delivered, dropped } = hub.broadcast(channel, payload);
  metrics.add('delivered', delivered);
  metrics.add('dropped', dropped);
});

// ── HTTP server: health + metrics + WS upgrade ─────────────────────────────
const server = http.createServer((req, res) => {
  res.setHeader('cache-control', 'no-store');
  res.setHeader('x-content-type-options', 'nosniff');
  if (req.method !== 'GET') { res.writeHead(405, { allow: 'GET' }); res.end(); return; }
  if (req.url === '/healthz') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, instance: config.instanceId }));
    return;
  }
  if (req.url === '/readyz') {
    const state = readiness.snapshot();
    res.writeHead(state.ready ? 200 : 503, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ instance: config.instanceId, ...state }));
    return;
  }
  if (req.url === '/metrics') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ...metrics.snapshot(), readiness: readiness.snapshot(), hub: hub.snapshot() }, null, 2));
    return;
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({
  noServer: true,
  maxPayload: config.limits.maxPayloadBytes,
  // Disabled for small simulated payloads; no compression A/B evidence retained.
  perMessageDeflate: false,
});

server.on('upgrade', (req, socket, head) => {
  // Origin is a browser boundary, NOT authentication or channel authorization.
  const reject = (code, reason) => {
    metrics.inc(code === 503 ? 'unreadyUpgrades' : 'rejectedUpgrades');
    socket.end(`HTTP/1.1 ${code} ${reason}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
  };
  socket.on('error', () => {});
  if (req.url !== '/') return reject(404, 'Not Found');
  if (config.allowedOrigins.length && !config.allowedOrigins.includes(req.headers.origin)) return reject(403, 'Forbidden');
  if (!readiness.snapshot().ready) return reject(503, 'Service Unavailable');
  wss.handleUpgrade(req, socket, head, (client) => wss.emit('connection', client, req));
});

wss.on('connection', (socket) => {
  metrics.inc('connectionsTotal');
  hub.addClient(socket);
  socket.isAlive = true;
  const messages = new TokenBucket(config.limits.messagesPerSecond, config.limits.messageBurst);

  socket.on('pong', () => { socket.isAlive = true; });

  socket.on('message', (raw, isBinary) => {
    if (socket.readyState !== 1) return;
    if (!messages.take()) { metrics.inc('rateLimitedConnections'); socket.close(1008, 'message rate exceeded'); return; }
    if (isBinary) { metrics.inc('invalidMessages'); socket.close(1003, 'text JSON required'); return; }
    let msg;
    try {
      msg = JSON.parse(raw.toString());
      if (!validMessage(msg)) throw new Error('invalid protocol message');
    } catch {
      metrics.inc('invalidMessages');
      socket.send(JSON.stringify({ type: 'error', error: 'invalid message' }));
      return;
    }

    if (msg.action === 'subscribe' && typeof msg.channel === 'string') {
      const result = hub.subscribe(socket, msg.channel);
      if (!result.ok) metrics.inc('subscriptionRejections');
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

  socket.on('close', () => { metrics.inc('connectionsClosed'); hub.removeClient(socket); });
  socket.on('error', (err) => {
    metrics.inc(err.code === 'WS_ERR_UNSUPPORTED_MESSAGE_LENGTH' ? 'oversizedMessages' : 'socketErrors');
    hub.removeClient(socket);
  });
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
  if (readiness.draining) return;
  readiness.drain();
  log('info', `${signal} received — draining`, { event: 'shutdown-start' });
  clearInterval(heartbeat);
  for (const socket of wss.clients) {
    socket.close(1001, 'server shutting down');
  }
  wss.close(() => {
    server.close(() => {
      sub.quit().catch(() => sub.disconnect()).finally(() => {
        metrics.close(); log('info', 'drain complete', { event: 'shutdown-complete' }); process.exit(0);
      });
    });
  });
  // Hard exit if draining hangs
  setTimeout(() => { log('error', 'drain deadline reached', { event: 'shutdown-forced' }); process.exit(1); }, 10_000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

function log(level, msg, fields = {}) {
  console.log(JSON.stringify({ level, instance: config.instanceId, msg, t: new Date().toISOString(), ...fields }));
}

server.listen(config.port, config.host, () => {
  log('info', `listening on :${config.port}`);
});
