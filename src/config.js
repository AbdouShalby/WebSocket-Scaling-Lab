'use strict';

/**
 * Central configuration — every knob is an environment variable so the same
 * image can run as a single node or as a horizontally-scaled fleet.
 */
function integer(name, fallback, max = 2147483647) {
  const raw = process.env[name] ?? String(fallback);
  if (!/^\d+$/.test(raw) || !Number.isSafeInteger(Number(raw)) || Number(raw) < 1 || Number(raw) > max) {
    throw new Error(`invalid positive integer configuration: ${name}`);
  }
  return Number(raw);
}
const originSetting = (process.env.ALLOWED_ORIGINS ?? '').trim();
const allowedOrigins = originSetting === '' ? [] : originSetting.split(',').map((v) => v.trim());
for (const origin of allowedOrigins) {
  try {
    const url = new URL(origin);
    if (!['http:', 'https:'].includes(url.protocol) || url.origin !== origin) throw new Error();
  } catch { throw new Error('ALLOWED_ORIGINS must contain exact HTTP(S) origins, no paths or wildcards'); }
}
const config = {
  // HTTP + WebSocket listen port
  port: integer('PORT', 8080, 65535),
  // Local lab by default; containers explicitly bind their private interface.
  host: process.env.HOST ?? '127.0.0.1',

  // Unique instance id (defaults to hostname — docker gives each replica its own)
  instanceId: process.env.INSTANCE_ID ?? require('os').hostname(),

  redis: {
    host: process.env.REDIS_HOST ?? '127.0.0.1',
    port: integer('REDIS_PORT', 6379, 65535),
  },

  // Redis pub/sub channel prefix for cross-instance fan-out
  channelPrefix: process.env.CHANNEL_PREFIX ?? 'ws:broadcast:',

  heartbeat: {
    // How often we ping clients; a client that misses one full interval is reaped
    intervalMs: integer('HEARTBEAT_INTERVAL_MS', 30000),
  },

  backpressure: {
    // If a client's socket buffer exceeds this, we skip (drop) messages for it
    // instead of letting one slow consumer exhaust server memory.
    maxBufferedBytes: integer('MAX_BUFFERED_BYTES', 1024 * 1024),
    // After this many consecutive drops the client is disconnected (slow-consumer policy)
    dropLimit: integer('DROP_LIMIT', 500),
  },

  limits: {
    // Max channels a single connection may subscribe to
    maxSubscriptionsPerConnection: integer('MAX_SUBS_PER_CONN', 50),
    // Max inbound message size (bytes) — protocol messages are tiny
    maxPayloadBytes: integer('MAX_PAYLOAD_BYTES', 4096),
    maxEnvelopeBytes: integer('MAX_ENVELOPE_BYTES', 1024 * 1024),
    messagesPerSecond: integer('MESSAGES_PER_SECOND', 100),
    messageBurst: integer('MESSAGE_BURST', 200),
  },
  allowedOrigins,
  readiness: { intervalMs: integer('REDIS_CHECK_INTERVAL_MS', 1000), timeoutMs: integer('REDIS_CHECK_TIMEOUT_MS', 1000) },
};

module.exports = config;
