'use strict';

/**
 * Central configuration — every knob is an environment variable so the same
 * image can run as a single node or as a horizontally-scaled fleet.
 */
const config = {
  // HTTP + WebSocket listen port
  port: parseInt(process.env.PORT ?? '8080', 10),

  // Unique instance id (defaults to hostname — docker gives each replica its own)
  instanceId: process.env.INSTANCE_ID ?? require('os').hostname(),

  redis: {
    host: process.env.REDIS_HOST ?? '127.0.0.1',
    port: parseInt(process.env.REDIS_PORT ?? '6379', 10),
  },

  // Redis pub/sub channel prefix for cross-instance fan-out
  channelPrefix: process.env.CHANNEL_PREFIX ?? 'ws:broadcast:',

  heartbeat: {
    // How often we ping clients; a client that misses one full interval is reaped
    intervalMs: parseInt(process.env.HEARTBEAT_INTERVAL_MS ?? '30000', 10),
  },

  backpressure: {
    // If a client's socket buffer exceeds this, we skip (drop) messages for it
    // instead of letting one slow consumer exhaust server memory.
    maxBufferedBytes: parseInt(process.env.MAX_BUFFERED_BYTES ?? String(1024 * 1024), 10),
    // After this many consecutive drops the client is disconnected (slow-consumer policy)
    dropLimit: parseInt(process.env.DROP_LIMIT ?? '500', 10),
  },

  limits: {
    // Max channels a single connection may subscribe to
    maxSubscriptionsPerConnection: parseInt(process.env.MAX_SUBS_PER_CONN ?? '50', 10),
    // Max inbound message size (bytes) — protocol messages are tiny
    maxPayloadBytes: parseInt(process.env.MAX_PAYLOAD_BYTES ?? '4096', 10),
  },
};

module.exports = config;
