'use strict';

/**
 * Minimal in-process counters exposed at /metrics as JSON.
 * Deliberately dependency-free — in production this would be prom-client,
 * but the lab keeps observability visible and greppable.
 */
const { monitorEventLoopDelay, performance } = require('node:perf_hooks');
const COUNTERS = ['connectionsTotal', 'connectionsClosed', 'socketErrors', 'invalidMessages', 'oversizedMessages',
  'subscriptionRejections', 'rateLimitedConnections', 'rejectedUpgrades', 'unreadyUpgrades', 'malformedEnvelopes',
  'envelopesReceived', 'delivered', 'dropped', 'reapedConnections', 'redisErrors', 'redisReconnects',
  'redisCheckFailures', 'redisSubscriptionAcks', 'readinessTransitions'];
class Metrics {
  constructor({ instanceId }) {
    this.instanceId = instanceId;
    this.startedAt = Date.now();
    this.counters = Object.create(null);
    for (const name of COUNTERS) this.counters[name] = 0;
    this.delay = monitorEventLoopDelay({ resolution: 20 });
    this.delay.enable();
    this.initialUtilization = performance.eventLoopUtilization();
  }

  inc(name) {
    this.add(name, 1);
  }

  add(name, n) {
    if (n === 0) return;
    this.counters[name] = (this.counters[name] ?? 0) + n;
  }

  snapshot() {
    const memory = process.memoryUsage();
    return {
      instance: this.instanceId,
      uptimeSeconds: Math.round((Date.now() - this.startedAt) / 1000),
      rss: memory.rss,
      heapUsed: memory.heapUsed,
      eventLoop: { meanDelayMs: Number.isFinite(this.delay.mean) ? this.delay.mean / 1e6 : 0,
        p99DelayMs: this.delay.percentile(99) / 1e6, maxDelayMs: this.delay.max / 1e6,
        utilization: performance.eventLoopUtilization(this.initialUtilization).utilization },
      ...this.counters,
    };
  }
  close() { this.delay.disable(); }
}

module.exports = { Metrics };
