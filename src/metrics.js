'use strict';

/**
 * Minimal in-process counters exposed at /metrics as JSON.
 * Deliberately dependency-free — in production this would be prom-client,
 * but the lab keeps observability visible and greppable.
 */
class Metrics {
  constructor({ instanceId }) {
    this.instanceId = instanceId;
    this.startedAt = Date.now();
    this.counters = Object.create(null);
  }

  inc(name) {
    this.add(name, 1);
  }

  add(name, n) {
    if (n === 0) return;
    this.counters[name] = (this.counters[name] ?? 0) + n;
  }

  snapshot() {
    return {
      instance: this.instanceId,
      uptimeSeconds: Math.round((Date.now() - this.startedAt) / 1000),
      rss: process.memoryUsage().rss,
      ...this.counters,
    };
  }
}

module.exports = { Metrics };
