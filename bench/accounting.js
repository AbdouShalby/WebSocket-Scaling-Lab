'use strict';

// Independent of sockets and clocks: boundary cases can be tested deterministically.
class DeliveryLedger {
  constructor(clients, runId) {
    this.clients = clients;
    this.runId = runId;
    this.audience = new Map();
    for (const c of clients) this.audience.set(c.channel, (this.audience.get(c.channel) || 0) + 1);
    this.reset();
  }
  reset() {
    this.events = new Map();
    this.seen = this.clients.map(() => new Set());
    this.instances = new Map();
    this.expected = this.received = this.inWindow = this.late = this.duplicates = this.unexpected = this.foreign = 0;
    this.latencies = new Map();
    this.maxLatency = this.start = this.end = null;
  }
  begin(now) { this.reset(); this.start = now; }
  endWindow(now) { this.end = now; }
  issue(seq, channel, now) {
    if (this.start === null || this.end !== null || this.events.has(seq)) throw new Error('invalid event issue');
    this.events.set(seq, { channel, sentAt: now });
    this.expected += this.audience.get(channel) || 0;
  }
  receive(clientId, msg, now) {
    if (msg.data?.runId !== this.runId || msg.data?.phase !== 'measurement') { this.foreign++; return; }
    if (this.start === null) return;
    const event = this.events.get(msg.data.seq);
    const client = this.clients[clientId];
    if (!event || event.channel !== msg.channel || client.channel !== msg.channel ||
        typeof msg.relayedBy !== 'string' || !msg.relayedBy || now < event.sentAt ||
        (client.instanceId && client.instanceId !== msg.relayedBy)) {
      this.unexpected++; return;
    }
    if (this.seen[clientId].has(msg.data.seq)) { this.duplicates++; return; }
    this.seen[clientId].add(msg.data.seq);
    this.received++;
    if (!this.instances.has(msg.relayedBy)) this.instances.set(msg.relayedBy, { clients: new Set(), received: 0 });
    const instance = this.instances.get(msg.relayedBy);
    instance.clients.add(clientId);
    instance.received++;
    if (this.end === null) {
      this.inWindow++;
      const latency = now - event.sentAt;
      // All samples in 1ms histogram buckets; no first-N sampling bias.
      const bucket = Math.ceil(latency);
      this.latencies.set(bucket, (this.latencies.get(bucket) || 0) + 1);
      this.maxLatency = Math.max(this.maxLatency ?? 0, latency);
    } else this.late++;
  }
  snapshot() {
    const elapsedMs = this.start !== null && this.end !== null ? this.end - this.start : 0;
    const histogram = [...this.latencies].sort((a, b) => a[0] - b[0]);
    const percentile = (p) => {
      if (!this.inWindow) return null;
      let count = 0;
      for (const [ms, n] of histogram) {
        count += n;
        if (count >= Math.ceil(this.inWindow * p)) return ms;
      }
      return null;
    };
    return {
      issuedEvents: this.events.size, expected: this.expected, received: this.received,
      inWindow: this.inWindow, late: this.late, duplicates: this.duplicates,
      missing: this.expected - this.received, unexpected: this.unexpected, foreign: this.foreign,
      clientsWithEvents: this.seen.filter((s) => s.size > 0).length,
      elapsedMs, deliveriesPerSecond: elapsedMs > 0 ? this.inWindow * 1000 / elapsedMs : 0,
      latencyMs: { samples: this.inWindow, resolutionMs: 1, p50: percentile(0.5),
        p95: percentile(0.95), p99: percentile(0.99), max: this.maxLatency, histogram },
      instances: Object.fromEntries([...this.instances].map(([id, v]) => [id, { clients: v.clients.size, received: v.received }])),
    };
  }
}

// A connection settles readiness once; later disconnects are a separate metric.
function settle(client, outcome) {
  if (client.outcome !== 'pending') return false;
  client.outcome = outcome;
  return true;
}

module.exports = { DeliveryLedger, settle };
