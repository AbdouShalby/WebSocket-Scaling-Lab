'use strict';

// Readiness belongs to the actual subscriber, not a separate Redis PING client.
// Every connection generation needs a fresh SUBSCRIBE acknowledgment.
class Readiness {
  constructor(sub, channel, { intervalMs = 1000, onTransition = () => {}, onFailure = () => {} } = {}) {
    this.sub = sub; this.channel = channel; this.onTransition = onTransition; this.onFailure = onFailure;
    this.ready = false; this.draining = false; this.subscribed = false; this.epoch = 0; this.inFlight = false;
    this.reason = 'starting'; this.changedAt = new Date().toISOString();
    sub.on('ready', () => { this.invalidate('subscribing'); void this.check(); });
    for (const event of ['close', 'end', 'reconnecting']) sub.on(event, () => this.invalidate('redis-unavailable'));
    this.timer = setInterval(() => { void this.check(); }, intervalMs);
    this.timer.unref();
  }
  transition(ready, reason) {
    if (this.draining) { ready = false; reason = 'draining'; }
    if (this.ready === ready && this.reason === reason) return;
    this.ready = ready; this.reason = reason; this.changedAt = new Date().toISOString();
    this.onTransition(this.snapshot());
  }
  invalidate(reason) {
    this.epoch++; this.inFlight = false; this.subscribed = false;
    this.transition(false, reason);
  }
  async check() {
    if (this.draining || this.inFlight || this.sub.status !== 'ready') return;
    const epoch = this.epoch;
    this.inFlight = true;
    try {
      if (!this.subscribed) {
        const count = await this.sub.subscribe(this.channel);
        if (count !== 1) throw new Error('unexpected subscription count');
      } else {
        const pong = await this.sub.ping('readiness');
        if (!Array.isArray(pong) || pong[0] !== 'pong' || pong[1] !== 'readiness') throw new Error('not in subscriber mode');
      }
      if (epoch !== this.epoch || this.draining || this.sub.status !== 'ready') return;
      this.subscribed = true;
      this.transition(true, 'subscribed');
    } catch {
      if (epoch !== this.epoch || this.draining) return;
      this.subscribed = false;
      this.transition(false, 'redis-check-failed');
      this.onFailure();
      // Clear timed-out commands instead of growing a queue behind a stalled peer.
      // ioredis reconnect=true retains its existing bounded reconnect backoff.
      this.sub.disconnect(true);
    } finally { if (epoch === this.epoch) this.inFlight = false; }
  }
  snapshot() {
    return { ready: this.ready && !this.draining, draining: this.draining, reason: this.reason,
      changedAt: this.changedAt, redis: { status: this.sub.status, subscribed: this.subscribed } };
  }
  drain() { this.draining = true; this.stop(); this.invalidate('draining'); }
  stop() { clearInterval(this.timer); }
}
module.exports = { Readiness };
