'use strict';
const { performance } = require('node:perf_hooks');
const object = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const validChannel = (value) => typeof value === 'string' && Buffer.byteLength(value) > 0 &&
  Buffer.byteLength(value) <= 128 && !/[\x00-\x20\x7f]/.test(value);

function validMessage(value) {
  if (!object(value)) return false;
  if (value.action === 'subscribe' || value.action === 'unsubscribe') return validChannel(value.channel);
  return value.action === 'ping' && (value.t == null || (typeof value.t === 'number' && Number.isFinite(value.t)));
}
function validEnvelope(value) {
  return object(value) && validChannel(value.channel) && Object.hasOwn(value, 'data') &&
    typeof value.publishedAt === 'number' && Number.isFinite(value.publishedAt) && value.publishedAt >= 0;
}

// Per-connection application messages, not distributed/IP/upgrade throttling.
class TokenBucket {
  constructor(rate, burst, clock = () => performance.now()) {
    this.rate = rate; this.burst = burst; this.tokens = burst; this.clock = clock; this.last = clock();
  }
  take() {
    const now = this.clock();
    this.tokens = Math.min(this.burst, this.tokens + Math.max(0, now - this.last) * this.rate / 1000);
    this.last = now;
    if (this.tokens < 1) return false;
    this.tokens -= 1; return true;
  }
}
module.exports = { validMessage, validEnvelope, validChannel, TokenBucket };
