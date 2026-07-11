'use strict';

/**
 * connect-storm — the honest benchmark client.
 *
 * Opens N WebSocket connections against the target (ramped, not all at once),
 * subscribes each to a product channel, then measures END-TO-END broadcast
 * latency: publisher's `publishedAt` timestamp → client receive time.
 *
 * Reports: connect success/failure, time-to-N-connections, latency
 * p50 / p95 / p99 / max, and messages received.
 *
 * NOTE on clocks: publisher and bench must run on the same machine (or
 * NTP-synced machines) for publishedAt deltas to be meaningful. In this lab's
 * docker-compose everything shares the host clock.
 *
 * Usage:
 *   node bench/connect-storm.js --url ws://localhost:8080 --conns 5000 \
 *        --channels 50 --ramp 200 --measure 30
 *     --conns     total connections to open           (default 1000)
 *     --channels  spread subscriptions over N channels (default 10)
 *     --ramp      new connections per 100ms tick       (default 100)
 *     --measure   seconds to measure after ramp        (default 30)
 */

const WebSocket = require('ws');

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  if (i === -1) return fallback;
  const v = process.argv[i + 1];
  return typeof fallback === 'number' ? Number(v) : v;
}

const URL = arg('url', 'ws://localhost:8080');
const CONNS = arg('conns', 1000);
const CHANNELS = arg('channels', 10);
const RAMP_PER_TICK = arg('ramp', 100);
const MEASURE_SECONDS = arg('measure', 30);

const latencies = [];
let connected = 0;
let failed = 0;
let received = 0;
let opened = 0;

const sockets = [];
const rampStart = Date.now();
let rampEnd = null;

console.log(`target=${URL} conns=${CONNS} channels=${CHANNELS} ramp=${RAMP_PER_TICK}/100ms measure=${MEASURE_SECONDS}s`);

function openOne(i) {
  const ws = new WebSocket(URL, { perMessageDeflate: false });
  sockets.push(ws);

  ws.on('open', () => {
    connected += 1;
    ws.send(JSON.stringify({ action: 'subscribe', channel: `product:${(i % CHANNELS) + 1}` }));
    if (connected === CONNS) {
      rampEnd = Date.now();
      console.log(`all ${CONNS} connected in ${((rampEnd - rampStart) / 1000).toFixed(1)}s — measuring for ${MEASURE_SECONDS}s`);
      setTimeout(finish, MEASURE_SECONDS * 1000);
    }
  });

  ws.on('message', (raw) => {
    const now = Date.now();
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (msg.type === 'event' && typeof msg.publishedAt === 'number') {
      received += 1;
      // Reservoir-ish cap so a long run doesn't hold millions of numbers
      if (latencies.length < 2_000_000) latencies.push(now - msg.publishedAt);
    }
  });

  ws.on('error', () => { failed += 1; });
}

const ramp = setInterval(() => {
  for (let i = 0; i < RAMP_PER_TICK && opened < CONNS; i++) {
    openOne(opened);
    opened += 1;
  }
  if (opened >= CONNS) clearInterval(ramp);
}, 100);

// Progress line every 5s
const progress = setInterval(() => {
  console.log(`  connected=${connected}/${CONNS} failed=${failed} received=${received}`);
}, 5000);

function pct(sorted, p) {
  if (sorted.length === 0) return NaN;
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}

function finish() {
  clearInterval(progress);
  latencies.sort((a, b) => a - b);
  console.log('\n══════════ RESULTS ══════════');
  console.log(`connections opened : ${connected}/${CONNS} (${failed} failed)`);
  console.log(`ramp time          : ${rampEnd ? ((rampEnd - rampStart) / 1000).toFixed(1) : '?'}s`);
  console.log(`events received    : ${received}`);
  if (latencies.length > 0) {
    console.log(`E2E latency  p50   : ${pct(latencies, 50)} ms`);
    console.log(`             p95   : ${pct(latencies, 95)} ms`);
    console.log(`             p99   : ${pct(latencies, 99)} ms`);
    console.log(`             max   : ${latencies[latencies.length - 1]} ms`);
  } else {
    console.log('no events received — is the publisher running?');
  }
  for (const ws of sockets) { try { ws.terminate(); } catch { /* noop */ } }
  process.exit(0);
}
