'use strict';
const { randomUUID } = require('node:crypto');
const { performance } = require('node:perf_hooks');
const WebSocket = require('ws');
const Redis = require('ioredis');
const { DeliveryLedger, settle } = require('./accounting');
const { validateOptions, validateRuntime } = require('./options');
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function runBenchmark(options, runtime, { publisher: injectedPublisher, onState = () => {} } = {}) {
  validateOptions(options);
  validateRuntime(runtime);
  const runId = randomUUID();
  const startedAt = new Date().toISOString();
  const clients = Array.from({ length: options.conns }, (_, id) => ({
    id, channel: `bench:${runId}:${id % options.channels}`, outcome: 'pending', opened: false, disconnected: false,
  }));
  const ledger = new DeliveryLedger(clients, runId);
  const failures = new Set();
  const transitions = [];
  let state, publisher, redisVersion = null, redisRuntime = null;
  let issued = 0, confirmed = 0, publishErrors = 0, phaseSeq = 0;
  const timers = new Set();
  const stateAt = performance.now();
  const transition = (next) => {
    state = next;
    transitions.push({ state, at: new Date().toISOString(), elapsedMs: performance.now() - stateAt });
    onState(state);
  };
  const failClient = (client, reason) => {
    if (['COMPLETE', 'FAILED', 'CLEANUP'].includes(state)) return;
    if (!settle(client, 'failed') && client.outcome === 'ready') client.disconnected = true;
    failures.add(reason);
  };
  try {
    transition('PREFLIGHT');
    publisher = injectedPublisher || new Redis({ host: options['redis-host'], port: options['redis-port'],
      lazyConnect: true, enableOfflineQueue: false, maxRetriesPerRequest: 0, retryStrategy: () => null,
      connectTimeout: options.timeout * 1000, commandTimeout: options.timeout * 1000 });
    publisher.on('error', () => failures.add('redis-error'));
    await publisher.connect();
    const info = await publisher.info('server');
    const infoField = (key) => info.match(new RegExp(`^${key}:(.+)$`, 'm'))?.[1]?.trim() || null;
    redisVersion = infoField('redis_version');
    redisRuntime = { version: redisVersion, os: infoField('os'), archBits: infoField('arch_bits'),
      mode: infoField('redis_mode'), buildId: infoField('redis_build_id') };
    if (!redisVersion) throw new Error('Redis runtime version unavailable');
    transition('RAMPING');
    for (let base = 0; base < clients.length; base += options.ramp) {
      for (const client of clients.slice(base, base + options.ramp)) {
        const socket = new WebSocket(options.url, { perMessageDeflate: false, handshakeTimeout: options.timeout * 1000 });
        client.socket = socket;
        const timer = setTimeout(() => { failClient(client, 'subscription-timeout'); socket.terminate(); }, options.timeout * 1000);
        timers.add(timer);
        socket.on('open', () => {
          client.opened = true;
          socket.send(JSON.stringify({ action: 'subscribe', channel: client.channel }));
        });
        socket.on('message', (raw) => {
          let msg;
          try { msg = JSON.parse(raw.toString()); } catch { failures.add('malformed-message'); return; }
          if (!msg || typeof msg !== 'object') { failures.add('malformed-message'); return; }
          if (msg.type === 'subscribed' && msg.channel === client.channel) {
            if (settle(client, 'ready')) { clearTimeout(timer); timers.delete(timer); }
          } else if (msg.type === 'error') failClient(client, 'subscription-rejected');
          else if (msg.type === 'event' && ['WARMUP', 'MEASURING', 'DRAINING'].includes(state)) {
            ledger.receive(client.id, msg, performance.now());
          }
        });
        socket.on('error', () => failClient(client, 'socket-error'));
        socket.on('close', () => failClient(client, 'socket-closed'));
      }
      if (base + options.ramp < clients.length) await sleep(100);
    }
    transition('SUBSCRIBING');
    while (clients.some((c) => c.outcome === 'pending')) await sleep(5);
    if (failures.size || clients.some((c) => c.outcome !== 'ready' || c.disconnected)) throw new Error('not all subscriptions are ready');
    transition('READY');
    const publishPhase = async (seconds, phase) => {
      const phaseStart = performance.now();
      const deadline = phaseStart + seconds * 1000;
      let next = phaseStart;
      while (performance.now() < deadline) {
        if (failures.size) throw new Error('transport failure');
        const delay = next - performance.now();
        if (delay > 0) await sleep(Math.min(delay, Math.max(0, deadline - performance.now())));
        if (performance.now() >= deadline) break;
        const seq = phaseSeq++;
        const channel = `bench:${runId}:${seq % options.channels}`;
        if (phase === 'measurement') { ledger.issue(seq, channel, performance.now()); issued++; }
        const envelope = { channel, publishedAt: Date.now(), data: { runId, phase, seq } };
        try {
          const subscribers = await publisher.publish(options['channel-prefix'] + 'all', JSON.stringify(envelope));
          if (subscribers < 1) throw new Error('no Redis subscribers');
          if (phase === 'measurement') confirmed++;
        } catch { publishErrors++; failures.add('publish-failed'); throw new Error('publish failed'); }
        // Delayed generators offer a lower measured rate, never a catch-up burst.
        next = Math.max(next + 1000 / options.rate, performance.now());
      }
    };
    transition('WARMUP');
    await publishPhase(options.warmup, 'warmup');
    phaseSeq = 0;
    ledger.begin(performance.now());
    transition('MEASURING');
    await publishPhase(options.measure, 'measurement');
    ledger.endWindow(performance.now());
    transition('DRAINING');
    await sleep(options.drain * 1000);
  } catch (error) {
    failures.add(error.message);
    if (ledger.start !== null && ledger.end === null) ledger.endWindow(performance.now());
  } finally {
    transition('CLEANUP');
    for (const timer of timers) clearTimeout(timer);
    for (const client of clients) client.socket?.terminate();
    publisher?.disconnect();
  }
  const accounting = ledger.snapshot();
  if (!accounting.inWindow) failures.add('no-measured-deliveries');
  if (accounting.missing) failures.add('missing-deliveries');
  if (accounting.duplicates) failures.add('duplicate-deliveries');
  if (accounting.unexpected) failures.add('unexpected-deliveries');
  if (accounting.clientsWithEvents !== options.conns) failures.add('clients-without-events');
  transition(failures.size ? 'FAILED' : 'COMPLETE');
  return {
    schemaVersion: 1, runId, startedAt, completedAt: new Date().toISOString(), status: state,
    failures: [...failures], lifecycle: transitions, options,
    connections: { requested: options.conns, opened: clients.filter((c) => c.opened).length,
      ready: clients.filter((c) => c.outcome === 'ready').length,
      failed: clients.filter((c) => c.outcome === 'failed').length,
      disconnected: clients.filter((c) => c.disconnected).length,
      unattempted: clients.filter((c) => !c.socket).length,
      unresolved: clients.filter((c) => c.socket && c.outcome === 'pending').length },
    publisher: { issued, confirmed, errors: publishErrors, redisVersion, redisRuntime,
      eventsPerSecond: accounting.elapsedMs ? issued * 1000 / accounting.elapsedMs : 0 },
    accounting,
  };
}
module.exports = { runBenchmark };
