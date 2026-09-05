'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter, once } = require('node:events');
const { WebSocketServer } = require('ws');
const { DeliveryLedger, settle } = require('../bench/accounting');
const { defaults, parseArgs, validateRuntime } = require('../bench/options');
const { runBenchmark } = require('../bench/runner');

const event = (seq, channel = 'a', extra = {}) => ({ type: 'event', channel, relayedBy: 'node-a',
  data: { runId: 'run', phase: 'measurement', seq }, ...extra });
const runtime = { schemaVersion: 1, environment: 'test doubles, not real Redis',
  topology: { description: 'test fixture', wsInstances: 1 }, server: { node: process.version, ws: 'fixture', ioredis: 'fixture', config: {} } };

test('measurement resets ramp/warmup and excludes drain from throughput and latency', () => {
  const l = new DeliveryLedger([{ channel: 'a' }, { channel: 'a' }], 'run');
  l.begin(0); l.issue(0, 'a', 1); l.receive(0, event(0), 50);
  l.begin(1000); l.issue(1, 'a', 1010); l.receive(0, event(1), 1012);
  l.endWindow(3000); l.receive(1, event(1), 3500);
  const s = l.snapshot();
  assert.equal(s.expected, 2); assert.equal(s.received, 2); assert.equal(s.inWindow, 1);
  assert.equal(s.late, 1); assert.equal(s.missing, 0); assert.equal(s.deliveriesPerSecond, 0.5);
  assert.equal(s.latencyMs.samples, 1); assert.equal(s.latencyMs.p99, 2);
});
test('expected ledger finds an entirely missing publication and unequal channel audiences', () => {
  const l = new DeliveryLedger([{ channel: 'a' }, { channel: 'a' }, { channel: 'b' }], 'run');
  l.begin(0); l.issue(0, 'a', 0); l.issue(1, 'b', 1); l.issue(2, 'a', 2);
  l.receive(0, event(0), 3); l.receive(1, event(0), 3); l.receive(2, event(1, 'b'), 3);
  l.endWindow(1000);
  assert.equal(l.snapshot().expected, 5); assert.equal(l.snapshot().missing, 2);
});
test('dedup is per client and event; wrong channel/unknown IDs cannot erase missing', () => {
  const l = new DeliveryLedger([{ channel: 'a' }, { channel: 'a' }], 'run');
  l.begin(0); l.issue(0, 'a', 0);
  l.receive(0, event(0), 1); l.receive(0, event(0), 2); l.receive(1, event(0), 3);
  l.receive(0, event(99), 4); l.receive(0, event(0, 'wrong'), 4);
  l.endWindow(1000);
  const s = l.snapshot();
  assert.equal(s.received, 2); assert.equal(s.duplicates, 1); assert.equal(s.unexpected, 2);
  assert.equal(s.missing, 0); assert.equal(s.instances['node-a'].clients, 2);
});
test('late warmup/foreign traffic and absent instance IDs do not become measurement samples', () => {
  const l = new DeliveryLedger([{ channel: 'a' }], 'run');
  l.begin(0); l.issue(0, 'a', 0);
  l.receive(0, event(0, 'a', { data: { runId: 'other', phase: 'measurement', seq: 0 } }), 1);
  l.receive(0, event(0, 'a', { data: { runId: 'run', phase: 'warmup', seq: 0 } }), 1);
  l.receive(0, event(0, 'a', { relayedBy: null }), 1);
  l.endWindow(1000);
  assert.equal(l.snapshot().inWindow, 0); assert.equal(l.snapshot().missing, 1);
  assert.equal(l.snapshot().foreign, 2); assert.equal(l.snapshot().latencyMs.p99, null);
});
test('error plus close or repeated acknowledgment settles readiness exactly once', () => {
  const failed = { outcome: 'pending' };
  assert.equal(settle(failed, 'failed'), true); assert.equal(settle(failed, 'failed'), false);
  assert.equal(settle(failed, 'ready'), false);
  const ready = { outcome: 'pending' };
  assert.equal(settle(ready, 'ready'), true); assert.equal(settle(ready, 'ready'), false);
  assert.equal(settle(ready, 'failed'), false); assert.equal(ready.outcome, 'ready');
});
test('invalid/unsafe parameters and missing provenance are rejected before networking', () => {
  for (const argv of [['--conns', 'NaN'], ['--conns', '0'], ['--measure', '-1'], ['--rate', 'Infinity'],
    ['--ramp', '1.2'], ['--unknown', '1'], ['--measure'], ['--rate', '2', '--rate', '3'],
    ['--url', 'ws://user:secret@localhost'], ['--url', 'ws://localhost?token=x'], ['--output-dir', '../outside']]) {
    assert.throws(() => parseArgs(argv));
  }
  assert.throws(() => validateRuntime({}));
});

async function fixture(t, mode = 'normal') {
  const wss = new WebSocketServer({ port: 0, host: '127.0.0.1' });
  await once(wss, 'listening');
  const sockets = [];
  const timers = [];
  let acknowledgments = 0;
  wss.on('connection', (ws) => {
    sockets.push(ws);
    ws.on('error', () => {});
    ws.on('message', (raw) => {
      const msg = JSON.parse(raw);
      ws.channel = msg.channel;
      if (mode === 'no-ack') return;
      if (mode === 'reject') { ws.send(JSON.stringify({ type: 'error', error: 'limit' })); return; }
      timers.push(setTimeout(() => {
        if (ws.readyState !== 1) return;
        acknowledgments++;
        ws.send(JSON.stringify({ type: 'subscribed', channel: msg.channel }));
      }, mode === 'delayed-ack' ? 80 : 0));
    });
  });
  t.after(async () => {
    timers.forEach(clearTimeout);
    for (const ws of sockets) ws.terminate();
    await new Promise((resolve) => wss.close(resolve));
  });
  const publisher = new EventEmitter();
  publisher.connect = async () => {};
  publisher.info = async () => 'redis_version:fixture\r\n';
  publisher.disconnect = () => {};
  publisher.publish = async (_channel, raw) => {
    assert.equal(acknowledgments, 2, 'must wait for all subscription acknowledgments before publishing');
    const envelope = JSON.parse(raw);
    if (mode === 'publish-error') throw new Error('uncertain publish');
    if (mode === 'disconnect') { sockets[0].terminate(); return 1; }
    if (mode === 'drop-all' || (mode === 'drop-event' && envelope.data.phase === 'measurement' && envelope.data.seq === 0)) return 1;
    for (const ws of sockets.filter((s) => s.channel === envelope.channel && s.readyState === 1)) {
      const payload = JSON.stringify({ type: 'event', ...envelope, relayedBy: 'fixture-node' });
      ws.send(payload);
      if (mode === 'duplicate') ws.send(payload);
    }
    return 1;
  };
  return { publisher, options: { ...defaults, url: `ws://127.0.0.1:${wss.address().port}`,
    conns: 2, channels: 1, ramp: 1, rate: 30, measure: 0.15, warmup: 0.08, drain: 0.05, timeout: 0.3 } };
}

test('complete lifecycle waits for real WS acknowledgments and resets warmup', async (t) => {
  const f = await fixture(t, 'delayed-ack');
  const r = await runBenchmark(f.options, runtime, { publisher: f.publisher });
  assert.equal(r.status, 'COMPLETE', JSON.stringify(r.failures));
  assert.deepEqual(r.lifecycle.map((s) => s.state), ['PREFLIGHT', 'RAMPING', 'SUBSCRIBING', 'READY', 'WARMUP', 'MEASURING', 'DRAINING', 'CLEANUP', 'COMPLETE']);
  assert.equal(r.accounting.expected, r.publisher.issued * 2);
  assert.equal(r.accounting.received, r.accounting.expected);
  assert.equal(r.accounting.latencyMs.samples, r.accounting.inWindow);
  assert.ok(r.accounting.elapsedMs >= 140);
});
for (const [mode, reason] of [['no-ack', 'subscription-timeout'], ['reject', 'subscription-rejected'],
  ['drop-all', 'no-measured-deliveries'], ['drop-event', 'missing-deliveries'], ['duplicate', 'duplicate-deliveries'],
  ['publish-error', 'publish-failed'], ['disconnect', 'socket-closed']]) {
  test(`failed run: ${mode}`, async (t) => {
    const f = await fixture(t, mode);
    const r = await runBenchmark(f.options, runtime, { publisher: f.publisher });
    assert.equal(r.status, 'FAILED'); assert.ok(r.failures.includes(reason), JSON.stringify(r.failures));
    assert.ok(r.connections.failed <= r.connections.requested);
    if (mode === 'no-ack' || mode === 'reject') assert.equal(r.publisher.issued, 0);
    if (mode === 'disconnect') { assert.equal(r.connections.ready, 2); assert.equal(r.connections.failed, 0); }
  });
}
