'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createHash, randomUUID } = require('node:crypto');
const { performance } = require('node:perf_hooks');
const WebSocket = require('ws');
const { Lab, waitFor, sleep } = require('./operational-lab');
const { ROOT } = require('./options');
const { provenance } = require('./connect-storm');

async function pair(lab) {
  await lab.startRedis(); const a = await lab.server('A'), b = await lab.server('B');
  await lab.subscribers(2); assert.notEqual(a.p.pid, b.p.pid);
  const ca = await lab.client(a, 'a'), cb = await lab.client(b, 'b');
  await Promise.all([lab.subscribe(ca), lab.subscribe(cb)]);
  await lab.publish('before-failure', [ca, cb]); return { a, b, ca, cb };
}
const scenarios = {
  async recovery(lab) {
    const { a, b, ca, cb } = await pair(lab), first = lab.redis;
    await lab.signal(first, 'SIGKILL'); assert.equal((await lab.exited(first)).signal, 'SIGKILL');
    await sleep(400);
    await assert.rejects(() => lab.pub.publish(lab.prefix + 'all', JSON.stringify({ channel: lab.channel, data: { id: 'outage-not-enqueued' } })));
    lab.record('outage-publish-rejected', { policy: 'fail-fast publisher, no offline queue or retry; no accepted publication claimed' });
    const health = await (await fetch(a.http + '/healthz')).json(); assert.equal(health.ok, true);
    lab.result.observations.healthDuringRedisOutage = health;
    assert.equal(ca.ws.readyState, WebSocket.OPEN); assert.equal(cb.ws.readyState, WebSocket.OPEN);
    const next = await lab.startRedis(); assert.notEqual(next.entry.runtime.runId, first.entry.runtime.runId);
    await lab.subscribers(2);
    assert.ok(!a.entry.exit && !b.entry.exit, 'same WebSocket processes must recover');
    await lab.publish('after-Redis-recovery', [ca, cb]);
    lab.result.observations.sameSocketsRecovered = true;
  },
  async crash(lab) {
    const { a, b, ca, cb } = await pair(lab);
    await lab.signal(a, 'SIGKILL'); assert.equal((await lab.exited(a)).signal, 'SIGKILL');
    await waitFor(() => ca.entry.close, 'crashed client close'); assert.equal(ca.entry.close.code, 1006);
    await lab.subscribers(1); await lab.publish('while-A-is-down', [cb]);
    const replacement = await lab.server('A-replacement'); await lab.subscribers(2);
    const fresh = await lab.client(replacement, 'a-reconnected');
    await lab.publish('before-explicit-resubscribe', [cb]); await sleep(150);
    assert.deepEqual(fresh.events, [], 'new connection must not inherit lost subscription/history');
    await lab.subscribe(fresh); await lab.publish('after-explicit-resubscribe', [fresh, cb]);
    assert.ok(!b.entry.exit); assert.equal(cb.ws.readyState, WebSocket.OPEN);
    lab.result.observations.recovery = 'harness explicitly starts replacement and reconnects/resubscribes; no replay of gap events';
  },
  async drain(lab) {
    const { a, b, ca, cb } = await pair(lab);
    await lab.signal(a, 'SIGTERM'); const exit = await lab.exited(a);
    await waitFor(() => ca.entry.close, 'cooperative close frame');
    assert.equal(ca.entry.close.code, 1001); assert.equal(ca.entry.close.reason, 'server shutting down');
    assert.equal(exit.code, 0); assert.equal(exit.signal, null);
    await lab.subscribers(1); await lab.publish('while-A-drained', [cb]);
    const reconnect = await lab.client(b, 'a-on-B');
    await lab.publish('before-client-resubscribe', [cb]); await sleep(150);
    assert.deepEqual(reconnect.events, []);
    await lab.subscribe(reconnect); await lab.publish('after-client-resubscribe', [cb, reconnect]);
    lab.result.observations.reconnect = 'explicit harness reconnect to surviving B; no automatic load balancer or client failover claim';
  },
  async 'forced-drain'(lab) {
    await lab.startRedis(); const a = await lab.server('A');
    const stubborn = await lab.client(a, 'non-cooperating'); await lab.subscribe(stubborn);
    stubborn.ws.pause(); lab.record('client-read-paused', { client: stubborn.id, purpose: 'withhold close handshake' });
    const start = performance.now(); await lab.signal(a, 'SIGTERM');
    await waitFor(() => a.entry.log.includes('SIGTERM received'), 'drain handler ran');
    await assert.rejects(() => lab.client(a, 'connection-during-drain'));
    lab.record('new-connection-rejected-during-drain');
    const exit = await lab.exited(a, 14000);
    const elapsed = performance.now() - start;
    assert.equal(exit.code, 1); assert.equal(exit.signal, null);
    assert.ok(elapsed >= 9000 && elapsed < 14000, `forced deadline elapsed=${elapsed}`);
    stubborn.ws.resume(); await waitFor(() => stubborn.entry.close, 'paused peer sees closure');
    await lab.subscribers(0); lab.result.observations.forcedExitElapsedMs = elapsed;
  },
  async backpressure(lab) {
    await lab.startRedis();
    const server = await lab.server('slow-consumer-server', { MAX_BUFFERED_BYTES: '65536', DROP_LIMIT: '3' });
    const healthy = await lab.client(server, 'healthy'), slow = await lab.client(server, 'paused');
    await Promise.all([lab.subscribe(healthy), lab.subscribe(slow)]);
    await lab.publish('both-ready', [healthy, slow]);
    slow.ws.pause(); lab.record('client-read-paused', { client: slow.id, purpose: 'real TCP receive backpressure' });
    const blob = 'x'.repeat(65536), before = await lab.metrics(server);
    let after, count = 0;
    for (; count < 1024; count++) {
      await lab.publish(`pressure-${count}`, [healthy], [slow], blob);
      after = await lab.metrics(server);
      if (after.hub.slowConsumersKicked > 0) { count++; break; }
    }
    assert.equal(after.hub.slowConsumersKicked, 1, 'real paused TCP peer must trigger backpressure kick within bounded offered bytes');
    assert.ok(after.hub.messagesDropped >= 3); assert.equal(after.hub.connections, 1);
    assert.equal(after.reapedConnections || 0, 0, 'heartbeat must not be mistaken for backpressure');
    assert.equal(healthy.ws.readyState, WebSocket.OPEN);
    slow.ws.resume(); await waitFor(() => slow.entry.close, 'slow consumer closed');
    assert.equal(slow.entry.close.code, 1006);
    await lab.publish('healthy-still-receives-after-kick', [healthy]);
    lab.result.observations.backpressure = { before, after, payloadBlobBytes: blob.length, pressurePublications: count,
      maxOfferedBlobBytes: 1024 * blob.length, behavior: 'healthy receiver awaited between publishes; only paused client kicked' };
  },
};

function options(argv) {
  const result = { scenario: 'all', 'output-dir': 'bench/results/ws2' }, seen = new Set();
  for (let i = 0; i < argv.length; i += 2) {
    const key = argv[i].slice(2);
    if (!argv[i].startsWith('--') || !Object.hasOwn(result, key) || argv[i + 1] === undefined || seen.has(key)) throw new Error('invalid operational option');
    result[key] = argv[i + 1]; seen.add(key);
  }
  if (result.scenario !== 'all' && !Object.hasOwn(scenarios, result.scenario)) throw new Error('unknown scenario');
  if (!path.resolve(ROOT, result['output-dir']).startsWith(ROOT + path.sep)) throw new Error('output-dir must stay inside repository');
  return result;
}
function capture(argv) {
  const meta = provenance(argv, { topology: 'owned real Redis and Node processes on loopback; POSIX signals; direct clients; no nginx' });
  for (const file of ['tests/operations.test.js', 'tests/fixtures/ws2-mutant.cjs']) {
    meta.git.sourceSha256[file] = createHash('sha256').update(fs.readFileSync(path.join(ROOT, file), 'utf8').replace(/\r\n/g, '\n')).digest('hex');
  }
  meta.testMutation = process.env.WS2_MUTATION || null;
  return meta;
}
async function main(argv) {
  const opts = options(argv);
  if (process.platform === 'win32') throw new Error('WS-2 requires POSIX signals and a real redis-server executable; run on Linux/WSL, no Windows substitute');
  const result = { schemaVersion: 1, milestone: 'WS-2', startedAt: new Date().toISOString(), status: 'RUNNING', provenance: capture(['bench/prove-operations.js', ...argv]), scenarios: [] };
  for (const name of opts.scenario === 'all' ? Object.keys(scenarios) : [opts.scenario]) {
    const lab = new Lab(name);
    try { await scenarios[name](lab); await lab.verify(); lab.result.status = 'PASSED'; }
    catch (e) { lab.result.status = 'FAILED'; lab.result.failures.push(e.message); }
    finally {
      try { await lab.cleanup(); } catch (e) { lab.result.status = 'FAILED'; lab.result.failures.push(`cleanup: ${e.message}`); }
    }
    result.scenarios.push(lab.result); console.log(`${name}: ${lab.result.status} ${lab.result.failures.join('; ')}`);
  }
  const after = capture(['bench/prove-operations.js', ...argv]);
  result.provenance.sourceChangedDuringRun = result.provenance.git.sha !== after.git.sha || JSON.stringify(result.provenance.git.sourceSha256) !== JSON.stringify(after.git.sourceSha256);
  result.status = result.scenarios.every((s) => s.status === 'PASSED') && !result.provenance.sourceChangedDuringRun ? 'PASSED' : 'FAILED';
  result.completedAt = new Date().toISOString();
  result.limitations = ['finite representative windows, no durable delivery/replay or arbitrary crash safety', 'client reconnect/replacement performed explicitly by harness', 'TCP pause is a slow-reader experiment, not a whole-process memory bound or Internet congestion model', 'same-host POSIX experiment; no multi-host or rolling deployment guarantee'];
  const directory = path.resolve(ROOT, opts['output-dir']); fs.mkdirSync(directory, { recursive: true });
  const file = path.join(directory, `${result.startedAt.replace(/[:.]/g, '-')}-${randomUUID()}.json`);
  fs.writeFileSync(file, JSON.stringify(result, null, 2) + '\n', { flag: 'wx' });
  const lines = result.scenarios.flatMap((s) => s.timeline.map((event) => JSON.stringify({ scenario: s.scenario, ...event })));
  lines.push(JSON.stringify({ status: result.status, scenarios: result.scenarios.map(({ scenario, status, failures }) => ({ scenario, status, failures })) }));
  fs.writeFileSync(file.replace(/\.json$/, '.log'), lines.join('\n') + '\n', { flag: 'wx' });
  console.log(`RESULT ${file}`); return result;
}
if (require.main === module) main(process.argv.slice(2)).then((r) => { process.exitCode = r.status === 'PASSED' ? 0 : 1; }).catch((e) => { console.error(e.message); process.exitCode = 1; });
module.exports = { main, options };
