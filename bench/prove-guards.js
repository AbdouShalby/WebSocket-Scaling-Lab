'use strict';
// WS-3: real sockets/Redis plus bounded, owned POSIX fault injection.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const net = require('node:net');
const { createHash, randomUUID } = require('node:crypto');
const WebSocket = require('ws');
const { Lab, waitFor, sleep } = require('./operational-lab');
const { ROOT } = require('./options');
const { provenance } = require('./connect-storm');

async function startServer(lab, overrides = {}) {
  const listener = net.createServer();
  await new Promise((resolve, reject) => { listener.once('error', reject); listener.listen(0, '127.0.0.1', resolve); });
  const port = listener.address().port;
  await new Promise((resolve) => listener.close(resolve));
  const config = { PORT: String(port), HOST: '127.0.0.1', INSTANCE_ID: `ws3-${randomUUID()}`, REDIS_HOST: '127.0.0.1',
    REDIS_PORT: String(lab.redisPort), CHANNEL_PREFIX: lab.prefix, ALLOWED_ORIGINS: '',
    MAX_SUBS_PER_CONN: '50', MAX_PAYLOAD_BYTES: '4096', MAX_ENVELOPE_BYTES: '1048576',
    MESSAGES_PER_SECOND: '100', MESSAGE_BURST: '200', REDIS_CHECK_INTERVAL_MS: '500', REDIS_CHECK_TIMEOUT_MS: '500',
    HEARTBEAT_INTERVAL_MS: '30000', MAX_BUFFERED_BYTES: '1048576', DROP_LIMIT: '500', ...overrides };
  const server = lab.child('ws3-server', process.execPath, ['src/server.js'], { ...process.env, ...config });
  Object.assign(server, { http: `http://127.0.0.1:${port}`, url: `ws://127.0.0.1:${port}`, instanceId: config.INSTANCE_ID });
  server.entry.config = config;
  await waitFor(() => {
    if (server.entry.exit || server.entry.spawnError) throw new Error('server failed: ' + server.entry.errors);
    return server.entry.log.includes('listening on');
  }, 'HTTP listener (not assumed ready)');
  return server;
}
async function http(lab, server, route, status) {
  const response = await fetch(server.http + route, { signal: AbortSignal.timeout(1500) });
  assert.equal(response.status, status, route);
  assert.equal(response.headers.get('cache-control'), 'no-store');
  const body = await response.json();
  lab.record('http-observation', { route, status, body }); return body;
}
async function ready(lab, server, expected) {
  await waitFor(async () => {
    const response = await fetch(server.http + '/readyz', { signal: AbortSignal.timeout(1500) });
    assert.ok([200, 503].includes(response.status), '/readyz must exist');
    const body = await response.json();
    assert.equal(response.status, body.ready ? 200 : 503);
    return body.ready === expected;
  }, `readiness=${expected}`, 8000);
  return http(lab, server, '/readyz', expected ? 200 : 503);
}
async function socket(lab, server, options = {}, rejectedStatus) {
  const ws = new WebSocket(server.url, { handshakeTimeout: 3000, perMessageDeflate: false, ...options });
  const c = { ws, messages: [], errors: [], status: null, close: null };
  lab.clients.push(c);
  ws.on('message', (raw) => c.messages.push(JSON.parse(raw)));
  ws.on('error', (err) => c.errors.push(err.message));
  ws.on('close', (code, reason) => { c.close = { code, reason: reason.toString() }; });
  ws.on('unexpected-response', (_request, response) => { c.status = response.statusCode; response.resume(); ws.terminate(); });
  await waitFor(() => ws.readyState === WebSocket.OPEN || c.close || c.status, 'WS handshake');
  if (rejectedStatus) {
    assert.equal(c.status, rejectedStatus, 'upgrade rejection');
    lab.record('upgrade-rejected', { status: c.status });
  } else assert.equal(ws.readyState, WebSocket.OPEN, c.errors.join(';'));
  return c;
}
async function reply(c, value, expectedType) {
  const n = c.messages.length;
  c.ws.send(typeof value === 'string' ? value : JSON.stringify(value));
  await waitFor(() => c.messages.length > n || c.close, 'protocol reply');
  assert.equal(c.messages[n]?.type, expectedType); return c.messages[n];
}
const scenarios = {
  async security(lab) {
    await lab.startRedis();
    const server = await startServer(lab, { ALLOWED_ORIGINS: 'https://allowed.example', MAX_SUBS_PER_CONN: '2' });
    await ready(lab, server, true);
    for (const origin of [undefined, 'null', 'https://evil.example', 'https://allowed.example.evil', 'https://allowed.example/']) {
      await socket(lab, server, origin ? { origin } : {}, 403);
    }
    await socket(lab, { ...server, url: server.url + '/wrong' }, { origin: 'https://allowed.example' }, 404);
    const client = await socket(lab, server, { origin: 'https://allowed.example' });
    for (const raw of ['null', '[]', '1', 'true', '"text"', '{', '{}',
      JSON.stringify({ action: 'subscribe', channel: '' }), JSON.stringify({ action: 'subscribe', channel: 'x\n' }),
      JSON.stringify({ action: 'subscribe', channel: 'é'.repeat(65) }), JSON.stringify({ action: 'ping', t: {} })]) {
      await reply(client, raw, 'error');
    }
    const channel = 'safe:channel';
    await reply(client, { action: 'subscribe', channel }, 'subscribed');
    await reply(client, { action: 'subscribe', channel: 'second' }, 'subscribed');
    await reply(client, { action: 'subscribe', channel }, 'subscribed'); // idempotent at limit
    assert.equal((await reply(client, { action: 'subscribe', channel: 'third' }, 'error')).error, 'subscription limit reached');
    const deep = '{"channel":"safe:channel","publishedAt":1,"data":' + '['.repeat(10000) + '0' + ']'.repeat(10000) + '}';
    for (const raw of ['null', '[]', '{}', '{', JSON.stringify({ channel, data: {}, publishedAt: 'wrong' }), 'x'.repeat(1048577), deep]) {
      await lab.pub.publish(lab.prefix + 'all', raw);
    }
    const envelope = { channel, publishedAt: Date.now(), data: { marker: 'after-invalid-input' } };
    await lab.pub.publish(lab.prefix + 'all', JSON.stringify(envelope));
    await waitFor(() => client.messages.some((m) => m.type === 'event'), 'valid event after malformed input');
    const event = client.messages.find((m) => m.type === 'event');
    assert.deepEqual(event.data, envelope.data); assert.equal(event.relayedBy, server.instanceId);
    assert.equal(event.publishedAt, envelope.publishedAt); assert.equal(event.channel, channel);
    lab.record('valid-event-after-invalid-input', { event });
    const binary = await socket(lab, server, { origin: 'https://allowed.example' });
    binary.ws.send(Buffer.from('{}')); await waitFor(() => binary.close, 'binary rejection');
    assert.equal(binary.close.code, 1003); lab.record('binary-close', binary.close);
    const large = await socket(lab, server, { origin: 'https://allowed.example' });
    large.ws.send('x'.repeat(4097)); await waitFor(() => large.close, 'oversized rejection');
    assert.equal(large.close.code, 1009); lab.record('oversized-close', large.close);
    const m = await http(lab, server, '/metrics', 200);
    assert.equal(m.invalidMessages, 12); assert.equal(m.malformedEnvelopes, 7);
    assert.equal(m.oversizedMessages, 1); assert.equal(m.subscriptionRejections, 1); assert.equal(m.rejectedUpgrades, 6);
    assert.equal(m.envelopesReceived, 1); assert.equal(m.delivered, 1); assert.equal(m.readiness.ready, true);
    assert.ok(m.connectionsClosed >= 2);
    assert.ok(m.rss > 0 && m.heapUsed > 0);
    for (const v of Object.values(m.eventLoop)) assert.ok(Number.isFinite(v) && v >= 0);
    assert.ok(m.eventLoop.utilization <= 1);
    await http(lab, server, '/healthz', 200);
    assert.ok(!server.entry.exit, 'malformed input must not crash server');
    lab.result.observations.securityMetrics = m;
  },
  async rate(lab) {
    await lab.startRedis();
    const server = await startServer(lab, { MESSAGES_PER_SECOND: '1', MESSAGE_BURST: '3' });
    await ready(lab, server, true);
    const healthy = await socket(lab, server), noisy = await socket(lab, server);
    for (let i = 0; i < 4; i++) noisy.ws.send('{'); // invalid messages consume tokens too
    await waitFor(() => noisy.close, 'rate-limit close');
    assert.equal(noisy.close.code, 1008); assert.equal(noisy.close.reason, 'message rate exceeded');
    assert.equal(noisy.messages.length, 3);
    await reply(healthy, { action: 'ping', t: 7 }, 'pong');
    const m = await http(lab, server, '/metrics', 200);
    assert.equal(m.rateLimitedConnections, 1); assert.equal(m.invalidMessages, 3);
    assert.equal(m.hub.connections, 1);
    lab.record('rate-close', noisy.close); lab.result.observations.rateMetrics = m;
  },
  async readiness(lab) {
    await lab.startRedis(); await lab.signal(lab.redis, 'SIGKILL'); await lab.exited(lab.redis);
    const server = await startServer(lab);
    await ready(lab, server, false); await http(lab, server, '/healthz', 200);
    await socket(lab, server, {}, 503);
    await lab.startRedis(); await ready(lab, server, true); await lab.subscribers(1);
    const client = await lab.client(server, 'same-client'); await lab.subscribe(client);
    await lab.publish('ready-before-outage', [client]);
    await lab.signal(lab.redis, 'SIGKILL'); await lab.exited(lab.redis);
    await ready(lab, server, false); await http(lab, server, '/healthz', 200); await socket(lab, server, {}, 503);
    assert.equal(client.ws.readyState, WebSocket.OPEN);
    await lab.startRedis(); await ready(lab, server, true); await lab.subscribers(1);
    await lab.publish('ready-after-outage', [client]);
    await lab.signal(lab.redis, 'SIGSTOP'); // TCP remains established; PING must time out
    await ready(lab, server, false); await http(lab, server, '/healthz', 200);
    await socket(lab, server, {}, 503);
    await lab.signal(lab.redis, 'SIGCONT'); await ready(lab, server, true); await lab.subscribers(1);
    await lab.publish('ready-after-stall', [client]);
    const m = await http(lab, server, '/metrics', 200);
    assert.ok(m.redisReconnects >= 2 && m.redisCheckFailures >= 1 && m.redisSubscriptionAcks >= 3);
    lab.result.observations.recoveryMetrics = m;
    client.ws.pause(); await lab.signal(server, 'SIGTERM');
    await waitFor(() => server.entry.log.includes('SIGTERM received'), 'shutdown handler');
    const state = await ready(lab, server, false); assert.equal(state.draining, true);
    await socket(lab, server, {}, 503);
    client.ws.resume(); assert.equal((await lab.exited(server)).code, 0);
    assert.equal(client.entry.close.code, 1001);
    const logs = server.entry.log.trim().split('\n').map((line) => JSON.parse(line));
    assert.ok(logs.every((r) => r.instance === server.instanceId && typeof r.t === 'string' && typeof r.level === 'string'));
    assert.ok(logs.some((r) => r.event === 'shutdown-complete'));
    assert.ok(logs.some((r) => r.event === 'readiness' && r.ready === false));
    lab.result.observations.structuredLogEntries = logs.length;
  },
  async 'subscription-ack'(lab) {
    await lab.startRedis();
    await lab.pub.call('ACL', 'SETUSER', 'default', '-subscribe'); // only this disposable broker
    const server = await startServer(lab);
    await waitFor(() => server.entry.log.includes('redis-check-failed'), 'actual SUBSCRIBE denial');
    const state = await ready(lab, server, false); assert.equal(state.redis.subscribed, false);
    await http(lab, server, '/healthz', 200); await socket(lab, server, {}, 503);
    await lab.pub.call('ACL', 'SETUSER', 'default', '+subscribe');
    await ready(lab, server, true); await lab.subscribers(1);
    const c = await lab.client(server, 'after-ack'); await lab.subscribe(c); await lab.publish('ack-gated-delivery', [c]);
  },
};
function capture(argv) {
  const meta = provenance(argv, { topology: 'owned real POSIX Redis/server processes and loopback sockets; no nginx', testProfile: 'WS-3 security/readiness/telemetry, not capacity' });
  for (const file of ['tests/guards.test.js', 'tests/guards-integration.test.js', 'tests/fixtures/ws3-mutant.cjs']) {
    meta.git.sourceSha256[file] = createHash('sha256').update(fs.readFileSync(path.join(ROOT, file), 'utf8').replace(/\r\n/g, '\n')).digest('hex');
  }
  meta.testMutation = process.env.WS3_MUTATION || null; return meta;
}
async function main(argv) {
  if (process.platform === 'win32') throw new Error('WS-3 real infrastructure proof requires Linux/WSL, no Windows or fake Redis fallback');
  const opts = { scenario: 'all', 'output-dir': 'bench/results/ws3' }, seen = new Set();
  for (let i = 0; i < argv.length; i += 2) {
    const key = argv[i].slice(2);
    if (!argv[i].startsWith('--') || !Object.hasOwn(opts, key) || !argv[i + 1] || seen.has(key)) throw new Error('invalid WS-3 option');
    opts[key] = argv[i + 1]; seen.add(key);
  }
  if (opts.scenario !== 'all' && !Object.hasOwn(scenarios, opts.scenario)) throw new Error('unknown WS-3 scenario');
  const directory = path.resolve(ROOT, opts['output-dir']);
  if (!directory.startsWith(ROOT + path.sep)) throw new Error('output-dir must stay inside repository');
  const command = ['bench/prove-guards.js', ...argv];
  const result = { schemaVersion: 1, milestone: 'WS-3', startedAt: new Date().toISOString(), status: 'RUNNING', provenance: capture(command), scenarios: [] };
  for (const name of opts.scenario === 'all' ? Object.keys(scenarios) : [opts.scenario]) {
    const lab = new Lab(`ws3-${name}`);
    try { await scenarios[name](lab); await lab.verify(); lab.result.status = 'PASSED'; }
    catch (e) { lab.result.status = 'FAILED'; lab.result.failures.push(e.message); }
    finally {
      try { await lab.cleanup(); } catch (e) { lab.result.status = 'FAILED'; lab.result.failures.push(`cleanup: ${e.message}`); }
    }
    result.scenarios.push(lab.result); console.log(`${name}: ${lab.result.status} ${lab.result.failures.join('; ')}`);
  }
  const after = capture(command);
  result.provenance.sourceChangedDuringRun = result.provenance.git.sha !== after.git.sha || JSON.stringify(result.provenance.git.sourceSha256) !== JSON.stringify(after.git.sourceSha256);
  result.status = result.scenarios.every((s) => s.status === 'PASSED') && !result.provenance.sourceChangedDuringRun ? 'PASSED' : 'FAILED';
  result.completedAt = new Date().toISOString();
  result.limitations = ['origin is not identity or authorization; unset permits origin-less benchmark clients', 'per-connection data-message limits, not DDoS or distributed/IP throttling', 'readiness is sampled subscriber health, not durable delivery or all-network-partition proof', 'JSON metrics are process-local; send counts are not receipt ACKs; loop delay is not message latency', 'same-host direct sockets, not hosted CI or load balancer enforcement'];
  fs.mkdirSync(directory, { recursive: true });
  const file = path.join(directory, `${result.startedAt.replace(/[:.]/g, '-')}-${randomUUID()}.json`);
  fs.writeFileSync(file, JSON.stringify(result, null, 2) + '\n', { flag: 'wx' });
  fs.writeFileSync(file.replace(/\.json$/, '.log'), result.scenarios.flatMap((s) => s.timeline.map((event) => JSON.stringify({ scenario: s.scenario, ...event }))).join('\n') + '\n', { flag: 'wx' });
  console.log(`RESULT ${file}`); return result;
}
if (require.main === module) main(process.argv.slice(2)).then((r) => { process.exitCode = r.status === 'PASSED' ? 0 : 1; }).catch((e) => { console.error(e.message); process.exitCode = 1; });
module.exports = { main };
