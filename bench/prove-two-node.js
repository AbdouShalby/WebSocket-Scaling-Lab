'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID, createHash } = require('node:crypto');
const { performance } = require('node:perf_hooks');
const WebSocket = require('ws');
const Redis = require('ioredis');
const { ROOT, parseArgs } = require('./options');
const { provenance } = require('./connect-storm');
const { startServer } = require('./run-local');
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function verifyTranscript(publications, receipts, clients) {
  const expected = new Map();
  for (const p of publications) for (const id of p.recipients) expected.set(`${id}/${p.data.seq}`, p);
  const seen = new Set();
  for (const receipt of receipts) {
    const key = `${receipt.client}/${receipt.message.data?.seq}`;
    assert.ok(expected.has(key), `unexpected recipient or event: ${key}`);
    assert.ok(!seen.has(key), `duplicate delivery: ${key}`);
    seen.add(key);
    const p = expected.get(key);
    const client = clients.find((c) => c.id === receipt.client);
    assert.equal(receipt.message.type, 'event');
    assert.equal(receipt.message.channel, p.channel);
    assert.equal(receipt.message.publishedAt, p.publishedAt);
    assert.deepEqual(receipt.message.data, p.data, `payload changed: ${key}`);
    assert.equal(receipt.message.relayedBy, client.instanceId, `wrong relay instance: ${key}`);
  }
  for (const key of expected.keys()) assert.ok(seen.has(key), `missing delivery: ${key}`);
  return { expected: expected.size, received: seen.size, missing: 0, duplicates: 0, unexpected: 0 };
}

async function runProof(options, argv) {
  const runId = randomUUID();
  const prefix = `ws1b:${runId}:`;
  const startedAt = new Date().toISOString();
  const startedMono = performance.now();
  const servers = [], clients = [], publications = [], receipts = [], operations = [], transportErrors = [];
  const result = { schemaVersion: 1, milestone: 'WS-1B', runId, startedAt, status: 'RUNNING',
    redis: { host: options['redis-host'], port: options['redis-port'], channel: prefix + 'all' },
    servers, clients, publications, receipts, operations, failures: [] };
  const resources = [], sockets = [];
  let closing = false, publisher;
  const record = (action, detail = {}) => operations.push({ action, elapsedMs: performance.now() - startedMono, ...detail });
  const waitFor = async (condition, description) => {
    const deadline = performance.now() + options.timeout * 1000;
    while (!condition()) {
      if (transportErrors.length) throw new Error(transportErrors.join('; '));
      if (performance.now() >= deadline) throw new Error(`timeout: ${description}`);
      await sleep(10);
    }
  };
  const captureProvenance = () => {
    const meta = provenance(argv, { topology: 'two independent Node processes, direct connections, same host; no LB', servers });
    for (const test of ['tests/two-node.test.js', 'tests/fixtures/ws1b-mutant.cjs']) {
      meta.git.sourceSha256[test] = createHash('sha256').update(fs.readFileSync(path.join(ROOT, test), 'utf8').replace(/\r\n/g, '\n')).digest('hex');
    }
    meta.testMutation = process.env.WS1B_MUTATION || null;
    meta.observation = { timeoutSeconds: options.timeout, quietWindowMs: 200 };
    return meta;
  };
  result.provenance = captureProvenance();
  try {
    publisher = new Redis({ host: options['redis-host'], port: options['redis-port'], lazyConnect: true,
      enableOfflineQueue: false, maxRetriesPerRequest: 0, retryStrategy: () => null,
      connectTimeout: options.timeout * 1000, commandTimeout: options.timeout * 1000 });
    publisher.on('error', () => { if (!closing) transportErrors.push('Redis transport error'); });
    await publisher.connect();
    const info = await publisher.info('server');
    result.redis.version = info.match(/^redis_version:(.+)$/m)?.[1]?.trim();
    result.redis.os = info.match(/^os:(.+)$/m)?.[1]?.trim();
    assert.match(result.redis.version || '', /^\d+\.\d+/);
    for (const name of ['A', 'B']) {
      const server = await startServer({ ...options, 'channel-prefix': prefix });
      resources.push(server);
      servers.push({ name, pid: server.pid, url: server.url, instanceId: server.runtime.server.config.instanceId,
        runtime: server.runtime.server });
    }
    assert.notEqual(servers[0].pid, servers[1].pid);
    assert.notEqual(servers[0].url, servers[1].url);
    assert.notEqual(servers[0].instanceId, servers[1].instanceId);
    for (const s of servers) { process.kill(s.pid, 0); assert.notEqual(s.pid, process.pid); }
    const count = await publisher.pubsub('NUMSUB', prefix + 'all');
    assert.equal(Number(count[1]), 2, 'exactly two Redis subscriber connections in isolated namespace');
    result.redis.subscribersBefore = Number(count[1]);
    record('both-processes-live-and-subscribed');
    const shared = prefix + 'shared';
    const pending = new Map();
    const control = async (client, action, channel) => {
      const expected = action === 'subscribe' ? 'subscribed' : 'unsubscribed';
      const request = { type: expected, channel, acknowledged: false };
      assert.ok(!pending.has(client.id), 'one pending control per client');
      pending.set(client.id, request);
      sockets[client.index].send(JSON.stringify({ action, channel }));
      await waitFor(() => request.acknowledged, `${action} acknowledgment for ${client.id}`);
      pending.delete(client.id);
      record('control-acknowledged', { client: client.id, operation: action, channel });
    };
    for (const server of servers) for (const role of ['shared-1', 'shared-2', 'private']) {
      const client = { id: `${server.name}-${role}`, index: clients.length, node: server.name,
        instanceId: server.instanceId, url: server.url, channel: role === 'private' ? prefix + server.name : shared, opened: false };
      clients.push(client);
      const socket = new WebSocket(server.url, { handshakeTimeout: options.timeout * 1000, perMessageDeflate: false });
      sockets.push(socket);
      socket.on('open', () => { client.opened = true; });
      socket.on('error', () => { if (!closing) transportErrors.push(`socket error: ${client.id}`); });
      socket.on('close', () => { if (!closing) transportErrors.push(`socket closed: ${client.id}`); });
      socket.on('message', (raw) => {
        let message;
        try { message = JSON.parse(raw.toString()); } catch { transportErrors.push('invalid JSON'); return; }
        if (message?.type === 'event') {
          const receipt = { client: client.id, message, elapsedMs: performance.now() - startedMono };
          receipts.push(receipt); record('received', receipt);
        } else {
          const request = pending.get(client.id);
          if (request && message?.type === request.type && message.channel === request.channel) request.acknowledged = true;
          else transportErrors.push(`unexpected control response: ${client.id}`);
        }
      });
    }
    await waitFor(() => clients.every((c) => c.opened), 'all six sockets open');
    await Promise.all(clients.map((c) => control(c, 'subscribe', c.channel)));
    record('all-six-clients-ready');
    const publish = async (label, channel, recipients) => {
      const envelope = { channel, publishedAt: Date.now(), data: { runId, seq: publications.length, label, value: 42 } };
      const publication = { ...envelope, recipients, elapsedMs: performance.now() - startedMono };
      publications.push(publication);
      record('publish-once', { seq: envelope.data.seq, channel, recipients });
      publication.redisSubscribers = await publisher.publish(prefix + 'all', JSON.stringify(envelope));
      assert.equal(publication.redisSubscribers, 2);
    };
    const observe = async (stage) => {
      const expected = publications.reduce((sum, p) => sum + p.recipients.length, 0);
      await waitFor(() => receipts.length >= expected, `deliveries for ${stage}`);
      // Positive receipts alone cannot prove absence of extra deliveries.
      // Observe a bounded quiet window, then validate the full transcript.
      await sleep(200);
      assert.deepEqual(transportErrors, []);
      result.accounting = verifyTranscript(publications, receipts, clients);
      record('stage-verified', { stage, observationMs: 200, ...result.accounting });
    };
    await Promise.all([
      publish('shared', shared, ['A-shared-1', 'A-shared-2', 'B-shared-1', 'B-shared-2']),
      publish('A-only', prefix + 'A', ['A-private']),
      publish('B-only', prefix + 'B', ['B-private']),
      publish('no-subscribers', prefix + 'empty', []),
    ]);
    await observe('cross-instance-and-channel-isolation');
    await control(clients[0], 'unsubscribe', shared);
    await publish('after-A-unsubscribe', shared, ['A-shared-2', 'B-shared-1', 'B-shared-2']);
    await observe('unsubscribe-remains-local');
    await control(clients[0], 'subscribe', shared);
    await control(clients[0], 'subscribe', shared);
    await publish('after-repeat-subscribe', shared, ['A-shared-1', 'A-shared-2', 'B-shared-1', 'B-shared-2']);
    await observe('repeat-subscription-has-no-duplicate');
    for (const server of servers) process.kill(server.pid, 0);
    assert.ok(sockets.every((s) => s.readyState === WebSocket.OPEN));
    result.redis.subscribersAfter = Number((await publisher.pubsub('NUMSUB', prefix + 'all'))[1]);
    assert.equal(result.redis.subscribersAfter, 2);
    result.byNode = Object.fromEntries(servers.map((s) => [s.name, {
      instanceId: s.instanceId, clients: clients.filter((c) => c.node === s.name).length,
      received: receipts.filter((r) => r.message.relayedBy === s.instanceId).length,
    }]));
    record('both-processes-and-six-sockets-still-live');
    result.status = 'PASSED';
  } catch (e) {
    result.status = 'FAILED';
    result.failures.push(e.message);
  } finally {
    closing = true;
    for (const socket of sockets) socket.terminate();
    for (const resource of resources) {
      try { await resource.stop(); } catch (e) { result.status = 'FAILED'; result.failures.push(`cleanup: ${e.message}`); }
    }
    if (publisher?.status === 'ready') {
      try {
        const remaining = await publisher.pubsub('NUMSUB', prefix + 'all');
        result.redis.subscribersAfterCleanup = Number(remaining[1]);
        assert.equal(Number(remaining[1]), 0);
      } catch (e) { result.status = 'FAILED'; result.failures.push(`cleanup: ${e.message}`); }
    }
    publisher?.disconnect();
    record('cleanup-completed');
  }
  result.transportErrors = transportErrors;
  if (transportErrors.length) { result.status = 'FAILED'; result.failures.push(...transportErrors); }
  const after = captureProvenance();
  result.provenance.sourceChangedDuringRun = result.provenance.git.sha !== after.git.sha ||
    JSON.stringify(result.provenance.git.sourceSha256) !== JSON.stringify(after.git.sourceSha256);
  if (result.provenance.sourceChangedDuringRun) { result.status = 'FAILED'; result.failures.push('source changed during proof'); }
  result.completedAt = new Date().toISOString();
  result.limitations = ['same-host two-process correctness proof; no nginx or multi-host proof',
    'finite observation windows; no exactly-once or delivery guarantee', 'no capacity, outage, reconnect or deployment experiment'];
  const directory = path.resolve(ROOT, options['output-dir']);
  fs.mkdirSync(directory, { recursive: true });
  const basename = `${startedAt.replace(/[:.]/g, '-')}-${runId}`;
  const file = path.join(directory, basename + '.json');
  fs.writeFileSync(file, JSON.stringify(result, null, 2) + '\n', { flag: 'wx' });
  fs.writeFileSync(path.join(directory, basename + '.log'), operations.map((o) => JSON.stringify(o)).join('\n') +
    '\n' + JSON.stringify({ status: result.status, failures: result.failures, accounting: result.accounting }) + '\n', { flag: 'wx' });
  console.log(`RESULT ${file}`);
  console.log(`${result.status}: ${result.accounting?.received ?? receipts.length} client deliveries across ${servers.length} processes`);
  return result;
}

if (require.main === module) {
  (async () => {
    const argv = process.argv.slice(2);
    for (let i = 0; i < argv.length; i += 2) {
      if (!['--redis-host', '--redis-port', '--output-dir', '--timeout'].includes(argv[i])) throw new Error(`unsupported proof option: ${argv[i]}`);
    }
    const options = parseArgs(argv);
    if (!argv.includes('--output-dir')) options['output-dir'] = 'bench/results/ws-1b';
    const result = await runProof(options, ['bench/prove-two-node.js', ...argv]);
    process.exitCode = result.status === 'PASSED' ? 0 : 1;
  })().catch((e) => { console.error(e.message); process.exitCode = 1; });
}
module.exports = { runProof, verifyTranscript };
