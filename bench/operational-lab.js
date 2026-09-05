'use strict';
const assert = require('node:assert/strict');
const net = require('node:net');
const { spawn } = require('node:child_process');
const { randomUUID, createHash } = require('node:crypto');
const { performance } = require('node:perf_hooks');
const Redis = require('ioredis');
const WebSocket = require('ws');
const { ROOT } = require('./options');
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function waitFor(predicate, label, ms = 5000) {
  const end = performance.now() + ms;
  while (!(await predicate())) {
    if (performance.now() >= end) throw new Error(`timeout: ${label}`);
    await sleep(20);
  }
}
async function port() {
  const listener = net.createServer();
  await new Promise((resolve, reject) => { listener.once('error', reject); listener.listen(0, '127.0.0.1', resolve); });
  const value = listener.address().port;
  await new Promise((resolve) => listener.close(resolve));
  return value;
}
const digest = (value) => createHash('sha256').update(value).digest('hex');

class Lab {
  constructor(name) {
    this.name = name; this.id = randomUUID(); this.prefix = `ws2:${this.id}:`; this.channel = this.prefix + 'events';
    this.start = performance.now(); this.children = []; this.clients = []; this.publishers = []; this.issued = new Map();
    this.result = { scenario: name, runId: this.id, status: 'RUNNING', timeline: [], processes: [], clients: [], publications: [], receipts: [], observations: {}, failures: [] };
  }
  record(action, detail = {}) { this.result.timeline.push({ action, elapsedMs: performance.now() - this.start, ...detail }); }
  child(role, executable, args, env = process.env) {
    const p = spawn(executable, args, { cwd: ROOT, env, stdio: ['ignore', 'pipe', 'pipe'] });
    const entry = { role, pid: p.pid, executable: role === 'redis' ? 'redis-server' : 'node', args, log: '', errors: '' };
    p.stdout.on('data', (b) => { entry.log = (entry.log + b).slice(-16000); });
    p.stderr.on('data', (b) => { entry.errors = (entry.errors + b).slice(-16000); });
    p.on('error', (e) => { entry.spawnError = e.code || e.message; });
    p.on('exit', (code, signal) => { entry.exit = { code, signal, elapsedMs: performance.now() - this.start }; this.record('process-exit', { role, pid: p.pid, code, signal }); });
    const owned = { p, entry }; this.children.push(owned); this.result.processes.push(entry);
    this.record('process-started', { role, pid: p.pid });
    return owned;
  }
  async signal(owned, signal) {
    assert.ok(!owned.entry.exit, 'cannot signal an exited process');
    this.record('signal', { pid: owned.p.pid, signal });
    assert.equal(owned.p.kill(signal), true);
  }
  async exited(owned, ms = 5000) { await waitFor(() => owned.entry.exit, `${owned.entry.role} exit`, ms); return owned.entry.exit; }
  async publisher() {
    const p = new Redis({ host: '127.0.0.1', port: this.redisPort, lazyConnect: true, retryStrategy: () => null,
      maxRetriesPerRequest: 0, enableOfflineQueue: false, connectTimeout: 1000, commandTimeout: 1000 });
    p.on('error', () => {}); this.publishers.push(p); await p.connect(); return p;
  }
  async startRedis() {
    this.redisPort ??= await port();
    const owned = this.child('redis', process.env.REDIS_SERVER || 'redis-server', ['--bind', '127.0.0.1', '--port', String(this.redisPort), '--save', '', '--appendonly', 'no', '--protected-mode', 'yes']);
    await waitFor(() => {
      if (owned.entry.spawnError || owned.entry.exit) throw new Error(`Redis startup failed: ${owned.entry.spawnError || owned.entry.log}`);
      return owned.entry.log.includes('Ready to accept connections');
    }, 'owned Redis startup');
    this.pub = await this.publisher();
    const info = await this.pub.info('server');
    const field = (k) => info.match(new RegExp(`^${k}:(.+)$`, 'm'))?.[1]?.trim();
    assert.equal(Number(field('process_id')), owned.p.pid, 'Redis must be the owned child, not another daemon');
    owned.entry.runtime = { version: field('redis_version'), os: field('os'), runId: field('run_id'), port: this.redisPort };
    assert.equal((await this.pub.config('GET', 'save'))[1], '');
    assert.equal((await this.pub.config('GET', 'appendonly'))[1], 'no');
    this.redis = owned; return owned;
  }
  async server(role, overrides = {}) {
    const listenPort = await port(), instanceId = `ws2-${role}-${randomUUID()}`;
    const config = { PORT: String(listenPort), INSTANCE_ID: instanceId, REDIS_HOST: '127.0.0.1', REDIS_PORT: String(this.redisPort),
      CHANNEL_PREFIX: this.prefix, HEARTBEAT_INTERVAL_MS: '30000', MAX_BUFFERED_BYTES: '1048576', DROP_LIMIT: '500',
      MAX_SUBS_PER_CONN: '50', MAX_PAYLOAD_BYTES: '4096', ...overrides };
    const owned = this.child(role, process.execPath, ['src/server.js'], { ...process.env, ...config });
    Object.assign(owned, { instanceId, url: `ws://127.0.0.1:${listenPort}`, http: `http://127.0.0.1:${listenPort}` });
    owned.entry.config = config;
    await waitFor(() => {
      if (owned.entry.spawnError || owned.entry.exit) throw new Error(`server startup failed: ${owned.entry.errors}`);
      return owned.entry.log.includes('listening on') && owned.entry.log.includes('subscribed to');
    }, `${role} ready`);
    return owned;
  }
  async subscribers(n) {
    await waitFor(async () => Number((await this.pub.pubsub('NUMSUB', this.prefix + 'all'))[1]) === n, `Redis subscribers=${n}`);
    this.record('Redis-subscriber-count', { count: n });
  }
  async client(server, id) {
    const ws = new WebSocket(server.url, { perMessageDeflate: false, handshakeTimeout: 3000 });
    const c = { id, server, ws, acks: [], events: [] };
    const entry = { id, url: server.url, instanceId: server.instanceId, errors: [] }; c.entry = entry;
    this.clients.push(c); this.result.clients.push(entry);
    ws.on('error', (e) => entry.errors.push(e.code || e.message));
    ws.on('close', (code, reason) => { entry.close = { code, reason: reason.toString() }; this.record('client-close', { id, ...entry.close }); });
    ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw);
        if (msg.type === 'subscribed') { c.acks.push(msg.channel); return; }
        assert.equal(msg.type, 'event');
        const publication = this.issued.get(msg.data.id);
        assert.ok(publication, 'unknown event'); assert.equal(msg.channel, this.channel);
        assert.equal(msg.relayedBy, server.instanceId); assert.equal(msg.publishedAt, publication.publishedAt);
        assert.deepEqual(msg.data, publication.data);
        c.events.push(msg.data.id);
        this.result.receipts.push({ client: id, id: msg.data.id, relayedBy: msg.relayedBy, bytes: raw.length, elapsedMs: performance.now() - this.start });
      } catch (e) { this.result.failures.push(`invalid receipt: ${e.message}`); }
    });
    await waitFor(() => {
      if (entry.errors.length || entry.close) throw new Error(`client ${id} failed to open`);
      return ws.readyState === WebSocket.OPEN;
    }, `client ${id} open`);
    this.record('client-open', { id, instanceId: server.instanceId }); return c;
  }
  async subscribe(c) {
    const count = c.acks.length;
    c.ws.send(JSON.stringify({ action: 'subscribe', channel: this.channel }));
    await waitFor(() => c.acks.length > count, `subscription ${c.id}`);
    assert.equal(c.acks.at(-1), this.channel); this.record('subscription-ack', { client: c.id });
  }
  async publish(label, required, optional = [], blob = '') {
    const id = `${this.id}/${this.result.publications.length}`;
    const message = { channel: this.channel, publishedAt: Date.now(), data: { id, label, blob } };
    this.issued.set(id, message);
    const entry = { id, label, required: required.map((c) => c.id), allowed: [...required, ...optional].map((c) => c.id), blobBytes: Buffer.byteLength(blob), blobSha256: digest(blob) };
    this.result.publications.push(entry);
    entry.redisSubscribers = await this.pub.publish(this.prefix + 'all', JSON.stringify(message));
    this.record('published', { id, label, redisSubscribers: entry.redisSubscribers });
    await waitFor(() => required.every((c) => c.events.includes(id)), `delivery ${label}`);
    return id;
  }
  async metrics(server) {
    const r = await fetch(server.http + '/metrics', { signal: AbortSignal.timeout(1500) });
    assert.equal(r.status, 200); return r.json();
  }
  async verify() {
    await sleep(250); // Explicit finite absence/duplicate observation window.
    for (const p of this.result.publications) {
      const seen = this.result.receipts.filter((r) => r.id === p.id);
      assert.equal(new Set(seen.map((r) => r.client)).size, seen.length, 'duplicate receipt');
      for (const r of seen) assert.ok(p.allowed.includes(r.client), 'unexpected recipient');
      for (const id of p.required) assert.ok(seen.some((r) => r.client === id), 'missing required receipt');
    }
    assert.deepEqual(this.result.failures, []);
  }
  async cleanup() {
    for (const c of this.clients) { c.ws.resume(); c.ws.terminate(); }
    for (const pub of this.publishers) pub.disconnect();
    for (const owned of [...this.children].reverse()) if (!owned.entry.exit && !owned.entry.spawnError) {
      owned.p.kill('SIGKILL'); await this.exited(owned);
    }
    assert.ok(this.children.every((c) => c.entry.exit || c.entry.spawnError));
    this.result.cleanup = { allOwnedProcessesExited: true };
    this.record('owned-resources-cleaned');
  }
}
module.exports = { Lab, waitFor, sleep };
