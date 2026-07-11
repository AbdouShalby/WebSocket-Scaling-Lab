'use strict';

/**
 * End-to-end smoke test: real server + real Redis + real WebSocket client.
 * Requires a Redis reachable at REDIS_HOST/REDIS_PORT (CI provides one as a
 * service container). Skips gracefully when Redis is absent so `npm test`
 * still passes locally without infrastructure.
 */

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const path = require('node:path');
const Redis = require('ioredis');
const WebSocket = require('ws');

const REDIS_HOST = process.env.REDIS_HOST ?? '127.0.0.1';
const REDIS_PORT = parseInt(process.env.REDIS_PORT ?? '6379', 10);
const PORT = 18090;

async function redisAvailable() {
  const r = new Redis({ host: REDIS_HOST, port: REDIS_PORT, lazyConnect: true, maxRetriesPerRequest: 0, retryStrategy: () => null });
  try {
    await r.connect();
    await r.ping();
    r.disconnect();
    return true;
  } catch {
    r.disconnect();
    return false;
  }
}

test('publish → redis → server → subscribed client (end to end)', { timeout: 20_000 }, async (t) => {
  if (!(await redisAvailable())) {
    t.skip('redis not available — skipping e2e test');
    return;
  }

  const server = spawn(process.execPath, [path.join(__dirname, '..', 'src', 'server.js')], {
    env: { ...process.env, PORT: String(PORT), REDIS_HOST, REDIS_PORT: String(REDIS_PORT), INSTANCE_ID: 'e2e-test' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  t.after(() => server.kill('SIGTERM'));

  // Wait for the server to listen
  await new Promise((resolve, reject) => {
    const deadline = setTimeout(() => reject(new Error('server did not start')), 10_000);
    server.stdout.on('data', (buf) => {
      if (buf.toString().includes('listening')) { clearTimeout(deadline); resolve(); }
    });
    server.on('exit', () => reject(new Error('server exited early')));
  });

  const ws = new WebSocket(`ws://127.0.0.1:${PORT}`);
  t.after(() => ws.terminate());

  await new Promise((resolve, reject) => {
    ws.on('open', resolve);
    ws.on('error', reject);
  });

  // Subscribe and await confirmation
  const subscribed = new Promise((resolve) => {
    ws.on('message', function onMsg(raw) {
      const msg = JSON.parse(raw.toString());
      if (msg.type === 'subscribed') { ws.off('message', onMsg); resolve(msg); }
    });
  });
  ws.send(JSON.stringify({ action: 'subscribe', channel: 'product:99' }));
  assert.equal((await subscribed).channel, 'product:99');

  // Publish through Redis and expect delivery
  const eventReceived = new Promise((resolve) => {
    ws.on('message', (raw) => {
      const msg = JSON.parse(raw.toString());
      if (msg.type === 'event') resolve(msg);
    });
  });

  const pub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
  await pub.publish('ws:broadcast:all', JSON.stringify({
    channel: 'product:99',
    publishedAt: Date.now(),
    data: { seq: 1, price: 42.5 },
  }));

  const event = await eventReceived;
  pub.disconnect();

  assert.equal(event.channel, 'product:99');
  assert.equal(event.data.price, 42.5);
  assert.equal(typeof event.publishedAt, 'number');
  assert.ok(Date.now() - event.publishedAt < 5000, 'delivered within 5s');
});
