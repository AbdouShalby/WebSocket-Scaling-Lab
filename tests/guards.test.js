'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { spawnSync } = require('node:child_process');
const { randomUUID } = require('node:crypto');
const { ROOT } = require('../bench/options');
const { validMessage, validEnvelope, validChannel, TokenBucket } = require('../src/protocol');
const { Readiness } = require('../src/readiness');

test('protocol rejects null, non-object, malformed channel and envelope shapes', () => {
  for (const value of [null, [], 1, 'text', true, {}, { action: 'subscribe', channel: '' },
    { action: 'subscribe', channel: 'x\n' }, { action: 'subscribe', channel: 'é'.repeat(65) },
    { action: 'ping', t: {} }, { action: 'unknown' }]) assert.equal(validMessage(value), false);
  for (const value of [null, [], {}, { channel: 'x' }, { channel: 1, data: {}, publishedAt: 1 },
    { channel: 'x', data: {}, publishedAt: 'now' }]) assert.equal(validEnvelope(value), false);
  assert.equal(validChannel('é'.repeat(64)), true);
  assert.equal(validMessage({ action: 'subscribe', channel: 'product:42' }), true);
  assert.equal(validMessage({ action: 'ping', t: 1 }), true);
  assert.equal(validEnvelope({ channel: 'product:42', data: null, publishedAt: 0 }), true);
});

test('per-connection token bucket refills monotonically and caps idle credit', () => {
  let now = 0;
  const a = new TokenBucket(2, 3, () => now), b = new TokenBucket(2, 3, () => now);
  assert.deepEqual([a.take(), a.take(), a.take(), a.take()], [true, true, true, false]);
  assert.equal(b.take(), true);
  now = 499; assert.equal(a.take(), false);
  now = 500; assert.equal(a.take(), true);
  now = 10000;
  assert.deepEqual([a.take(), a.take(), a.take(), a.take()], [true, true, true, false]);
});

test('readiness waits for ACK; stale ACK after disconnect or drain cannot resurrect it', async () => {
  const sub = new EventEmitter(); sub.status = 'ready';
  const pending = [];
  sub.subscribe = () => new Promise((resolve) => pending.push(resolve));
  sub.ping = async () => ['pong', 'readiness'];
  const state = new Readiness(sub, 'fanout', { intervalMs: 60000 });
  try {
    sub.emit('ready'); assert.equal(state.snapshot().ready, false);
    sub.status = 'reconnecting'; sub.emit('close');
    pending.shift()(1); await new Promise(setImmediate);
    assert.equal(state.snapshot().ready, false);
    sub.status = 'ready'; sub.emit('ready'); pending.shift()(1); await new Promise(setImmediate);
    assert.equal(state.snapshot().ready, true);
    state.drain(); assert.equal(state.snapshot().ready, false);
    sub.emit('ready'); assert.equal(pending.length, 0);
  } finally { state.stop(); }
});

test('failed subscription stays unready and a later check retries; failed ping revokes readiness', async () => {
  const sub = new EventEmitter(); sub.status = 'ready';
  let denied = true;
  sub.disconnect = () => {};
  sub.subscribe = async () => { if (denied) throw new Error('denied'); return 1; };
  sub.ping = async () => { throw new Error('timeout'); };
  const state = new Readiness(sub, 'fanout', { intervalMs: 60000 });
  try {
    await state.check(); assert.equal(state.snapshot().ready, false);
    denied = false; await state.check(); assert.equal(state.snapshot().ready, true);
    await state.check(); assert.equal(state.snapshot().ready, false);
  } finally { state.stop(); }
});

test('drain wins over an outstanding subscription ACK', async () => {
  const sub = new EventEmitter(); sub.status = 'ready';
  let ack; sub.subscribe = () => new Promise((resolve) => { ack = resolve; });
  const state = new Readiness(sub, 'fanout', { intervalMs: 60000 });
  sub.emit('ready'); state.drain(); ack(1); await new Promise(setImmediate);
  assert.equal(state.snapshot().ready, false); assert.equal(state.snapshot().draining, true);
});

test('configuration rejects malformed limits and origin allowlists without echoing values', () => {
  // Synthetic per-run input: parsed and rejected locally, never sent to a service.
  const credentialOrigin = new URL('https://example.test');
  credentialOrigin.username = randomUUID(); credentialOrigin.password = randomUUID();
  for (const [name, value] of [['MAX_PAYLOAD_BYTES', '0'], ['MESSAGE_BURST', '2x'], ['MESSAGES_PER_SECOND', '-1'],
    ['PORT', '65536'], ['MAX_SUBS_PER_CONN', 'NaN'], ['REDIS_CHECK_TIMEOUT_MS', 'Infinity'],
    ['ALLOWED_ORIGINS', '*'], ['ALLOWED_ORIGINS', ','], ['ALLOWED_ORIGINS', 'https://allowed.example,'],
    ['ALLOWED_ORIGINS', credentialOrigin.href]]) {
    const result = spawnSync(process.execPath, ['-e', "require('./src/config')"], { cwd: ROOT, env: { ...process.env, [name]: value }, encoding: 'utf8' });
    assert.equal(result.status, 1, name); assert.match(result.stderr, new RegExp(name));
    for (const part of [credentialOrigin.username, credentialOrigin.password, credentialOrigin.href]) {
      assert.ok(!result.stderr.includes(part), 'configuration errors must not echo synthetic credentials');
    }
  }
});
