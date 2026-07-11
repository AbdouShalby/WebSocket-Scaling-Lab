'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Hub } = require('../src/hub');

const WS_OPEN = 1;

/** Fake socket implementing the minimal surface Hub needs. */
function fakeSocket({ buffered = 0 } = {}) {
  return {
    readyState: WS_OPEN,
    bufferedAmount: buffered,
    sent: [],
    terminated: false,
    send(payload) { this.sent.push(payload); },
    terminate() { this.terminated = true; },
  };
}

test('broadcast reaches only subscribers of the channel', () => {
  const hub = new Hub();
  const a = fakeSocket();
  const b = fakeSocket();
  hub.addClient(a);
  hub.addClient(b);
  hub.subscribe(a, 'product:1');
  hub.subscribe(b, 'product:2');

  const result = hub.broadcast('product:1', 'payload');

  assert.equal(result.delivered, 1);
  assert.equal(a.sent.length, 1);
  assert.equal(b.sent.length, 0);
});

test('unsubscribe stops delivery and empty channels are garbage-collected', () => {
  const hub = new Hub();
  const a = fakeSocket();
  hub.addClient(a);
  hub.subscribe(a, 'product:1');
  hub.unsubscribe(a, 'product:1');

  const result = hub.broadcast('product:1', 'payload');

  assert.equal(result.delivered, 0);
  assert.equal(hub.channels.size, 0, 'empty channel Set must be removed');
});

test('removeClient cleans every channel the socket was in', () => {
  const hub = new Hub();
  const a = fakeSocket();
  hub.addClient(a);
  hub.subscribe(a, 'product:1');
  hub.subscribe(a, 'product:2');

  hub.removeClient(a);

  assert.equal(hub.channels.size, 0);
  assert.equal(hub.subscriptions.size, 0);
});

test('subscription limit is enforced', () => {
  const hub = new Hub({ maxSubscriptionsPerConnection: 2 });
  const a = fakeSocket();
  hub.addClient(a);
  assert.equal(hub.subscribe(a, 'c1').ok, true);
  assert.equal(hub.subscribe(a, 'c2').ok, true);
  const third = hub.subscribe(a, 'c3');
  assert.equal(third.ok, false);
  assert.match(third.error, /limit/);
  // Re-subscribing to an existing channel is allowed (idempotent)
  assert.equal(hub.subscribe(a, 'c1').ok, true);
});

test('slow consumer gets messages dropped, not queued', () => {
  const hub = new Hub({ maxBufferedBytes: 100 });
  const slow = fakeSocket({ buffered: 10_000 });
  const fast = fakeSocket({ buffered: 0 });
  hub.addClient(slow);
  hub.addClient(fast);
  hub.subscribe(slow, 'product:1');
  hub.subscribe(fast, 'product:1');

  const result = hub.broadcast('product:1', 'payload');

  assert.equal(result.delivered, 1);
  assert.equal(result.dropped, 1);
  assert.equal(slow.sent.length, 0);
  assert.equal(fast.sent.length, 1);
  assert.equal(hub.stats.messagesDropped, 1);
});

test('slow consumer is disconnected after dropLimit consecutive drops', () => {
  const hub = new Hub({ maxBufferedBytes: 100, dropLimit: 3 });
  const slow = fakeSocket({ buffered: 10_000 });
  hub.addClient(slow);
  hub.subscribe(slow, 'product:1');

  hub.broadcast('product:1', 'p1');
  hub.broadcast('product:1', 'p2');
  assert.equal(slow.terminated, false, 'still under the drop limit');

  hub.broadcast('product:1', 'p3');
  assert.equal(slow.terminated, true, 'kicked at the drop limit');
  assert.equal(hub.stats.slowConsumersKicked, 1);
  assert.equal(hub.subscriptions.size, 0, 'kicked client fully removed');
});

test('recovered consumer resets its consecutive-drop counter', () => {
  const hub = new Hub({ maxBufferedBytes: 100, dropLimit: 3 });
  const sock = fakeSocket({ buffered: 10_000 });
  hub.addClient(sock);
  hub.subscribe(sock, 'product:1');

  hub.broadcast('product:1', 'p1');
  hub.broadcast('product:1', 'p2');

  sock.bufferedAmount = 0; // client drained its buffer
  hub.broadcast('product:1', 'p3');
  assert.equal(sock.sent.length, 1);

  sock.bufferedAmount = 10_000; // slow again — counter must restart from 0
  hub.broadcast('product:1', 'p4');
  hub.broadcast('product:1', 'p5');
  assert.equal(sock.terminated, false, 'counter was reset by the successful send');
});

test('closed sockets are skipped without counting as drops', () => {
  const hub = new Hub();
  const closed = fakeSocket();
  closed.readyState = 3; // CLOSED
  hub.addClient(closed);
  hub.subscribe(closed, 'product:1');

  const result = hub.broadcast('product:1', 'payload');

  assert.equal(result.delivered, 0);
  assert.equal(result.dropped, 0);
});

test('broadcast to a channel with no subscribers is a cheap no-op', () => {
  const hub = new Hub();
  const result = hub.broadcast('ghost', 'payload');
  assert.deepEqual(result, { delivered: 0, dropped: 0 });
});
