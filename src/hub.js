'use strict';

/**
 * Hub — the in-memory subscription registry and fan-out engine for ONE instance.
 *
 * Design notes:
 * - `channels` maps channel name -> Set of sockets. Fan-out is O(subscribers),
 *   not O(total connections): broadcasting to "product:42" never touches
 *   clients subscribed only to "product:7".
 * - Backpressure: ws exposes `bufferedAmount` (bytes queued on the socket but
 *   not yet flushed to the kernel). A client on a slow link accumulates buffer;
 *   without a policy, one slow consumer can hold gigabytes of queued frames and
 *   OOM the server. Policy here: DROP messages for that client while it is
 *   over the threshold, and DISCONNECT it after `dropLimit` consecutive drops.
 *   Dropped counts are exported in metrics — drops are a signal, not a secret.
 * - The hub is transport-agnostic on purpose: it only needs objects with
 *   `readyState`, `bufferedAmount`, `send()` and `terminate()`, which makes it
 *   unit-testable without opening a single real socket.
 */

const WS_OPEN = 1;

class Hub {
  constructor({ maxBufferedBytes = 1024 * 1024, dropLimit = 500, maxSubscriptionsPerConnection = 50 } = {}) {
    this.maxBufferedBytes = maxBufferedBytes;
    this.dropLimit = dropLimit;
    this.maxSubscriptionsPerConnection = maxSubscriptionsPerConnection;

    /** @type {Map<string, Set<object>>} channel -> sockets */
    this.channels = new Map();
    /** @type {Map<object, Set<string>>} socket -> channels (for O(1) cleanup) */
    this.subscriptions = new Map();

    this.stats = {
      messagesSent: 0,
      messagesDropped: 0,
      slowConsumersKicked: 0,
    };
  }

  /** Register a socket. Call once per connection. */
  addClient(socket) {
    this.subscriptions.set(socket, new Set());
    socket._consecutiveDrops = 0;
  }

  /** Fully remove a socket from every channel. Call on close. */
  removeClient(socket) {
    const subs = this.subscriptions.get(socket);
    if (!subs) return;
    for (const channel of subs) {
      const set = this.channels.get(channel);
      if (set) {
        set.delete(socket);
        if (set.size === 0) this.channels.delete(channel);
      }
    }
    this.subscriptions.delete(socket);
  }

  /**
   * Subscribe a socket to a channel.
   * @returns {{ok: boolean, error?: string}}
   */
  subscribe(socket, channel) {
    const subs = this.subscriptions.get(socket);
    if (!subs) return { ok: false, error: 'client not registered' };
    if (subs.size >= this.maxSubscriptionsPerConnection && !subs.has(channel)) {
      return { ok: false, error: 'subscription limit reached' };
    }
    subs.add(channel);
    if (!this.channels.has(channel)) this.channels.set(channel, new Set());
    this.channels.get(channel).add(socket);
    return { ok: true };
  }

  /** Unsubscribe a socket from a channel. */
  unsubscribe(socket, channel) {
    const subs = this.subscriptions.get(socket);
    if (subs) subs.delete(channel);
    const set = this.channels.get(channel);
    if (set) {
      set.delete(socket);
      if (set.size === 0) this.channels.delete(channel);
    }
    return { ok: true };
  }

  /**
   * Fan a message out to every open subscriber of `channel`, applying the
   * slow-consumer policy. `payload` must already be a serialized string —
   * we serialize ONCE per broadcast, not once per recipient.
   * @returns {{delivered: number, dropped: number}}
   */
  broadcast(channel, payload) {
    const set = this.channels.get(channel);
    if (!set || set.size === 0) return { delivered: 0, dropped: 0 };

    let delivered = 0;
    let dropped = 0;

    for (const socket of set) {
      if (socket.readyState !== WS_OPEN) continue;

      if (socket.bufferedAmount > this.maxBufferedBytes) {
        // Slow consumer: drop this message for this client.
        dropped += 1;
        this.stats.messagesDropped += 1;
        socket._consecutiveDrops = (socket._consecutiveDrops ?? 0) + 1;

        if (socket._consecutiveDrops >= this.dropLimit) {
          this.stats.slowConsumersKicked += 1;
          socket.terminate();
          this.removeClient(socket);
        }
        continue;
      }

      socket._consecutiveDrops = 0;
      socket.send(payload);
      delivered += 1;
      this.stats.messagesSent += 1;
    }

    return { delivered, dropped };
  }

  /** Snapshot for the /metrics endpoint. */
  snapshot() {
    return {
      channels: this.channels.size,
      connections: this.subscriptions.size,
      ...this.stats,
    };
  }
}

module.exports = { Hub };
