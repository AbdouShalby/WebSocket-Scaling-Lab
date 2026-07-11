'use strict';

/**
 * Event publisher — simulates the "marketplace" side of the system:
 * price/stock updates flowing into Redis, to be fanned out by the WS fleet.
 *
 * Every event carries `publishedAt` (epoch ms) so bench clients can measure
 * TRUE end-to-end latency: publish → Redis → instance → client receive.
 *
 * Usage:
 *   node src/publisher.js --rate 100 --channels 50 --duration 60
 *     --rate      events per second (default 10)
 *     --channels  number of distinct product channels to rotate over (default 10)
 *     --duration  seconds to run, 0 = forever (default 0)
 */

const Redis = require('ioredis');
const config = require('./config');

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i > -1 ? Number(process.argv[i + 1]) : fallback;
}

const rate = arg('rate', 10);
const channels = arg('channels', 10);
const duration = arg('duration', 0);

const redis = new Redis(config.redis);
const REDIS_FANOUT_CHANNEL = config.channelPrefix + 'all';

let seq = 0;
const startedAt = Date.now();

console.log(`publishing ${rate} ev/s across ${channels} channels${duration ? ` for ${duration}s` : ''}`);

const timer = setInterval(() => {
  const batch = [];
  for (let i = 0; i < rate; i++) {
    seq += 1;
    const productId = (seq % channels) + 1;
    const envelope = JSON.stringify({
      channel: `product:${productId}`,
      publishedAt: Date.now(),
      data: {
        seq,
        productId,
        price: Math.round((50 + Math.sin(seq / 20) * 25) * 100) / 100,
        stock: 100 - (seq % 100),
      },
    });
    batch.push(redis.publish(REDIS_FANOUT_CHANNEL, envelope));
  }
  Promise.all(batch).catch((e) => console.error('publish error:', e.message));

  if (duration && (Date.now() - startedAt) / 1000 >= duration) {
    clearInterval(timer);
    console.log(`done — published ${seq} events`);
    redis.quit();
  }
}, 1000);
