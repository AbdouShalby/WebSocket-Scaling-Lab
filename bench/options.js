'use strict';
const path = require('node:path');
const ROOT = path.resolve(__dirname, '..');
const defaults = {
  url: 'ws://127.0.0.1:8080', conns: 1000, channels: 10, ramp: 100,
  measure: 30, warmup: 1, drain: 1, timeout: 10, rate: 50,
  'redis-host': '127.0.0.1', 'redis-port': 6379, 'channel-prefix': 'ws:broadcast:',
  'output-dir': 'bench/results', runtime: '',
};
function parseArgs(argv) {
  const options = { ...defaults };
  const used = new Set();
  for (let i = 0; i < argv.length; i += 2) {
    const key = argv[i].slice(2);
    if (!argv[i].startsWith('--') || !Object.hasOwn(defaults, key) || used.has(key) || argv[i + 1] === undefined) {
      throw new Error(`unknown, duplicate, or incomplete option: ${argv[i]}`);
    }
    used.add(key);
    options[key] = typeof defaults[key] === 'number' ? Number(argv[i + 1]) : argv[i + 1];
  }
  validateOptions(options);
  return options;
}
function validateOptions(o) {
  for (const key of ['conns', 'channels', 'ramp', 'rate', 'redis-port']) {
    if (!Number.isSafeInteger(o[key]) || o[key] <= 0) throw new Error(`${key} must be a positive integer`);
  }
  for (const key of ['measure', 'warmup', 'drain', 'timeout']) {
    if (!Number.isFinite(o[key]) || o[key] < (key === 'warmup' ? 0 : 0.01) || o[key] > 3600) {
      throw new Error(`${key} must be finite, bounded seconds`);
    }
  }
  if (o.channels > o.conns || o['redis-port'] > 65535) throw new Error('invalid channel count or Redis port');
  if (o.conns > 100000 || Math.ceil(o.rate * o.measure) * Math.ceil(o.conns / o.channels) > 5_000_000) {
    throw new Error('run exceeds exact-accounting limits (100k clients / 5 million expected deliveries)');
  }
  const url = new URL(o.url);
  if (!['ws:', 'wss:'].includes(url.protocol) || url.username || url.password || url.search || url.hash) {
    throw new Error('use a ws/wss URL without credentials, query, or fragment (arguments become artifacts)');
  }
  if (!o['redis-host'] || /[\s/@]/.test(o['redis-host']) || !o['channel-prefix']) throw new Error('invalid Redis endpoint');
  if (!path.resolve(ROOT, o['output-dir']).startsWith(ROOT + path.sep)) throw new Error('output-dir must stay inside repository');
}
function validateRuntime(r) {
  if (r?.schemaVersion !== 1 || typeof r.environment !== 'string' || !r.environment ||
      !r.topology?.description || !Number.isSafeInteger(r.topology.wsInstances) || r.topology.wsInstances < 1 ||
      !r.server?.node || !r.server?.ws || !r.server?.ioredis || !r.server?.config) {
    throw new Error('runtime metadata requires schemaVersion=1, environment, topology and server versions/config');
  }
}
module.exports = { ROOT, defaults, parseArgs, validateOptions, validateRuntime };
