'use strict';
// Explicit negative controls only; application files are never rewritten.
const fs = require('node:fs'), path = require('node:path'), Module = require('node:module');
const original = Module._extensions['.js'];
const mode = process.env.WS2_MUTATION;
const targets = {
  // WS-3 owns ACK-aware resubscription. Suppress every attempt after the first
  // connection generation, including periodic retries; same missing-recovery bug.
  recovery: ['src/readiness.js', 'if (this.draining || this.inFlight ||', 'if (this.epoch > 1 || this.draining || this.inFlight ||'],
  drain: ['src/server.js', "process.on('SIGTERM', () => shutdown('SIGTERM'));", '// negative control: no SIGTERM handler'],
  backpressure: ['src/hub.js', 'if (socket.bufferedAmount > this.maxBufferedBytes)', 'if (false)'],
};
const target = targets[mode];
if (!target) throw new Error('unknown WS2 mutation');
Module._extensions['.js'] = (mod, filename) => {
  if (filename !== path.resolve(__dirname, '../..', target[0])) return original(mod, filename);
  const source = fs.readFileSync(filename, 'utf8');
  if (!source.includes(target[1])) throw new Error('mutation target missing');
  mod._compile(source.replace(target[1], target[2]), filename);
};
