'use strict';
// Negative controls only: mutate module text in memory, never source files.
const fs = require('node:fs'), path = require('node:path'), Module = require('node:module');
const original = Module._extensions['.js'];
const targets = {
  origin: ['src/server.js', 'if (config.allowedOrigins.length &&', 'if (false &&'],
  schema: ['src/server.js', "if (!validMessage(msg)) throw new Error('invalid protocol message');", '// missing shape guard'],
  rate: ['src/server.js', 'if (!messages.take())', 'if (false)'],
  ready: ['src/readiness.js', 'ready: this.ready && !this.draining', 'ready: !this.draining'],
};
const target = targets[process.env.WS3_MUTATION];
if (!target) throw new Error('unknown WS-3 mutation');
Module._extensions['.js'] = (mod, filename) => {
  if (filename !== path.resolve(__dirname, '../..', target[0])) return original(mod, filename);
  const source = fs.readFileSync(filename, 'utf8');
  if (!source.includes(target[1])) throw new Error('mutation target missing');
  mod._compile(source.replace(target[1], target[2]), filename);
};
