'use strict';
// Test-only preload: changes loaded application code in memory, never disk.
// Inherited by the two child servers only in explicit negative-control runs.
const fs = require('node:fs');
const path = require('node:path');
const Module = require('node:module');
const original = Module._extensions['.js'];
const target = path.resolve(__dirname, '../../src/server.js');
const replacements = {
  suppress: 'const { delivered, dropped } = { delivered: 0, dropped: 0 };',
  duplicate: 'hub.broadcast(channel, payload); const { delivered, dropped } = hub.broadcast(channel, payload);',
};
Module._extensions['.js'] = (mod, filename) => {
  if (filename !== target) return original(mod, filename);
  const replacement = replacements[process.env.WS1B_MUTATION];
  if (!replacement) throw new Error('unknown WS1B mutation');
  const source = fs.readFileSync(filename, 'utf8');
  const needle = 'const { delivered, dropped } = hub.broadcast(channel, payload);';
  if (!source.includes(needle)) throw new Error('mutation target missing');
  mod._compile(source.replace(needle, replacement), filename);
};
