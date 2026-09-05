'use strict';
// Reproducible one-server methodology check. Owns only its child server process.
const net = require('node:net');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const { randomUUID } = require('node:crypto');
const { ROOT, parseArgs } = require('./options');
const { execute } = require('./connect-storm');

async function startServer(options) {
  const reservation = net.createServer();
  reservation.listen(0, '127.0.0.1');
  await once(reservation, 'listening');
  const port = reservation.address().port;
  await new Promise((resolve) => reservation.close(resolve));
  const config = { port, instanceId: `ws-1a-${randomUUID()}`, heartbeatMs: 30000,
    maxBufferedBytes: 1048576, dropLimit: 500, maxSubscriptions: 50, maxPayloadBytes: 4096,
    channelPrefix: options['channel-prefix'] };
  const child = spawn(process.execPath, ['src/server.js'], { cwd: ROOT, windowsHide: true,
    env: { ...process.env, PORT: String(port), INSTANCE_ID: config.instanceId,
      REDIS_HOST: options['redis-host'], REDIS_PORT: String(options['redis-port']), CHANNEL_PREFIX: config.channelPrefix,
      HEARTBEAT_INTERVAL_MS: String(config.heartbeatMs), MAX_BUFFERED_BYTES: String(config.maxBufferedBytes),
      DROP_LIMIT: String(config.dropLimit), MAX_SUBS_PER_CONN: String(config.maxSubscriptions), MAX_PAYLOAD_BYTES: String(config.maxPayloadBytes) },
    stdio: ['ignore', 'pipe', 'pipe'] });
  let output = '';
  let errors = '';
  child.stdout.on('data', (chunk) => { output += chunk; });
  child.stderr.on('data', (chunk) => { errors += chunk; });
  const stop = async () => {
    if (child.exitCode !== null || child.signalCode !== null) return;
    const exited = once(child, 'exit');
    child.kill('SIGTERM');
    const force = setTimeout(() => child.kill('SIGKILL'), 12000);
    await exited;
    clearTimeout(force);
  };
  try {
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => done(new Error(`server startup timed out: ${errors}`)), options.timeout * 1000);
      const poll = setInterval(() => {
        if (output.includes('listening on') && output.includes('subscribed to')) done();
      }, 10);
      const done = (err) => {
        clearTimeout(timer); clearInterval(poll);
        child.off('exit', onExit); child.off('error', done);
        err ? reject(err) : resolve();
      };
      const onExit = () => done(new Error(`server exited before ready: ${errors}`));
      child.once('exit', onExit); child.once('error', done);
    });
    return { url: `ws://127.0.0.1:${port}`, stop, runtime: {
      schemaVersion: 1, evidenceSource: 'run-local spawned this server with explicit configuration',
      environment: 'controller, publisher and one server on this host; Redis endpoint recorded in options and INFO',
      topology: { description: 'direct one-server loopback, no nginx, external real Redis', wsInstances: 1 },
      server: { node: process.version, ws: require('ws/package.json').version,
        ioredis: require('ioredis/package.json').version, config },
    } };
  } catch (e) { await stop(); throw e; }
}

async function main() {
  const argv = process.argv.slice(2);
  if (argv.includes('--url') || argv.includes('--runtime')) throw new Error('run-local discovers its own URL and runtime; use connect-storm for an existing target');
  const options = parseArgs(argv);
  const server = await startServer(options);
  try {
    const result = await execute({ ...options, url: server.url }, server.runtime, ['bench/run-local.js', ...argv]);
    process.exitCode = result.status === 'COMPLETE' ? 0 : 1;
  } finally { await server.stop(); }
}
if (require.main === module) main().catch((e) => { console.error(e.message); process.exitCode = 1; });
module.exports = { startServer };
