'use strict';
const assert = require('node:assert/strict');

function plan(connections, repeats) {
  const jobs = [], nodes = [1, 2, 4];
  for (let repetition = 1; repetition <= repeats; repetition++) {
    for (const conns of connections) {
      for (let offset = 0; offset < 3; offset++) jobs.push({ repetition, conns, nodes: nodes[(repetition - 1 + offset) % 3] });
    }
  }
  return jobs;
}
function stats(values) {
  assert.ok(values.length && values.every(Number.isFinite), 'finite samples required');
  const ordered = [...values].sort((a, b) => a - b), middle = Math.floor(ordered.length / 2);
  return { median: ordered.length % 2 ? ordered[middle] : (ordered[middle - 1] + ordered[middle]) / 2,
    min: ordered[0], max: ordered.at(-1) };
}
function summarize(trials, config) {
  const expected = plan(config.connections, config.repeats);
  assert.equal(trials.length, expected.length, 'incomplete matrix');
  const keys = new Set();
  let source = null;
  for (const t of trials) {
    const key = `${t.repetition}/${t.conns}/${t.nodes}`;
    assert.ok(!keys.has(key), 'duplicate matrix cell'); keys.add(key);
    assert.ok(expected.some((j) => j.repetition === t.repetition && j.conns === t.conns && j.nodes === t.nodes), 'unexpected matrix cell');
    const r = t.result, a = r.accounting;
    assert.equal(r.status, 'COMPLETE', 'failed trial cannot be aggregated');
    assert.deepEqual(r.failures, []);
    assert.equal(r.provenance.sourceChangedDuringRun, false);
    const fingerprint = JSON.stringify([r.provenance.git.sha, r.provenance.git.sourceSha256]);
    if (source !== null) assert.equal(fingerprint, source, 'source differs between trials');
    source = fingerprint;
    assert.equal(r.fleet.requestedNodes, t.nodes);
    assert.equal(r.fleet.servers.length, t.nodes);
    assert.equal(new Set(r.fleet.servers.map((s) => s.pid)).size, t.nodes);
    assert.equal(r.fleet.subscribersBefore, t.nodes); assert.equal(r.fleet.subscribersAfter, t.nodes);
    assert.equal(r.fleet.subscribersAfterCleanup, 0);
    assert.equal(r.options.conns, t.conns);
    for (const field of ['channels', 'rate', 'warmup', 'measure', 'drain', 'ramp', 'timeout']) {
      assert.equal(r.options[field], config.options[field], `different workload: ${field}`);
    }
    assert.equal(r.connections.ready, t.conns); assert.equal(r.connections.disconnected, 0);
    assert.equal(a.expected, r.publisher.issued * t.conns / r.options.channels);
    assert.equal(a.received, a.expected); assert.equal(a.received, a.inWindow + a.late);
    assert.equal(a.missing + a.duplicates + a.unexpected, 0);
    assert.ok(a.elapsedMs >= r.options.measure * 1000 - 5, 'measurement window too short');
    assert.equal(a.deliveriesPerSecond, a.inWindow * 1000 / a.elapsedMs);
    assert.equal(a.latencyMs.samples, a.inWindow);
    assert.equal(a.latencyMs.histogram.reduce((sum, [, n]) => sum + n, 0), a.inWindow);
    assert.equal(r.publisher.issued, r.publisher.confirmed); assert.equal(r.publisher.errors, 0);
    assert.equal(r.publisher.eventsPerSecond, r.publisher.issued * 1000 / a.elapsedMs);
    assert.equal(Object.keys(a.instances).length, t.nodes);
    for (const s of r.fleet.servers) {
      assert.equal(a.instances[s.instanceId].clients, t.conns / t.nodes);
      assert.equal(a.instances[s.instanceId].received, a.expected / t.nodes);
    }
  }
  return config.connections.flatMap((conns) => [1, 2, 4].map((nodes) => {
    const group = trials.filter((t) => t.conns === conns && t.nodes === nodes).map((t) => t.result);
    assert.equal(group.length, config.repeats);
    return { conns, nodes, repetitions: group.length,
      targetDeliveriesPerSecond: config.options.rate * conns / config.options.channels,
      issuedEventsPerSecond: stats(group.map((r) => r.publisher.eventsPerSecond)),
      deliveriesPerSecond: stats(group.map((r) => r.accounting.deliveriesPerSecond)),
      p50Ms: stats(group.map((r) => r.accounting.latencyMs.p50)),
      p95Ms: stats(group.map((r) => r.accounting.latencyMs.p95)),
      p99Ms: stats(group.map((r) => r.accounting.latencyMs.p99)),
      late: stats(group.map((r) => r.accounting.late)),
      controllerCpuCorePercent: stats(group.map((r) => r.controllerMeasurement.cpuCorePercent)),
      controllerEventLoopUtilization: stats(group.map((r) => r.controllerMeasurement.eventLoopUtilization)),
    };
  }));
}
function markdown(rows) {
  const range = (s) => `${s.median.toFixed(2)} [${s.min.toFixed(2)}, ${s.max.toFixed(2)}]`;
  return '# Same-host WS-1C curves\n\nMedian [observed min, max] across repetitions; not confidence intervals.\n' +
    'p95/p99 columns summarize per-run percentiles, not a pooled percentile.\n\n' +
    '| Total clients | Processes | Offered deliveries/s | Issued events/s | Received in-window/s | p95 ms | p99 ms | Late receipts |\n' +
    '|---:|---:|---:|---|---|---|---|---|\n' + rows.map((r) =>
      `| ${r.conns} | ${r.nodes} | ${r.targetDeliveriesPerSecond} | ${range(r.issuedEventsPerSecond)} | ${range(r.deliveriesPerSecond)} | ${range(r.p95Ms)} | ${range(r.p99Ms)} | ${range(r.late)} |`).join('\n') + '\n';
}
function svg(rows) {
  const colors = ['#2563eb', '#ea580c', '#059669'], connections = [...new Set(rows.map((r) => r.conns))];
  let body = '<rect width="1100" height="390" fill="white"/><g font-family="sans-serif" font-size="12" fill="#111827">' +
    '<text x="30" y="25" font-size="18">WS-1C: same-host offered-load curves</text>' +
    '<text x="30" y="47">Median and observed min/max; direct connections; no capacity or multi-host claim</text>';
  for (const [panel, field, label] of [[0, 'deliveriesPerSecond', 'Unique in-window deliveries / second'], [1, 'p99Ms', 'Per-run p99 latency (ms)']]) {
    const left = 80 + panel * 540, top = 95, width = 440, height = 220;
    const max = Math.max(1, ...rows.map((r) => r[field].max)) * 1.12;
    const x = (n) => left + (n - connections[0]) / (connections.at(-1) - connections[0]) * width;
    const y = (v) => top + height - v / max * height;
    body += `<text x="${left}" y="78">${label}</text>`;
    for (let tick = 0; tick <= 4; tick++) {
      const v = max * tick / 4;
      body += `<path d="M${left} ${y(v)}h${width}" stroke="#e5e7eb"/><text x="${left - 8}" y="${y(v) + 4}" text-anchor="end">${v.toFixed(0)}</text>`;
    }
    for (const n of connections) body += `<text x="${x(n)}" y="338" text-anchor="middle">${n}</text>`;
    body += `<text x="${left + 150}" y="360">Total concurrent clients</text>`;
    for (const [i, nodes] of [1, 2, 4].entries()) {
      const points = rows.filter((r) => r.nodes === nodes);
      body += `<polyline fill="none" stroke="${colors[i]}" stroke-width="2" points="${points.map((r) => `${x(r.conns)},${y(r[field].median)}`).join(' ')}"/>`;
      for (const r of points) body += `<path d="M${x(r.conns)} ${y(r[field].min)}V${y(r[field].max)}" stroke="${colors[i]}" stroke-width="3"/><circle cx="${x(r.conns)}" cy="${y(r[field].median)}" r="4" fill="${colors[i]}"/>`;
      body += `<text x="${left + i * 135}" y="382" fill="${colors[i]}">${nodes} process${nodes === 1 ? '' : 'es'}</text>`;
    }
  }
  return `<svg xmlns="http://www.w3.org/2000/svg" width="1100" height="390" viewBox="0 0 1100 390">${body}</g></svg>\n`;
}
module.exports = { plan, stats, summarize, markdown, svg };
