# WS-1A: benchmark integrity

## Discovery and scope

Baseline: clean `main` at `e8648b4d07106300077aa974f0b5d9ff0592a195`.
The old benchmark mixes ramp traffic into a nominal 30-second result, counts
open/error events independently, and cannot know which publications were lost.

Plan: keep the application and Pub/Sub architecture unchanged. Give each run
an isolated channel namespace and a controlled publisher inside the benchmark
process. Record every issued event before publishing it. Wait for every
subscription acknowledgment, warm up, reset accounting, measure using a
monotonic clock, then count late deliveries in a separate drain window.

Validate accounting with deterministic regression tests, CLI failure tests,
and a real Redis plus one real server integration run. Capture versioned JSON,
source hashes, exact arguments, runtime metadata, and timestamped logs. No
multi-node proof, failure recovery, authentication, or TypeScript is in scope.

## Methodology

Lifecycle: PREFLIGHT -> RAMPING -> SUBSCRIBING -> READY -> WARMUP -> MEASURING
-> DRAINING -> CLEANUP -> COMPLETE/FAILED. A failed precondition goes to cleanup
and FAILED. Invalid arguments or missing runtime metadata fail before a run starts.

- All requested sockets must acknowledge their exact assigned subscription.
  Open is not readiness. Each socket settles readiness once; a subsequent
  disconnect is a separate failure. Nothing publishes before all acknowledgments.
- The controller owns a rate-paced simulated publisher. Unique run IDs and logical
  channels isolate it from the ordinary demo publisher and other test runs. The
  application server is unchanged. No expected counts are inferred from traffic
  that happened to reach a client.
- Each measurement publication is entered in the ledger before Redis PUBLISH.
  Expected deliveries are its channel audience, even if the publish fails or
  nobody receives the event. A failed/uncertain publish invalidates the run.
- Warmup uses a different phase marker. On entering measurement all delivery,
  duplicate, latency, and instance observations reset. Delayed warmup events and
  foreign-run traffic are excluded and counted separately.
- Received means unique `(client, sequence)` pairs in the issued measurement
  cohort. `received = inWindow + late`; `missing = expected - received` after a
  bounded drain. Duplicates and unexpected IDs/channels are separate counters.
- Throughput is `inWindow / actual monotonic elapsed seconds`. The drain is not
  included in the numerator or denominator. Late delivery may complete the
  cohort without raising throughput. Arrival after drain remains missing.
- Latency starts immediately before Redis PUBLISH and ends on the same process's
  monotonic receive clock. Only unique in-window deliveries contribute samples.
  All samples are retained in a 1ms upper-bound histogram; p50/p95/p99 use nearest
  rank, and max retains the unrounded measurement. No reservoir/sample truncation.
- The publisher awaits each Redis response and does not catch up with bursts.
  Configured rate is an offered target; actual issued rate is also recorded.
- `relayedBy` is required on measured events. Distinct IDs and client/delivery
  counts are captured as observations, not a multi-node validation claim.

Non-zero exit: missing readiness, connection errors/disconnects, publisher error,
missing/duplicate/unexpected delivery, no measured delivery, or any client with
no event. Default drain allows late deliveries; COMPLETE does not mean every
delivery arrived within the measurement window. There is no reliability SLA.

## Reproduce

From the repository root, with real Redis reachable:

```sh
npm ci
node bench/run-local.js --redis-port 6379 --conns 12 --channels 3 --ramp 4 --rate 30 --warmup 0.2 --measure 2 --drain 0.2
npm run test:ws1a
```

`REDIS_HOST`/`REDIS_PORT` select the integration test's Redis. Benchmark CLI flags
select the benchmark's Redis explicitly. The launcher creates one server on an
available port, waits for both listening and Redis subscription log events,
records explicit server config and actual loaded versions, and cleans up its own
child. Port reservation is released before the child binds; a competing bind
fails startup rather than silently selecting another server.

An existing target uses `node bench/connect-storm.js --runtime <metadata.json>`
with the same flags. Metadata must be a JSON object with `schemaVersion: 1`, an
`environment` description, `topology: {description, wsInstances}`, and
`server: {node, ws, ioredis, config}`. These target facts are operator declarations,
not remotely verified versions. Review metadata for private content before use.
For repeatable captured metadata, prefer the one-server launcher in WS-1A.

| Flag | Default | Meaning |
|---|---|---|
| `--url` | `ws://127.0.0.1:8080` | Existing-target URL; launcher discovers its own |
| `--conns` / `--channels` | 1000 / 10 | Requested sockets / logical channels |
| `--ramp` | 100 | New sockets per 100ms batch |
| `--rate` | 50 | Total source events/sec, rotated over channels |
| `--warmup` / `--measure` / `--drain` | 1 / 30 / 1 | Phase lengths in seconds |
| `--timeout` | 10 | Connection/ACK/Redis-command deadline in seconds |
| `--redis-host` / `--redis-port` | 127.0.0.1 / 6379 | Explicit publisher endpoint |
| `--channel-prefix` | `ws:broadcast:` | Existing server's Redis channel prefix |
| `--output-dir` | `bench/results` | Repository-local artifact directory |
| `--runtime` | required for existing target | Target metadata JSON |

Inputs are validated, with at most 100k clients / 5 million expected deliveries
to bound exact bookkeeping. These are harness limits, not demonstrated capacity.
Small runs still need enough source events to reach every channel.

## Artifact contract (schemaVersion 1)

Each started run produces a UTC-timestamp/UUID JSON file and matching lifecycle
log, using exclusive creation. Normal results are ignored; only reviewed evidence
is selected for Git. Invalid CLI/runtime input exits non-zero without an artifact.

JSON contains lifecycle timestamps, requested/effective workload, readiness and
disconnect counts, publication/acknowledgment counts, Redis INFO runtime fields,
expected/received/missing/duplicate/late accounting, per-instance observations,
histogram, measurement elapsed time, and explicit limitations.

Provenance records exact argv (as an array to preserve quoting), Git SHA, dirty
state, SHA-256 of executable sources/config/lockfile (UTF-8, CRLF normalized to LF),
controller hardware/OS/actual Node and library versions, plus target metadata.
No environment dump, credentials or hostname is collected. URLs containing
credentials/query strings are rejected. A dirty result names its base commit and
source hashes honestly; it must not be attributed to an unchanged base SHA.

## Limitations and deferred work

The publisher and clients share one controller event loop and CPU. Metadata does
not prove saturation of either the target or load generator. Redis PUBLISH reply
counts subscribers, not WS delivery. There are no retry, ACK/replay, reconnection,
durable messaging or delivery guarantees added to the application.

This is a small, single-server methodology validation. Scaling curves, real
two-node fan-out, Redis outages, node killing, deployment drain, readiness,
authentication, TypeScript, Kubernetes and compression experiments are deferred.
The July 12 benchmark numbers are historical/unverified and unusable as current
CV capacity evidence. No professional production-experience claim was added.

## Validation and sign-off (2026-09-05)

Environment: Windows controller and server on Node v24.15.0, ws 8.21.0, ioredis
5.11.1; real Redis 8.0.5 on WSL2. Docker Desktop's normal startup failed on its
existing `sailor-ingest.sock`; validation used the already-installed Redis binary
on a dedicated loopback port 16379, with RDB saving and AOF disabled. No Docker
reset, pruning, image/volume removal, or system reconfiguration was performed.

Redis reproduction for this retained local run (inside WSL/Linux):

```sh
redis-server --bind 127.0.0.1 --port 16379 --save "" --appendonly no --protected-mode yes
```

Then from the repository root on Windows:

```powershell
$env:REDIS_HOST = '127.0.0.1'
$env:REDIS_PORT = '16379'
npm.cmd test
npm.cmd run test:ws1a
node bench/run-local.js --redis-port 16379 --conns 24 --channels 3 --ramp 8 --rate 30 --warmup 1 --measure 3 --drain 0.5 --output-dir docs/evidence/ws-1a
```

- `npm test`: 24 passed, 0 failed, 0 skipped, including the existing real Redis E2E.
- `npm run test:ws1a`: 24 passed, 0 failed, 0 skipped. The two commands share
  23 tests; their real integration tests differ (25 distinct tests in total).
- Final combined command `node --test tests/hub.test.js tests/benchmark.test.js tests/e2e.test.js tests/benchmark-integration.test.js`: 25 passed, 0 failed, 0 skipped.
- Negative control with Redis port 16378: integration failed with exit 1 and
  connection refused; it did not skip or substitute an in-memory implementation.
- Four in-memory mutations were rejected by the existing benchmark tests:
  remove counter reset; count late receives in throughput; disable dedup;
  allow readiness to settle repeatedly. Source files were not mutated on disk.
- Every source, benchmark, and test JavaScript file passes syntax validation.

Retained [JSON](evidence/ws-1a/2026-09-05T02-41-59-459Z-9786088b-c9f7-4595-b7e7-cf841f256085.json)
and [raw lifecycle log](evidence/ws-1a/2026-09-05T02-41-59-459Z-9786088b-c9f7-4595-b7e7-cf841f256085.log):
24/24 subscriptions ready; 90 measurement publications; 720 expected and received;
0 missing, duplicate, unexpected, or late; actual window 3001.4462ms. The computed
239.8844 deliveries/sec is a small offered-workload validation, not capacity.
The JSON records base SHA `e8648b4...`, `dirty: true`, and the normalized source
hashes for the WS-1A implementation tested before its commit. It does not claim
these results came from the unmodified base commit. The hash set is checked
against the reviewed commit contents; source changes during a run invalidate it.

Acceptance: benchmark integrity repaired, historical results reclassified,
regressions and real one-server validation passed. No application source changed.
CI is configured for Node 22/Redis 7; hosted CI was not executed during this local
sign-off and is not represented as having passed. Local validation used the
versions above. No capacity, multi-node, failure-recovery, or production proof was
added. Next milestone is WS-1B; it is not started as part of WS-1A.

Career Ops recommendation only: add evidence for controlled benchmark design,
failure-sensitive accounting tests, and reproducible Node/Redis experiments when
separately authorized. Keep production, AI, TypeScript, and capacity claims
unchanged. No Career Ops files were edited.
