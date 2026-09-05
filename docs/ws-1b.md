# WS-1B: real two-instance fan-out proof

## Read-only discovery and plan

Baseline: clean `main` at `23e72ba69a92e5725ec604347cc90b683ba7febd`.
WS-1A is committed. The server already subscribes to Redis and forwards envelopes
to a local Hub, with `relayedBy` identifying the instance. Existing integration
coverage starts one server only. No application change is needed.

Plan: reuse the server launcher, exposing its child PID for identity assertions.
Start two independent Node server processes with distinct ports and instance IDs,
connect six concurrent real WebSocket clients (three to each server), and wait
for exact subscription acknowledgments. Use a UUID Redis channel namespace so
other runs cannot satisfy this proof. Assert two Redis subscribers and both live
processes. Publish each event once, record individual receipts and verify the
exact expected client/event pairs, payloads and relaying instances.

Scenarios: shared channel, A-only/B-only channel isolation, local unsubscribe,
repeat subscribe without duplicate delivery, and an empty channel. Retain
timestamped JSON and logs with runtime/command/source provenance. Require Redis
in CI; no skip, fake Redis, synchronous queue or single-server fallback.

The proof is about two server processes on one host. It does not demonstrate
nginx routing, multi-host deployment, throughput curves, failover, reconnect or
production delivery guarantees. WS-1C and later milestones remain deferred.

## Reproduce

Install the locked dependencies with `npm ci` and provide a real Redis endpoint.
With the repository's Compose Redis service: `docker compose up -d redis`.

```bash
npm run test:ws1b
npm run proof:two-node -- --redis-host 127.0.0.1 --redis-port 6379 --timeout 3
```

The test gate accepts `REDIS_HOST` / `REDIS_PORT`; the standalone CLI accepts
explicit arguments. Missing Redis fails with a nonzero exit; it never skips or
substitutes an in-memory broker. Routine JSON/log artifacts are ignored under
`bench/results/`. The CLI also accepts a repository-local `--output-dir` for
deliberately retained evidence. It does not start, flush or stop your Redis.

## What the proof checks

Both server PIDs are live together, differ from the controller and each other,
and have different instance IDs and loopback ports. Redis `PUBSUB NUMSUB` must
report exactly two subscribers in the run's isolated UUID namespace. All six
client sockets are opened concurrently, then subscriptions are acknowledged
before publishing. There is no load balancer in this experiment.

| Stage | Publications and expected client receipts |
|---|---|
| Initial | One shared event to four clients; one A-private and one B-private event to their respective clients; one empty-channel event to nobody: six receipts |
| Local unsubscribe | A-shared-1 acknowledges unsubscribe; a new shared event reaches the other three clients: three receipts |
| Repeat subscribe | A-shared-1 acknowledges two subscribe requests; a new shared event reaches each of four clients once: four receipts |

Each of the six events is published once to Redis, which reports two subscriber
connections. The receiver-side ledger verifies all 13 expected client/event
pairs, exact payload/channel/timestamp, and the correct `relayedBy` instance.
It rejects missing, duplicate and unexpected receipts rather than trusting
server counters. Both servers and all six sockets must still be live at the end.
Cleanup stops only owned sockets/processes and checks the namespace has zero
remaining Redis subscribers.

Each stage waits for expected receipts with a bounded timeout and then observes
a 200 ms quiet window for extra deliveries. Absence checks apply only to these
finite windows; they do not guarantee future behavior or exactly-once delivery.
Redis Pub/Sub supplies no durable history, catch-up or client acknowledgments.

## Regression sensitivity

Seven synthetic transcript tests exercise the verifier, separately from the
mandatory real-infrastructure test. Two additional negative-control tests run
the same proof against real Redis and two actual server processes, using an
explicit Node preload to change fan-out **in memory only**. Suppressing fan-out
must fail with missing-delivery timeout; broadcasting twice must fail with a
duplicate-delivery error. The fixture never changes application files and is
not loaded by normal server/proof commands. Negative artifacts are marked with
`provenance.testMutation` and are not successful application evidence.

## Provenance and limitations

JSON records the command, base Git revision/dirty state, normalized source
SHA-256 hashes, controller/dependency/OS metadata, each server's actual
runtime/configuration/PID/port, Redis version, publication ledger, raw receipts,
and ordered lifecycle operations. A matching JSON-lines log is retained.
Source changes during a run invalidate it. Pre-commit evidence deliberately
records the previous commit plus dirty source hashes; it cannot contain its
own future commit hash. Hashes identify the implementation tested.

This is a production-inspired architecture lab with simulated events. A local
two-process proof is useful interview evidence of cross-instance coordination
and explicit correctness checks. It does **not** establish multi-host scaling,
nginx balancing, capacity gains, a 30k connection ceiling, provider durability,
Redis outage recovery, reconnect behavior or production operating history.
The controller and both servers share one host; scheduler/network/resource
effects are not isolated. No application feature or architecture was changed.

## Local validation and WS-1B sign-off — 2026-09-05

Retained successful run:
[raw JSON](evidence/ws-1b/2026-09-05T02-55-24-709Z-73185e26-f74d-464e-b71f-af3ab83ca525.json)
and [operation log](evidence/ws-1b/2026-09-05T02-55-24-709Z-73185e26-f74d-464e-b71f-af3ab83ca525.log).
Node v24.15.0 on Windows, ws 8.21.0, ioredis 5.11.1; real Redis 8.0.5
inside local WSL2 Ubuntu at loopback port 16379. Docker Desktop was unavailable;
no Docker recovery/reset was attempted for this milestone. A dedicated,
non-persistent Redis process was used, not an in-memory Redis substitute.

- Two independent live server PIDs: A 26992 and B 38184, with distinct ports/IDs.
- Six clients simultaneously open, three per server; six Redis publications.
- Expected/received 13/13; missing, duplicate and unexpected receipts all zero.
- A received six client/event pairs; B received seven. Redis subscriber count
  was two before/after the scenarios and zero after owned-server cleanup.
- Both negative controls failed for their intended reasons (suppressed and
  duplicated real-server fan-out); the test suite therefore passed those checks.
- Full regression command below: **35 passed, 0 failed, 0 skipped**. This includes
  the previous 25 tests and ten WS-1B tests; overlapping npm scripts are not
  counted twice.
- Standalone proof against absent Redis on port 16378 exited 1 with a FAILED
  artifact and zero server processes started. No fallback or skipped proof.
- Syntax checks passed for the launcher, proof, WS-1B tests and mutation fixture;
  `git diff --check` passed. Application `src/` files remain unchanged.

```powershell
$env:REDIS_HOST='127.0.0.1'
$env:REDIS_PORT='16379'
node --test tests/hub.test.js tests/benchmark.test.js tests/e2e.test.js tests/benchmark-integration.test.js tests/two-node.test.js
node bench/prove-two-node.js --redis-port 16379 --timeout 3 --output-dir docs/evidence/ws-1b
```

The local validation uses Node 24 / Redis 8, while CI is configured for Node 22 /
Redis 7. Remote CI was not executed as part of this local sign-off. WS-1B is
complete within the stated correctness scope. WS-1C (scaling curves) is the next
milestone, but has **not** started; no capacity or production claim follows from
this sign-off. Portfolio wording may cite this reproducible two-process proof,
not production usage or measured scaling gains.
