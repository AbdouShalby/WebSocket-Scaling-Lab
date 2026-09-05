# WS-3: evidence-critical security, readiness and observability

Historical milestone snapshot. For current validation and claim boundaries, see [reproduction](reproduce.md) and the [evidence index](evidence/README.md).

## Audit and bounded design

Baseline: clean `main` at `7cd086e097f9a740984dec92270c8408af9bc3fb` (WS-2).
No pre-existing uncommitted work was present. Historical WS-1/WS-2 artifacts are
retained unchanged; they describe those source versions, not WS-3 performance.

Read-only inspection found two reproducible process crashes: send text `null`
from a real WebSocket client, or publish `null` to the Redis fan-out channel.
Both exited 1 with TypeError at the unguarded `msg.action` / envelope destructure.
The minimal real-process probe used the existing owned `Lab`, one broker and
one server per case; all probe processes were cleaned up. `/readyz` returned 404.
Other gaps: unrestricted Origin, no client message-rate bound, malformed
configuration could disable limits, no subscriber readiness state, sparse
telemetry and all-interface local/Compose exposure.

The implementation/diagnosis workflow required red-capable checks before fixing
the two boundary crashes. The bounded design keeps the Hub/fan-out architecture
and dependencies unchanged: validate at ingress, add per-connection controls,
track subscriber health, expose low-cardinality process telemetry. No users,
JWTs, sessions, tenant authorization, durable broker, tracing stack, TypeScript
migration or speculative redesign. WS-4 is not part of this milestone.

## Security contract and assumptions

- Direct `npm start` binds `127.0.0.1` by default (`HOST` overrides it). Compose
  binds the internal WS process to `0.0.0.0`, but publishes Redis and nginx only
  on host `127.0.0.1`. `docker compose config --format json` validates those
  settings; Docker runtime/firewall isolation is not asserted by this check.
- Upgrade path is exactly `/`; other paths/queries return 404. When
  `ALLOWED_ORIGINS=https://example.test,https://other.test` is set, only exact
  header matches pass. Missing Origin, `null`, suffix tricks and trailing slash
  variants fail 403. Invalid allowlist configuration fails startup, not open.
  Unset/empty preserves the origin-less local benchmark workflow.
- **Origin is not authentication.** Non-browser clients can forge it. All
  admitted clients may subscribe to any syntactically valid logical channel.
  Use only simulated/public lab events. No private tenant-data boundary exists.
- Client messages must be JSON objects, with subscribe/unsubscribe + a valid
  channel, or ping + an optional finite numeric/null timestamp. Channels are
  1..128 UTF-8 bytes without ASCII whitespace/control bytes. Extra object keys
  are ignored. Invalid JSON/shapes return a fixed error, never reflect input.
  Binary messages close 1003. The existing ws maxPayload limit closes 1009.
- The existing subscription cap remains idempotent for an already-subscribed
  channel. A monotonic token bucket counts every inbound application message,
  including invalid JSON. Exhaustion closes 1008; each connection has its own
  bucket. This is not IP/distributed throttling, handshake protection, a global
  connection cap or protection from control-frame/connection-flood attacks.
- Redis envelopes must be objects with a valid channel, `data`, and a finite
  non-negative numeric `publishedAt`. Oversized/invalid/unserializable envelopes
  are counted and ignored. Outgoing serialization is protected too: a deeply
  nested valid JSON value can exceed the serializer stack. The size check bounds
  JSON parsing, not the network buffer that
  ioredis already allocated. Redis/publishers remain a trusted lab boundary.
- Logs never include client payloads, arbitrary Origin values or Redis error
  messages/credentials. They retain fixed event names, instance, time, level and
  bounded state fields. The process-local JSON endpoints are unauthenticated:
  do not expose this lab publicly without a separate, tested deployment boundary.

| Configuration | Default | Meaning |
|---|---|---|
| HOST | 127.0.0.1 | Direct server bind address |
| ALLOWED_ORIGINS | empty | Optional exact HTTP(S) Origin allowlist |
| MAX_PAYLOAD_BYTES | 4096 | Existing inbound WS payload cap |
| MAX_SUBS_PER_CONN | 50 | Existing channel cap per connection |
| MAX_ENVELOPE_BYTES | 1048576 | Redis envelope pre-parse byte limit |
| MESSAGES_PER_SECOND | 100 | Per-connection token refill rate |
| MESSAGE_BURST | 200 | Maximum token credit |
| REDIS_CHECK_INTERVAL_MS | 1000 | Subscriber health polling interval |
| REDIS_CHECK_TIMEOUT_MS | 1000 | Redis command timeout |

All numeric config values must be positive integers within their parser bounds;
invalid strings/zero/infinity fail startup. These settings are not capacity
recommendations and do not establish peak memory or throughput limits.

## Readiness and recovery contract

`GET /healthz` remains 200 liveness while the HTTP process responds. It intentionally
does not imply Redis availability. `GET /readyz` returns 200 only after a fresh
SUBSCRIBE acknowledgment for the current connection generation, otherwise 503.
Both and `/metrics` send `Cache-Control: no-store`. Other HTTP methods return 405.

The application explicitly subscribes on every ioredis `ready` event, with
automatic resubscribe/resend and offline queuing disabled. This is a small
ownership change: readiness can now observe the actual subscription ACK rather
than infer it from a TCP connection. A periodic PING on that **same subscriber**
must return the subscriber-mode response. A failed/timed-out check revokes
readiness and disconnects with reconnect enabled, clearing stalled command work.
The existing capped reconnect backoff remains; a subsequent fresh ACK can restore
readiness. An epoch guard prevents stale completions after disconnect or drain
from marking the process ready. No separate Redis health client is substituted.

New WebSocket upgrades return 503 while unready/draining. Existing sockets and
local subscriptions survive a broker outage, but outage events are not durable
or replayed. SIGTERM marks unready before closing clients. Repeated signals are
idempotent. Existing close-1001 / exit-0 and forced-10s / exit-1 semantics remain.
Health requests may stop succeeding once the HTTP listener is closed.

Readiness is a sampled observation, not an instantaneous proof of delivery.
Detection includes the configured interval/timeout plus event-loop scheduling;
there is a race between any probe and the next real failure. No asymmetric
partition, Redis HA, rolling load-balancer draining or durable recovery claim.
The nginx example does not actively poll `/readyz`; server-side admission enforces
the local gate, not fleet rerouting or a deployment orchestration policy.

Implementation references: [ioredis connection events and auto-reconnect](https://github.com/redis/ioredis#auto-reconnect)
and [ws upgrade handling and limits](https://github.com/websockets/ws/blob/master/doc/ws.md).
The installed ioredis 5.11.1 event handler was inspected: its `ready` event precedes
automatic resubscription completion. WS-3 therefore does not equate the two.

## Observability contract

`/metrics` remains dependency-free JSON, not Prometheus exposition. Counters are
initialized to zero, cumulative and per-process; restart resets them. Existing
names are retained. New counters cover invalid/oversized input, rejected
subscriptions/upgrades, rate-limited/closed sockets, valid Redis envelopes,
Redis errors/reconnect attempts/check failures/subscription ACKs, and readiness
transitions. Hub counters still distinguish drops and slow-client evictions.

`delivered` / `hub.messagesSent` count **send calls**, not delivery acknowledgments.
Client receipts remain independently checked by the benchmark ledger. RSS and
heapUsed are snapshots, not peaks or leak evidence. `eventLoop` records cumulative
20ms-resolution loop-delay mean/p99/max (milliseconds) and utilization since
metrics initialization. These are not message-latency percentiles, SLOs, fleet
aggregates or a performance-cost study. Scraping does not reset the histogram.

Structured logs include readiness transitions, Redis errors and shutdown
start/completion/deadline. No per-message logs or channel/client metric labels
are added; no dashboard, exporter, tracing backend or alerting service is implied.

## Reproduction and evidence requirements

```sh
npm ci
npm run test:ws3
npm run proof:guards -- --output-dir bench/results/ws3
```

Real infrastructure checks require Linux/WSL, Node >=20 and `redis-server`
(or `REDIS_SERVER`). They own non-persistent brokers on reserved loopback ports,
verify Redis INFO PID/config, and clean up all owned processes. No existing
daemon, Docker/WSL reset, shared ACL or volume is changed. Temporary ACL changes
target only a disposable test-owned broker. Native Windows/missing Redis fail
closed, never skip/fall back to fake infrastructure.

Scenarios: `security`, `rate`, `readiness`, `subscription-ack` (default `all`).
The proof retains timestamp/UUID JSON and a JSON-lines timeline, exact argv,
baseline SHA, dirty flag, normalized source hashes, versions, process IDs/exits,
configs, HTTP/WS outcomes and receipts. All scenarios run against the real server;
unit tests separately exercise deterministic bucket time and stale ACK races.

- Security: exact Origin matching, path admission, scalar/null/array/invalid JSON,
  binary/oversized data, channel/subscription caps, malformed broker envelopes,
  then successful real fan-out and exact counters.
- Rate: a real noisy client closes 1008 after the configured burst; a distinct
  healthy connection still gets pong. No sequential loop is presented as a
  concurrency or capacity benchmark.
- Readiness: boot with broker unavailable; 503 admission; Redis startup, actual
  kill/restart, actual SIGSTOP/SIGCONT (established TCP but stalled commands),
  recovery on the same WS client, and 503 during real SIGTERM before close1001.
- Subscription ACK: real Redis ACL denies SUBSCRIBE while the broker is running;
  readiness remains 503 until permission is restored and the ACK arrives.

Four in-memory negative controls remove Origin filtering, client shape validation,
message-rate enforcement or force always-ready. Each must fail for its intended
assertion, not an unrelated startup error. The existing WS-2 recovery mutation
now targets the ACK-aware resubscription owner (all attempts after first connection
generation); the original missing-subscriber/delivery assertions are unchanged.
The legacy E2E setup now waits for `/readyz` instead of treating listening as
subscription readiness; no delivery assertion or old threshold was removed.

The Linux operational CI job includes WS-3. Hosted CI is not claimed to have run.
Historical benchmark curves are unchanged; no WS-3 capacity/performance rerun is
claimed.

## Independent review

The review skill was applied to the uncommitted diff against the WS-2 baseline,
including new files, without introducing issue-tracker tooling. Its independent
standards/correctness review found one additional ingress crash: a roughly 20 KB
Redis envelope containing 10,000 nested arrays parsed and validated but caused an
uncaught RangeError during outgoing serialization. The real security scenario
reproduced exit 1; moving serialization into the existing guarded boundary made
the same scenario pass. The test now requires seven malformed/unserializable
envelopes counted and normal delivery afterward. The reviewer closed the finding.

The separate spec review found no remaining scope/claim blockers. No existing
delivery assertion, benchmark threshold or historical raw evidence was weakened
or rewritten. Final validation and the exact scoped commit boundary follow.

## Final local validation — 2026-09-05

Retained [raw JSON](evidence/ws-3/2026-09-05T15-31-52-483Z-b99b50ac-be39-4f72-940a-519f26eda0ce.json)
and [timeline](evidence/ws-3/2026-09-05T15-31-52-483Z-b99b50ac-be39-4f72-940a-519f26eda0ce.log).
All four scenarios passed with real Redis 8.0.5, Node v24.15.0, ws 8.21.0 and
ioredis 5.11.1 on Linux/WSL2. The recorded baseline SHA is WS-2 with `dirty:true`;
27 normalized source hashes identify the tested WS-3 code before commit. The
before/after source check passed, as did independent on-disk hash verification.
Raw JSON is 58,214 bytes; timeline is 14,684 bytes. All ten owned child processes
exited. Security/rate socket responses are asserted live and summarized in the
timeline/metrics; the publication receipt ledger covers recovery/ACK scenarios.

Observed results (not performance claims):

- Six rejected upgrades in the security scenario (five Origin cases and wrong
  path); twelve invalid WS messages including binary, one oversized close1009,
  one rejected extra subscription, and seven rejected Redis envelopes including
  deep serialization. A subsequent normal Redis event reached its subscriber.
- A noisy socket exceeded burst3/rate1 and closed1008; another remained responsive.
  Three invalid messages consumed its burst. The retained policy profile is
  deliberate test configuration, not the default throughput limit.
- Unavailable startup, actual broker kill and a SIGSTOP stall produced 503
  readiness/admission while liveness stayed200. Recovery delivered three markers
  to the same client across the healthy/recovered states. Before drain, metrics
  recorded three reconnect attempts, two failed checks and three subscription ACKs.
  Check interval/timeout were each500ms for this test, versus defaults1000ms.
- Real ACL-denied SUBSCRIBE stayed unready; restoring permission yielded one
  acknowledged subscription and one client receipt. SIGTERM revoked readiness,
  rejected a new upgrade, closed the existing client1001 and exited0. Structured
  lifecycle logs were verified. No broker history or durable replay was added.

Final full suite, after the review fix: **60 passed, 0 failed, 0 skipped**
(62.35 seconds). It includes all49 earlier tests and11 WS-3 tests: six unit/config
checks plus the full real proof and four real negative controls. The previous
WS-2 operational scenarios and three negative controls still pass; no old test
was dropped. A separate owned Redis was supplied to earlier suites; WS-2/WS-3
started their own isolated brokers. All were cleaned up.

```sh
REDIS_HOST=127.0.0.1 REDIS_PORT=<dedicated-owned-port> node --test tests/hub.test.js tests/benchmark.test.js tests/e2e.test.js tests/benchmark-integration.test.js tests/two-node.test.js tests/scaling.test.js tests/scaling-integration.test.js tests/operations.test.js tests/guards.test.js tests/guards-integration.test.js
node bench/prove-guards.js --output-dir docs/evidence/ws-3
```

Local Windows launch used the already-existing ignored WS-2 portable runtime:

```powershell
wsl -d Ubuntu --exec /mnt/e/Abdou/Projects/careergithubupgrades/websocket-scaling-lab/bench/results/ws2-tools/node-v24.15.0-linux-x64/bin/node bench/prove-guards.js --output-dir docs/evidence/ws-3
```

Additional checks: native Windows proof invocation rejected with explicit POSIX
requirement; missing Redis executable returned exit1/FAILED/ENOENT, not a skip.
JavaScript syntax, `git diff --check`, source hashes, and parsed Compose loopback
settings passed. Hosted CI and Docker runtime were not exercised. This milestone
does not rerun the historical capacity curves or measure telemetry overhead.

## Sign-off and commit boundary

WS-3 is complete as a production-inspired, local architecture-lab checkpoint.
The remaining risks are explicit: no identity/channel authorization, ephemeral
Pub/Sub loss, sampled readiness, per-connection-only abuse controls, unauthenticated
local telemetry, and no proven multi-host/LB/public-security deployment. No claim
of production usage, exactly-once delivery, arbitrary-failure safety or new capacity
is justified. Evidence now supports input-boundary hardening, ACK-aware readiness,
real fault injection and operational telemetry reasoning.

Career Ops recommendation only: these are additional backend/reliability showcase
proof points, not new production, AI or TypeScript experience. No Career Ops/CV/
profile files or unrelated repositories were changed. No external communication
or push is included. Human input is not required to close WS-3. WS-4 is next in
the roadmap but **has not been started**.

Exact WS-3 commit files (19):

```text
.github/workflows/ci.yml
.gitignore
README.md
docker-compose.yml
package.json
src/config.js
src/metrics.js
src/server.js
src/protocol.js
src/readiness.js
bench/prove-guards.js
tests/e2e.test.js
tests/fixtures/ws2-mutant.cjs
tests/fixtures/ws3-mutant.cjs
tests/guards.test.js
tests/guards-integration.test.js
docs/ws-3.md
docs/evidence/ws-3/2026-09-05T15-31-52-483Z-b99b50ac-be39-4f72-940a-519f26eda0ce.json
docs/evidence/ws-3/2026-09-05T15-31-52-483Z-b99b50ac-be39-4f72-940a-519f26eda0ce.log
```

Dependencies/lockfile, Hub, publisher, prior benchmark sources and historical
evidence remain unchanged. Runtime downloads, dependencies and routine test/failure
artifacts remain ignored; only the curated proof above is included. One scoped
commit on the current branch, without resetting, rewriting history or pushing.
