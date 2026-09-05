# WS-2: operational failure and recovery evidence

## Read-only audit and plan

Baseline: clean main at `b70f6a22e132fd665c84375c17d504d9495c1bec`.
The server uses ioredis reconnect/backoff and default resubscription; WebSocket
subscriptions remain in each process's memory. Pub/Sub has no replay history.
Clients have no automatic reconnect/resubscribe implementation in this repo.
SIGTERM sends close code 1001 and has a fixed 10s forced-exit fallback. The Hub
drops over-threshold slow consumers and terminates them at the configured limit.
Existing tests do not prove these operational paths with real failures.

Plan: a POSIX-only, fail-closed harness owns ephemeral loopback Redis and Node
children. Test Redis kill/restart, server SIGKILL with a surviving peer, explicit
client reconnect/resubscribe, cooperative SIGTERM, a non-cooperating peer and
the forced deadline, and TCP backpressure from a paused client while a healthy
client receives. Retain timelines, receipts, exits, Redis counts, metrics and
source/runtime provenance. Use negative controls before any application fix.
No durable replay, automatic client recovery, readiness, authentication, new
application telemetry, WS-3 or WS-4 work.

Windows process termination is not POSIX SIGTERM proof. Local validation uses
WSL2 Ubuntu with a portable Linux Node v24.15.0 archive from nodejs.org, checked
against its official SHA-256 manifest:
`472655581fb851559730c48763e0c9d3bc25975c59d518003fc0849d3e4ba0f6`.
Runtime files stay in ignored `bench/results/ws2-tools/`; no system Node install,
Docker recovery, WSL configuration or unrelated service changes.

## Reproduce and safety boundary

Use Linux/WSL with Node >=20, the locked npm dependencies and a real
`redis-server` executable on PATH (or explicitly set `REDIS_SERVER`).

```sh
npm ci
npm run test:ws2
npm run proof:operations -- --output-dir bench/results/ws2
```

WS-2 owns its Redis: it does **not** accept a remote/existing Redis endpoint.
Every scenario reserves a loopback port, starts a non-persistent Redis child,
checks the Redis INFO PID against that child, and starts independent Node server
children. RDB saving and AOF must be disabled. Signals target only stored owned
child handles; cleanup waits for every owned process to exit. No FLUSH, prune,
Docker reset, volume deletion, system Redis restart or WSL shutdown is used.
Port binding conflicts fail startup rather than reusing another service.

Native Windows fails closed before starting scenarios: its process termination
is not substituted for POSIX signals. Missing Redis also fails, never skips.
Optional `--scenario` values: recovery, crash, drain, forced-drain, backpressure;
default all. `--output-dir` must be inside the repository. Artifacts use unique
UTC timestamp/UUID filenames and exclusive creation.

The separate Linux CI job installs a Redis executable and runs `test:ws2`.
Any distribution-managed Redis is unused: test children use verified private
ports/PIDs. CI configuration is not a claim that hosted CI ran successfully.

## Scenarios and exact meaning

| Scenario | Real action and required observation | Not implied |
|---|---|---|
| Redis outage/recovery | SIGKILL the owned Redis; fail-fast publish rejects; restart on the same port with a different Redis run ID; both existing server processes resubscribe and deliver to the same open clients | No durable buffering, Redis HA, accepted outage delivery or arbitrary outage recovery |
| WebSocket process crash | SIGKILL A; client sees abnormal close 1006; B remains open and receives gap events; harness starts a replacement; fresh client gets no events until explicit subscribe | No process supervisor, automatic client reconnect, subscription migration or history replay |
| Cooperative SIGTERM | Real SIGTERM invokes handler; client sees close 1001 with reason; process exits 0; B continues; explicit client reconnect to B requires subscribe | No connection-preserving/lossless deploy, automatic LB reroute or ACK-based drain |
| Non-cooperating close peer | Pause real client reads to withhold close response; SIGTERM rejects a new WS connection; original 10s deadline exits process with code 1 | Not successful graceful drain; peer cannot hold the process indefinitely in this representative case |
| Slow reader/backpressure | Pause a real WS client's TCP reads, publish bounded 64 KiB simulated blobs while awaiting healthy receipts; actual server counters show drops and exactly one slow-client kick; healthy client remains open and receives the next event | Not a fake bufferedAmount fixture, Internet-congestion model, default-profile capacity or whole-process memory bound |

The crash/drain gap events really reach the surviving subscriber. They never
reappear on the new connection, before or after resubscription, within the
bounded observation window. Redis outage publication is explicitly rejected by
the harness's fail-fast publisher (offline queue/retry disabled), not counted
as an accepted publication. The application has no publisher durability feature.

The slow-reader experiment uses `MAX_BUFFERED_BYTES=65536`, `DROP_LIMIT=3`,
heartbeat 30000ms and a maximum of 1024 x 65536 blob bytes (64 MiB offered), so
failure stays bounded. These are recorded **test settings**, not default-load
claims. The healthy client is awaited between publications; the slow client is
paused through the public `ws.pause()` API, not by changing its bufferedAmount.
Heartbeat reaping must not be mistaken for the backpressure kick. RSS values
come from two existing metrics snapshots, not peak-memory or leak profiling.
The policy checks before send: one frame can overshoot the threshold, kernel
buffers are separate, and this is not a strict total-memory bound.

## Evidence and regression sensitivity

Artifacts contain exact command, base SHA/dirty state, normalized source hashes,
OS/Node/library versions, per-scenario timelines, actual PIDs and exits, server
configuration, Redis version/run IDs, publication identities, required/allowed
recipients, receiver identities, payload sizes/hashes and metrics snapshots.
Large blobs are not duplicated in artifacts. The live harness compares complete
payloads and timestamps; offline evidence retains identities and blob hashes.
The JSON-lines companion contains the ordered operational events.

Every required client/event pair must appear once, and no receipt may belong to
an unexpected recipient/instance/event. A 250ms final quiet window checks finite
absence/duplicates; it is not a future-delivery guarantee. Client errors/close
codes are retained, including expected abnormal closes and the rejected new
connection during shutdown. Cleanup records are separate from fault injection:
remaining test-owned processes are killed only after scenario assertions.

Three explicit test-only preloads change application code **in memory**, never
on disk: disable ioredis auto-resubscribe, remove SIGTERM handler, disable the
Hub pressure check. Each real scenario must then fail for its intended reason;
mere Redis/startup failure cannot satisfy those negative tests. Negative results
carry `provenance.testMutation`; they are not normal application evidence.

The diagnostics skill guided red-capable experiments before proposing fixes.
Normal scenarios passed on the existing application, and negative controls
proved the checks detect the targeted regressions. No application fix or rewrite
was justified; only harness/tests/docs/CI are changed.

## Assumptions and limitations

- Same-host POSIX loopback test with real Redis/processes; not multi-host/LB proof.
- Recovery is observed after a short representative broker interruption from
  an already healthy system; initial broker-unavailable boot, prolonged outages,
  asymmetric network partitions and arbitrary crash timing are not proven.
- `/healthz` still returns `{ok:true}` during Redis outage. This is observed
  liveness-only behavior, not readiness. WS-3 may address it; WS-2 does not.
- Client reconnection, resubscription and replacement process start are explicit
  harness actions, not implemented automatic product capabilities.
- Pub/Sub remains ephemeral with no replay, ACK persistence or delivery guarantee.
- SIGTERM sends close frames; it does not wait for business-message acknowledgment.
  Shutdown while Redis is also down is not covered by the cooperative-exit claim.
- Authentication/readiness/observability upgrades, TypeScript, durable brokers,
  deployment orchestration, WS-3 and WS-4 are not implemented here.

## Final local validation — 2026-09-05

Retained [raw JSON](evidence/ws-2/2026-09-05T14-55-12-178Z-15bc2891-16a3-4cd5-8126-45d600114127.json)
and [operational timeline](evidence/ws-2/2026-09-05T14-55-12-178Z-15bc2891-16a3-4cd5-8126-45d600114127.log).
All five scenarios passed on Linux/WSL2 with Node v24.15.0, Redis 8.0.5,
ws 8.21.0 and ioredis 5.11.1. Exact runtime and source hashes are in the JSON.
Base SHA is the WS-1C commit with `dirty:true`; tested source hashes identify the
WS-2 implementation before commit, not an unchanged WS-1C checkout.

- Redis: original process was killed; replacement had a different run ID;
  two existing WebSocket processes resubscribed and the same clients received
  the recovery event. Outage publication rejected; health still reported true.
- Crash: A exited by SIGKILL and client closed 1006; B delivered during the gap.
  Replacement/new connection required explicit subscription; no gap replay seen.
- Cooperative drain: A exited 0 after client close 1001. The surviving B served
  an explicitly reconnected/resubscribed client without inherited history.
- Non-cooperating drain: new WS connection rejected; exit 1 after **10017.25ms**.
- Backpressure: a paused real TCP reader triggered **3 counted drops and one
  eviction** after 46 pressure publications of 65536 blob bytes. The healthy
  client received every required event, including the post-eviction marker.
  Slow-client receipts are intentionally optional during pressure; counters of
  send calls are not receipts and queued data may be lost when terminating it.
- All **15 owned child processes** exited. No test-owned Node/Redis process was
  left running; unrelated system Redis/PostgreSQL services were not targeted.

Validation commands/results:

- `node --test tests/operations.test.js`: **5 passed, 0 failed, 0 skipped**.
  This includes three real negative controls: disabled resubscribe, absent
  SIGTERM handler and disabled backpressure, each rejected for its intended cause.
- Combined previous tests plus WS-2: **49 passed, 0 failed, 0 skipped**. Earlier
  suites used a separately owned ephemeral Redis, cleaned up by the launcher;
  WS-2 cases used their own private Redis instances, not that shared endpoint.
- Native Windows operational invocation: exit 1, explicit POSIX requirement.
- Linux with `REDIS_SERVER=/nonexistent-ws2-redis`: exit 1 and FAILED artifact
  with ENOENT, no skip/substitute. Routine negative/probe artifacts stay ignored.
- Independent raw audit verified required/allowed client-event identities,
  relay identities, payload hashes, all source hashes and all process exits.
  Syntax, scoped diff and cached whitespace/hash checks passed before commit.

The combined test command (with a dedicated Redis supplied to the earlier suites):

```sh
REDIS_HOST=127.0.0.1 REDIS_PORT=<dedicated-port> node --test tests/hub.test.js tests/benchmark.test.js tests/e2e.test.js tests/benchmark-integration.test.js tests/two-node.test.js tests/scaling.test.js tests/scaling-integration.test.js tests/operations.test.js
```

Local portable-runtime reproduction from Windows, repository root:

```powershell
wsl -d Ubuntu --exec /mnt/e/Abdou/Projects/careergithubupgrades/websocket-scaling-lab/bench/results/ws2-tools/node-v24.15.0-linux-x64/bin/node bench/prove-operations.js --output-dir docs/evidence/ws-2
```

This machine-specific runtime path is only a local recipe; ordinary Linux/CI
uses Node on PATH. CI targets Node 22 and its distribution Redis; hosted CI has
not been run or claimed to pass during local sign-off.

## Sign-off, portfolio recommendation and commit boundary

WS-2 is complete as representative operational evidence for a production-inspired
lab. Tests and real negative controls did not justify changing application code.
Remaining risks are the documented readiness gap, ephemeral Pub/Sub loss,
explicit/manual client recovery, bounded shutdown fallback and untested broader
network/deployment conditions. Do not translate this into production-proven or
guaranteed recovery claims.

Career Ops recommendation only: recognize evidence for failure injection,
process lifecycle, Redis resubscription, client recovery semantics and TCP
backpressure under Backend/Real-Time/Distributed Systems archetypes. Production,
capacity, AI, TypeScript and leadership claims remain unchanged. No CV, profile,
Career Ops or unrelated repository files were changed.

Exact WS-2 commit files (11):

```text
.github/workflows/ci.yml
.gitignore
README.md
package.json
bench/operational-lab.js
bench/prove-operations.js
tests/operations.test.js
tests/fixtures/ws2-mutant.cjs
docs/ws-2.md
docs/evidence/ws-2/2026-09-05T14-55-12-178Z-15bc2891-16a3-4cd5-8126-45d600114127.json
docs/evidence/ws-2/2026-09-05T14-55-12-178Z-15bc2891-16a3-4cd5-8126-45d600114127.log
```

Previous application/benchmark sources, lockfile and historical evidence remain
unchanged. The portable runtime, downloads, dependencies and routine test results
are ignored and excluded. One scoped commit only; no push. Human input is not
required to close WS-2. WS-3 is next, but **not started**; WS-4 is also untouched.
