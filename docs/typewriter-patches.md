# Typewriter patches on wasmCloud 2.9

Base: upstream `v2.9.0`, commit `68ebece9c537f8bb4b5c9999f274ec68d60f35a9`.
Previous published fork: `76dc551a3a407d4dcb63d3303eab8c05a948fd28`.

The fork follows native `wasmcloud:nats@0.1.0`. Typewriter components import Core NATS and JetStream and export `core-handler`. The private `wasmcloud:messaging@0.4.0` contract and its custom transport implementations are retired.

## Patch disposition

- `049e0c1d1`, `19e660466`, `e742187d8`: retire the private async messaging contract, template, and migration documentation. Native NATS provides async operations, headers, typed errors, and JetStream acknowledgements.
- `3a045389a`, `264e5ed02`, `3cc7f9fa8`: retain exact guest span conversion, component resource identity, W3C propagation, and OpenTelemetry 0.32 compatibility.
- `6d5a026de`, `1d5e899d1`: migrate propagation into native NATS headers and the shared HTTP boundary. `GuestJob` carries the invocation span to dispatch. Native NATS and HTTP calls then associate it with the Wasmtime guest task ID, so host imports recover the correct context while concurrent calls interleave.
- `9d154ec20`: retain component log capture and deterministic HTTP shutdown. Replace the in memory messaging test transport with isolated real NATS and the production plugin.
- `6bf96376e`: replace custom runtime activity tracking with Core NATS queued delivery ownership, completion generations, broker synchronization, and reader fences.
- `8bcc2ee6e`: retire protobuf build dependency gating. The upstream runtime no longer has that build script.
- `e0a31f09d`, `ddfcc4996`, `76dc551a3`: retire fixture generation repairs tied to the private messaging ABI. Native plugin tests and Typewriter component tests provide the migration coverage.
- `e20d4c0ec`: retain injectable component span processors. Tests capture telemetry per fixture without replacing production transport.

## Host policy

System subjects follow normal host subject grants. There is no special `$SYS` prohibition or auth callout exception machinery. Production grants `$SYS.REQ.USER.AUTH` explicitly alongside application subjects. Other system subjects remain unavailable without a matching host grant. Workloads cannot widen those grants when `workloadConfig = deny`.

## Interface entry configuration

Component selectors and subscription lists belong to their individual interface entries. Upstream folded them into one connection configuration, which rejected composed workloads or copied a subscription onto the wrong handler. The binding schema now preserves entry settings while resolving connections, credentials, grants, and other shared settings once per binding. Host ownership checks still apply to entry settings.

## Concurrent trace ownership

The Wasmtime scheduler executes guest code outside the future awaiting its result. Instrumenting that future alone loses the context at host imports. Each native NATS delivery and P3 HTTP request registers its ingress span against the guest task ID; the OpenTelemetry boundary resolves the nearest registered caller through the async call stack. An owned registration removes the context on completion, failure, or cancellation. Concurrent calls share a store without sharing trace identity.

## Completion contract

`wasmcloud_nats::synchronize` waits for a broker round trip on the client's own inbox. `Client::flush` alone only flushes the client buffer.

`WasmcloudNats::wait_core_idle` covers queued and executing Core NATS handlers. Callers stop injecting input and synchronize their input connection first. Two rounds synchronize producer and consumer connections, reader fences drain already received messages, and activity generations detect work completed or started during observation. The caller owns the overall timeout. This does not claim completion of JetStream consumers, KV watches, timers, or unrelated background tasks.

## Custom host

`HostCommand::handle_with_plugins` runs the upstream host lifecycle with additional native plugins registered before binding validation. The Typewriter host uses this for SurrealDB. Host NATS connections retain credentials file support. Probe handling, quotas, startup retries, and scheduler lifecycle remain upstream owned.

## Rollout constraint

The old private messaging ABI and native NATS ABI are incompatible. Host images and component images must be published before their GitOps references change. A coordinated cutover must prevent old and new queue groups from simultaneously processing the same traffic. Publishing the Infrastructure default branch can trigger this rollout immediately.
