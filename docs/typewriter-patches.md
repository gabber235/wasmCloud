# Typewriter patches on wasmCloud 2.12.2

Base: upstream `v2.12.2`, commit `a8da8ac34`.
Previous published fork: tag `typewriter-v2.10.3-1` (base `v2.10.3`, commit `548bb21b6`).

The fork provides native `wasmcloud:nats@0.1.3` and accepts compatible `0.1.0` imports through upstream interface matching. Typewriter components import Core NATS and JetStream and export `core-handler`.

The carry is a merge of `v2.12.2` into the previous fork followed by focused commits, the same method as the 2.9 and 2.10.3 carries.

## Disposition of the 2.10.3 patches

Dropped, upstream now provides the guarantee:

- OpenTelemetry `0.32` and tracing OpenTelemetry `0.33` pin. Upstream moved to OpenTelemetry `0.32`, SDK `0.32.1` and tracing OpenTelemetry `0.33`. `Cargo.toml` and `Cargo.lock` are upstream's.
- Security dependency bumps (Wasmtime `48.0.6`, rustls `0.23.46`, quinn-proto `0.11.19`). Upstream locks Wasmtime `48.0.8`, rustls `0.23.46` and quinn-proto `0.11.19`.
- Host NATS credentials file support. Upstream `washlet::NatsAuth` reads credentials, nkey seeds, tokens and passwords from files and rereads them on reconnect. The flags keep their names (`--scheduler-nats-creds`, `--data-nats-creds`); the environment variables are now upstream's `WASH_SCHEDULER_NATS_CREDS` and `WASH_DATA_NATS_CREDS`.
- Strict configured service name (`0d5784c31`). Upstream `default_service_name` treats `unknown_service` and `unknown_service:<executable>` as unnamed, so a deployment that sets `OTEL_SERVICE_NAME`, or `OTEL_RESOURCE_ATTRIBUTES` without `service.name`, keeps the name it configured.

Rewritten smaller:

- Deterministic HTTP ingress shutdown. Rebuilt on upstream `ConnectionLimit::drained`. The fork's connection `JoinSet` and stored server result are gone. See [Ingress shutdown](#ingress-shutdown).

Kept on the new base:

- Exact guest span conversion, now with upstream's guest log levels, quieter per call logging, cumulative metric deduplication and overflow safe log timestamps. Upstream's guest id generator (`wasi_otel/ids.rs`) is deleted. See [Guest telemetry](#guest-telemetry).
- Per component resource identity, injectable span processors (`WasiOtelConfig::span_processor_factory`) and component log sinks.
- Guest task bound invocation trace context. `GuestJob` and the upstream `GuestTask` carry the invocation span; native NATS deliveries and P3 HTTP requests register it against the Wasmtime guest task.
- W3C trace context over NATS headers and outbound HTTP.
- Per entry NATS configuration, merged with upstream's `Key::identity` and retyped for `wit::Config`.
- Broker synchronization, `WasmcloudNats::wait_core_idle` and activity generations.
- System subjects through normal host grants, with one time reply grants limited to the server's own `$SYS._INBOX.` inboxes.
- `HostCommand::handle_with_plugins` and `ClusterHostBuilder::with_plugin(Arc<dyn HostPlugin>)`.
- The `fork-release` workflow and repository guards on upstream publishing workflows.

## Guest telemetry

Guest spans reach the component's span processor as the guest reported them: trace, span and parent ids, trace state, event timestamps, dropped counts, instrumentation scope and typed attribute values. Upstream still rebuilds each span through a host tracer with a substituted id generator, which loses event timestamps, dropped counts, the guest scope and trace state.

An empty or all zero parent span id now makes a root span, as upstream treats it. The previous fork rejected an all zero parent and dropped the span. This only exports spans that were dropped before.

Logs and metrics use upstream's conversion. Guest warn and error records log at warn and error on the host; lower severities log at debug. Cumulative guest sums add only their growth per guest instance, bounded per instance by series count and bytes.

The fork hands guest spans whose sampled flag is clear to the component's span processor, as it did on 2.10.3, and the default batch processor exports them. Upstream drops them through its host tracer's sampler. An injected processor that filters on the sampled flag, such as `SimpleSpanProcessor`, still drops them.

## Ingress shutdown

`HostHandler::stop` on `Ingress` ends the accept loop, tells every established connection to finish the request it is serving and close, waits for `ConnectionLimit::drained`, then releases the routing tables, and only then returns. A caller that awaited it knows the listener is closed and every routed workload is let go. A drain still open at the end of `WASH_INGRESS_DRAIN_TIMEOUT_SECS` releases the routes anyway and returns an error. `Host::stop` still stops every plugin after an HTTP stop error and returns that error.

Upstream instead releases the routes from a detached task after the drain and keeps serving new requests on established connections while it drains, so a fronting proxy does not see a withdrawn route. The fork closes established connections gracefully instead, as it did on 2.10.3, because embedded hosts in Typewriter's component tests must stop within a bounded time while clients keep idle connections open.

## Host policy

System subjects follow normal host subject grants. There is no reserved `$SYS` prefix and no auth callout exception to the grant check. Production grants `$SYS.REQ.USER.AUTH` explicitly alongside application subjects. Other system subjects remain unavailable without a matching host grant. Workloads cannot widen those grants when `workloadConfig = deny`.

A one time reply grant covers a system subject only under `$SYS._INBOX.`, where nats-server puts the reply inbox of a request it sends itself, such as an auth callout. The requester picks the `reply-to`, so without this limit a requester could aim a responder at `$SYS.REQ.SERVER.PING` or any other system request. The server's other reply subject, `$SYS._INBOX_.<server id>`, answers account connection counts between servers and stays reachable only through a host grant.

## Interface entry configuration

Component selectors and subscription lists belong to their individual interface entries. Upstream folds them into one connection configuration, which rejects composed workloads or copies a subscription onto the wrong handler. The binding schema preserves entry settings while resolving connections, credentials, grants and other shared settings once per binding. Host ownership checks still apply to entry settings.

## Concurrent trace ownership

The Wasmtime scheduler executes guest code outside the future awaiting its result. Instrumenting that future alone loses the context at host imports. Each native NATS delivery and P3 HTTP request registers its ingress span against the guest task ID; the OpenTelemetry boundary resolves the nearest registered caller through the async call stack. An owned registration removes the context on completion, failure, or cancellation. Concurrent calls share a store without sharing trace identity.

Core NATS publish and request, and JetStream publish, open a producer span and inject its context into the message headers. The operations new in `0.1.3` (direct gets, subject purge, message delete, stream creation time, KV create with TTL and delete by revision) send no guest message and carry no headers, so they get no producer span.

## Completion contract

`wasmcloud_nats::synchronize` waits for a broker round trip on the client's own inbox. `Client::flush` alone only flushes the client buffer.

`WasmcloudNats::wait_core_idle` covers queued and executing Core NATS handlers. Callers stop injecting input and synchronize their input connection first. Two rounds synchronize producer and consumer connections, reader fences drain already received messages, and activity generations detect work completed or started during observation. The caller owns the overall timeout. This does not claim completion of JetStream consumers, KV watches, timers, or unrelated background tasks.

## Custom host

`HostCommand::handle_with_plugins` runs the upstream host lifecycle with additional native plugins registered before workloads bind, so binding validation sees them like built in plugins. The Typewriter host uses this for SurrealDB. Host NATS authentication, probe handling, quotas, startup retries, component plugins and scheduler lifecycle remain upstream owned.

## Rollout constraint

Typewriter still imports native NATS `0.1.0`. Rebuild and exercise those actual components against the new host to verify compatible interface linking. Async guests must be rebuilt for Wasmtime 48.

Publish host and component images before changing their GitOps references. Local infrastructure uses its own Git mirror; publishing the Infrastructure default branch can deploy to production and is a separate operation.

## Fork binaries

Run the `fork-release` GitHub Actions workflow on `main` with a new tag such as `typewriter-v2.12.2-1`. The upstream version must match the workspace version, and the final integer identifies the fork revision. Existing tags and releases cannot be replaced by this workflow.

The workflow builds macOS arm64 and Linux arm64 and amd64 binaries with the default Cargo features. Linux targets glibc 2.17, and each Linux binary is run in a stock Debian Bookworm container as a glibc compatibility check. CLI and runtime tests, ingress lifetime tests, and NATS routing tests must pass before publication.

Each release records its source commit, SHA256 checksums, and build provenance. Nix locks the explicit release metadata and verifies the digests of the macOS arm64 and Linux amd64 assets it installs. Advance all consumers together after their checks pass; `wash update` does not manage the Nix installation.
