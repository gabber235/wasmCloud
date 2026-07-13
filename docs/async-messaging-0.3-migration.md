# Native-async `wasmcloud:messaging@0.3.0` migration plan

## Context

`wasmcloud:messaging@0.2.0` looks asynchronous inside the host but is synchronous at the component boundary:

- The WIT functions are ordinary `func`s in `wit/messaging/wit/world.wit`.
- Wasmtime generates Rust `async fn` host methods, and the implementations await `async-nats`, but the guest remains blocked in each imported call.
- NATS and in-memory subscriber loops already dispatch messages concurrently by creating a fresh store/instance and spawning a task per message.
- `async_nats::Client::publish(...).await` means the client accepted the publish command; it is not a server or consumer acknowledgement.

The desired change is a **breaking, local-first replacement** with only `wasmcloud:messaging@0.3.0`. The data model and broker behavior stay the same; only the component-model ABI becomes natively asynchronous. All first-party consumers should ultimately move to 0.3, but registry publication and the external operator test image are distribution follow-ups, not blockers for using the implementation locally.

> [!WARNING]
> The user-mandated local `@0.3.0` coordinate conflicts with the historical wasmCloud 1.5-1.9 messaging contract under older distribution paths. The current `wasmcloud.com` OCI path has no 0.3 manifest, so local work proceeds with 0.3 as requested. Any upstream or public release requires maintainer agreement on the coordinate and may need `@0.4.0`. Do not silently change the version during local development.

## Goals

- Replace `wasmcloud:messaging@0.2.0` with `@0.3.0`; do not retain a compatibility implementation.
- Make all broker-facing WIT operations native Component Model async functions:
  - `consumer.request`
  - `consumer.publish`
  - `handler.handle-message`
- Convert standalone NATS, standalone in-memory, and multiplexed/named messaging host bindings to Wasmtime's concurrent ABI.
- Preserve current routing, request/reply, timeout, publish, subscription, lifecycle, concurrency, and observability behavior.
- Migrate test guests and first-party Rust template sources to await the new APIs.
- Add end-to-end coverage that executes both default and named async host bindings through real guest components.
- Keep the implementation useful from a local checkout without waiting for OCI/WIT publication.

## Non-goals

- No guest-managed subscription resource or `stream<broker-message>`.
- No typed error redesign; errors remain `string`.
- No change to `broker-message` fields.
- No JetStream, queue groups, durable consumers, acknowledgements, retries, or delivery guarantees.
- No `flush()` added to publish; publish keeps its current async-nats client-acceptance semantics.
- No conversion of unrelated templates or guests to WASI HTTP 0.3. Exported async messaging handlers can remain `wasm32-wasip2`, but callers that initiate native async imports need a concurrent-ABI async entrypoint. The distributed-workloads requester therefore requires P3 HTTP conversion in step 13.
- No long-lived service/reactor message delivery; that is separate work in upstream PRs #5312/#5314.
- No arbitrary WIT-over-NATS/wRPC transport.

## Current behavior

### Contract

`wit/messaging/wit/world.wit` and `crates/wash-runtime/wit/deps/wasmcloud-messaging-0.2.0/package.wit` define:

```wit
handle-message: func(msg: broker-message) -> result<_, string>;
request: func(subject: string, body: list<u8>, timeout-ms: u32)
    -> result<broker-message, string>;
publish: func(msg: broker-message) -> result<_, string>;
```

The runtime world imports/exports those interfaces in `crates/wash-runtime/wit/world.wit:28-32`.

### Standalone NATS

`crates/wash-runtime/src/plugin/wasmcloud_messaging/nats.rs`:

- Implements request/publish as Rust async host methods (`:90-143`).
- Tracks handler components and configured subjects (`:168-229`).
- Establishes NATS subscriptions and performs a server round-trip readiness barrier (`:234-304`).
- Creates a fresh store and instance for each incoming message, then spawns handler execution (`:331-386`).
- Cancels/aborts the subscriber loop on unbind (`:415-432`).

### Standalone in-memory

`crates/wash-runtime/src/plugin/wasmcloud_messaging/in_memory.rs`:

- Maintains workload-local pending request reply channels (`:34-40`, `:143-206`).
- Routes messages into bounded per-component queues using NATS-style subject matching (`:58-115`).
- Resolves replies by `_INBOX.*` subject (`:209-240`).
- Uses the same fresh-store, fresh-instance, detached handler model (`:337-437`).

### Multiplexed outbound messaging

`crates/wash-runtime/src/plugin/wasmcloud_messaging/multiplexed.rs` routes named `consumer` imports to async `MsgBackend` implementations. The backend trait and NATS implementation are already async, but generated host methods still use the old store-exclusive ABI (`&mut ActiveCtx`).

### Existing native-async patterns to reuse

- `crates/wash-runtime/src/plugin/wasmcloud_postgres/async_p3/bindings.rs`: `store | async | trappable | tracing` bindgen configuration.
- `crates/wash-runtime/src/plugin/wasmcloud_postgres/async_p3.rs:291-309`: clone plugin/component state from an `Accessor` before awaiting.
- `crates/wash-runtime/src/plugin/wasi_keyvalue/multiplexed_async.rs`: `HostWithStore<T>` and named async import routing.
- `crates/wash-runtime/src/host/http_p3.rs:121-220`: drive typed async exports inside `Store::run_concurrent`.
- `crates/wash-runtime/src/engine/workload.rs:587-624`: nested `run_concurrent` / generated call / WIT result handling.
- `crates/wash-runtime/src/observability.rs:172-211`: bracket finite guest execution with fuel measurement.

## Proposed behavior

### WIT 0.3 contract

Replace the package with:

```wit
package wasmcloud:messaging@0.3.0;

interface types {
  record broker-message {
    subject: string,
    body: list<u8>,
    reply-to: option<string>,
  }
}

interface handler {
  use types.{broker-message};
  handle-message: async func(msg: broker-message) -> result<_, string>;
}

interface consumer {
  use types.{broker-message};

  request: async func(
    subject: string,
    body: list<u8>,
    timeout-ms: u32,
  ) -> result<broker-message, string>;

  publish: async func(msg: broker-message) -> result<_, string>;
}
```

This changes how the guest can schedule calls, not what a successful call means.

### Compatibility

- A component importing/exporting `@0.3.0` binds to the new plugin.
- A component still using `@0.2.0` is intentionally unsupported and must be rebuilt.
- No plugin advertises, links, or detects 0.2 after the migration.
- The historical wasmCloud 1.9 interface also used a different `@0.3.0` shape (`producer`, `request-reply`, `incoming-handler`), but that package is not present at the current `wasmcloud.com` registry path. This migration targets the current monorepo contract lineage and should be documented as such before public release.

### Preserved semantics

- `request` publishes once, awaits one response, and honors `timeout-ms`.
- Cancelling an async request stops waiting locally but cannot recall an already-published NATS request.
- `publish` does not wait for a consumer or handler and does not add a NATS server flush.
- Handler subjects remain runtime configuration (`subscriptions`), not guest API state.
- NATS component-local subscription config continues to override host-interface config.
- NATS with no configured subjects does not subscribe; in-memory with no subject list continues to match all.
- Each incoming message still gets a separate store/instance and can overlap other handler invocations.
- A handler's returned `err(string)` is logged distinctly from a Wasmtime trap, but has no NATS acknowledgement consequence.

## Implementation approach

### 1. Replace the WIT package and all runtime references

- Change canonical source to `wasmcloud:messaging@0.3.0` and mark all three methods `async func`.
- Replace the runtime dependency directory:
  - remove `crates/wash-runtime/wit/deps/wasmcloud-messaging-0.2.0/`
  - add `crates/wash-runtime/wit/deps/wasmcloud-messaging-0.3.0/package.wit`
- Change `crates/wash-runtime/wit/world.wit` imports/exports to 0.3.
- Replace the shared fixture dependency copy under `p2-wit-deps` with 0.3. The messaging fixtures may still build for `wasm32-wasip2`: that label identifies their WASI dependencies/build target, not whether custom WIT functions use the concurrent ABI.
- Update every exact `WitInterface` version string and semver value from 0.2 to 0.3.
- Regenerate registry-backed `wkg.lock` files only after 0.3 is published. For local work, vendored `wit/deps` files are authoritative and avoid a registry dependency.

### 2. Convert standalone NATS imports to `Accessor`

In `nats.rs`, configure bindgen for store-aware async imports:

```rust
crate::wasmtime::component::bindgen!({
    world: "messaging",
    imports: { default: store | async | trappable | tracing },
    exports: { default: async | tracing },
});
```

Generated async WIT methods move from `consumer::Host for ActiveCtx` to `consumer::HostWithStore<T> for SharedCtx`. Resolve and clone plugin state in a short synchronous accessor borrow:

```rust
fn plugin<T>(store: &Accessor<T, SharedCtx>) -> wasmtime::Result<Arc<NatsMessaging>> {
    store.with(|mut access| {
        access
            .get()
            .try_get_plugin::<NatsMessaging>(PLUGIN_MESSAGING_ID)
    })
}

impl consumer::Host for ActiveCtx<'_> {}

impl<T> consumer::HostWithStore<T> for SharedCtx {
    async fn request(
        store: &Accessor<T, Self>,
        subject: String,
        body: Vec<u8>,
        timeout_ms: u32,
    ) -> wasmtime::Result<Result<types::BrokerMessage, String>> {
        let plugin = plugin(store)?;
        // Existing timeout + async-nats request logic, unchanged.
    }

    async fn publish(
        store: &Accessor<T, Self>,
        msg: types::BrokerMessage,
    ) -> wasmtime::Result<Result<(), String>> {
        let plugin = plugin(store)?;
        // Existing publish/publish_with_reply logic, unchanged.
    }
}
```

Never retain `Access`, `ActiveCtx`, or a table borrow across `.await`.

Keep `types::Host for ActiveCtx` because `types` has data only.

### 3. Convert in-memory imports to `Accessor`

Apply the same bindgen/trait conversion in `in_memory.rs`. Clone both plugin and workload ID before awaiting:

```rust
fn plugin_and_workload<T>(
    store: &Accessor<T, SharedCtx>,
) -> wasmtime::Result<(Arc<InMemoryMessaging>, String)> {
    store.with(|mut access| {
        let view = access.get();
        Ok((
            view.try_get_plugin::<InMemoryMessaging>(PLUGIN_MESSAGING_MEMORY_ID)?,
            view.workload_id.to_string(),
        ))
    })
}
```

Retain the existing pending-request map, queue limit, subject matching, queue notification, timeout cleanup, and reply routing.

### 4. Convert multiplexed named imports

Keep `MsgBackend`, `MsgId`, `NatsMsgBackend`, and `InMemoryMsgBackend` behavior unchanged; they are already async. Only adapt generated named host bindings:

```rust
crate::wasmtime::component::bindgen!({
    world: "messaging",
    imports: { default: store | async | trappable | tracing },
    exports: { default: async | tracing },
    named_imports: {
        "wasmcloud:messaging/consumer@0.3.0": super::MsgId,
    },
});

impl named_consumer::Host for ActiveCtx<'_> {}

impl<T> named_consumer::HostWithStore<T> for SharedCtx {
    async fn request(
        _store: &Accessor<T, Self>,
        id: MsgId,
        subject: String,
        body: Vec<u8>,
        timeout_ms: u32,
    ) -> wasmtime::Result<Result<BrokerMessage, String>> {
        Ok(id.request(subject, body, timeout_ms).await)
    }

    async fn publish(
        _store: &Accessor<T, Self>,
        id: MsgId,
        msg: BrokerMessage,
    ) -> wasmtime::Result<Result<(), String>> {
        Ok(id.publish(msg).await)
    }
}
```

Continue using the existing multiplexer/provider pool; no new backend abstraction is needed for the ABI migration.

### 5. Drive async handler exports with `run_concurrent`

Both NATS and in-memory subscriber paths must replace the old store-exclusive proxy call with the concurrent driver. Keep fresh store/instance creation and per-message `tokio::spawn`.

The handler task should flatten Wasmtime's two runtime error layers while retaining the WIT result as data:

```rust
let result = fuel_meter
    .observe(&attributes, &mut store, async move |store| {
        let call = store
            .run_concurrent(async move |accessor| {
                proxy
                    .wasmcloud_messaging_handler()
                    .call_handle_message(accessor, &msg)
                    .instrument(span)
                    .await
            })
            .await
            .map_err(anyhow::Error::from)?;

        call.map_err(anyhow::Error::from)
    })
    .await;

match result {
    Ok(Ok(())) => debug!("message handled successfully"),
    Ok(Err(message)) => warn!(error = %message, "handler rejected message"),
    Err(error) => warn!(error = %error, "handler invocation failed"),
}
```

Validate the exact generated nesting against the pinned Wasmtime fork during compilation; the intended separation is:

1. `run_concurrent` driver failure,
2. generated export call/trap failure,
3. guest-declared `result<_, string>`.

The fuel meter remains around the finite `run_concurrent` call, so it measures guest handling and nested guest work but not idle broker waiting.

### 6. Migrate fixtures and add guest-level coverage

Update `messaging-handler` and `messaging-echo`:

- WIT references become 0.3.
- Enable `wit-bindgen` features `async-spawn` and `inter-task-wakeup`.
- Implement `async fn handle_message`.
- Await `consumer::publish`; pass owned values if required by regenerated async bindings.
- Rebuild `messaging_handler.wasm` and `messaging_echo.wasm` with `cargo xtask build-fixtures`.

Keep the fixtures on the `wasm32-wasip2` build path to deliberately prove that P2 WASI imports (for example, `wasi:logging`) can coexist with native-async custom messaging.

Add or extend tests to cover:

- **Standalone NATS full path:** external NATS request → async exported handler → async imported publish → reply.
- **Standalone in-memory full path:** an HTTP/requester guest awaits `consumer.request`; an echo guest handles and publishes the reply inside the same workload.
- **Multiplexed real guest path:** a fixture with two labeled `consumer@0.3.0` imports publishes/requests through two configured NATS clusters, proving generated `HostWithStore` and named routing—not only backend registry calls.
- **Lifecycle/readiness:** retain the NATS server subscription check and protocol fault tests.
- **Version replacement:** assertions and fixture metadata contain 0.3 only; repository search finds no runtime 0.2 support.

### 7. Migrate the distributed-workloads template to a concurrent HTTP entrypoint

In `templates/http-api-with-distributed-workloads`:

- Change messaging imports/exports to 0.3.
- Upgrade workspace `wit-bindgen` to the repository's P3-capable version and enable `async-spawn` + `inter-task-wakeup`.
- Convert the requester from P2 `wstd` HTTP to a native async WASI HTTP 0.3 export and the repository's P3 build path. A sync-ABI P2 HTTP export does not establish the component-async task context needed to call an `async func` import.
- Await `consumer::request` in `http-api`.
- Make both worker `handle_message` methods async and await `consumer::publish`; exported async handlers may remain `wasm32-wasip2` because the host invokes them through the concurrent ABI.
- Update README examples to 0.3 and describe native-async request/publish/handling.

Before registry publication, verify this locally by staging the canonical 0.3 package into the template's ignored `wit/deps/wasmcloud-messaging-0.3.0/` directory and running Cargo directly.

### 8. Distribution follow-ups (not required for local use)

The following cannot be finalized until artifacts exist outside the branch:

1. Publish `wit/messaging` as `wasmcloud:messaging@0.3.0` to the current registry.
2. Regenerate `crates/wash-runtime/wkg.lock`, template `wkg.lock`, and fixture locks against the published digest. The stale 0.2 messaging stanzas are removed locally; no 0.3 digest is fabricated.
3. Publish a P3-async messaging echo OCI component. The only currently available echo image, `ghcr.io/wasmcloud/components/messaging-echo-rust:0.1.0`, exports messaging 0.2.
4. Change `runtime-operator/config/samples/messaging.yaml`, `runtime-operator/test/e2e/messaging_test.go`, and `.github/workflows/wash.yml` to 0.3 and the new image tag. These references intentionally remain unchanged until the compatible image exists; changing only the declared interface would break or silently skip public CI.
5. Run template CI with normal `wash wit fetch` rather than ignored local dependency staging.
6. Update WIT publication CI tooling. The installed released `/opt/homebrew/bin/wash` rejects the `async func` syntax, while the repository-built `wash` parses and builds the package successfully. `.github/workflows/wit.yml` currently installs `wash` through `setup-wash`, so publication requires either a `wash` release containing the newer parser or a workflow change that builds and uses the repository version.

Until then, local runtime integration tests use checked-in WIT copies and embedded rebuilt Wasm fixtures, while the template build uses ignored local WIT dependencies. The core implementation is testable and usable.

## Files to modify

### Contract and runtime WIT

- `wit/messaging/wit/world.wit` — package version and three async functions.
- `crates/wash-runtime/wit/world.wit` — 0.3 imports/export.
- `crates/wash-runtime/wit/deps/wasmcloud-messaging-0.2.0/package.wit` — remove.
- `crates/wash-runtime/wit/deps/wasmcloud-messaging-0.3.0/package.wit` — add.
- `crates/wash-runtime/tests/fixtures/p2-wit-deps/wasmcloud-messaging-0.2.0/package.wit` — remove.
- `crates/wash-runtime/tests/fixtures/p2-wit-deps/wasmcloud-messaging-0.3.0/package.wit` — add.
- `crates/wash-runtime/wkg.lock` and consumer `wkg.lock` files — regenerate after publication; do not fabricate registry digests.

### Runtime implementation

- `crates/wash-runtime/src/plugin/wasmcloud_messaging/nats.rs` — Accessor imports, 0.3 registration, `run_concurrent` handler.
- `crates/wash-runtime/src/plugin/wasmcloud_messaging/in_memory.rs` — Accessor imports, 0.3 registration, `run_concurrent` handler.
- `crates/wash-runtime/src/plugin/wasmcloud_messaging/multiplexed.rs` — versioned named async bindings and `HostWithStore`.
- `crates/wash-runtime/src/plugin/wasmcloud_messaging/multiplexed/nats.rs` — expected to remain behaviorally unchanged; adjust generated message type imports only if compilation requires it.
- `crates/wash-runtime/src/plugin/wasmcloud_messaging/multiplexed/in_memory.rs` — same.

### Fixtures and runtime tests

- `crates/wash-runtime/tests/fixtures/messaging-handler/{Cargo.toml,src/lib.rs,wit/world.wit,wkg.lock}`.
- `crates/wash-runtime/tests/fixtures/messaging-echo/{Cargo.toml,src/lib.rs,wit/world.wit}`.
- `crates/wash-runtime/tests/fixtures/Cargo.toml` / `Cargo.lock` — async features and any new requester/named fixture.
- `xtask/src/main.rs` — register only genuinely new fixtures; existing messaging fixtures remain P2-target fixtures.
- `crates/wash-runtime/tests/wasm/messaging_handler.wasm` and `messaging_echo.wasm` — regenerated ignored artifacts used by tests.
- `crates/wash-runtime/tests/integration_nats_messaging.rs` — host-interface 0.3 and rebuilt guest.
- `crates/wash-runtime/tests/integration_nats_messaging_fault.rs` — 0.3 and rebuilt guest.
- `crates/wash-runtime/tests/integration_messaging_multiplexed.rs` — real async guest path plus existing backend isolation assertions.
- New focused in-memory async messaging integration/fixture if the existing test harness cannot initiate a component-side request.
- `crates/wash-runtime/src/engine/workload.rs` — messaging-specific 0.2 test literals/artifact metadata become 0.3.

### First-party consumers and distribution assets

- `templates/http-api-with-distributed-workloads/Cargo.toml` and member manifests — async bindgen support.
- `templates/http-api-with-distributed-workloads/wit/world.wit` — messaging 0.3.
- `templates/http-api-with-distributed-workloads/http-api/src/lib.rs` — await request.
- `templates/http-api-with-distributed-workloads/task-{leet,reverse}/src/lib.rs` — async handlers and awaited publish.
- `templates/http-api-with-distributed-workloads/README.md` — version/async documentation.
- `templates/http-api-with-distributed-workloads/{Cargo.lock,wkg.lock}` — regenerate at the appropriate local/public stage.
- `runtime-operator/config/samples/messaging.yaml` — 0.3 and new image after publication.
- `runtime-operator/test/e2e/messaging_test.go` — 0.3 after image publication.
- `.github/workflows/wash.yml` — new e2e image after publication.
- `.github/scripts/build-wit-matrix.mjs` — update stale version example comment.

## Reuse

- Preserve `sync_with_server` in `nats.rs`; it fixes the subscription readiness race from #5074.
- Preserve `parse_subscriptions` in `wasmcloud_messaging/mod.rs`.
- Preserve in-memory `subject_matches`, queueing, pending reply map, and bounded queue.
- Preserve `MsgBackend`, providers, pooling, and `Multiplexer` routing.
- Reuse the Postgres/keyvalue `Accessor` + `HostWithStore` pattern rather than inventing a messaging-specific executor.
- Reuse `Store::run_concurrent` patterns from P3 HTTP and CLI execution.
- Reuse `FuelConsumptionMeter::observe` around finite handler execution.
- Reuse existing NATS containers, server synchronization helpers, and fault-injection tests.
- Reuse the P3 HTTP request/response stream patterns from `examples/oci-registry` and the Postgres P3 fixture; remove `wstd` from the template requester.

## Open questions and assumptions

### Resolved decisions

- **Breaking version:** only 0.3 is supported after migration.
- **Scope:** NATS, in-memory, multiplexed, fixtures, tests, and first-party consumers all migrate.
- **Contract shape:** minimal ABI migration; no semantic redesign.
- **Inbound model:** configured handler callback remains; no subscription stream.
- **Publish guarantee:** unchanged async-nats client acceptance.
- **Template HTTP:** requester converts to P3 HTTP because callers initiating native async imports require a concurrent-ABI entrypoint.
- **Priority:** local usability first; PR/registry packaging later.

### Non-blocking implementation assumptions

- The pinned Wasmtime fork generates the same `HostWithStore`/`Accessor` shapes used by async Postgres and keyvalue.
- `wit-bindgen 0.58` can build async messaging handler exports for `wasm32-wasip2`. Guests that initiate native async imports require a concurrent-ABI entrypoint, such as a P3 HTTP export.
- Existing detached handler tasks remain acceptable even though unbind does not join them; changing shutdown semantics is outside this migration.

### Distribution blockers

- The current registry does not yet contain `wasmcloud:messaging@0.3.0`.
- The only available operator e2e image, `ghcr.io/wasmcloud/components/messaging-echo-rust:0.1.0`, exports 0.2 and must be rebuilt/published before `runtime-operator/config/samples/messaging.yaml`, `runtime-operator/test/e2e/messaging_test.go`, and `.github/workflows/wash.yml` can switch together.
- Registry-derived lockfile digests must not be guessed or hand-written. Local lockfiles therefore contain no messaging package stanza until 0.3 is published.
- The installed released `/opt/homebrew/bin/wash` rejects `async func` syntax. The repository-built `wash` succeeds, but `.github/workflows/wit.yml` uses `setup-wash`; publication CI therefore needs a release with the newer parser or must build and use repository `wash`.

## Implementation steps

1. [x] Change canonical messaging WIT to `@0.3.0` and mark request, publish, and handle-message `async func`.
2. [x] Replace runtime and fixture vendored 0.2 WIT copies with 0.3 and update the runtime world.
3. [x] Directly convert NATS bindgen/host imports to `store | async` and `HostWithStore<T>` using an `Accessor` helper.
4. [x] Directly convert in-memory bindgen/host imports, cloning plugin/workload state before awaits.
5. [x] Convert multiplexed named consumer imports to the version-qualified 0.3 `HostWithStore<T>` implementation.
6. [x] Update all three plugins' advertised/imported/exported interface versions and handler detection to 0.3.
7. [x] Change NATS inbound handler invocation to `run_concurrent`, preserve fuel measurement, and distinguish guest errors from traps.
8. [x] Apply the same concurrent handler invocation to in-memory messaging.
9. [x] Migrate existing messaging fixtures to async guest signatures and rebuild embedded Wasm.
10. [x] Update NATS integration/fault tests and workload interface-matching unit tests to 0.3.
11. [x] Add a component-to-component in-memory request/reply integration test.
12. [x] Exercise multiplexed 0.3 named imports through a real guest against two NATS clusters.
13. [x] Migrate distributed-workloads template source/WIT/docs, converting its requester to P3 HTTP while allowing exported messaging handlers to remain `wasm32-wasip2`.
14. [x] Perform a repository-wide 0.2 search and classify remaining hits as publication-stage operator/e2e references; unrelated 0.2 packages are not messaging migration findings.
15. [x] Locally stage 0.3 WIT into registry-dependent consumer `wit/deps` directories and verify builds without waiting for publication. Template verification used ignored local dependency staging.
16. [ ] Later: update WIT publication CI tooling, publish 0.3 WIT, regenerate locks, publish echo image, and update operator/public CI assets.

## Verification

### Static WIT and source checks

```bash
# WIT parses and canonical package builds with the repository-built wash.
# The installed released /opt/homebrew/bin/wash rejects `async func` syntax.
(
  cd wit/messaging
  cargo run --manifest-path ../../crates/wash/Cargo.toml -- \
    wit build --output-file /tmp/wasmcloud-messaging-0.3.0.wasm
)

# Runtime source must no longer advertise or bind 0.2.
git grep -n 'wasmcloud:messaging.*0\.2\.0\|wasmcloud-messaging-0\.2\.0' -- \
  crates/wash-runtime wit templates runtime-operator .github
```

Expected: no active runtime/template 0.2 references. Exact messaging 0.2 references remain in the operator sample and e2e test only because their configured OCI echo image exports 0.2; the workflow keeps that same image. Unrelated `0.2.0` versions such as `wasi:http@0.2.0` are outside this audit.

### Build and unit tests

```bash
cargo xtask build-fixtures
cargo fmt --all -- --check
cargo check -p wash-runtime --all-features
cargo test -p wash-runtime --lib --all-features
```

### Messaging integration tests

```bash
# Non-Docker in-memory/default async tests.
cargo test -p wash-runtime --test integration_in_memory_messaging --all-features

# Named routing test (real guest path; feature required).
cargo test -p wash-runtime \
  --features wasm_component_model_implements \
  --test integration_messaging_multiplexed -- --include-ignored --nocapture

# Live NATS handler/request/reply and readiness.
cargo test -p wash-runtime --test integration_nats_messaging \
  -- --include-ignored --nocapture

# Protocol/lifecycle fault coverage.
cargo test -p wash-runtime --test integration_nats_messaging_fault \
  -- --include-ignored --nocapture
```

### Required behavioral checks

- A 0.3 guest can await `request` without blocking the executor thread.
- An incoming NATS request invokes async `handle-message`; its awaited async `publish` returns the reply.
- Request timeout still returns the same error behavior and removes in-memory pending state.
- `reply-to` still selects `publish_with_reply`/reply routing.
- Exact, `*`, and trailing `>` subscriptions behave unchanged.
- Multiple configured components receive only matching subjects.
- NATS subscriptions are active before workload start reports success.
- Named imports route to the configured cluster and do not leak to the other cluster.
- Guest `Err(string)` and Wasmtime trap are logged as failures, not success.
- Unbind still cancels the subscriber processor and drops subscriptions.

### Template local verification before registry publication

Stage the repository's local P3 WASI dependencies, P2 config dependency, and
canonical messaging package into the ignored dependency directory, then build
with the existing target:

```bash
cp -R crates/wash-runtime/tests/fixtures/p3-wit-deps/* \
  templates/http-api-with-distributed-workloads/wit/deps/
cp -R crates/wash-runtime/tests/fixtures/p2-wit-deps/{wasi-config-0.2.0-rc.1,wasi-io-0.2.2} \
  templates/http-api-with-distributed-workloads/wit/deps/
mkdir -p templates/http-api-with-distributed-workloads/wit/deps/wasmcloud-messaging-0.3.0
cp wit/messaging/wit/world.wit \
  templates/http-api-with-distributed-workloads/wit/deps/wasmcloud-messaging-0.3.0/package.wit

cargo +nightly fmt --manifest-path \
  templates/http-api-with-distributed-workloads/Cargo.toml -- --check
cargo clippy \
  --manifest-path templates/http-api-with-distributed-workloads/Cargo.toml \
  --target wasm32-wasip2 --locked -- -D warnings
cargo build \
  --manifest-path templates/http-api-with-distributed-workloads/Cargo.toml \
  --workspace --target wasm32-wasip2 --release
```

Then run `wash dev` with a locally resolvable 0.3 package and manually verify both `/task` routes return their transformed replies.

### Publication-stage verification

After 0.3 WIT and the echo component are published:

```bash
wash wit fetch --clean  # in each registry-consuming project
# Regenerate and commit lockfiles, then run the repository's template and operator e2e workflows.
```

Verify the operator sample reaches Ready, NATS reports the configured subscription, and an external request receives the echoed body from the new 0.3 component.
