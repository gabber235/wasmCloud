# Observability contract matrix

The coordinated tracing cutover reserves these canonical interface versions:

| Package | Version | Change |
|---|---:|---|
| `wasi:otel` | `0.2.0-rc.2` | Unchanged completed telemetry contract |
| `wasmcloud:observability` | `0.1.0` | W3C trace-context carrier |
| `wasmcloud:messaging` | `0.4.0` | Optional producer parent context |
| `seamlezz:surrealdb` | `0.3.0` | Reserved for explicit operation context |

The rollout is coordinated. Older messaging and SurrealDB versions are not compatibility targets. Transport context remains outside message handler payloads.
