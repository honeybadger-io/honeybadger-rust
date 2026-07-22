# Honeybadger Rust SDK — Design

**Date:** 2026-07-22
**Status:** Approved pending review
**Phase covered:** Phase 1 (error notices). Phase 2 (Insights events) is sketched only where it constrains Phase 1 architecture.

## What this is

The official Honeybadger Rust SDK, built from a blank slate against the current Honeybadger service. It is informed by — but not a port of — the two existing clients:

- **honeybadger-ruby (v6.9.1)** supplies the wire-protocol and worker-semantics reference: endpoints, throttle behavior, queue bounds, flush/shutdown semantics, sanitizer limits.
- **honeybadger-elixir** supplies the modernization reference: leaner payload, header-only API key, ~30-option config surface, explicit stacktrace capture, pluggable HTTP transport.
- **honeybadger-rs (fussybeaver, 2020)** is superseded; its repo is left untouched as the historical community crate.

Where the two references disagree, this design records which side we took and why.

## Identity and distribution

- Official Honeybadger SDK. Repo: this directory (`honeybadger-rust`), destined for the honeybadger-io GitHub org.
- Crate name: `honeybadger`, contingent on transferring the crates.io name from its current owner (fussybeaver — outreach happens in parallel with development; publishing is the final step regardless). Fallback if transfer falls through: `honeybadger-sdk`. Nothing in the code depends on the crate name beyond `Cargo.toml` and doc links.
- Edition 2024, MSRV 1.85. Single crate in Phase 1; module boundaries are drawn so Phase 2 (events) and later integration crates (`honeybadger-tower`, a `tracing` layer) attach without restructuring.

## Phasing

- **Phase 1 (this spec):** full notices — rich payload, breadcrumbs, panic hook, before_notify hooks, background worker with Ruby-parity delivery semantics.
- **Phase 2:** Insights events (`POST /v1/events`, NDJSON, batching 1000/30s, deterministic request-id sampling, separate worker with suspend-on-throttle semantics). The `Transport` trait and worker module are designed so the events worker is a sibling, not a rework.
- **Later:** check-ins, deploy tracking, metrics registry, `tracing` layer (per-request context + auto-breadcrumbs), tower/axum middleware, panic = per-request scoping. Explicitly out of Phase 1.

## Public API

Explicit `Client` core plus a thin global facade. The facade exists because the panic hook needs a globally reachable client and because drop-in ergonomics matter for adoption; the explicit `Client` exists for tests and multi-project use.

```rust
fn main() {
    let _guard = honeybadger::init(
        honeybadger::Config::builder()
            .api_key("hbp_...")             // or HONEYBADGER_API_KEY
            .env("production")
            .revision(env!("GIT_SHA"))
            .before_notify(|notice| { notice.context.insert("region".into(), "us-east-1".into()); true })
            .build()
    ).unwrap();
    // _guard drop → flush(timeout) then worker shutdown

    honeybadger::context([("user_id", 123.into())]);          // global context, merged into every notice
    honeybadger::add_breadcrumb("Cache miss", "query", None);  // 40-entry ring buffer

    if let Err(e) = do_work() {
        honeybadger::notify(&e);                               // any E: std::error::Error
    }

    honeybadger::notify_notice(
        honeybadger::Notice::from_error(&err)
            .tags(["checkout"])
            .fingerprint("custom-group")
            .context([("order_id", 42.into())])
    );

    honeybadger::flush(std::time::Duration::from_secs(5));
}
```

Surface (all free functions delegate to the global `Client`; every one exists as a method on `Client` too):

| Function | Behavior |
|---|---|
| `init(config) -> Result<Guard, Error>` | Builds the client, spawns the worker, installs the panic hook (unless disabled), registers the global. Errors on missing api_key or double-init. Guard drop = `flush` + shutdown. |
| `notify(&impl Error)` | Builds a notice (backtrace captured here), runs hooks, enqueues. Fire-and-forget: returns `()`; failures surface via the `log` facade. |
| `notify_notice(Notice)` | Same pipeline for a hand-built notice. `Notice::message(class, msg)` covers no-error-value reporting; `Notice::from_error(&e)` is the builder entry. |
| `context(iter)` / `clear_context()` | Merge into / clear the process-global context (`serde_json::Value` values). Setting a key to `Value::Null` removes it (Elixir convention). |
| `add_breadcrumb(message, category, metadata)` | Push onto the global 40-entry ring buffer. No-op when breadcrumbs disabled. |
| `flush(timeout) -> bool` | Block until the queue drains or timeout; returns whether it drained. |

Context and breadcrumbs are **process-global** in Phase 1 (mutex-guarded; contention is negligible at error-reporting rates). Thread-locals are deliberately rejected — async tasks migrate across threads, so thread-local context is silently wrong in exactly the apps that matter. Per-request scoping is the job of the Phase 3 `tracing` integration; nothing in the payload assembly assumes global-ness.

## Module layout

```
src/
  lib.rs         — crate docs, global facade, Guard
  client.rs      — Client: config + shared state (context, breadcrumbs) + worker handle
  config.rs      — Config, ConfigBuilder, env-var resolution
  notice.rs      — Notice + payload types + builder + serialization
  backtrace.rs   — capture, frame mapping, filtering, source excerpts
  breadcrumbs.rs — Breadcrumb, RingBuffer(40)
  sanitizer.rs   — filter_keys redaction, depth cap, string truncation
  worker.rs      — OS thread, bounded channel, throttle/suspend, flush/shutdown
  transport.rs   — Transport trait; Server (ureq), Null, Test
  panic.rs       — panic hook install/uninstall, previous-hook chaining
  error.rs       — SDK Error enum (thiserror)
```

Each module is independently testable; `worker` knows nothing about HTTP beyond the `Transport` trait, `notice` knows nothing about delivery.

## Delivery architecture

`notify()` does all payload work on the caller's thread — backtrace capture + symbol resolution, breadcrumb/context snapshot, sanitization, hooks — then `try_send`s the finished notice into a bounded (`max_queue_size`, default 100) channel. When the channel is full the notice is dropped with a `log::warn!`. A dedicated OS worker thread (named `honeybadger-worker`) owns a blocking HTTP client and loops on the channel.

Worker semantics, lifted from Ruby's `worker.rb` (the Elixir client's no-retry cast model was considered and rejected — the throttle math is cheap and protects the service):

- **Send:** one `POST /v1/notices` per notice. After every send, sleep the current throttle interval.
- **Throttle:** interval = `1.05^n − 1` seconds. `n` increments on 429/503, decrements on 201. (~2 minutes after ~100 consecutive throttles.)
- **Suspend:** 402 (payment required) and 403 (unauthorized/inactive) log one warning and suspend the worker for 1 hour; queued and incoming notices are dropped while suspended.
- **413:** log a warning (payload too large), continue.
- **Other non-2xx / transport errors:** log a warning, continue. No retry of individual notices.
- **Flush:** a marker message with an ack channel; `flush(timeout)` waits on the ack.
- **Shutdown:** sentinel + join with timeout, triggered by `Guard` drop. If suspended or throttled hard, shutdown abandons the queue rather than blocking process exit (Ruby behavior).

The runtime story falls out of this design: the crate works identically in tokio apps, async-std apps, and plain sync binaries, because it never touches an async runtime. This also makes the panic hook reliable — it does not depend on any runtime still being alive.

### Panic hook

Installed by `init` (config-disableable). On panic: extract the message (`&str`/`String` payload downcast, else `"Box<dyn Any>"`), capture the backtrace, build a notice with class `panic`, the panic's `location()` prepended as the top backtrace frame (backtrace-capture frames below the hook are stripped), enqueue, then `flush(2s)` inline so the report survives `panic = "abort"` and end-of-`main` unwinds. Always chains to the previously installed hook afterward. Uninstalled (restored) when the `Guard` drops.

## Transport

```rust
trait Transport: Send + Sync {
    fn deliver(&self, payload: &[u8]) -> Result<Delivery, TransportError>;  // Delivery = status code
}
```

- **`Server`** — the real one: `ureq` with rustls (small, genuinely synchronous — `reqwest::blocking` was rejected because it embeds a tokio runtime). Connect timeout 2s, request timeout 5s (Ruby's values). `native-tls` may become a cargo feature later; not Phase 1.
- **`Null`** — selected automatically when the environment is excluded (see Config); logs at debug, reports success.
- **`Test`** — captures payloads into `Arc<Mutex<Vec<...>>>`; public, so users can assert on notices in their own test suites (Ruby's `test` backend precedent).

## Wire format

`POST {endpoint}/v1/notices`. Headers: `X-API-Key` (the API key is **header-only**, not in the body — Elixir decision), `Content-Type: application/json`, `Accept: application/json`, `Content-Encoding: deflate`, `User-Agent: Honeybadger Rust {version}` (Elixir's simple UA format). Body: zlib-deflated JSON (`flate2`) — Ruby's compression kept, Elixir's plain JSON rejected, because backtraces with source excerpts are chunky and deflate is effectively free.

### Notice payload

The lean Elixir shape, plus `error.causes` (kept because Rust `source()` chains are idiomatic and common — Elixir dropped causes only because Elixir exceptions rarely nest) and `server.pid` (kept for multi-process correlation; trivially cheap). Omitted relative to Ruby, deliberately: `api_key` (header-only), `error.token`, top-level `details`, `request.local_variables` (impossible in Rust), `server.stats`, `server.time`.

```json
{
  "notifier": {"name": "honeybadger-rust", "url": "https://github.com/honeybadger-io/honeybadger-rust", "version": "<crate version>", "language": "rust"},
  "breadcrumbs": {"enabled": true, "trail": [{"message": "...", "category": "custom", "metadata": {}, "timestamp": "<iso8601 ms utc>"}]},
  "error": {
    "class": "std::io::Error",
    "message": "<Display output>",
    "backtrace": [{"number": "42", "file": "[PROJECT_ROOT]/src/main.rs", "method": "my_app::run", "source": {"41": "...", "42": "...", "43": "..."}}],
    "fingerprint": null,
    "tags": ["checkout"],
    "causes": [{"class": "...", "message": "..."}]
  },
  "request": {"context": {"user_id": 123}},
  "server": {"project_root": "/app", "revision": "abc123", "environment_name": "production", "hostname": "web-1", "pid": 12345},
  "correlation_context": {"request_id": "..."}
}
```

- `class` = `std::any::type_name::<E>()` for typed errors; caller-supplied for `Notice::message`. `message` = `Display`.
- `causes` = the `source()` chain, capped at 5 (Ruby's `MAX_EXCEPTION_CAUSES`). Causes carry class/message only (Rust causes have no independent backtraces).
- `correlation_context.request_id` is lifted from context when a `request_id` key is present (Elixir behavior); the key also remains in `request.context`.
- `fingerprint` is sent as the caller provided it (the service hashes; we do not SHA1 client-side — simplification over Ruby, matching Elixir which sends it raw).

### Backtraces

Captured with the `backtrace` crate (std's engine with a public frame API), at the `notify()` call site or inside the panic hook — **explicit capture only**. There is no attempt to infer where an error originated; this matches both Rust reality (errors don't carry traces) and the lesson Elixir codified when it deprecated implicit stacktrace inference. Optional richer capture from `anyhow`/`eyre` backtraces is future work, noted and excluded.

Frame processing: resolve symbols; map to `{number, file, method}`; substitute `config.root` prefix with `[PROJECT_ROOT]`; drop frames from this crate's internals and pre-`main` runtime scaffolding (`std::rt`, `__libc_start_main`, panic machinery below the hook); cap at 1,000 frames. `source` = ±2 lines read from `file` when it exists and is readable (silently absent in stripped/production deploys — that is fine and expected).

## Config

Precedence: **builder > env var > default**. No config file (a Ruby-ism both newer clients rejected). ~14 options:

| Option | Env var | Default | Notes |
|---|---|---|---|
| `api_key` | `HONEYBADGER_API_KEY` | — | required; `init` errors without it |
| `env` | `HONEYBADGER_ENV` | `None` | unset means "report" (production assumed), with one `log::info` |
| `exclude_envs` | — | `["development", "test"]` | matching env → `Null` transport |
| `enabled` | `HONEYBADGER_ENABLED` | `None` | explicit override of env gating, both directions |
| `endpoint` | `HONEYBADGER_ENDPOINT` | `https://api.honeybadger.io` | proxy/EU/self-hosted routing |
| `root` | `HONEYBADGER_ROOT` | current dir | drives `[PROJECT_ROOT]` substitution |
| `hostname` | `HONEYBADGER_HOSTNAME` | OS hostname | |
| `revision` | `HONEYBADGER_REVISION` | `None` | |
| `filter_keys` | — | `["password", "credit_card", "secret"]` | case-insensitive key match |
| `ignore_classes` | — | `[]` | exact class-string match, notice dropped pre-queue |
| `breadcrumbs_enabled` | — | `true` | |
| `install_panic_hook` | — | `true` | |
| `max_queue_size` | — | `100` | |
| `connect_timeout` / `request_timeout` | — | 2s / 5s | |

`before_notify` hooks are registered on the builder (not listed above; they are code, not data).

## Filtering and hooks

Two layers, run in this order on the caller's thread:

1. **Structural sanitization (always on — Elixir philosophy):** `filter_keys` matching replaces values with `"[FILTERED]"` in `request.context` and breadcrumb metadata (case-insensitive key comparison). Depth cap 20 with `"[DEPTH]"` markers; strings truncated at 64KB with `"[TRUNCATED]"`. Breadcrumb metadata sanitized to depth 1 (both existing clients agree). Redaction of common secrets never depends on user code.
2. **`before_notify` closures (Ruby philosophy):** `Fn(&mut Notice) -> bool + Send + Sync`, run in registration order; returning `false` halts the notice (it is never enqueued). Arbitrary mutation — context, tags, fingerprint, message — happens here.

`ignore_classes` is checked before hooks run (cheapest rejection first).

## Error handling within the SDK

The SDK must never panic and never surface errors into the host app's control flow. `notify` returns `()`; all internal failures (queue full, serialization, transport, suspend) are reported through the `log` facade at `warn` (actionable) or `debug` (expected, e.g. Null transport). `init` is the one fallible surface (`Result`): missing api_key, double-init. The worker thread catches and logs transport panics rather than dying; if it does die, subsequent notifies log a warning rather than blocking or panicking.

## Testing strategy

- **Unit:** payload serialization against golden JSON (every field, lean-shape omissions asserted); sanitizer (filter/depth/truncation); backtrace frame mapping and filtering with synthetic frames; config precedence (builder/env/default) using scoped env vars; ring buffer semantics.
- **Worker semantics** against `Test` transport with tuned-down intervals: queue overflow drops + warns, throttle increment/decrement arithmetic, suspend on 402/403, flush ack, shutdown drains, shutdown-while-throttled abandons.
- **HTTP integration** against mockito with the real `Server` transport: header set, deflate round-trip (inflate the received body and compare), status→behavior mapping.
- **Panic hook:** integration test spawning a child process (`std::process::Command` on a test binary) asserting the notice is delivered before exit, including under `panic = "abort"`.
- **CI:** GitHub Actions — `cargo fmt --check`, `clippy --all-targets -D warnings`, `cargo test`, examples build, MSRV (1.85) check job.

## Deferred / out of scope for Phase 1

- Insights events (Phase 2 — worker/transport designed for it, nothing implemented).
- Check-ins (`GET /v1/check_in/{id}`), deploy tracking, metrics.
- `tracing` layer, tower/axum middleware, per-request context/breadcrumb scoping.
- `anyhow`/`eyre` backtrace extraction; `native-tls` feature; client-side rate-limit persistence.
- crates.io publication (blocked on the name conversation; code is name-agnostic).

## Decision log (Ruby vs Elixir reconciliations)

| Decision | Followed | Rejected | Why |
|---|---|---|---|
| API key header-only | Elixir | Ruby (body + header) | one source of truth |
| Lean payload (no token/details/stats/time) | Elixir | Ruby | fields unused or uncollectible in Rust |
| Keep `error.causes` | Ruby | Elixir | `source()` chains are idiomatic Rust |
| Keep `server.pid` | Ruby | Elixir | cheap, useful for multi-process correlation |
| Deflate compression | Ruby | Elixir (plain) | backtraces + source excerpts are large |
| Worker throttle/suspend semantics | Ruby | Elixir (fire-and-forget cast) | protects the service, cheap to implement |
| Explicit backtrace capture only | Elixir | (Ruby n/a) | matches Rust reality; Elixir deprecated implicit capture |
| Free-form `before_notify` hooks | Ruby | Elixir (typed filter behaviours) | flexibility; structural filtering still built-in |
| Built-in `filter_keys` sanitization | Both | — | redaction must not depend on user code |
| ~14-option config, no config file | Elixir | Ruby (~110 + yaml) | Rust apps configure in code/env |
| Simple User-Agent | Elixir | Ruby triplet | no information the server uses |
| Raw fingerprint (no client SHA1) | Elixir | Ruby | server hashes; less magic |
