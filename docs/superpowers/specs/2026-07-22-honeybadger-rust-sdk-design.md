# Honeybadger Rust SDK — Design

**Date:** 2026-07-22
**Status:** Approved pending review; revised same day after external (Codex) design review — see Review Revisions at the bottom.
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
- Edition 2024, MSRV 1.88 (raised from 1.85 in Phase 2, which uses let-chains). Single crate in Phase 1; module boundaries are drawn so Phase 2 (events) and later integration crates (`honeybadger-tower`, a `tracing` layer) attach without restructuring.

## Phasing

- **Phase 1 (this spec):** full notices — rich payload, breadcrumbs, panic hook, before_notify hooks, background worker with Ruby-parity delivery semantics.
- **Phase 2:** Insights events (`POST /v1/events`, NDJSON, batching 1000/30s, deterministic request-id sampling, separate worker with suspend-on-throttle semantics). The `Transport` request descriptor and worker module are designed so the events worker is a sibling, not a rework. Phase 2 introduces `events_*` config options; `flush()` is defined as all-pipeline from day one so its meaning never changes.
- **Later:** check-ins, deploy tracking, metrics registry, `tracing` layer (per-request context + auto-breadcrumbs), tower/axum middleware, per-request scoping. Explicitly out of Phase 1.

## Public API

Explicit `Client` core plus a thin global facade. The facade exists because the panic hook needs a globally reachable client and because drop-in ergonomics matter for adoption; the explicit `Client` exists for tests and multi-project use.

```rust
fn main() {
    let _guard = honeybadger::init(
        honeybadger::Config::builder()
            .api_key("hbp_...")             // or HONEYBADGER_API_KEY
            .env("production")
            .revision(env!("GIT_SHA"))
            .before_notify(|notice| { notice.set_context("region", "us-east-1"); true })
            .build()
    ).unwrap();
    // _guard drop → flush(timeout) then worker shutdown

    honeybadger::context([("user_id", 123.into())]);          // current scope (global in Phase 1)
    honeybadger::add_breadcrumb("Cache miss", "query", None);  // 40-entry ring buffer

    if let Err(e) = do_work() {
        honeybadger::notify(&e);                               // any E: Error + ?Sized, incl. &dyn Error
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
| `init(config) -> Result<Guard, Error>` | See init/shutdown lifecycle below. `Guard` is `#[must_use]` (diagnostic: dropping it immediately shuts reporting down). |
| `notify<E: std::error::Error + ?Sized>(&E)` | Builds a notice (backtrace captured here), runs the pipeline, enqueues. Fire-and-forget: returns `()`; failures surface via the `log` facade. `?Sized` so `&dyn Error` works; for erased types the class falls back to the Display output (see Payload). |
| `notify_notice(Notice)` | Same pipeline for a hand-built notice. `Notice::message(class, msg)` covers no-error-value reporting; `Notice::from_error(&e)` is the builder entry (backtrace captured at this call). |
| `context(iter)` / `clear_context()` | Merge into / clear the **current scope's** context (`serde_json::Value` values). Setting a key to `Value::Null` removes it (Elixir convention). `clear_context()` clears the breadcrumb trail as well — it resets the whole accumulated diagnostic scope, matching Ruby's `Honeybadger.clear!`. |
| `add_breadcrumb(message, category, metadata)` | Push onto the current scope's 40-entry ring buffer. No-op when breadcrumbs disabled. |
| `flush(timeout) -> bool` | Block until every notice **enqueued before this call** is delivered (or dropped), or timeout. Returns whether the barrier was reached. Defined as all-pipeline from day one (Phase 2 events flush under the same call). |

**Scope contract (forward compatibility):** `context()` and `add_breadcrumb()` write to the *current scope*. In Phase 1 exactly one scope exists — the process-global one (mutex-guarded; contention is negligible at error-reporting rates). When a scoping integration (the Phase 3+ `tracing` layer or tower middleware) is active, the current scope becomes the request/span scope. This is documented from day one so the later semantic refinement is an advertised feature, not silent breakage. Until a scoping integration exists the hazard is real and must be documented as such, not merely reserved: in a concurrent server, request B's `context()` overwrites request A's, and A's next notice is attributed to B's user — a privacy problem, not just a confusing graph. The crate docs, README, and the `context`/`clear_context`/`add_breadcrumb` rustdoc therefore state plainly that the store is process-wide and direct request data to `Notice::context`, which travels with the notice and cannot be clobbered. Thread-locals are deliberately rejected — async tasks migrate across threads, so thread-local context is silently wrong in exactly the apps that matter.

**`Notice` encapsulation:** `Notice` fields are private and the type is `#[non_exhaustive]`; mutation goes through methods (`set_context(key, value)`, `set_class`, `set_message`, `set_fingerprint`, `add_tag`, …) and read access through accessors. This keeps the payload representation free to evolve without breaking the hook API.

### Client, init, and shutdown lifecycle

`Client` is usable standalone: `Client::new(config) -> Result<Client, Error>` spawns its own worker; `Client::builder()` additionally accepts `.transport(Arc<dyn Transport>)` for injection (this is how the `Test` transport is selected). `Client` is cheaply cloneable (`Arc` inner); `client.shutdown(timeout)` stops its worker, after which its `notify` logs a warning and drops. A standalone `Client`'s lifecycle is fully independent of the global.

The global follows an atomic state machine: `Uninitialized → Running → Uninitialized`. `init`:

1. Atomically claims the global slot (compare-exchange; concurrent/double `init` returns `Error::AlreadyInitialized` immediately, before any side effect).
2. Builds the `Client` (worker spawn included). On failure, releases the slot and returns the error.
3. Installs the panic dispatcher (see Panic hook) and registers the client with it.
4. Returns the `Guard`.

`Guard::drop`: deregisters the client from the panic dispatcher (the dispatcher itself is never uninstalled — see Panic hook), calls `flush(default timeout)`, shuts the worker down, and resets the global slot to `Uninitialized`. Re-`init` after drop is supported and creates a fresh client and worker.

`init` failure modes are enumerated: missing API key **when the resolved transport is `Server`** (excluded envs don't need credentials — see Config), invalid endpoint URL (validated in `Config::build`), worker thread spawn failure, and `AlreadyInitialized`. Environmental lookups never fail `init`: unreadable current dir or hostname degrade to empty defaults with a `log::debug`.

## Module layout

```
src/
  lib.rs         — crate docs, global facade, Guard
  client.rs      — Client: config + shared state (context, breadcrumbs) + worker handle
  config.rs      — Config, ConfigBuilder, env resolution (injectable env source)
  notice.rs      — Notice + payload types + builder + serialization
  backtrace.rs   — capture, frame mapping, filtering, source excerpts
  breadcrumbs.rs — Breadcrumb, RingBuffer(40)
  sanitizer.rs   — filter_keys redaction, depth cap, string truncation
  worker.rs      — OS thread, bounded channel + control channel, throttle/suspend, flush/shutdown
  transport.rs   — Transport trait + request descriptor; Server (ureq), Null, Test
  panic.rs       — permanent panic dispatcher, client registration, recursion guard
  error.rs       — SDK Error enum (thiserror)
```

Each module is independently testable; `worker` knows nothing about HTTP beyond the `Transport` trait, `notice` knows nothing about delivery.

## Notify pipeline

Everything below runs on the caller's thread, in this order:

1. **Build**: capture + resolve backtrace, snapshot breadcrumbs and scope context, assemble the `Notice`. For `notify_notice`, caller-supplied values win: notice-local context keys override scope context keys; a backtrace or breadcrumb trail already present on the notice is preserved, otherwise captured/snapshotted here ("capture only if absent"). `correlation_context.request_id` is lifted from the *merged* context.
2. **Ignore check**: `ignore_classes` exact match on the class → drop (cheapest rejection first).
3. **before_notify hooks**: in registration order, each `Fn(&mut Notice) -> bool + Send + Sync`; `false` halts. Hook panics are caught (`catch_unwind`), logged, and treated as `true` (don't let one bad hook silence errors).
4. **Ignore recheck**: hooks may have changed the class; the final class is checked against `ignore_classes` again.
5. **Sanitize (always last, so hook-introduced data is covered)**: `filter_keys` redaction to `"[FILTERED]"` in context and breadcrumb metadata (case-insensitive key match), depth cap 20 with `"[DEPTH]"`, string truncation at 64KB **on a UTF-8 character boundary** with `"[TRUNCATED]"`. Breadcrumb metadata sanitized to depth 1.
6. **Serialize + size cap**: the notice is serialized to bytes *before* enqueueing. Payloads whose serialized JSON exceeds **262,144 bytes** — the Reporting API's documented maximum — are dropped with a `log::warn` rather than sent to be 413'd. The cap is applied pre-compression because that is what the service measures. This bounds queue memory to `notice_queue_size × 256 KiB` worst-case and keeps the worker payload-agnostic.
7. **Enqueue**: `try_send` into the bounded notice channel; on full, drop + `log::warn`.

`notify()` is therefore not free: symbol resolution and source reads cost milliseconds. This is documented, accepted for Phase 1 (error reporting is exceptional-path), and revisitable later with a config flag to skip enrichment or defer it — noted as future work, not built now.

## Delivery architecture

A dedicated OS worker thread (named `honeybadger-worker`) owns a blocking HTTP client and selects over **two channels** (`crossbeam-channel`): the bounded notice channel (serialized payloads) and an unbounded control channel (flush markers with ack senders, shutdown). Control messages can therefore never be blocked out by a full notice queue, and every wait in the worker — including throttle sleeps and suspension — is a `select` with timeout on the control channel, so flush/shutdown interrupt any waiting state.

Worker states and semantics (lifted from Ruby's `worker.rb`; the Elixir client's no-retry cast model was considered and rejected — the throttle math is cheap and protects the service):

- **Running:** one `POST /v1/notices` per payload. After each send, wait the current throttle interval (interruptible).
- **Throttle:** interval = `1.05^n − 1` seconds; `n` increments on 429/503, decrements on 201. Both `n` and the interval are **saturated** (interval cap: 300s) so the arithmetic can never overflow or produce an unrepresentable `Duration`.
- **Suspended** (entered on 402 payment-required / 403 unauthorized, with a single `log::warn`): drains and drops the notice queue, then waits out 1 hour — but keeps servicing control messages: flush acks immediately (returning `true`; the queue is empty by definition), shutdown proceeds normally. Throttle counter resets on resume.
- **413:** log a warning (payload too large), continue. **Other non-2xx / transport errors:** log a warning, continue. No per-notice retry.
- **Flush:** the marker establishes a barrier for notices enqueued before the `flush` call (channel ordering guarantees this); notices enqueued concurrently *after* the call may remain — `flush` promises a happens-before barrier, not global emptiness.
- **Shutdown:** control-channel sentinel + join. Join timeout is bounded below by the transport request timeout (an in-flight send is allowed to finish). If the join times out the handle is dropped — the thread is explicitly documented as detached and harmless (it will exit after its current send when it sees the closed channel). Guard drop triggers this; if suspended or deeply throttled, shutdown abandons the queue rather than blocking process exit (Ruby behavior).

The runtime story falls out of this design: the crate works identically in tokio apps, async-std apps, and plain sync binaries, because it never touches an async runtime. This also makes the panic hook reliable — it does not depend on any runtime still being alive.

### Panic hook

A **permanent dispatcher** is installed once per process (`std::panic::set_hook` on first `init`, chaining to whatever hook was previously installed). It is never uninstalled — `Guard::drop` merely *deregisters* the client from it. This avoids two failure modes of naive install/restore: calling the std hook-management functions from a panicking thread (double-panic → abort), and clobbering hooks that other libraries installed after us.

Dispatcher behavior on panic, guarded by a per-thread recursion flag (a panic inside our own reporting path must not recurse):

1. If no client is registered or reporting is disabled → chain to the previous hook and return.
2. Build the notice inside `catch_unwind`: class `panic`, message from the payload downcast (`&str`/`String`, else `"Box<dyn Any>"`), backtrace captured here with the panic's `location()` prepended as the top frame (hook-machinery frames stripped). Scope context/breadcrumbs snapshotted; before_notify hooks run (each wrapped in `catch_unwind`); sanitization as normal.
3. **Direct delivery** — the panic path does not use the queue (a full queue must not drop the report, and worker throttling must not delay it): the payload is handed to the transport synchronously with panic-specific short timeouts (connect 1s, request 2s). One attempt; failure is logged and abandoned.
4. Chain to the previous hook.

This holds under `panic = "abort"` (delivery completes before the hook returns) and end-of-`main` unwinds. The allocation-during-panic caveat is accepted deliberately: the hook runs before unwinding, the allocator is in a normal state in safe-Rust programs, and this is the established practice of comparable SDKs.

## Transport

```rust
#[non_exhaustive]
pub struct TransportRequest<'a> {
    pub kind: RequestKind,        // Notices (Phase 1) | Events (Phase 2) — non_exhaustive
    pub path: &'a str,            // "/v1/notices"
    pub content_type: &'a str,    // "application/json" | "application/x-ndjson" (Phase 2)
    pub body: &'a [u8],           // already compressed
}

pub trait Transport: Send + Sync {
    fn deliver(&self, req: &TransportRequest) -> Result<u16, TransportError>;  // HTTP status
}
```

The descriptor (rather than a bare byte slice) exists so the Phase 2 events path is a new `RequestKind`, not a breaking trait redesign.

- **`Server`** — the real one: `ureq` with rustls (small, genuinely synchronous — `reqwest::blocking` was rejected because it embeds a tokio runtime). Connect timeout 2s, request timeout 5s (Ruby's values); the panic path passes its shorter timeouts per-request. `native-tls` may become a cargo feature later; not Phase 1.
- **`Null`** — selected automatically when the environment is excluded (see Config); logs at debug, reports success.
- **`Test`** — captures `TransportRequest`s into `Arc<Mutex<Vec<...>>>`; public, so users can assert on notices in their own test suites (Ruby's `test` backend precedent). Injected via `Client::builder().transport(...)`.

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

- **`class`** = `std::any::type_name::<E>()` for sized typed errors. `type_name` output is best-effort, not compiler-stable — acceptable as a *default* because grouping also weighs message and backtrace, but the class is always caller-overridable (`Notice::from_error(&e).class("PaymentError")`), and that override is the documented answer for teams that need stable grouping. For type-erased errors (`&dyn Error`, the tail of a `source()` chain), the concrete type name is unrecoverable on stable Rust; class falls back to the first line of the `Display` output (truncated at 255 chars).
- **`causes`** = the `source()` chain, capped at 5 (Ruby's `MAX_EXCEPTION_CAUSES`). Each cause: `class` = first line of its Display output (per the fallback rule above — cause types are always erased), `message` = full Display output. Causes carry no independent backtraces.
- **`environment_name`** is omitted from the payload when `env` is unset (unset means "report anyway", it does not mean "production" in the payload).
- `correlation_context.request_id` is lifted from the merged context when a `request_id` key is present (Elixir behavior); the key also remains in `request.context`.
- `fingerprint` is sent as the caller provided it (the service hashes; we do not SHA1 client-side — simplification over Ruby, matching Elixir which sends it raw).
- In optimized/stripped release builds, symbolication may lose file/line/method per frame: those fields are omitted per-frame (never fabricated), and the degraded shape is part of the golden-payload test matrix.

### Backtraces

Captured with the `backtrace` crate (std's engine with a public frame API), at the `notify()` call site or inside the panic hook — **explicit capture only**. There is no attempt to infer where an error originated; this matches both Rust reality (errors don't carry traces) and the lesson Elixir codified when it deprecated implicit stacktrace inference. Optional richer capture from `anyhow`/`eyre` backtraces is future work, noted and excluded.

Frame processing: resolve symbols; map to `{number, file, method}`; substitute `config.root` prefix with `[PROJECT_ROOT]`; drop frames from this crate's internals and pre-`main` runtime scaffolding (`std::rt`, `__libc_start_main`, panic machinery below the hook); cap at 1,000 frames. `source` = ±2 lines read from `file` **only for frames whose canonicalized path is under `config.root`** — dependency, toolchain, and build-host files are never read or transmitted (data-leak hardening; Ruby reads any readable file, we deliberately don't).

## Config

Precedence: **builder > env var > default**. No config file (a Ruby-ism both newer clients rejected). Env vars are read through an injectable environment source (a `fn(&str) -> Option<String>` held by the builder, defaulting to `std::env::var`) — this exists for test isolation, since Edition 2024 makes `env::set_var` unsafe and env-mutating tests race under parallel execution. ~14 options:

| Option | Env var | Default | Notes |
|---|---|---|---|
| `api_key` | `HONEYBADGER_API_KEY` | — | required **only when the resolved transport is `Server`** — excluded envs initialize fine without credentials |
| `env` | `HONEYBADGER_ENV` | `None` | unset means "report" (with one `log::info`); payload omits `environment_name` |
| `exclude_envs` | — | `["development", "test"]` | matching env → `Null` transport |
| `enabled` | `HONEYBADGER_ENABLED` | `None` | explicit override of env gating, both directions |
| `endpoint` | `HONEYBADGER_ENDPOINT` | `https://api.honeybadger.io` | proxy/EU/self-hosted routing; validated at `build()` |
| `root` | `HONEYBADGER_ROOT` | current dir | drives `[PROJECT_ROOT]` substitution + source-excerpt boundary |
| `hostname` | `HONEYBADGER_HOSTNAME` | OS hostname | lookup failure → empty, non-fatal |
| `revision` | `HONEYBADGER_REVISION` | `None` | |
| `filter_keys` | — | `["password", "credit_card", "secret"]` | case-insensitive key match |
| `ignore_classes` | — | `[]` | exact class-string match; checked before *and* after hooks |
| `breadcrumbs_enabled` | — | `true` | |
| `install_panic_hook` | — | `true` | |
| `notice_queue_size` | — | `100` | namespaced from day one: Phase 2 events get `events_*` options (`events_queue_size`, `events_batch_size`, …), later pipelines likewise |
| `connect_timeout` / `request_timeout` | — | 2s / 5s | |

`before_notify` hooks are registered on the builder (not listed above; they are code, not data).

## Error handling within the SDK

The SDK's guarantee, stated precisely: **SDK-authored code never panics; user-supplied code cannot panic the host through us.** All user-controlled call sites — `Display`/`source()` impls, `before_notify` hooks, custom `Transport`s — are wrapped in `catch_unwind`, with the failure logged and the pipeline continuing sensibly (a panicking Display yields class/message `"<panic in Display>"`; a panicking hook is treated as pass; a panicking transport counts as a transport error). This containment is unwinding-only: under `panic = "abort"` no `catch_unwind` in any crate can intercept a panic, so the guarantee is stated in the public docs with that limit attached (panic *reporting* still works there — the hook runs before the abort). Internal mutexes recover from poisoning via `PoisonError::into_inner` (our critical sections are simple data updates; a poisoned snapshot is still coherent). `notify` returns `()`; all internal failures (queue full, serialization, size cap, transport, suspend) are reported through the `log` facade at `warn` (actionable) or `debug` (expected, e.g. Null transport). `init` is the fallible surface, with its failure modes enumerated in the lifecycle section. The worker catches transport panics rather than dying; if the worker is ever gone, `notify` logs a warning and drops.

## Testing strategy

- **Unit:** payload serialization against golden JSON (every field, lean-shape omissions asserted, degraded stripped-build frame shape included); sanitizer (filter/depth/UTF-8-boundary truncation); backtrace frame mapping and filtering with synthetic frames; config precedence via the injectable env source (no process-global env mutation); ring buffer semantics; notice assembly collision rules (`notify_notice` local-wins merge).
- **Worker semantics** against `Test` transport with tuned-down intervals: queue overflow drops + warns, control channel unaffected by full queue, throttle increment/decrement and saturation, suspend on 402/403 (control messages still serviced, flush acks true, queue dropped), flush barrier ordering with a concurrent producer, shutdown drains, shutdown-while-throttled abandons and interrupts the sleep.
- **HTTP integration** against mockito with the real `Server` transport: header set, deflate round-trip (inflate the received body and compare), status→behavior mapping.
- **Panic hook:** integration tests against **dedicated fixture binaries** (not `#[test]` functions — the test harness overrides panic behavior): one built with default unwind, one with `panic = "abort"` in its profile, each asserting the notice reaches a local mock endpoint before process exit. Plus a recursion test (panic inside a before_notify hook during panic handling) asserting no abort and no infinite loop, and a hook-chaining test (previously installed hook still runs).
- **CI:** GitHub Actions — `cargo fmt --check`, `clippy --all-targets -D warnings`, `cargo test`, examples build, MSRV (1.88) check job.

## Deferred / out of scope for Phase 1

- Insights events (Phase 2 — worker/transport designed for it, nothing implemented).
- Check-ins (`GET /v1/check_in/{id}`), deploy tracking, metrics.
- `tracing` layer, tower/axum middleware, per-request context/breadcrumb scoping (the scope contract above reserves the semantics).
- `anyhow`/`eyre` backtrace extraction; `native-tls` feature; deferred/disableable backtrace enrichment; client-side rate-limit persistence.
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

## Review revisions (2026-07-22, after Codex design review)

Material changes from the reviewed draft, for the record:

1. **Panic hook redesigned as a permanent dispatcher** with client registration/deregistration — never uninstalled, fixing the Drop-during-unwind double-panic and the clobbering of later-installed hooks. Recursion guard + `catch_unwind` throughout; **panic notices bypass the queue** and deliver synchronously with short timeouts (the queued path could drop or delay them, making the delivery guarantee false).
2. **Sanitization moved after before_notify hooks** (hooks could reintroduce secrets past an earlier sanitization pass); `ignore_classes` rechecked after hooks.
3. **Serialize-before-enqueue with a 256 KiB payload cap** (the service's documented maximum), bounding queue memory.
4. **Separate control channel** (flush/shutdown) so a full notice queue can't block control; all worker waits are interruptible; throttle arithmetic saturated; suspended-state behavior fully specified.
5. **Flush defined as a happens-before barrier**, not global emptiness; all-pipeline from day one.
6. **`type_name` instability acknowledged**; class is caller-overridable; erased-type and cause class fallback rules specified. `notify` takes `E: Error + ?Sized`.
7. **`Notice` fields private/non_exhaustive** with mutation methods; `Guard` is `#[must_use]`; init/shutdown specified as an atomic state machine with enumerated failure modes; standalone `Client` construction/transport-injection specified.
8. **`api_key` required only for `Server` transport** (dev/test initializes without credentials); env-var reads injectable for test isolation; UTF-8-boundary truncation; source excerpts restricted to `config.root`; `environment_name` omitted when unset; `Transport` takes a request descriptor for Phase 2 compatibility.
9. **Scope contract documented** for `context()`/`add_breadcrumb()` (current scope = global in Phase 1, request scope under later integrations) — reviewer suggested renaming to `set_global_context`; declined in favor of the documented scope contract, matching the sentry-style model and keeping the common API pleasant.
10. Reviewer suggested renaming `max_queue_size` to `notice_queue_size`; initially declined for cross-client naming consistency, then **adopted at Ben's direction** — the Ruby/Elixir names predate Insights, and namespacing per pipeline (`notice_*`, `events_*`) is the right call when starting fresh.
