# Phase 2 — Insights Events Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an Insights events pipeline to the Honeybadger Rust SDK — a manual `event()` API, a batching delivery worker, and the `POST /v1/events` NDJSON transport — plus two Phase 1 corrections found while designing it.

**Architecture:** A second dedicated OS thread mirrors the shipped notices worker but batches: events are serialized to NDJSON lines on the caller's thread, accumulate until a count, byte, or time trigger cuts a batch, and are delivered with targeted retry. The worker is spawned lazily behind a mutex-guarded lifecycle so a program that never sends events never pays for a thread, and so a worker can never be created after shutdown.

**Tech Stack:** Rust edition 2024, `crossbeam-channel`, `serde_json`, `flate2` (deflate + CRC32), `ureq` 3, `mockito` for integration tests. **No new dependencies.**

**Authority:** `docs/superpowers/specs/2026-07-24-honeybadger-rust-insights-events-design.md`. Where this plan and the spec disagree, the spec wins — stop and ask.

## Global Constraints

- Rust edition 2024, MSRV 1.85. Do not raise either.
- **`export PATH="$HOME/.cargo/bin:$PATH"` in every shell.** `cargo` is not on the non-interactive PATH.
- **No new dependencies.** CRC32 comes from `flate2::Crc`; there is deliberately no `rand`.
- `src/lib.rs` has `#![warn(missing_docs)]`. Every public item needs a doc comment or the build warns.
- Every task ends green on all three: `cargo test`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`.
- Commit locally after every task. **Never push.**
- Public API additions must be documented with the process-wide hazard language the spec requires; copy the tone of the existing `context()` docs in `src/global.rs`.
- Constants, verbatim: `MAX_EVENT_BYTES = 102_400`, `BATCH_BYTE_LIMIT = 4_500_000`, drop-summary rate limit `60s`, suspend interval `3600s`.

## File Structure

| File | Responsibility |
| --- | --- |
| `src/drops.rs` (new) | Consolidated drop accounting shared by both pipelines |
| `src/event.rs` (new) | Event assembly, validation, sampling, line serialization |
| `src/events_worker.rs` (new) | Batching worker: triggers, budget, retry queue, failure matrix |
| `src/panic_hook.rs` | Gains a thread-local suppression guard |
| `src/worker.rs` | Reports drop summaries; exposes `flush_begin` |
| `src/transport.rs` | `RequestKind::Events`, `TransportRequest::events`, `CapturedRequest::kind` |
| `src/config.rs` | `events_*` options, `before_event`, validation |
| `src/client.rs` | Event methods, event context, request id, lifecycle, all-pipeline flush |
| `src/notice.rs` | `request_id` fallback from the shared slot |
| `src/global.rs` | Free-function facade for the new API |
| `src/lib.rs` | Module declarations, re-exports, crate docs |

---

### Task 1: Consolidated drop counter

Phase 1 logs one `log::warn!` per dropped notice, so a 10,000-notice storm produces 10,000 log lines during exactly the incident that caused it. This task fixes that and gives the events pipeline the same mechanism.

**Files:**
- Create: `src/drops.rs`
- Modify: `src/lib.rs` (add `mod drops;`)
- Modify: `src/worker.rs` (accept a counter; report on success and at shutdown)
- Modify: `src/client.rs:74-80` (record instead of warning per drop)

**Interfaces:**
- Consumes: nothing.
- Produces:
  ```rust
  pub(crate) struct DropCounter;
  impl DropCounter {
      pub(crate) const fn new(label: &'static str) -> Self;
      pub(crate) fn record(&self);
      pub(crate) fn record_many(&self, n: u64);
      pub(crate) fn report(&self) -> Option<u64>;        // rate-limited
      pub(crate) fn report_final(&self) -> Option<u64>;  // ignores the rate limit
      #[cfg(test)] pub(crate) fn pending(&self) -> u64;
  }
  // worker::spawn and spawn_with_intervals gain a trailing `drops: Arc<DropCounter>` parameter.
  ```

- [ ] **Step 1: Write the failing test**

Create `src/drops.rs` containing only the test module for now:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accumulates_and_reports_once_then_rate_limits() {
        let c = DropCounter::new("notices");
        assert_eq!(c.report(), None, "nothing pending, nothing logged");

        c.record();
        c.record_many(4);
        assert_eq!(c.pending(), 5);

        assert_eq!(c.report(), Some(5), "first report emits the running total");
        assert_eq!(c.pending(), 0, "reporting clears the counter");

        // Within the rate-limit window a second report is suppressed, and
        // crucially the count is RETAINED rather than discarded.
        c.record();
        assert_eq!(c.report(), None, "rate limited");
        assert_eq!(c.pending(), 1, "suppressed reports must not lose the count");

        // Shutdown ignores the rate limit so nothing is silently lost.
        assert_eq!(c.report_final(), Some(1));
        assert_eq!(c.pending(), 0);
        assert_eq!(c.report_final(), None, "nothing left to report");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib drops`
Expected: FAIL — `cannot find struct DropCounter`.

- [ ] **Step 3: Write the implementation**

Put this above the test module in `src/drops.rs`:

```rust
//! Consolidated drop accounting shared by both delivery pipelines.
//!
//! A queue only fills during a storm, which is precisely when one log line per
//! dropped item turns a bad situation into an unreadable one. Counts accumulate
//! here and are summarised at most once a minute, on the next successful
//! delivery and again at shutdown.
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const MIN_LOG_INTERVAL: Duration = Duration::from_secs(60);

pub(crate) struct DropCounter {
    label: &'static str,
    dropped: AtomicU64,
    last_log: Mutex<Option<Instant>>,
}

impl DropCounter {
    pub(crate) const fn new(label: &'static str) -> Self {
        DropCounter {
            label,
            dropped: AtomicU64::new(0),
            last_log: Mutex::new(None),
        }
    }

    pub(crate) fn record(&self) {
        self.record_many(1);
    }

    pub(crate) fn record_many(&self, n: u64) {
        self.dropped.fetch_add(n, Ordering::Relaxed);
    }

    /// Emits a summary if anything is pending and the rate limit allows.
    /// A suppressed report leaves the count intact for the next one.
    pub(crate) fn report(&self) -> Option<u64> {
        if self.dropped.load(Ordering::Relaxed) == 0 {
            return None;
        }
        let mut last = self.last_log.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        if let Some(prev) = *last
            && now.duration_since(prev) < MIN_LOG_INTERVAL
        {
            return None;
        }
        *last = Some(now);
        self.emit()
    }

    /// Emits whatever is pending regardless of the rate limit. For shutdown.
    pub(crate) fn report_final(&self) -> Option<u64> {
        let mut last = self.last_log.lock().unwrap_or_else(|e| e.into_inner());
        let emitted = self.emit();
        if emitted.is_some() {
            *last = Some(Instant::now());
        }
        emitted
    }

    fn emit(&self) -> Option<u64> {
        let n = self.dropped.swap(0, Ordering::Relaxed);
        if n == 0 {
            return None;
        }
        log::warn!("honeybadger: dropped {n} {} (queue full)", self.label);
        Some(n)
    }

    #[cfg(test)]
    pub(crate) fn pending(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib drops`
Expected: PASS (1 test).

- [ ] **Step 5: Declare the module**

In `src/lib.rs`, add `mod drops;` to the module list, keeping alphabetical order (after `mod config;`, before `mod error;`).

- [ ] **Step 6: Wire the counter into the notices worker**

In `src/worker.rs`:

Add to the imports: `use crate::drops::DropCounter;`

Add a field to `struct Worker`:
```rust
    drops: Arc<DropCounter>,
```

Change both spawn functions to take and forward the counter:
```rust
pub(crate) fn spawn(
    transport: Arc<dyn Transport>,
    queue_size: usize,
    drops: Arc<DropCounter>,
) -> std::io::Result<WorkerHandle> {
    spawn_with_intervals(transport, queue_size, SUSPEND_INTERVAL, drops)
}

pub(crate) fn spawn_with_intervals(
    transport: Arc<dyn Transport>,
    queue_size: usize,
    suspend_interval: Duration,
    drops: Arc<DropCounter>,
) -> std::io::Result<WorkerHandle> {
```
and add `drops,` to the `Worker { .. }` literal inside the spawned closure.

In `Worker::send_one`, in the success arm only, report after the throttle decays:
```rust
            Ok(status) if (200..300).contains(&status) => {
                self.throttle = self.throttle.saturating_sub(1);
                self.drops.report();
                SendOutcome::Continue
            }
```

In `Worker::handle_control`, in the `Control::Shutdown` arm, emit the final summary immediately before acknowledging:
```rust
                self.drops.report_final();
                let _ = ack.send(());
                true
```

Every existing call site in `src/worker.rs`'s own test module needs the new argument. Add this helper to that test module and use it in each `spawn`/`spawn_with_intervals` call:
```rust
    fn drops() -> Arc<DropCounter> {
        Arc::new(DropCounter::new("notices"))
    }
```

- [ ] **Step 7: Wire the counter into the client**

In `src/client.rs`, add `use crate::drops::DropCounter;` and a field on `struct Inner`:
```rust
    notice_drops: Arc<DropCounter>,
```

In `ClientBuilder::build`, construct it before the worker and pass a clone in:
```rust
        let notice_drops = Arc::new(DropCounter::new("notices"));
        let worker = crate::worker::spawn(
            transport.clone(),
            config.notice_queue_size,
            notice_drops.clone(),
        )
        .map_err(Error::WorkerSpawn)?;
```
and add `notice_drops,` to the `Inner { .. }` literal.

Replace the per-drop warning in `notify_notice`:
```rust
    pub fn notify_notice(&self, notice: Notice) {
        if let Some(payload) = self.run_pipeline(notice)
            && !self.0.worker.try_enqueue(payload)
        {
            self.0.notice_drops.record();
        }
    }
```

- [ ] **Step 8: Add a client-level regression test**

Add to the test module in `src/client.rs`:

```rust
    #[test]
    fn test_queue_full_drops_are_counted_not_logged_individually() {
        // Suspend the worker with a 402 so it stops consuming, then overfill.
        let transport = Arc::new(TestTransport::new());
        transport.respond_with(402);
        let client = test_client_with(transport.clone(), |b| b.notice_queue_size(2));
        client.notify_notice(crate::Notice::message("X", "y")); // consumed -> suspends
        std::thread::sleep(Duration::from_millis(200));
        for _ in 0..20 {
            client.notify_notice(crate::Notice::message("X", "y"));
        }
        assert!(
            client.0.notice_drops.pending() > 0,
            "overflow must be accumulated in the counter"
        );
        client.shutdown(Duration::from_secs(5));
    }
```

- [ ] **Step 9: Verify everything is green**

Run: `cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check`
Expected: all pass.

- [ ] **Step 10: Commit**

```bash
git add src/drops.rs src/lib.rs src/worker.rs src/client.rs
git commit -m "fix: consolidate dropped-notice logging into a rate-limited summary

One warning per dropped notice turned an error storm into a log storm
during exactly the incident that filled the queue. Counts now accumulate
and are summarised on the next successful delivery and at shutdown."
```

---

### Task 2: Panic-suppression guard

`catch_unwind` does not stop Rust's panic hook from firing first, so a panicking `before_notify` hook currently reports an urgent panic notice on the caller's thread before control returns to the catch. That pays the urgent HTTP timeout and reports a panic the SDK deliberately contained.

**Files:**
- Modify: `src/panic_hook.rs`
- Modify: `src/client.rs:140-149` (guard the hook catch)
- Create: `examples/hook_panic_fixture.rs`
- Modify: `tests/panic_hook.rs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  ```rust
  pub(crate) fn suppress_reporting() -> Suppressed;  // RAII; restores the previous value on drop
  pub(crate) struct Suppressed;
  ```

- [ ] **Step 1: Write the failing unit test**

Add to `src/panic_hook.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_suppression_guard_nests_and_restores() {
        assert!(!is_suppressed());
        {
            let _outer = suppress_reporting();
            assert!(is_suppressed());
            {
                let _inner = suppress_reporting();
                assert!(is_suppressed());
            }
            assert!(
                is_suppressed(),
                "an inner guard dropping must not clear the outer one"
            );
        }
        assert!(!is_suppressed(), "outermost drop restores reporting");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib panic_hook`
Expected: FAIL — `cannot find function suppress_reporting`.

- [ ] **Step 3: Implement the guard**

In `src/panic_hook.rs`, add below the existing `IN_HOOK` thread-local:

```rust
thread_local! {
    /// Set while the SDK is deliberately running user code it expects to catch.
    static SUPPRESSED: Cell<bool> = const { Cell::new(false) };
}

/// RAII guard: while alive, our dispatcher does not report panics on this
/// thread. Restores the previous value on drop, so guards nest correctly.
pub(crate) struct Suppressed(bool);

pub(crate) fn suppress_reporting() -> Suppressed {
    SUPPRESSED.with(|s| {
        let previous = s.get();
        s.set(true);
        Suppressed(previous)
    })
}

impl Drop for Suppressed {
    fn drop(&mut self) {
        let previous = self.0;
        SUPPRESSED.with(|s| s.set(previous));
    }
}

fn is_suppressed() -> bool {
    SUPPRESSED.with(|s| s.get())
}
```

- [ ] **Step 4: Honor the guard in the dispatcher**

Replace the body of `dispatch` in `src/panic_hook.rs`:

```rust
fn dispatch(info: &PanicHookInfo<'_>, previous: &(dyn Fn(&PanicHookInfo<'_>) + Send + Sync)) {
    let reentered = IN_HOOK.with(|flag| flag.replace(true));
    if !reentered {
        // A panic inside user code we are about to catch is contained, not
        // reported: reporting it would pay the urgent HTTP timeout on the
        // caller's thread and file a notice for a panic that never escaped.
        if !is_suppressed() {
            let client = PANIC_CLIENT
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if let Some(client) = client {
                let _ = catch_unwind(AssertUnwindSafe(|| report(&client, info)));
            }
        }
        IN_HOOK.with(|flag| flag.set(false));
    }
    // Always chain: non-Honeybadger panic handling must be unaffected.
    previous(info);
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test --lib panic_hook`
Expected: PASS.

- [ ] **Step 6: Guard the `before_notify` catch**

In `src/client.rs`, replace the hook loop in `run_pipeline` (step 3 of the pipeline):

```rust
        // 3. before_notify hooks; panics caught and treated as pass. The guard
        //    stops our own panic hook from reporting a panic we are containing.
        for hook in &inner.config.before_notify {
            let hook = hook.clone();
            let keep = {
                let _suppressed = crate::panic_hook::suppress_reporting();
                catch_unwind(AssertUnwindSafe(|| hook(&mut notice))).unwrap_or_else(|_| {
                    log::warn!("honeybadger: before_notify hook panicked; continuing");
                    true
                })
            };
            if !keep {
                return None;
            }
        }
```

- [ ] **Step 7: Write the integration fixture**

Create `examples/hook_panic_fixture.rs`:

```rust
//! Fixture for `tests/panic_hook.rs`: a `before_notify` hook that panics must
//! be contained WITHOUT the SDK reporting a panic notice of its own.
//!
//! Usage: `hook_panic_fixture <endpoint>`. Reports exactly one notice.
fn main() {
    let endpoint = std::env::args().nth(1).expect("endpoint argument");
    let _guard = honeybadger::init(
        honeybadger::Config::builder()
            .env_source(|_| None)
            .api_key("test-key")
            .env("production")
            .endpoint(endpoint)
            .install_panic_hook(true)
            .before_notify(|_| panic!("hook blew up"))
            .build()
            .expect("config"),
    )
    .expect("init");

    honeybadger::notify_notice(honeybadger::Notice::message("Kept", "survived the hook"));
    honeybadger::flush(std::time::Duration::from_secs(5));
}
```

`env_source` and `Notice::message` are already public; no new API is needed here.

- [ ] **Step 8: Write the integration test**

Add to `tests/panic_hook.rs`, following the existing fixture-spawning pattern already in that file (reuse its helper for locating the built example binary and its mockito setup):

```rust
#[test]
fn test_panicking_before_notify_hook_reports_no_panic_notice() {
    let mut server = mockito::Server::new();
    let mock = server
        .mock("POST", "/v1/notices")
        .with_status(201)
        .expect(1) // exactly one: the notice itself, never a panic notice
        .create();

    run_fixture("hook_panic_fixture", &server.url());

    mock.assert();
}
```

If `tests/panic_hook.rs` does not already expose a `run_fixture(name, endpoint)` helper, extract one from the existing tests' fixture-spawning code and use it in all of them rather than duplicating the spawn logic.

- [ ] **Step 9: Verify everything is green**

Run: `cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check`
Expected: all pass. The new integration test must show 1 request, not 2.

- [ ] **Step 10: Commit**

```bash
git add src/panic_hook.rs src/client.rs examples/hook_panic_fixture.rs tests/panic_hook.rs
git commit -m "fix: contain hook panics without self-reporting a panic notice

catch_unwind cannot stop the panic hook from firing first, so a panicking
before_notify hook filed an urgent notice on the caller's thread for a
panic that never escaped. A thread-local guard suppresses our dispatcher
around expected catches while still chaining to external hooks."
```

---

### Task 3: Events transport

**Files:**
- Modify: `src/transport.rs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  ```rust
  pub(crate) const EVENTS_PATH: &str = "/v1/events";
  pub enum RequestKind { Notices, Events }
  impl<'a> TransportRequest<'a> { pub fn events(body: &'a [u8]) -> Self; }
  pub struct CapturedRequest { pub kind: RequestKind, /* existing fields */ }  // now #[non_exhaustive]
  ```

- [ ] **Step 1: Write the failing test**

Add to the test module in `src/transport.rs`:

```rust
    #[test]
    fn test_events_request_shape() {
        let body = compress(b"{\"a\":1}\n{\"b\":2}");
        let req = TransportRequest::events(&body);
        assert_eq!(req.kind, RequestKind::Events);
        assert_eq!(req.path, "/v1/events");
        assert_eq!(req.content_type, "application/x-ndjson");
        assert!(!req.urgent, "events are never delivered on the urgent path");
    }

    #[test]
    fn test_test_transport_records_kind() {
        let t = TestTransport::new();
        let body = compress(b"{}");
        t.deliver(&TransportRequest::notices(&body, false)).unwrap();
        t.deliver(&TransportRequest::events(&body)).unwrap();
        let kinds: Vec<RequestKind> = t.requests().iter().map(|r| r.kind).collect();
        assert_eq!(kinds, vec![RequestKind::Notices, RequestKind::Events]);
    }

    #[test]
    fn test_server_transport_posts_ndjson_to_events() {
        let mut server = mockito::Server::new();
        let mock = server
            .mock("POST", "/v1/events")
            .match_header("X-API-Key", "test-key")
            .match_header("Content-Type", "application/x-ndjson")
            .match_header("Content-Encoding", "deflate")
            .with_status(201)
            .create();
        let t = ServerTransport::new(
            server.url(),
            "test-key".into(),
            std::time::Duration::from_secs(2),
            std::time::Duration::from_secs(5),
        );
        let body = compress(b"{\"event_type\":\"x\"}");
        assert_eq!(t.deliver(&TransportRequest::events(&body)).unwrap(), 201);
        mock.assert();
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib transport`
Expected: FAIL — no variant `Events`, no function `events`, no field `kind`.

- [ ] **Step 3: Implement**

In `src/transport.rs`:

Add beside `NOTICES_PATH`:
```rust
pub(crate) const EVENTS_PATH: &str = "/v1/events";
```

Add the variant to `RequestKind` (already `#[non_exhaustive]`, so this is not a breaking change):
```rust
    /// The Insights events API, `POST /v1/events`.
    Events,
```

Add the constructor next to `TransportRequest::notices`:
```rust
    /// Builds a request against the Insights events API. The body is a batch of
    /// newline-delimited JSON objects, deflated like every other request.
    pub fn events(body: &'a [u8]) -> Self {
        TransportRequest {
            kind: RequestKind::Events,
            path: EVENTS_PATH,
            content_type: "application/x-ndjson",
            body,
            urgent: false,
        }
    }
```

Make `CapturedRequest` non-exhaustive and add the field:
```rust
/// A request captured by [`TestTransport`].
#[non_exhaustive]
pub struct CapturedRequest {
    /// Which API the request targeted.
    pub kind: RequestKind,
    /// Path the request was sent to.
    pub path: String,
    // ... existing fields unchanged
}
```

Both places that build a `CapturedRequest` — `TestTransport::requests` and `TestTransport::deliver` — need `kind: r.kind,` and `kind: req.kind,` respectively.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib transport`
Expected: PASS.

- [ ] **Step 5: Verify and commit**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
git add src/transport.rs
git commit -m "feat: add the Events request kind to the transport seam

RequestKind was already non_exhaustive for exactly this. CapturedRequest
gains a kind field and becomes non_exhaustive too, so tests assert on
pipeline rather than string-matching paths."
```

---

### Task 4: Events configuration

**Files:**
- Modify: `src/config.rs`
- Modify: `src/error.rs` (new `InvalidConfig` variant)

**Interfaces:**
- Consumes: nothing.
- Produces:
  ```rust
  pub type BeforeEventHook = dyn Fn(&mut serde_json::Map<String, serde_json::Value>) -> bool + Send + Sync;

  // Config fields (pub(crate)):
  events_enabled: bool,
  events_batch_size: usize,
  events_flush_interval: Duration,
  events_queue_size: usize,
  events_max_retries: u32,
  events_sample_rate: u8,
  events_attach_hostname: bool,
  events_attach_environment: bool,
  before_event: Vec<Arc<BeforeEventHook>>,

  // Builder setters of the same names, plus:
  ConfigBuilder::before_event<F: Fn(&mut Map<String, Value>) -> bool + Send + Sync + 'static>(self, f: F) -> Self

  // Error:
  Error::InvalidConfig(String)
  ```

- [ ] **Step 1: Write the failing test**

Add to the test module in `src/config.rs`:

```rust
    #[test]
    fn test_events_defaults() {
        let cfg = Config::builder()
            .env_source(no_env)
            .api_key("k")
            .build()
            .unwrap();
        assert!(cfg.events_enabled);
        assert_eq!(cfg.events_batch_size, 1000);
        assert_eq!(cfg.events_flush_interval, Duration::from_secs(30));
        assert_eq!(cfg.events_queue_size, 10_000);
        assert_eq!(cfg.events_max_retries, 3);
        assert_eq!(cfg.events_sample_rate, 100);
        assert!(cfg.events_attach_hostname);
        assert!(cfg.events_attach_environment);
    }

    #[test]
    fn test_events_env_vars() {
        let cfg = Config::builder()
            .env_source(|k| match k {
                "HONEYBADGER_EVENTS_BATCH_SIZE" => Some("50".into()),
                "HONEYBADGER_EVENTS_FLUSH_INTERVAL" => Some("5".into()),
                "HONEYBADGER_EVENTS_ENABLED" => Some("false".into()),
                _ => None,
            })
            .api_key("k")
            .build()
            .unwrap();
        assert_eq!(cfg.events_batch_size, 50);
        assert_eq!(cfg.events_flush_interval, Duration::from_secs(5));
        assert!(!cfg.events_enabled);
    }

    #[test]
    fn test_zero_interval_and_sizes_are_rejected() {
        // A zero flush interval would turn recv_timeout into a busy loop.
        for build in [
            || Config::builder().env_source(no_env).api_key("k").events_flush_interval(Duration::ZERO).build(),
            || Config::builder().env_source(no_env).api_key("k").events_batch_size(0).build(),
            || Config::builder().env_source(no_env).api_key("k").events_queue_size(0).build(),
        ] {
            assert!(
                matches!(build().unwrap_err(), crate::Error::InvalidConfig(_)),
                "invalid events settings must be rejected at build time"
            );
        }
    }

    #[test]
    fn test_sample_rate_clamps_and_bad_numbers_error() {
        let cfg = Config::builder()
            .env_source(no_env)
            .api_key("k")
            .events_sample_rate(250)
            .build()
            .unwrap();
        assert_eq!(cfg.events_sample_rate, 100, "out-of-range rate clamps");

        let err = Config::builder()
            .env_source(|k| (k == "HONEYBADGER_EVENTS_BATCH_SIZE").then(|| "lots".to_string()))
            .api_key("k")
            .build()
            .unwrap_err();
        assert!(matches!(err, crate::Error::InvalidConfig(_)));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib config`
Expected: FAIL — no field `events_enabled`.

- [ ] **Step 3: Add the error variant**

In `src/error.rs`, add to `enum Error`:
```rust
    /// A configuration value was present but unusable — out of range, or an
    /// environment variable that could not be parsed.
    #[error("invalid Honeybadger configuration: {0}")]
    InvalidConfig(String),
```

- [ ] **Step 4: Implement the config changes**

In `src/config.rs`:

Add the hook type beside `BeforeNotifyHook`:
```rust
/// A hook run against every event before delivery; returning `false` drops it.
/// Receives the fully assembled event object and may mutate it freely.
pub type BeforeEventHook =
    dyn Fn(&mut serde_json::Map<String, serde_json::Value>) -> bool + Send + Sync;
```

Add the nine fields to `struct Config` and matching `Option<..>` fields to `ConfigBuilder` (with `before_event: Vec<Arc<BeforeEventHook>>` non-optional, like `before_notify`). Initialize all builder options to `None` and `before_event` to `Vec::new()` in `Default`.

Add the setters, each documented with its default and env var. For example:
```rust
    /// Whether the Insights events pipeline is active. Default: `true`. Env:
    /// `HONEYBADGER_EVENTS_ENABLED`. When false, [`crate::event`] is a no-op and
    /// the events worker thread is never spawned.
    pub fn events_enabled(mut self, v: bool) -> Self {
        self.events_enabled = Some(v);
        self
    }

    /// Events per batch before one is cut and sent. Default: `1000`. Env:
    /// `HONEYBADGER_EVENTS_BATCH_SIZE`.
    pub fn events_batch_size(mut self, v: usize) -> Self {
        self.events_batch_size = Some(v);
        self
    }

    /// How long a partially filled batch waits before being sent anyway.
    /// Default: 30s. Env: `HONEYBADGER_EVENTS_FLUSH_INTERVAL`, **in seconds**.
    pub fn events_flush_interval(mut self, v: Duration) -> Self {
        self.events_flush_interval = Some(v);
        self
    }

    /// Total events allowed outstanding — queued, batching, and awaiting retry
    /// combined. Default: `10_000`. Env: `HONEYBADGER_EVENTS_QUEUE_SIZE`.
    /// Beyond this the oldest retained batch is dropped first.
    pub fn events_queue_size(mut self, v: usize) -> Self {
        self.events_queue_size = Some(v);
        self
    }

    /// Retries **after** the initial attempt for a batch that failed
    /// retryably. Default: `3`, so four attempts in total. Env:
    /// `HONEYBADGER_EVENTS_MAX_RETRIES`.
    pub fn events_max_retries(mut self, v: u32) -> Self {
        self.events_max_retries = Some(v);
        self
    }

    /// Percentage of events to keep, 0–100, clamped. Default: `100`. Env:
    /// `HONEYBADGER_EVENTS_SAMPLE_RATE`. Events sharing a `request_id` share one
    /// sampling decision, so a sampled request keeps all of its events or none.
    pub fn events_sample_rate(mut self, v: u8) -> Self {
        self.events_sample_rate = Some(v);
        self
    }

    /// Adds `hostname` to every event. Default: `true`. Env:
    /// `HONEYBADGER_EVENTS_ATTACH_HOSTNAME`.
    pub fn events_attach_hostname(mut self, v: bool) -> Self {
        self.events_attach_hostname = Some(v);
        self
    }

    /// Adds `environment` to every event. Default: `true`. Env:
    /// `HONEYBADGER_EVENTS_ATTACH_ENVIRONMENT`.
    pub fn events_attach_environment(mut self, v: bool) -> Self {
        self.events_attach_environment = Some(v);
        self
    }

    /// Registers a hook run against every event just before delivery, in
    /// registration order. Returning `false` drops the event. A panicking hook
    /// is caught, logged, and treated as `true`.
    ///
    /// Hooks run *before* validation, so a hook that deletes `event_type` drops
    /// the event rather than producing a malformed one.
    pub fn before_event<F>(mut self, f: F) -> Self
    where
        F: Fn(&mut serde_json::Map<String, serde_json::Value>) -> bool + Send + Sync + 'static,
    {
        self.before_event.push(Arc::new(f));
        self
    }
```

In `build()`, add typed environment parsing above the `Config` literal:
```rust
        let parse_num = |key: &str, raw: Option<String>| -> Result<Option<u64>, Error> {
            match raw {
                None => Ok(None),
                Some(raw) => raw.parse::<u64>().map(Some).map_err(|_| {
                    Error::InvalidConfig(format!(
                        "{key} must be a non-negative integer, got {raw:?}"
                    ))
                }),
            }
        };
        let env_batch = parse_num("HONEYBADGER_EVENTS_BATCH_SIZE", ev("HONEYBADGER_EVENTS_BATCH_SIZE"))?;
        let env_interval = parse_num("HONEYBADGER_EVENTS_FLUSH_INTERVAL", ev("HONEYBADGER_EVENTS_FLUSH_INTERVAL"))?;
        let env_queue = parse_num("HONEYBADGER_EVENTS_QUEUE_SIZE", ev("HONEYBADGER_EVENTS_QUEUE_SIZE"))?;
        let env_retries = parse_num("HONEYBADGER_EVENTS_MAX_RETRIES", ev("HONEYBADGER_EVENTS_MAX_RETRIES"))?;
        let env_rate = parse_num("HONEYBADGER_EVENTS_SAMPLE_RATE", ev("HONEYBADGER_EVENTS_SAMPLE_RATE"))?;
```

Add the resolved fields to the `Config` literal:
```rust
            events_enabled: self
                .events_enabled
                .or_else(|| ev("HONEYBADGER_EVENTS_ENABLED").map(parse_bool))
                .unwrap_or(true),
            events_batch_size: self
                .events_batch_size
                .or(env_batch.map(|n| n as usize))
                .unwrap_or(1000),
            events_flush_interval: self
                .events_flush_interval
                .or(env_interval.map(Duration::from_secs))
                .unwrap_or(Duration::from_secs(30)),
            events_queue_size: self
                .events_queue_size
                .or(env_queue.map(|n| n as usize))
                .unwrap_or(10_000),
            events_max_retries: self
                .events_max_retries
                .or(env_retries.map(|n| n as u32))
                .unwrap_or(3),
            events_sample_rate: self
                .events_sample_rate
                .or(env_rate.map(|n| n.min(100) as u8))
                .unwrap_or(100)
                .min(100),
            events_attach_hostname: self
                .events_attach_hostname
                .or_else(|| ev("HONEYBADGER_EVENTS_ATTACH_HOSTNAME").map(parse_bool))
                .unwrap_or(true),
            events_attach_environment: self
                .events_attach_environment
                .or_else(|| ev("HONEYBADGER_EVENTS_ATTACH_ENVIRONMENT").map(parse_bool))
                .unwrap_or(true),
            before_event: self.before_event,
```

Then validate immediately before `Ok(config)`:
```rust
        if config.events_flush_interval.is_zero() {
            return Err(Error::InvalidConfig(
                "events_flush_interval must be greater than zero".into(),
            ));
        }
        if config.events_batch_size == 0 {
            return Err(Error::InvalidConfig(
                "events_batch_size must be at least 1".into(),
            ));
        }
        if config.events_queue_size == 0 {
            return Err(Error::InvalidConfig(
                "events_queue_size must be at least 1".into(),
            ));
        }
```

Note `build()`'s signature already returns `Result<Config, Error>`; the `?` on `parse_num` needs no change to it.

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test --lib config`
Expected: PASS.

- [ ] **Step 6: Verify and commit**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
git add src/config.rs src/error.rs
git commit -m "feat: events_* configuration with build-time validation

Zero intervals and sizes are rejected rather than degrading into a busy
loop, and unparseable numeric env values error instead of silently
falling back to the default."
```

---

### Task 5: Sampling

**Files:**
- Create: `src/event.rs` (sampler only; assembly lands in Task 6)
- Modify: `src/lib.rs` (add `mod event;`)

**Interfaces:**
- Consumes: nothing.
- Produces:
  ```rust
  pub(crate) struct Sampler;
  impl Sampler {
      pub(crate) fn new(rate: u8) -> Self;                  // per-process seed
      pub(crate) fn with_seed(rate: u8, seed: u64) -> Self;  // tests
      pub(crate) fn keep(&self, request_id: Option<&str>) -> bool;
  }
  ```

- [ ] **Step 1: Write the failing test**

Create `src/event.rs` with only this test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_and_zero_rates_short_circuit() {
        let all = Sampler::with_seed(100, 0);
        let none = Sampler::with_seed(0, 0);
        for _ in 0..10 {
            assert!(all.keep(None));
            assert!(all.keep(Some("req-1")));
            assert!(!none.keep(None));
            assert!(!none.keep(Some("req-1")));
        }
    }

    #[test]
    fn test_request_id_sampling_is_deterministic() {
        let s = Sampler::with_seed(50, 0);
        let first = s.keep(Some("req-abc"));
        for _ in 0..50 {
            assert_eq!(s.keep(Some("req-abc")), first, "same id, same fate");
        }
        // Different ids must not all land on the same side.
        let ids: Vec<bool> = (0..200).map(|i| s.keep(Some(&format!("req-{i}")))).collect();
        assert!(ids.iter().any(|k| *k) && ids.iter().any(|k| !*k));
    }

    #[test]
    fn test_counter_fallback_hits_the_rate_over_a_full_cycle() {
        let s = Sampler::with_seed(25, 0);
        let kept = (0..100).filter(|_| s.keep(None)).count();
        assert_eq!(kept, 25, "exact over a complete cycle");
    }

    #[test]
    fn test_seed_prevents_every_process_keeping_its_first_event() {
        // The bug this seed exists to prevent: an unseeded counter starts at 0,
        // and 0 % 100 < rate holds for any positive rate, so every short-lived
        // process would keep its first event regardless of the sample rate.
        let unseeded = Sampler::with_seed(1, 0);
        let seeded = Sampler::with_seed(1, 50);
        assert!(unseeded.keep(None), "counter at 0 keeps");
        assert!(!seeded.keep(None), "a different seed must not");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib event`
Expected: FAIL — `cannot find struct Sampler`.

- [ ] **Step 3: Implement**

Put this above the test module in `src/event.rs`:

```rust
//! Event assembly and sampling for the Insights pipeline.
use flate2::Crc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Sampling decision, deterministic per request where a request id exists.
///
/// Events sharing a `request_id` share one fate, so a sampled request tells a
/// coherent story rather than a randomly punctured one. Events without a
/// request id fall back to a counter, which needs no `rand` dependency and
/// gives an exact rate over a complete cycle.
pub(crate) struct Sampler {
    rate: u8,
    counter: AtomicU64,
}

impl Sampler {
    pub(crate) fn new(rate: u8) -> Self {
        Sampler::with_seed(rate, process_seed())
    }

    pub(crate) fn with_seed(rate: u8, seed: u64) -> Self {
        Sampler {
            rate: rate.min(100),
            counter: AtomicU64::new(seed),
        }
    }

    pub(crate) fn keep(&self, request_id: Option<&str>) -> bool {
        if self.rate >= 100 {
            return true;
        }
        if self.rate == 0 {
            return false;
        }
        match request_id {
            Some(id) => u64::from(crc32(id.as_bytes()) % 100) < u64::from(self.rate),
            None => self.counter.fetch_add(1, Ordering::Relaxed) % 100 < u64::from(self.rate),
        }
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = Crc::new();
    crc.update(bytes);
    crc.sum()
}

/// Distinct per process, so a fleet of short-lived processes does not all make
/// the same decision for its first event.
fn process_seed() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let mut crc = Crc::new();
    crc.update(&std::process::id().to_le_bytes());
    crc.update(&nanos.to_le_bytes());
    u64::from(crc.sum() % 100)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib event`
Expected: PASS (4 tests).

- [ ] **Step 5: Declare the module**

In `src/lib.rs`, add `mod event;` after `mod error;`.

- [ ] **Step 6: Verify and commit**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
git add src/event.rs src/lib.rs
git commit -m "feat: deterministic request-id sampling with a seeded counter fallback

CRC32 comes from flate2, so no new dependency. The counter is seeded per
process: starting at zero would make every short-lived process keep its
first event, inflating low sample rates by orders of magnitude."
```

---

### Task 6: Event assembly

**Files:**
- Modify: `src/event.rs`

**Interfaces:**
- Consumes: `Sampler` (Task 5), `Config` + `BeforeEventHook` (Task 4), `Sanitizer` (shipped), `panic_hook::suppress_reporting` (Task 2).
- Produces:
  ```rust
  pub(crate) const MAX_EVENT_BYTES: usize = 102_400;

  /// Returns the NDJSON line to enqueue, or None if the event was dropped.
  pub(crate) fn assemble(
      event_type: Option<&str>,
      payload: Value,
      scope: &Map<String, Value>,
      request_id: Option<&str>,
      config: &Config,
      sanitizer: &Sanitizer,
      sampler: &Sampler,
  ) -> Option<String>;
  ```

- [ ] **Step 1: Write the failing test**

Add to the test module in `src/event.rs`:

```rust
    use crate::sanitizer::Sanitizer;
    use serde_json::{Map, Value, json};

    fn cfg() -> crate::Config {
        crate::Config::builder()
            .env_source(|_| None)
            .api_key("k")
            .env("production")
            .hostname("web-1")
            .build()
            .unwrap()
    }

    fn assemble_default(event_type: Option<&str>, payload: Value) -> Option<Value> {
        assemble_with(event_type, payload, &Map::new(), None, &cfg())
    }

    fn assemble_with(
        event_type: Option<&str>,
        payload: Value,
        scope: &Map<String, Value>,
        request_id: Option<&str>,
        config: &crate::Config,
    ) -> Option<Value> {
        let sanitizer = Sanitizer::new(config.filter_keys.iter());
        let sampler = Sampler::with_seed(100, 0);
        assemble(
            event_type, payload, scope, request_id, config, &sanitizer, &sampler,
        )
        .map(|line| serde_json::from_str(&line).expect("a valid JSON line"))
    }

    #[test]
    fn test_golden_event() {
        let mut scope = Map::new();
        scope.insert("tenant".into(), json!("acme"));
        let out = assemble_with(
            Some("user.created"),
            json!({ "user_id": 7 }),
            &scope,
            Some("req-9"),
            &cfg(),
        )
        .unwrap();

        assert_eq!(out["event_type"], json!("user.created"));
        assert_eq!(out["user_id"], json!(7));
        assert_eq!(out["tenant"], json!("acme"));
        assert_eq!(out["request_id"], json!("req-9"));
        assert_eq!(out["hostname"], json!("web-1"));
        assert_eq!(out["environment"], json!("production"));
        assert!(
            out["ts"].as_str().unwrap().ends_with('Z'),
            "ts is ISO 8601 UTC"
        );
    }

    #[test]
    fn test_precedence_payload_beats_scope_beats_injected() {
        let mut scope = Map::new();
        scope.insert("shared".into(), json!("scope"));
        scope.insert("hostname".into(), json!("from-scope"));
        let out = assemble_with(
            Some("t"),
            json!({ "shared": "payload" }),
            &scope,
            Some("req-1"),
            &cfg(),
        )
        .unwrap();
        assert_eq!(out["shared"], json!("payload"));
        assert_eq!(out["hostname"], json!("from-scope"));
    }

    #[test]
    fn test_event_type_argument_always_wins_and_ts_is_kept() {
        let out = assemble_default(
            Some("real"),
            json!({ "event_type": "fake", "ts": "2020-01-01T00:00:00.000Z" }),
        )
        .unwrap();
        assert_eq!(out["event_type"], json!("real"));
        assert_eq!(out["ts"], json!("2020-01-01T00:00:00.000Z"));
    }

    #[test]
    fn test_non_object_payloads_are_dropped() {
        assert!(assemble_default(Some("t"), json!(42)).is_none());
        assert!(assemble_default(Some("t"), json!("a string")).is_none());
        assert!(assemble_default(Some("t"), json!([1, 2])).is_none());
        assert!(assemble_default(Some("t"), Value::Null).is_none());
    }

    #[test]
    fn test_event_value_requires_event_type_in_the_payload() {
        assert!(assemble_default(None, json!({ "a": 1 })).is_none());
        assert!(assemble_default(None, json!({ "event_type": "", "a": 1 })).is_none());
        assert!(assemble_default(None, json!({ "event_type": 42 })).is_none());
        assert!(assemble_default(None, json!({ "event_type": "ok" })).is_some());
    }

    #[test]
    fn test_hooks_mutate_drop_and_are_validated_after() {
        let config = crate::Config::builder()
            .env_source(|_| None)
            .api_key("k")
            .before_event(|e| {
                e.insert("hooked".into(), json!(true));
                true
            })
            .before_event(|e| e.get("event_type") != Some(&json!("halt")))
            .before_event(|e| {
                if e.get("event_type") == Some(&json!("sabotage")) {
                    e.remove("event_type");
                }
                true
            })
            .build()
            .unwrap();

        let kept = assemble_with(Some("keep"), json!({}), &Map::new(), None, &config).unwrap();
        assert_eq!(kept["hooked"], json!(true));

        assert!(
            assemble_with(Some("halt"), json!({}), &Map::new(), None, &config).is_none(),
            "a hook returning false drops the event"
        );
        assert!(
            assemble_with(Some("sabotage"), json!({}), &Map::new(), None, &config).is_none(),
            "validation runs after hooks: a deleted event_type drops the event"
        );
    }

    #[test]
    fn test_panicking_hook_is_caught_and_treated_as_pass() {
        let config = crate::Config::builder()
            .env_source(|_| None)
            .api_key("k")
            .before_event(|_| panic!("bad hook"))
            .build()
            .unwrap();
        assert!(assemble_with(Some("t"), json!({}), &Map::new(), None, &config).is_some());
    }

    #[test]
    fn test_sanitizing_applies_but_filter_keys_do_not() {
        let config = crate::Config::builder()
            .env_source(|_| None)
            .api_key("k")
            .filter_keys(["password"])
            .build()
            .unwrap();
        let out = assemble_with(
            Some("t"),
            json!({ "password": "hunter2", "long": "y".repeat(70_000) }),
            &Map::new(),
            None,
            &config,
        )
        .unwrap();
        assert_eq!(
            out["password"], json!("hunter2"),
            "events are not key-redacted; every field was written by hand"
        );
        assert!(
            out["long"].as_str().unwrap().ends_with("[TRUNCATED]"),
            "structural sanitizing still applies"
        );
    }

    #[test]
    fn test_oversized_events_are_dropped() {
        // The sanitizer truncates single strings at 64 KiB, so exceed the
        // 100 kB event limit with many keys instead of one huge value.
        let mut payload = Map::new();
        for i in 0..40 {
            payload.insert(format!("k{i}"), json!("y".repeat(60_000)));
        }
        assert!(assemble_default(Some("big"), Value::Object(payload)).is_none());
    }

    #[test]
    fn test_sampling_drops_and_non_string_request_id_is_kept_but_unsampled() {
        let config = cfg();
        let sanitizer = Sanitizer::new(config.filter_keys.iter());
        let none = Sampler::with_seed(0, 0);
        assert!(
            assemble(Some("t"), json!({}), &Map::new(), None, &config, &sanitizer, &none).is_none()
        );

        // A non-string request_id survives into the payload but must not be
        // hashed for sampling; at rate 0 nothing is kept either way, so use a
        // full-rate sampler and simply assert the field round-trips.
        let all = Sampler::with_seed(100, 0);
        let line = assemble(
            Some("t"),
            json!({ "request_id": 12345 }),
            &Map::new(),
            None,
            &config,
            &sanitizer,
            &all,
        )
        .unwrap();
        let out: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(out["request_id"], json!(12345));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib event`
Expected: FAIL — `cannot find function assemble`.

- [ ] **Step 3: Implement**

Add to `src/event.rs` (imports at the top, function below `Sampler`):

```rust
use crate::breadcrumbs::now_iso8601_ms;
use crate::config::Config;
use crate::sanitizer::Sanitizer;
use serde_json::{Map, Value};
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Per-event ceiling documented by the Insights API (100 kB).
pub(crate) const MAX_EVENT_BYTES: usize = 102_400;

/// Builds the NDJSON line for one event, or `None` if it was dropped.
///
/// `event_type` is `Some` for `event()` — where the argument always wins — and
/// `None` for `event_value()`, where the caller owns the field.
pub(crate) fn assemble(
    event_type: Option<&str>,
    payload: Value,
    scope: &Map<String, Value>,
    request_id: Option<&str>,
    config: &Config,
    sanitizer: &Sanitizer,
    sampler: &Sampler,
) -> Option<String> {
    // 2. Shape. No user code runs here: the payload is already a Value.
    let Value::Object(fields) = payload else {
        log::warn!("honeybadger: event payload must be a JSON object; dropped");
        return None;
    };

    // 3. Merge, caller's payload winning over event context.
    let mut event = scope.clone();
    event.extend(fields);

    // 4-7. Injected fields. event_type is unconditional; the rest fill gaps.
    if let Some(t) = event_type {
        event.insert("event_type".into(), Value::String(t.to_owned()));
    }
    event
        .entry("ts")
        .or_insert_with(|| Value::String(now_iso8601_ms()));
    if let Some(id) = request_id {
        event
            .entry("request_id")
            .or_insert_with(|| Value::String(id.to_owned()));
    }
    if config.events_attach_hostname && !config.hostname.is_empty() {
        event
            .entry("hostname")
            .or_insert_with(|| Value::String(config.hostname.clone()));
    }
    if config.events_attach_environment
        && let Some(env) = &config.env
    {
        event
            .entry("environment")
            .or_insert_with(|| Value::String(env.clone()));
    }

    // 8. Hooks. Panics are caught and treated as pass; the guard stops our own
    //    panic hook from reporting a panic we are containing.
    for hook in &config.before_event {
        let hook = hook.clone();
        let keep = {
            let _suppressed = crate::panic_hook::suppress_reporting();
            catch_unwind(AssertUnwindSafe(|| hook(&mut event))).unwrap_or_else(|_| {
                log::warn!("honeybadger: before_event hook panicked; continuing");
                true
            })
        };
        if !keep {
            return None;
        }
    }

    // 9. Validate after hooks. An invalid event provokes a 422, and a 422
    //    discards the whole batch — one bad event must not destroy 999 good ones.
    match event.get("event_type").and_then(Value::as_str) {
        Some(t) if !t.is_empty() => {}
        _ => {
            log::warn!("honeybadger: event has no non-empty string event_type; dropped");
            return None;
        }
    }

    // 10. Sampling. Only a string request_id can be hashed.
    let sampling_id = event.get("request_id").and_then(Value::as_str);
    if !sampler.keep(sampling_id) {
        return None;
    }

    // 11. Structural sanitizing, last, so hook-introduced data is covered.
    //     Deliberately no filter_keys redaction; see the spec's decision 5.
    let mut value = Value::Object(event);
    sanitizer.sanitize(&mut value);

    // 12. Render and enforce the per-event ceiling.
    let line = match serde_json::to_string(&value) {
        Ok(line) => line,
        Err(e) => {
            log::warn!("honeybadger: failed to serialize event: {e}");
            return None;
        }
    };
    if line.len() > MAX_EVENT_BYTES {
        let event_type = value
            .get("event_type")
            .and_then(Value::as_str)
            .unwrap_or("<unknown>");
        log::warn!(
            "honeybadger: event {event_type} is {} bytes, over the {MAX_EVENT_BYTES}-byte limit; dropped",
            line.len()
        );
        return None;
    }
    Some(line)
}
```

`now_iso8601_ms` is currently `pub(crate)` in `src/breadcrumbs.rs`; no visibility change is needed.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib event`
Expected: PASS (all assembly and sampling tests).

- [ ] **Step 5: Verify and commit**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
git add src/event.rs
git commit -m "feat: event assembly with post-hook validation

Validation runs after hooks because a hook can delete event_type, and an
invalid event provokes a 422 that discards the entire batch."
```

---

### Task 7: Events worker — batching and barriers

Delivery is stubbed in this task so batching can be tested in isolation; Task 8 replaces the stub with the real failure matrix.

**Files:**
- Create: `src/events_worker.rs`
- Modify: `src/lib.rs` (add `mod events_worker;`)

**Interfaces:**
- Consumes: `DropCounter` (Task 1), `Transport`/`TransportRequest::events`/`compress` (Task 3).
- Produces:
  ```rust
  pub(crate) struct EventsWorkerHandle {
      pub(crate) pid: u32,
  }
  impl EventsWorkerHandle {
      pub(crate) fn try_enqueue(&self, line: String) -> bool;
      pub(crate) fn flush_begin(&self) -> Option<Receiver<bool>>;
      pub(crate) fn shutdown(&self, timeout: Duration);
  }
  pub(crate) struct EventsConfig {
      pub(crate) batch_size: usize,
      pub(crate) flush_interval: Duration,
      pub(crate) queue_size: usize,
      pub(crate) max_retries: u32,
      pub(crate) suspend_interval: Duration,
  }
  pub(crate) fn spawn(
      transport: Arc<dyn Transport>,
      cfg: EventsConfig,
      drops: Arc<DropCounter>,
  ) -> std::io::Result<EventsWorkerHandle>;
  pub(crate) const BATCH_BYTE_LIMIT: usize = 4_500_000;
  ```

- [ ] **Step 1: Write the failing test**

Create `src/events_worker.rs` with only this test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::drops::DropCounter;
    use crate::transport::TestTransport;
    use std::io::Read;

    fn cfg() -> EventsConfig {
        EventsConfig {
            batch_size: 3,
            flush_interval: Duration::from_millis(200),
            queue_size: 100,
            max_retries: 3,
            suspend_interval: Duration::from_secs(30),
        }
    }

    fn lines(transport: &TestTransport) -> Vec<Vec<String>> {
        transport
            .requests()
            .iter()
            .map(|r| {
                let mut s = String::new();
                flate2::read::ZlibDecoder::new(&r.body[..])
                    .read_to_string(&mut s)
                    .unwrap();
                s.lines().map(str::to_owned).collect()
            })
            .collect()
    }

    fn worker(transport: Arc<TestTransport>, cfg: EventsConfig) -> EventsWorkerHandle {
        spawn(transport, cfg, Arc::new(DropCounter::new("events"))).unwrap()
    }

    #[test]
    fn test_count_trigger_cuts_a_batch() {
        let transport = Arc::new(TestTransport::new());
        let w = worker(transport.clone(), cfg());
        for i in 0..3 {
            assert!(w.try_enqueue(format!("{{\"n\":{i}}}")));
        }
        // The third event fills the batch, so delivery happens without a flush.
        std::thread::sleep(Duration::from_millis(300));
        let batches = lines(&transport);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 3, "one batch of exactly batch_size");
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_time_trigger_starts_with_the_batch_not_the_last_send() {
        let transport = Arc::new(TestTransport::new());
        let w = worker(transport.clone(), cfg());
        // Idle well past the interval, then send a single event. If the
        // deadline ran "since the last send" it would already have expired and
        // this would ship immediately as a batch of one.
        std::thread::sleep(Duration::from_millis(500));
        assert!(w.try_enqueue("{\"n\":1}".into()));
        std::thread::sleep(Duration::from_millis(80));
        assert!(
            transport.requests().is_empty(),
            "a fresh batch must get a full interval, not a stale deadline"
        );
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(lines(&transport).len(), 1);
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_byte_trigger_cuts_before_the_count_trigger() {
        let transport = Arc::new(TestTransport::new());
        let w = worker(
            transport.clone(),
            EventsConfig {
                batch_size: 100_000,
                ..cfg()
            },
        );
        // Each line is ~50 KB; enough of them exceed BATCH_BYTE_LIMIT long
        // before the count trigger could fire.
        let big = format!("{{\"blob\":\"{}\"}}", "y".repeat(50_000));
        for _ in 0..100 {
            w.try_enqueue(big.clone());
        }
        std::thread::sleep(Duration::from_millis(500));
        assert!(
            !transport.requests().is_empty(),
            "the byte ceiling must cut a batch on its own"
        );
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_flush_is_a_barrier_for_a_partial_batch() {
        let transport = Arc::new(TestTransport::new());
        let w = worker(
            transport.clone(),
            EventsConfig {
                flush_interval: Duration::from_secs(3600),
                ..cfg()
            },
        );
        w.try_enqueue("{\"n\":1}".into());
        let rx = w.flush_begin().unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)), Ok(true));
        assert_eq!(lines(&transport), vec![vec!["{\"n\":1}".to_string()]]);
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_shutdown_force_cuts_a_partial_batch() {
        let transport = Arc::new(TestTransport::new());
        let w = worker(
            transport.clone(),
            EventsConfig {
                batch_size: 1000,
                flush_interval: Duration::from_secs(3600),
                ..cfg()
            },
        );
        for i in 0..5 {
            w.try_enqueue(format!("{{\"n\":{i}}}"));
        }
        w.shutdown(Duration::from_secs(5));
        assert_eq!(
            lines(&transport).concat().len(),
            5,
            "shutdown must not silently discard an uncut batch"
        );
    }

    #[test]
    fn test_enqueue_after_shutdown_returns_false() {
        let transport = Arc::new(TestTransport::new());
        let w = worker(transport, cfg());
        w.shutdown(Duration::from_secs(5));
        assert!(!w.try_enqueue("{\"n\":1}".into()));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib events_worker`
Expected: FAIL — `cannot find function spawn`.

- [ ] **Step 3: Implement**

Put this above the test module in `src/events_worker.rs`:

```rust
//! The events delivery worker: dedicated OS thread, bounded event channel plus
//! an unbounded control channel, batching on count, bytes, or time.
use crate::drops::DropCounter;
use crate::transport::{Transport, TransportRequest, compress};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TrySendError, bounded, unbounded};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Cut a batch at this many uncompressed bytes, leaving margin under the API's
/// documented 5 MB request ceiling. Without it, `batch_size` events at the
/// 100 kB per-event limit would be a 100 MB request.
pub(crate) const BATCH_BYTE_LIMIT: usize = 4_500_000;

/// How long the worker parks when it has nothing pending at all.
const IDLE_PARK: Duration = Duration::from_secs(3600);

pub(crate) struct EventsConfig {
    pub(crate) batch_size: usize,
    pub(crate) flush_interval: Duration,
    pub(crate) queue_size: usize,
    pub(crate) max_retries: u32,
    pub(crate) suspend_interval: Duration,
}

pub(crate) enum Control {
    Flush(Sender<bool>),
    Shutdown(Sender<()>),
}

pub(crate) struct EventsWorkerHandle {
    events: Sender<String>,
    control: Sender<Control>,
    join: Mutex<Option<JoinHandle<()>>>,
    /// Recorded at spawn so a forked child can detect it has no worker thread.
    pub(crate) pid: u32,
}

pub(crate) fn spawn(
    transport: Arc<dyn Transport>,
    cfg: EventsConfig,
    drops: Arc<DropCounter>,
) -> std::io::Result<EventsWorkerHandle> {
    let (event_tx, event_rx) = bounded(cfg.queue_size);
    let (control_tx, control_rx) = unbounded();
    let join = std::thread::Builder::new()
        .name("honeybadger-events".into())
        .spawn(move || {
            EventsWorker {
                transport,
                events: event_rx,
                control: control_rx,
                drops,
                cfg,
                current: Vec::new(),
                current_bytes: 0,
                deadline: None,
                retry_at: None,
                retries: VecDeque::new(),
                outstanding: 0,
                throttle: 0,
                stopped: false,
            }
            .run()
        })?;
    Ok(EventsWorkerHandle {
        events: event_tx,
        control: control_tx,
        join: Mutex::new(Some(join)),
        pid: std::process::id(),
    })
}

impl EventsWorkerHandle {
    /// Returns false if the line was dropped (queue full or worker gone).
    pub(crate) fn try_enqueue(&self, line: String) -> bool {
        match self.events.try_send(line) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => false,
        }
    }

    /// Starts a flush and returns the acknowledgement channel, so a caller can
    /// begin flushes on several pipelines before waiting on any of them.
    pub(crate) fn flush_begin(&self) -> Option<Receiver<bool>> {
        let (ack_tx, ack_rx) = bounded(1);
        self.control.send(Control::Flush(ack_tx)).ok()?;
        Some(ack_rx)
    }

    pub(crate) fn shutdown(&self, timeout: Duration) {
        let (ack_tx, ack_rx) = bounded(1);
        if self.control.send(Control::Shutdown(ack_tx)).is_err() {
            return;
        }
        if ack_rx.recv_timeout(timeout).is_ok() {
            if let Some(handle) = self.join.lock().unwrap_or_else(|e| e.into_inner()).take() {
                let _ = handle.join();
            }
        } else {
            log::warn!("honeybadger: events worker did not stop within {timeout:?}; detaching");
            self.join.lock().unwrap_or_else(|e| e.into_inner()).take();
        }
    }
}

struct Batch {
    body: Vec<u8>,
    attempts: u32,
    events: usize,
}

struct EventsWorker {
    transport: Arc<dyn Transport>,
    events: Receiver<String>,
    control: Receiver<Control>,
    drops: Arc<DropCounter>,
    cfg: EventsConfig,
    current: Vec<String>,
    current_bytes: usize,
    deadline: Option<Instant>,
    retry_at: Option<Instant>,
    retries: VecDeque<Batch>,
    /// Events held anywhere the worker owns: current batch plus retry queue.
    outstanding: usize,
    throttle: u32,
    stopped: bool,
}

impl EventsWorker {
    fn run(mut self) {
        while !self.stopped {
            let wake = self.next_wake();
            crossbeam_channel::select! {
                recv(self.control) -> msg => match msg {
                    Ok(control) => self.handle_control(control),
                    Err(_) => return,
                },
                recv(self.events) -> msg => match msg {
                    Ok(line) => self.accept(line),
                    Err(_) => return,
                },
                default(wake) => {
                    if self.deadline.is_some_and(|d| d <= Instant::now()) {
                        self.cut();
                    }
                    self.send_pending();
                }
            }
        }
    }

    fn next_wake(&self) -> Duration {
        let now = Instant::now();
        let mut wake = IDLE_PARK;
        if let Some(deadline) = self.deadline {
            wake = wake.min(deadline.saturating_duration_since(now));
        }
        if let Some(retry_at) = self.retry_at {
            wake = wake.min(retry_at.saturating_duration_since(now));
        }
        wake
    }

    fn accept(&mut self, line: String) {
        // Shed the stalest data first so a stalled head batch cannot pin the
        // pipeline; only refuse the new event once nothing older is left.
        while self.outstanding >= self.cfg.queue_size {
            match self.retries.pop_front() {
                Some(batch) => {
                    self.outstanding -= batch.events;
                    self.drops.record_many(batch.events as u64);
                    log::warn!(
                        "honeybadger: dropped a retained batch of {} events to stay within events_queue_size",
                        batch.events
                    );
                }
                None => {
                    self.drops.record();
                    return;
                }
            }
        }

        if self.current.is_empty() {
            self.deadline = Some(Instant::now() + self.cfg.flush_interval);
        }
        self.current_bytes += line.len() + 1; // the joining newline
        self.current.push(line);
        self.outstanding += 1;

        if self.current.len() >= self.cfg.batch_size || self.current_bytes >= BATCH_BYTE_LIMIT {
            self.cut();
            self.send_pending();
        }
    }

    /// Moves the accumulating batch into the retry queue as compressed bytes.
    fn cut(&mut self) {
        if self.current.is_empty() {
            return;
        }
        let lines = std::mem::take(&mut self.current);
        let events = lines.len();
        self.current_bytes = 0;
        self.deadline = None;
        self.retries.push_back(Batch {
            body: compress(lines.join("\n").as_bytes()),
            attempts: 0,
            events,
        });
    }

    /// Replaced with the real failure matrix in the next task.
    fn send_pending(&mut self) {
        while !self.retries.is_empty() {
            let outcome = {
                let body = &self.retries.front().expect("non-empty").body;
                let req = TransportRequest::events(body);
                self.transport.deliver(&req)
            };
            let batch = self.retries.pop_front().expect("non-empty");
            self.outstanding -= batch.events;
            match outcome {
                Ok(status) if (200..300).contains(&status) => {
                    self.drops.report();
                }
                _ => self.drops.record_many(batch.events as u64),
            }
        }
        self.retry_at = None;
    }

    fn handle_control(&mut self, control: Control) {
        match control {
            Control::Flush(ack) => {
                self.drain_channel();
                self.cut();
                self.send_pending();
                let _ = ack.send(true);
            }
            Control::Shutdown(ack) => {
                self.drain_channel();
                self.cut(); // force-cut: a partial batch must not be lost
                self.send_pending();
                self.drops.report_final();
                let _ = ack.send(());
                self.stopped = true;
            }
        }
    }

    /// Pulls everything already queued into the current batch, so a flush is a
    /// barrier over every event enqueued before the flush call.
    fn drain_channel(&mut self) {
        while let Ok(line) = self.events.try_recv() {
            self.accept(line);
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib events_worker`
Expected: PASS (6 tests).

- [ ] **Step 5: Declare the module**

In `src/lib.rs`, add `mod events_worker;` after `mod event;`.

- [ ] **Step 6: Verify and commit**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
git add src/events_worker.rs src/lib.rs
git commit -m "feat: events worker batching on count, bytes, and time

The flush deadline belongs to the current batch rather than the last
send, so an idle worker does not ship a batch of one. Shutdown
force-cuts a partial batch instead of discarding it."
```

---

### Task 8: Events worker — failure matrix and retry

**Files:**
- Modify: `src/events_worker.rs` (replace the `send_pending` stub, add suspension)

**Interfaces:**
- Consumes: `crate::worker::throttle_interval` (shipped; make it `pub(crate)` if it is not already — it is).
- Produces: no new public names; `send_pending` gains the real matrix.

- [ ] **Step 1: Write the failing test**

Add to the test module in `src/events_worker.rs`:

```rust
    fn attempts_for(status: u16) -> usize {
        let transport = Arc::new(TestTransport::new());
        for _ in 0..10 {
            transport.respond_with(status);
        }
        let w = worker(
            transport.clone(),
            EventsConfig {
                batch_size: 1,
                flush_interval: Duration::from_millis(50),
                max_retries: 2, // 3 attempts in total
                ..cfg()
            },
        );
        w.try_enqueue("{\"n\":1}".into());
        std::thread::sleep(Duration::from_millis(600));
        let n = transport.requests().len();
        w.shutdown(Duration::from_secs(5));
        n
    }

    #[test]
    fn test_retryable_failures_are_retried_to_the_limit() {
        assert_eq!(attempts_for(500), 3, "5xx retries up to max_retries + 1");
    }

    #[test]
    fn test_poison_pill_statuses_are_dropped_on_the_first_response() {
        // The Go client burns its whole retry budget on batches the server has
        // already called unprocessable. We must not.
        assert_eq!(attempts_for(413), 1, "413 is never retried");
        assert_eq!(attempts_for(422), 1, "422 is never retried");
        assert_eq!(attempts_for(400), 1, "other 4xx are never retried");
        assert_eq!(attempts_for(301), 1, "unexpected statuses drop, not hang");
    }

    #[test]
    fn test_429_burns_no_retry_budget_but_503_does() {
        // A rate limit is an instruction to wait, not a failure, so it must not
        // age a batch out. A 503 carries no such instruction.
        assert!(
            attempts_for(429) > 3,
            "429 keeps retrying past max_retries + 1"
        );
        assert_eq!(attempts_for(503), 3, "503 burns the retry budget");
    }

    #[test]
    fn test_suspend_on_402_drops_everything_and_still_serves_control() {
        let transport = Arc::new(TestTransport::new());
        transport.respond_with(402);
        let drops = Arc::new(DropCounter::new("events"));
        let w = spawn(
            transport.clone(),
            EventsConfig {
                batch_size: 1,
                suspend_interval: Duration::from_secs(30),
                ..cfg()
            },
            drops.clone(),
        )
        .unwrap();
        w.try_enqueue("{\"n\":1}".into());
        std::thread::sleep(Duration::from_millis(300));
        w.try_enqueue("{\"n\":2}".into());

        let rx = w.flush_begin().unwrap();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(2)),
            Ok(true),
            "flush must acknowledge while suspended"
        );
        assert_eq!(
            transport.requests().len(),
            1,
            "nothing is delivered while suspended"
        );
        // Must return promptly despite the 30s suspension.
        w.shutdown(Duration::from_secs(2));
    }

    #[test]
    fn test_budget_drops_the_oldest_retained_batch_first() {
        // A permanently throttled endpoint retains its head batch forever; the
        // budget must still let newer events in by shedding the stalest.
        let transport = Arc::new(TestTransport::new());
        for _ in 0..50 {
            transport.respond_with(429);
        }
        let drops = Arc::new(DropCounter::new("events"));
        let w = spawn(
            transport.clone(),
            EventsConfig {
                batch_size: 2,
                queue_size: 4,
                flush_interval: Duration::from_millis(50),
                ..cfg()
            },
            drops.clone(),
        )
        .unwrap();
        for i in 0..40 {
            w.try_enqueue(format!("{{\"n\":{i}}}"));
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            drops.pending() > 0,
            "the budget must shed data rather than grow without bound"
        );
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_panicking_transport_does_not_kill_the_worker() {
        use crate::transport::{Transport, TransportError, TransportRequest};
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct PanicOnFirst {
            calls: AtomicUsize,
        }
        impl Transport for PanicOnFirst {
            fn deliver(&self, _req: &TransportRequest) -> Result<u16, TransportError> {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("transport blew up");
                }
                Ok(201)
            }
        }

        let transport = Arc::new(PanicOnFirst {
            calls: AtomicUsize::new(0),
        });
        let w = spawn(
            transport.clone(),
            EventsConfig {
                batch_size: 1,
                ..cfg()
            },
            Arc::new(DropCounter::new("events")),
        )
        .unwrap();
        w.try_enqueue("{\"n\":1}".into());
        std::thread::sleep(Duration::from_millis(200));
        assert!(w.try_enqueue("{\"n\":2}".into()));
        let rx = w.flush_begin().unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)), Ok(true));
        assert!(transport.calls.load(Ordering::SeqCst) >= 2);
        w.shutdown(Duration::from_secs(5));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib events_worker`
Expected: FAIL — the stub drops every non-2xx batch, so the retry and throttle tests fail.

- [ ] **Step 3: Implement the matrix**

In `src/events_worker.rs`, add the outcome type above `struct EventsWorker`:

```rust
/// What a delivery attempt means for the batch at the head of the queue.
enum Outcome {
    Success,
    /// Retry later, spending one attempt.
    Retry,
    /// Unrecoverable for this batch: never retry.
    Drop,
    /// Back off. `burn` distinguishes 503 (a server error) from 429 (an
    /// instruction to wait, which must not age a batch out).
    Throttle { burn: bool },
    Suspend,
}

fn classify(status: u16) -> Outcome {
    match status {
        200..=299 => Outcome::Success,
        402 | 403 => Outcome::Suspend,
        429 => Outcome::Throttle { burn: false },
        503 => Outcome::Throttle { burn: true },
        400..=499 => Outcome::Drop,
        500..=599 => Outcome::Retry,
        _ => Outcome::Drop, // 1xx, 3xx, and anything out of range
    }
}
```

Add `use std::panic::{AssertUnwindSafe, catch_unwind};` to the imports, then replace `send_pending` and add the helpers:

```rust
    fn send_pending(&mut self) {
        while !self.retries.is_empty() {
            let outcome = {
                let body = &self.retries.front().expect("non-empty").body;
                let req = TransportRequest::events(body);
                // `Transport` is user-implementable: a panicking impl must not
                // take the worker down.
                match catch_unwind(AssertUnwindSafe(|| self.transport.deliver(&req))) {
                    Ok(Ok(status)) => classify(status),
                    Ok(Err(e)) => {
                        log::warn!("honeybadger: events delivery failed: {e}");
                        Outcome::Retry
                    }
                    Err(_) => {
                        log::warn!("honeybadger: events transport panicked");
                        Outcome::Retry
                    }
                }
            };

            match outcome {
                Outcome::Success => {
                    self.pop_head();
                    self.throttle = self.throttle.saturating_sub(1);
                    self.drops.report();
                }
                Outcome::Drop => {
                    let batch = self.pop_head();
                    log::warn!(
                        "honeybadger: dropped a batch of {} events the API rejected",
                        batch.events
                    );
                    self.drops.record_many(batch.events as u64);
                }
                Outcome::Retry => {
                    if self.charge_attempt() {
                        continue; // aged out; try the next batch
                    }
                    self.schedule_retry();
                    return;
                }
                Outcome::Throttle { burn } => {
                    self.throttle = self.throttle.saturating_add(1);
                    log::debug!("honeybadger: events throttled (n={})", self.throttle);
                    if burn && self.charge_attempt() {
                        continue;
                    }
                    self.schedule_retry();
                    return;
                }
                Outcome::Suspend => {
                    self.suspend();
                    return;
                }
            }
        }
        self.retry_at = None;
    }

    fn pop_head(&mut self) -> Batch {
        let batch = self.retries.pop_front().expect("non-empty");
        self.outstanding -= batch.events;
        batch
    }

    /// Spends one attempt on the head batch. Returns true if it aged out and
    /// was dropped.
    fn charge_attempt(&mut self) -> bool {
        let head = self.retries.front_mut().expect("non-empty");
        head.attempts += 1;
        if head.attempts <= self.cfg.max_retries {
            return false;
        }
        let batch = self.pop_head();
        log::warn!(
            "honeybadger: dropped a batch of {} events after {} attempts",
            batch.events,
            batch.attempts
        );
        self.drops.record_many(batch.events as u64);
        true
    }

    fn schedule_retry(&mut self) {
        // Reuse the shipped notices curve, floored at one flush interval so a
        // batch retry never becomes a hot loop.
        let backoff = crate::worker::throttle_interval(self.throttle).max(self.cfg.flush_interval);
        self.retry_at = Some(Instant::now() + backoff);
    }

    /// 402/403: nothing will change until a human acts, so discard everything
    /// outstanding and wait out the interval, still servicing control.
    fn suspend(&mut self) {
        let dropped = self.outstanding;
        self.retries.clear();
        self.current.clear();
        self.current_bytes = 0;
        self.deadline = None;
        self.retry_at = None;
        self.outstanding = 0;
        if dropped > 0 {
            self.drops.record_many(dropped as u64);
        }
        log::warn!(
            "honeybadger: events delivery suspended for {:?}",
            self.cfg.suspend_interval
        );

        let until = Instant::now() + self.cfg.suspend_interval;
        loop {
            let remaining = until.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                self.throttle = 0;
                self.discard_queued();
                return;
            }
            match self.control.recv_timeout(remaining) {
                Ok(Control::Flush(ack)) => {
                    self.discard_queued();
                    let _ = ack.send(true); // nothing is pending by definition
                }
                Ok(Control::Shutdown(ack)) => {
                    self.discard_queued();
                    self.drops.report_final();
                    let _ = ack.send(());
                    self.stopped = true;
                    return;
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    self.stopped = true;
                    return;
                }
            }
        }
    }

    fn discard_queued(&mut self) {
        let mut n = 0u64;
        while self.events.try_recv().is_ok() {
            n += 1;
        }
        if n > 0 {
            self.drops.record_many(n);
        }
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib events_worker`
Expected: PASS (all batching and failure-matrix tests).

- [ ] **Step 5: Verify and commit**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
git add src/events_worker.rs
git commit -m "feat: targeted retry and the events failure matrix

Retries are spent only where retrying can help: transport errors and
5xx. 4xx drops on the first response instead of costing four round
trips, 429 backs off without aging a batch out, and 503 burns budget so
an unhealthy endpoint cannot pin the queue head forever."
```

---

### Task 9: Client — events API and lifecycle

**Files:**
- Modify: `src/client.rs`
- Modify: `src/worker.rs` (add `flush_begin`)

**Interfaces:**
- Consumes: everything from Tasks 1–8.
- Produces:
  ```rust
  impl Client {
      pub fn event(&self, event_type: &str, payload: Value);
      pub fn event_value(&self, payload: Value);
      pub fn event_context<I, K>(&self, entries: I) where I: IntoIterator<Item = (K, Value)>, K: Into<String>;
      pub fn clear_event_context(&self);
      pub fn request_id(&self, id: impl Into<String>);
      pub fn clear_request_id(&self);
      pub(crate) fn current_request_id(&self) -> Option<String>;
      // flush(timeout) now covers both pipelines; shutdown stops both.
  }
  ```

- [ ] **Step 1: Add `flush_begin` to the notices worker**

In `src/worker.rs`, add to `impl WorkerHandle` and rewrite `flush` in terms of it:

```rust
    /// Starts a flush and returns its acknowledgement channel, so a caller can
    /// begin flushes on both pipelines before waiting on either.
    pub(crate) fn flush_begin(&self) -> Option<Receiver<bool>> {
        let (ack_tx, ack_rx) = bounded(1);
        self.control.send(Control::Flush(ack_tx)).ok()?;
        Some(ack_rx)
    }

    pub(crate) fn flush(&self, timeout: Duration) -> bool {
        match self.flush_begin() {
            Some(rx) => rx.recv_timeout(timeout).unwrap_or(false),
            None => false,
        }
    }
```

- [ ] **Step 2: Write the failing test**

Add to the test module in `src/client.rs`:

```rust
    fn events_delivered(transport: &TestTransport) -> Vec<serde_json::Value> {
        transport
            .requests()
            .iter()
            .filter(|r| r.kind == crate::RequestKind::Events)
            .flat_map(|r| {
                let mut s = String::new();
                flate2::read::ZlibDecoder::new(&r.body[..])
                    .read_to_string(&mut s)
                    .unwrap();
                s.lines()
                    .map(|l| serde_json::from_str(l).unwrap())
                    .collect::<Vec<serde_json::Value>>()
            })
            .collect()
    }

    #[test]
    fn test_event_delivers_with_context_and_request_id() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.event_context([("tenant", json!("acme"))]);
        client.request_id("req-9");
        client.event("user.created", json!({ "user_id": 7 }));
        assert!(client.flush(Duration::from_secs(5)));

        let events = events_delivered(&transport);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["event_type"], json!("user.created"));
        assert_eq!(events[0]["user_id"], json!(7));
        assert_eq!(events[0]["tenant"], json!("acme"));
        assert_eq!(events[0]["request_id"], json!("req-9"));
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_event_value_and_batching_share_one_request() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        for i in 0..5 {
            client.event_value(json!({ "event_type": "tick", "n": i }));
        }
        assert!(client.flush(Duration::from_secs(5)));
        assert_eq!(events_delivered(&transport).len(), 5);
        assert_eq!(
            transport
                .requests()
                .iter()
                .filter(|r| r.kind == crate::RequestKind::Events)
                .count(),
            1,
            "a flush cuts one batch, not five requests"
        );
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_flush_covers_both_pipelines_in_one_timeout() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.notify_notice(crate::Notice::message("X", "y"));
        client.event("t", json!({}));
        assert!(client.flush(Duration::from_secs(5)));
        assert_eq!(delivered(&transport).len(), 1, "the notice went out");
        assert_eq!(events_delivered(&transport).len(), 1, "the event went out");
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_flush_succeeds_when_the_events_worker_was_never_spawned() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.notify_notice(crate::Notice::message("X", "y"));
        assert!(
            client.flush(Duration::from_secs(5)),
            "an unspawned events pipeline must flush as a no-op success"
        );
        assert!(
            transport
                .requests()
                .iter()
                .all(|r| r.kind == crate::RequestKind::Notices),
            "flushing must never spawn the worker it is flushing"
        );
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_event_after_shutdown_never_spawns_a_worker() {
        // The race Once cannot close: one clone shuts down, another then calls
        // event() and would otherwise spawn a worker nobody will ever stop.
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        let clone = client.clone();
        client.shutdown(Duration::from_secs(5));
        clone.event("t", json!({}));
        assert!(
            events_delivered(&transport).is_empty(),
            "no events pipeline may be created after shutdown"
        );
    }

    #[test]
    fn test_events_disabled_never_spawns() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client_with(transport.clone(), |b| b.events_enabled(false));
        client.event("t", json!({}));
        assert!(client.flush(Duration::from_secs(5)));
        assert!(events_delivered(&transport).is_empty());
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_concurrent_events_during_a_flush_neither_deadlock_nor_lose_acks() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        let mut threads = Vec::new();
        for t in 0..4 {
            let client = client.clone();
            threads.push(std::thread::spawn(move || {
                for i in 0..50 {
                    client.event("load", json!({ "t": t, "i": i }));
                }
            }));
        }
        // Flush repeatedly while producers are still running. Each must return
        // within its own timeout rather than blocking behind the producers.
        for _ in 0..5 {
            assert!(
                client.flush(Duration::from_secs(10)),
                "a flush must acknowledge even under concurrent production"
            );
        }
        for t in threads {
            t.join().unwrap();
        }
        assert!(client.flush(Duration::from_secs(10)));
        assert_eq!(
            events_delivered(&transport).len(),
            200,
            "no event may be lost when flushes interleave with producers"
        );
        client.shutdown(Duration::from_secs(10));
    }

    #[test]
    fn test_clear_context_clears_the_whole_scope() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.context([("a", json!(1))]);
        client.event_context([("b", json!(2))]);
        client.request_id("req-1");
        client.clear_context();

        client.event("t", json!({}));
        assert!(client.flush(Duration::from_secs(5)));
        let events = events_delivered(&transport);
        assert_eq!(events[0].get("b"), None, "event context cleared");
        assert_eq!(events[0].get("request_id"), None, "request id cleared");
        client.shutdown(Duration::from_secs(5));
    }
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test --lib client`
Expected: FAIL — no method `event`.

- [ ] **Step 4: Implement**

In `src/client.rs`, add imports:
```rust
use crate::event::{Sampler, assemble};
use crate::events_worker::{EventsConfig, EventsWorkerHandle};
use std::time::Instant;
```

Add the lifecycle type above `struct Inner`:
```rust
/// Events-worker lifecycle. `Once` cannot express this: a worker must never be
/// created after shutdown, which means the state has to be checked and changed
/// under one lock.
enum EventsState {
    NotStarted,
    Running(EventsWorkerHandle),
    Stopped,
    Failed,
}
```

Add fields to `struct Inner`:
```rust
    event_context: Mutex<Map<String, Value>>,
    request_id: Mutex<Option<String>>,
    events: Mutex<EventsState>,
    event_drops: Arc<DropCounter>,
    sampler: Sampler,
```

Initialize them in `ClientBuilder::build`'s `Inner { .. }` literal:
```rust
            event_context: Mutex::new(Map::new()),
            request_id: Mutex::new(None),
            events: Mutex::new(EventsState::NotStarted),
            event_drops: Arc::new(DropCounter::new("events")),
            sampler: Sampler::new(config.events_sample_rate),
```
`sampler` reads `config.events_sample_rate`, so construct it before `config` is moved into the struct — bind `let sampler = Sampler::new(config.events_sample_rate);` immediately after the worker spawn.

Add the events methods to `impl Client`:

```rust
    /// Sends an Insights event. `event_type` always wins over any `event_type`
    /// key in `payload`.
    ///
    /// The payload must be a JSON **object**; anything else is logged and
    /// dropped. Fire-and-forget: assembly happens on your thread, delivery does
    /// not. To send a struct, convert it explicitly with
    /// [`serde_json::to_value`] — that conversion is deliberately visible,
    /// because it is the one way an event can carry a field nobody enumerated.
    pub fn event(&self, event_type: &str, payload: Value) {
        self.enqueue_event(Some(event_type), payload);
    }

    /// Sends an Insights event whose `event_type` is already in the payload.
    /// An event without a non-empty string `event_type` is dropped.
    pub fn event_value(&self, payload: Value) {
        self.enqueue_event(None, payload);
    }

    fn enqueue_event(&self, event_type: Option<&str>, payload: Value) {
        let inner = &*self.0;
        if !inner.config.events_enabled {
            return;
        }
        let scope = inner
            .event_context
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let request_id = self.current_request_id();
        let Some(line) = assemble(
            event_type,
            payload,
            &scope,
            request_id.as_deref(),
            &inner.config,
            &inner.sanitizer,
            &inner.sampler,
        ) else {
            return;
        };
        let enqueued = self
            .with_events_worker(|handle| handle.try_enqueue(line))
            .unwrap_or(false);
        if !enqueued {
            inner.event_drops.record();
        }
    }

    /// Runs `f` against the events worker, spawning it if this is the first
    /// event. Returns None when no worker exists or may be created.
    fn with_events_worker<R>(&self, f: impl FnOnce(&EventsWorkerHandle) -> R) -> Option<R> {
        let inner = &*self.0;
        let mut state = inner.events.lock().unwrap_or_else(|e| e.into_inner());

        // A forked child inherits channel state but no thread; enqueueing into
        // it would silently discard events and a flush would wait out its whole
        // timeout for an acknowledgement that can never arrive.
        if let EventsState::Running(handle) = &*state
            && handle.pid != std::process::id()
        {
            log::warn!("honeybadger: events worker did not survive fork; restarting");
            *state = EventsState::NotStarted;
        }

        if matches!(*state, EventsState::NotStarted) {
            let cfg = EventsConfig {
                batch_size: inner.config.events_batch_size,
                flush_interval: inner.config.events_flush_interval,
                queue_size: inner.config.events_queue_size,
                max_retries: inner.config.events_max_retries,
                suspend_interval: Duration::from_secs(3600),
            };
            match crate::events_worker::spawn(
                inner.transport.clone(),
                cfg,
                inner.event_drops.clone(),
            ) {
                Ok(handle) => *state = EventsState::Running(handle),
                Err(e) => {
                    log::error!(
                        "honeybadger: could not start the events worker; events are disabled for this process: {e}"
                    );
                    *state = EventsState::Failed;
                }
            }
        }

        match &*state {
            EventsState::Running(handle) => Some(f(handle)),
            _ => None,
        }
    }

    /// Merges key/value pairs into this client's **event** context, attached to
    /// every later event. Setting a key to [`serde_json::Value::Null`] removes it.
    ///
    /// **Shared by every thread and task using this client — it is not
    /// request-scoped.** Concurrent requests overwrite each other here. Use it
    /// for process-wide facts and put per-request data in the event payload,
    /// where it travels with the event and cannot be clobbered.
    pub fn event_context<I, K>(&self, entries: I)
    where
        I: IntoIterator<Item = (K, Value)>,
        K: Into<String>,
    {
        let mut ctx = self.0.event_context.lock().unwrap_or_else(|e| e.into_inner());
        for (k, v) in entries {
            let key = k.into();
            if v.is_null() {
                ctx.remove(&key);
            } else {
                ctx.insert(key, v);
            }
        }
    }

    /// Clears this client's event context, leaving notice context untouched.
    pub fn clear_event_context(&self) {
        self.0
            .event_context
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    /// Sets the request id correlating this client's notices and events, and
    /// driving deterministic sampling — every event sharing an id shares one
    /// sampling decision.
    ///
    /// **This slot is process-wide, exactly like [`Client::context`].** The name
    /// describes what you put in it, not a scoping guarantee. Under concurrency
    /// one request's id overwrites another's, so an event can be attributed
    /// *and sampled* as the wrong request. Use it in programs that handle one
    /// unit of work at a time — a CLI, a cron job, a serialized consumer — and
    /// in a concurrent server put `request_id` in the event payload instead.
    pub fn request_id(&self, id: impl Into<String>) {
        *self.0.request_id.lock().unwrap_or_else(|e| e.into_inner()) = Some(id.into());
    }

    /// Clears the request id set by [`Client::request_id`].
    pub fn clear_request_id(&self) {
        *self.0.request_id.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    pub(crate) fn current_request_id(&self) -> Option<String> {
        self.0
            .request_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
```

Extend `clear_context` to cover the whole scope:
```rust
    pub fn clear_context(&self) {
        self.0
            .context
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        self.0
            .breadcrumbs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        self.clear_event_context();
        self.clear_request_id();
    }
```
Update its doc comment to say it also clears event context and the request id.

Replace `flush` and `shutdown`:
```rust
    /// Blocks until everything enqueued so far on **both** pipelines has been
    /// attempted, or `timeout` expires. Returns whether the barrier completed.
    ///
    /// Both flushes start before either is waited on, so the timeout is the
    /// number you passed rather than twice it. An events pipeline that was
    /// never started flushes as a no-op success.
    pub fn flush(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let notices = self.0.worker.flush_begin();
        let events = {
            let state = self.0.events.lock().unwrap_or_else(|e| e.into_inner());
            match &*state {
                EventsState::Running(handle) => handle.flush_begin(),
                _ => None, // never spawn a worker in order to flush it
            }
        };

        let notices_ok = match notices {
            Some(rx) => rx.recv_deadline(deadline).unwrap_or(false),
            None => false,
        };
        let events_ok = match events {
            Some(rx) => rx.recv_deadline(deadline).unwrap_or(false),
            None => true,
        };
        notices_ok && events_ok
    }

    /// Stops both delivery threads, giving queued work up to `timeout` to
    /// drain. The client accepts nothing further afterwards.
    pub fn shutdown(&self, timeout: Duration) {
        // Mark Stopped before touching the worker, so a concurrent event() can
        // never spawn one behind our back.
        let previous = {
            let mut state = self.0.events.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::replace(&mut *state, EventsState::Stopped)
        };
        if let EventsState::Running(handle) = previous {
            handle.shutdown(timeout);
        }
        self.0.worker.shutdown(timeout);
    }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test --lib client`
Expected: PASS.

- [ ] **Step 6: Verify and commit**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
git add src/client.rs src/worker.rs
git commit -m "feat: Client events API with a shutdown-safe lifecycle

The events worker spawns lazily behind a mutex-guarded state machine, so
a program that never sends events pays nothing and a worker can never be
created after shutdown. flush() now covers both pipelines inside one
timeout budget."
```

---

### Task 10: Notice correlation fallback

**Files:**
- Modify: `src/notice.rs` (`assemble` signature and `correlation_context`)
- Modify: `src/client.rs` (pass the slot through)

**Interfaces:**
- Consumes: `Client::current_request_id` (Task 9).
- Produces: `notice::assemble` gains a trailing `request_id_fallback: Option<&str>` parameter.

- [ ] **Step 1: Write the failing test**

Add to the test module in `src/notice.rs`:

```rust
    #[test]
    fn test_notice_context_request_id_still_wins() {
        let notice = Notice::message("X", "y").context([("request_id", json!("from-notice"))]);
        let payload = assemble(
            &notice,
            &test_config(),
            None,
            None,
            1,
            Some("from-slot"),
        );
        assert_eq!(
            payload["correlation_context"]["request_id"],
            json!("from-notice"),
            "merged notice context remains authoritative"
        );
    }

    #[test]
    fn test_slot_is_used_only_as_a_fallback() {
        let payload = assemble(
            &Notice::message("X", "y"),
            &test_config(),
            None,
            None,
            1,
            Some("from-slot"),
        );
        assert_eq!(
            payload["correlation_context"]["request_id"],
            json!("from-slot")
        );

        let payload = assemble(&Notice::message("X", "y"), &test_config(), None, None, 1, None);
        assert_eq!(payload.get("correlation_context"), None);
    }
```

Every existing `assemble(..)` call in that test module needs a trailing `None`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib notice`
Expected: FAIL — `assemble` takes 5 arguments.

- [ ] **Step 3: Implement**

In `src/notice.rs`, add the parameter to `assemble` and replace the correlation block:

```rust
    if let Some(request_id) = notice
        .context
        .get("request_id")
        .cloned()
        .or_else(|| request_id_fallback.map(|id| Value::String(id.to_owned())))
    {
        payload.insert(
            "correlation_context".into(),
            json!({ "request_id": request_id }),
        );
    }
```

- [ ] **Step 4: Pass the slot from the client**

In `src/client.rs`'s `run_pipeline`, capture the slot alongside the scope context in step 1 and forward it in step 6:

```rust
        let request_id_fallback = self.current_request_id();
```
```rust
        let payload = assemble(
            &notice,
            &inner.config,
            breadcrumbs,
            frames,
            std::process::id(),
            request_id_fallback.as_deref(),
        );
```

- [ ] **Step 5: Add a client-level test**

Add to the test module in `src/client.rs`:

```rust
    #[test]
    fn test_request_id_slot_correlates_notices_with_events() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.request_id("req-42");
        client.notify_notice(crate::Notice::message("X", "y"));
        client.event("t", json!({}));
        assert!(client.flush(Duration::from_secs(5)));

        assert_eq!(
            delivered(&transport)[0]["correlation_context"]["request_id"],
            json!("req-42")
        );
        assert_eq!(events_delivered(&transport)[0]["request_id"], json!("req-42"));
        client.shutdown(Duration::from_secs(5));
    }
```

- [ ] **Step 6: Verify and commit**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
git add src/notice.rs src/client.rs
git commit -m "feat: correlate notices with events via the request id slot

Merged notice context stays authoritative for correlation_context so
existing callers are unaffected; the slot only fills the gap."
```

---

### Task 11: Global facade

**Files:**
- Modify: `src/global.rs`
- Modify: `src/lib.rs` (re-exports)

**Interfaces:**
- Consumes: the `Client` methods from Task 9.
- Produces: `honeybadger::{event, event_value, event_context, clear_event_context, request_id, clear_request_id}`.

- [ ] **Step 1: Write the failing test**

Add to the test module in `src/global.rs`:

```rust
    #[test]
    fn test_event_functions_are_no_ops_before_init() {
        // Must not panic, must not spawn anything.
        crate::event("t", serde_json::json!({ "a": 1 }));
        crate::event_value(serde_json::json!({ "event_type": "t" }));
        crate::event_context([("k", serde_json::json!(1))]);
        crate::request_id("req-1");
        crate::clear_request_id();
        crate::clear_event_context();
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib global`
Expected: FAIL — `cannot find function event in crate`.

- [ ] **Step 3: Implement**

Add to `src/global.rs`:

```rust
/// Sends an Insights event through the global client. A no-op before [`init`].
///
/// `event_type` always wins over any `event_type` key in `payload`, and the
/// payload must be a JSON object. See [`crate::Client::event`].
///
/// ```rust,no_run
/// use serde_json::json;
/// honeybadger::event("user.created", json!({ "user_id": 7, "plan": "pro" }));
/// ```
pub fn event(event_type: &str, payload: Value) {
    with_client(|c| c.event(event_type, payload));
}

/// Sends an Insights event whose `event_type` is already in the payload.
/// A no-op before [`init`].
pub fn event_value(payload: Value) {
    with_client(|c| c.event_value(payload));
}

/// Merges key/value pairs into the **process-wide** event context attached to
/// every later event. A no-op before [`init`].
///
/// Like [`context`], this store is shared by the whole process and is not
/// request-scoped; see [the crate docs](crate#context-is-process-wide). Setting
/// a key to [`serde_json::Value::Null`] removes it.
pub fn event_context<I, K>(entries: I)
where
    I: IntoIterator<Item = (K, Value)>,
    K: Into<String>,
{
    with_client(|c| c.event_context(entries));
}

/// Clears the **process-wide** event context, leaving notice context untouched.
/// A no-op before [`init`].
pub fn clear_event_context() {
    with_client(|c| c.clear_event_context());
}

/// Sets the **process-wide** request id correlating notices with events and
/// driving deterministic sampling. A no-op before [`init`].
///
/// This slot carries the same hazard as [`context`]: under concurrency one
/// request's id overwrites another's, so an event can be attributed and sampled
/// as the wrong request. In a concurrent server put `request_id` in the event
/// payload instead. See [`crate::Client::request_id`].
pub fn request_id(id: impl Into<String>) {
    with_client(|c| c.request_id(id));
}

/// Clears the process-wide request id. A no-op before [`init`].
pub fn clear_request_id() {
    with_client(|c| c.clear_request_id());
}
```

`with_client` closures take `&Client`, and `event` needs to move `payload` in — the existing `with_client(f: impl FnOnce(&Client))` signature already permits that.

In `src/lib.rs`, extend the `global` re-export:
```rust
pub use crate::global::{
    Guard, add_breadcrumb, clear_context, clear_event_context, clear_request_id, context, event,
    event_context, event_value, flush, init, notify, notify_notice, request_id,
};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib global`
Expected: PASS.

- [ ] **Step 5: Verify and commit**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
git add src/global.rs src/lib.rs
git commit -m "feat: global facade for the events API"
```

---

### Task 12: Docs, example, and integration coverage

**Files:**
- Modify: `src/lib.rs` (crate docs)
- Modify: `README.md`
- Create: `examples/events.rs`
- Create: `tests/events.rs`

**Interfaces:**
- Consumes: the full public API.
- Produces: no new names.

- [ ] **Step 1: Write the integration test**

Create `tests/events.rs`:

```rust
//! End-to-end coverage for the events pipeline against a real HTTP server.
use serde_json::json;
use std::io::Read;
use std::time::Duration;

#[test]
fn test_events_post_ndjson_batches_to_the_events_endpoint() {
    let mut server = mockito::Server::new();
    let mock = server
        .mock("POST", "/v1/events")
        .match_header("content-type", "application/x-ndjson")
        .match_header("content-encoding", "deflate")
        .with_status(201)
        .expect(1)
        .create();

    let client = honeybadger::Client::new(
        honeybadger::Config::builder()
            .env_source(|_| None)
            .api_key("test-key")
            .env("production")
            .endpoint(server.url())
            .events_batch_size(1000)
            .build()
            .unwrap(),
    )
    .unwrap();

    for i in 0..25 {
        client.event("bulk.tick", json!({ "n": i }));
    }
    assert!(client.flush(Duration::from_secs(10)));
    client.shutdown(Duration::from_secs(10));

    mock.assert();
}

#[test]
fn test_batch_body_is_one_json_object_per_line() {
    let mut server = mockito::Server::new();
    let mock = server
        .mock("POST", "/v1/events")
        .with_status(201)
        .expect(1)
        .create();

    let client = honeybadger::Client::new(
        honeybadger::Config::builder()
            .env_source(|_| None)
            .api_key("test-key")
            .env("production")
            .endpoint(server.url())
            .build()
            .unwrap(),
    )
    .unwrap();
    for i in 0..3 {
        client.event("line.check", json!({ "n": i }));
    }
    assert!(client.flush(Duration::from_secs(10)));
    client.shutdown(Duration::from_secs(10));

    mock.assert();
    // The captured body is deflated NDJSON: inflate it and parse every line.
    let received = server.received_requests().unwrap();
    let body = received
        .iter()
        .find(|r| r.path() == "/v1/events")
        .and_then(|r| r.body.clone())
        .expect("an events request");
    let mut ndjson = String::new();
    flate2::read::ZlibDecoder::new(&body[..])
        .read_to_string(&mut ndjson)
        .unwrap();
    let lines: Vec<&str> = ndjson.lines().collect();
    assert_eq!(lines.len(), 3);
    for line in lines {
        let value: serde_json::Value = serde_json::from_str(line).expect("each line parses");
        assert_eq!(value["event_type"], json!("line.check"));
        assert!(value["ts"].is_string());
    }
}
```

`flate2` is a normal dependency, so it is available to integration tests without a dev-dependency entry.

- [ ] **Step 2: Run the integration test**

Run: `cargo test --test events`
Expected: PASS (2 tests).

- [ ] **Step 3: Write the example**

Create `examples/events.rs`:

```rust
//! Sending Insights events.
//!
//! Run with a real key to see them arrive:
//!   HONEYBADGER_API_KEY=... cargo run --example events
use serde_json::json;
use std::time::Duration;

fn main() {
    let _guard = honeybadger::init(
        honeybadger::Config::builder()
            .env("production")
            .build()
            .expect("config"),
    )
    .expect("init — set HONEYBADGER_API_KEY");

    // Correlate everything below, and give it all one sampling fate.
    honeybadger::request_id("demo-request-1");
    honeybadger::event_context([("service", json!("checkout"))]);

    honeybadger::event("cart.viewed", json!({ "items": 3 }));
    honeybadger::event("payment.attempted", json!({ "amount_cents": 4200 }));
    honeybadger::event("payment.failed", json!({ "code": "card_declined" }));

    // A struct is converted explicitly — the conversion is visible on purpose.
    #[derive(serde::Serialize)]
    struct Timing {
        step: &'static str,
        ms: u64,
    }
    let timing = Timing {
        step: "checkout",
        ms: 91,
    };
    honeybadger::event(
        "step.timed",
        serde_json::to_value(&timing).expect("serializable"),
    );

    // Events batch in the background; flush before exiting a short program.
    honeybadger::flush(Duration::from_secs(10));
    println!("sent 4 events");
}
```

`serde` is already a dependency with the `derive` feature enabled, so the example compiles without Cargo changes.

- [ ] **Step 4: Verify the example builds and runs offline**

Run: `cargo build --example events`
Expected: builds clean.
Run: `HONEYBADGER_ENV=test cargo run --example events`
Expected: prints `sent 4 events` — the `test` environment is excluded, so it uses the null transport and sends nothing.

- [ ] **Step 5: Document the pipeline in the crate docs**

In `src/lib.rs`, add a section after the "Context is process-wide" section:

````rust
//! # Insights events
//!
//! Beyond errors, the SDK sends structured events to
//! [Honeybadger Insights](https://www.honeybadger.io/insights/):
//!
//! ```rust,no_run
//! use serde_json::json;
//!
//! honeybadger::event("user.created", json!({ "user_id": 7, "plan": "pro" }));
//! ```
//!
//! Events batch in the background — 1000 of them, 30 seconds, or 4.5 MB,
//! whichever comes first — and the worker thread starts on your first
//! [`event`] call, so a program that only reports errors never pays for it.
//! [`flush`] covers events and notices together.
//!
//! The payload is a [`serde_json::Value`] rather than anything `Serialize`.
//! That is deliberate: passing a struct would send every field it happens to
//! carry, so a struct must be converted explicitly with
//! [`serde_json::to_value`]. Because every field is written by hand,
//! `filter_keys` redaction does **not** apply to events the way it does to
//! notice context.
//!
//! Set [`request_id`] to correlate an error with the events around it. It also
//! drives sampling: events sharing a request id share one decision, so a
//! sampled request keeps all of its events or none. Like [`context`], the slot
//! is process-wide — see the warning above.
````

- [ ] **Step 6: Document the pipeline in the README**

Add an "Insights events" section to `README.md` mirroring the crate docs above: the `event` call, the batching triggers, the `Value`-not-`Serialize` rationale, `request_id` correlation and sampling, and a table of the `events_*` configuration options with their defaults copied from `src/config.rs`.

- [ ] **Step 7: Verify everything is green**

Run: `cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check`
Expected: all pass, including doctests.

- [ ] **Step 8: Commit**

```bash
git add src/lib.rs README.md examples/events.rs tests/events.rs
git commit -m "docs: document the Insights events pipeline

Adds an events example, end-to-end NDJSON coverage, and crate and README
sections covering batching, correlation, sampling, and why the payload
is a Value rather than impl Serialize."
```

---

## Verification checklist

Before declaring Phase 2 done, confirm each of these by running the command and reading the output — not by assuming:

- [ ] `cargo test` — all unit, integration, and doc tests pass.
- [ ] `cargo clippy --all-targets -- -D warnings` — clean.
- [ ] `cargo fmt --check` — clean.
- [ ] `cargo build --examples` — every example compiles.
- [ ] `cargo doc --no-deps` — no `missing_docs` warnings.
- [ ] `cargo tree | grep -c .` — confirm no dependency was added versus `main`.
- [ ] Live smoke test: run `examples/events.rs` with a real API key and confirm the events appear in Insights.
