# Honeybadger Rust SDK — Phase 1 (Notices) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Phase 1 of the official Honeybadger Rust SDK per the approved spec: error notices with breadcrumbs, panic hook, before_notify hooks, and an OS-thread worker with throttle/suspend/flush semantics.

**Architecture:** Bottom-up by module dependency: leaf modules first (error, sanitizer, breadcrumbs, config, backtrace), then notice assembly, transport, worker, client pipeline, global facade, panic dispatcher, and finally examples/docs/CI. Every task ends with `cargo test` green and a commit.

**Tech Stack:** Rust edition 2024 (MSRV 1.85). Deps: `backtrace`, `crossbeam-channel`, `flate2`, `hostname`, `jiff`, `log`, `serde`, `serde_json`, `thiserror`, `ureq` (rustls). Dev: `mockito`.

**Spec:** `docs/superpowers/specs/2026-07-22-honeybadger-rust-sdk-design.md` is the authority. Where this plan and the spec disagree, the spec wins — stop and flag it.

## Global Constraints

- Every task ends with `cargo test` fully green before its commit step.
- `cargo`/`rustc` are at `~/.cargo/bin`: every shell session starts with `export PATH="$HOME/.cargo/bin:$PATH"`. Working directory is the repo root `/Users/ben/Code/honeybadger/honeybadger-rust`.
- Commit locally only. **Never push to origin.**
- The SDK's no-panic rule (spec "Error handling within the SDK") applies to all implementation code: no `unwrap()`/`expect()` outside `#[cfg(test)]` except on mutex locks recovered via `unwrap_or_else(|e| e.into_inner())`.
- Public API surface must match the spec exactly: `notice_queue_size` (not `max_queue_size`), `Guard` is `#[must_use]`, `Notice` fields private, `notify<E: Error + ?Sized>`.
- External-crate APIs (`ureq` 3, `jiff`, `mockito` 1): the code below is written against their documented APIs; if a builder-method name has drifted in the released version, consult docs.rs and adjust the call site — the *behavior* specified here (timeouts, no-status-error mode, zlib format) is the requirement.

---

### Task 1: Crate scaffold and SDK error type

**Files:**
- Create: `Cargo.toml`, `.gitignore`, `src/lib.rs`, `src/error.rs`

**Interfaces:**
- Produces: `honeybadger::Error` enum with variants `AlreadyInitialized`, `MissingApiKey`, `InvalidEndpoint(String)`, `WorkerSpawn(std::io::Error)` — used by Tasks 4 (config validation), 9 (Client::new), 10 (init).

- [ ] **Step 1: Write `Cargo.toml`**

```toml
[package]
name = "honeybadger"
version = "0.1.0"
edition = "2024"
rust-version = "1.85"
description = "The official Honeybadger error-tracking SDK for Rust"
license = "MIT"
repository = "https://github.com/honeybadger-io/honeybadger-rust"
readme = "README.md"
categories = ["development-tools::debugging", "web-programming"]
keywords = ["honeybadger", "error", "monitoring", "exception"]

[dependencies]
backtrace = "0.3"
crossbeam-channel = "0.5"
flate2 = "1"
hostname = "0.4"
jiff = "0.2"
log = "0.4"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
ureq = { version = "3", default-features = false, features = ["rustls", "gzip"] }

[dev-dependencies]
mockito = "1"
```

And `.gitignore`:

```
/target
tests/fixtures/*/target
tests/fixtures/*/Cargo.lock
```

(`Cargo.lock` for the SDK itself IS committed — do not ignore it.)

- [ ] **Step 2: Write `src/error.rs` with tests**

```rust
//! Errors returned by the SDK's fallible surfaces (`init`, `Client::new`, `Config::build`).
use thiserror::Error;

/// Errors returned by [`crate::init`] and [`crate::Client::new`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("Honeybadger is already initialized; drop the previous Guard before calling init again")]
    AlreadyInitialized,

    #[error("an API key is required to report to the Honeybadger service (set HONEYBADGER_API_KEY or Config::builder().api_key(...))")]
    MissingApiKey,

    #[error("invalid Honeybadger endpoint URL: {0}")]
    InvalidEndpoint(String),

    #[error("failed to spawn the honeybadger worker thread")]
    WorkerSpawn(#[source] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        assert!(Error::AlreadyInitialized.to_string().contains("already initialized"));
        assert!(Error::InvalidEndpoint("ftp://x".into()).to_string().contains("ftp://x"));
    }
}
```

- [ ] **Step 3: Write interim `src/lib.rs`**

```rust
//! The official Honeybadger error-tracking SDK for Rust. (Docs land in the final task.)

mod error;

pub use crate::error::Error;
```

- [ ] **Step 4: Run tests**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test`
Expected: 1 test passes.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: crate scaffold and SDK error type"
```

---

### Task 2: Sanitizer

**Files:**
- Create: `src/sanitizer.rs`
- Modify: `src/lib.rs` (add `mod sanitizer;`)

**Interfaces:**
- Produces (consumed by Task 9's pipeline step 5 and Task 3's breadcrumb sanitization):
  - `pub(crate) struct Sanitizer`; `Sanitizer::new<I: IntoIterator<Item = S>, S: AsRef<str>>(filter_keys: I) -> Sanitizer`
  - `sanitize(&self, value: &mut serde_json::Value)` — full depth (20)
  - `sanitize_shallow(&self, value: &mut serde_json::Value)` — depth 1 (breadcrumb metadata)
  - Constants `FILTERED`, `DEPTH_MARKER`, `TRUNCATED`, `MAX_DEPTH: usize = 20`, `MAX_STRING_BYTES: usize = 65_536`

- [ ] **Step 1: Write the failing tests** (bottom of the new `src/sanitizer.rs`, module skeleton with `todo!()` bodies above them)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sanitizer() -> Sanitizer {
        Sanitizer::new(["password", "credit_card", "secret"])
    }

    #[test]
    fn test_filters_keys_case_insensitively() {
        let mut v = json!({"PassWord": "hunter2", "user": {"secret": "x", "name": "ok"}});
        sanitizer().sanitize(&mut v);
        assert_eq!(v, json!({"PassWord": "[FILTERED]", "user": {"secret": "[FILTERED]", "name": "ok"}}));
    }

    #[test]
    fn test_depth_cap() {
        // Build a value nested deeper than MAX_DEPTH.
        let mut v = json!("leaf");
        for _ in 0..(MAX_DEPTH + 2) {
            v = json!({ "k": v });
        }
        sanitizer().sanitize(&mut v);
        let s = serde_json::to_string(&v).unwrap();
        assert!(s.contains(DEPTH_MARKER));
        assert!(!s.contains("leaf"));
    }

    #[test]
    fn test_shallow_depth_for_breadcrumb_metadata() {
        let mut v = json!({"a": {"b": 1}, "c": "keep"});
        sanitizer().sanitize_shallow(&mut v);
        assert_eq!(v, json!({"a": "[DEPTH]", "c": "keep"}));
    }

    #[test]
    fn test_truncates_long_strings_on_char_boundary() {
        // 'é' is 2 bytes; an odd byte limit boundary must not split it.
        let long = "é".repeat(MAX_STRING_BYTES); // 2 × MAX bytes
        let mut v = json!({ "msg": long });
        sanitizer().sanitize(&mut v);
        let out = v["msg"].as_str().unwrap();
        assert!(out.ends_with(TRUNCATED));
        assert!(out.len() <= MAX_STRING_BYTES + TRUNCATED.len());
        assert!(out.is_char_boundary(out.len() - TRUNCATED.len()));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test sanitizer`
Expected: FAIL (`todo!()` panics or missing items).

- [ ] **Step 3: Implement**

Full module above the tests:

```rust
//! Structural sanitization: key redaction, depth capping, string truncation.
//! Runs LAST in the notify pipeline so hook-introduced data is covered (spec).
use serde_json::Value;

pub(crate) const FILTERED: &str = "[FILTERED]";
pub(crate) const DEPTH_MARKER: &str = "[DEPTH]";
pub(crate) const TRUNCATED: &str = "[TRUNCATED]";
pub(crate) const MAX_DEPTH: usize = 20;
pub(crate) const MAX_STRING_BYTES: usize = 65_536;

pub(crate) struct Sanitizer {
    filter_keys: Vec<String>, // lowercased
}

impl Sanitizer {
    pub(crate) fn new<I, S>(filter_keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Sanitizer {
            filter_keys: filter_keys.into_iter().map(|k| k.as_ref().to_lowercase()).collect(),
        }
    }

    pub(crate) fn sanitize(&self, value: &mut Value) {
        self.walk(value, MAX_DEPTH);
    }

    pub(crate) fn sanitize_shallow(&self, value: &mut Value) {
        self.walk(value, 1);
    }

    fn walk(&self, value: &mut Value, depth_left: usize) {
        match value {
            Value::String(s) => truncate_string(s),
            Value::Object(map) => {
                for (key, val) in map.iter_mut() {
                    if self.filter_keys.iter().any(|f| key.to_lowercase() == *f) {
                        *val = Value::String(FILTERED.into());
                    } else if depth_left <= 1 && (val.is_object() || val.is_array()) {
                        *val = Value::String(DEPTH_MARKER.into());
                    } else {
                        self.walk(val, depth_left - 1);
                    }
                }
            }
            Value::Array(items) => {
                for val in items.iter_mut() {
                    if depth_left <= 1 && (val.is_object() || val.is_array()) {
                        *val = Value::String(DEPTH_MARKER.into());
                    } else {
                        self.walk(val, depth_left - 1);
                    }
                }
            }
            _ => {}
        }
    }
}

pub(crate) fn truncate_string(s: &mut String) {
    if s.len() <= MAX_STRING_BYTES {
        return;
    }
    let mut cut = MAX_STRING_BYTES;
    while !s.is_char_boundary(cut) {
        cut -= 1;
    }
    s.truncate(cut);
    s.push_str(TRUNCATED);
}
```

Add `mod sanitizer;` to `src/lib.rs`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test sanitizer` — expected: 4 pass. Then `cargo test` — all green.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: sanitizer (filter_keys redaction, depth cap, UTF-8-safe truncation)"
```

---

### Task 3: Breadcrumbs

**Files:**
- Create: `src/breadcrumbs.rs`
- Modify: `src/lib.rs` (add `mod breadcrumbs;`)

**Interfaces:**
- Produces (consumed by Tasks 6 payload, 9 client):
  - `pub struct Breadcrumb` (Serialize; fields private): `Breadcrumb::new(message: &str, category: &str, metadata: Option<serde_json::Map<String, serde_json::Value>>) -> Breadcrumb`; `pub(crate) fn with_timestamp(..., timestamp: String)` for deterministic tests.
  - `pub(crate) struct RingBuffer` — `RingBuffer::new() -> Self` (capacity 40), `push(&mut self, Breadcrumb)`, `snapshot(&self) -> Vec<Breadcrumb>`, `clear(&mut self)`.
  - `pub(crate) fn now_iso8601_ms() -> String` — shared timestamp helper (also used by nothing else in Phase 1, but events reuse it in Phase 2).

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_buffer_drops_oldest_beyond_capacity() {
        let mut buf = RingBuffer::new();
        for i in 0..45 {
            buf.push(Breadcrumb::new(&format!("crumb {i}"), "custom", None));
        }
        let snap = buf.snapshot();
        assert_eq!(snap.len(), 40);
        assert_eq!(snap.first().unwrap().message, "crumb 5");
        assert_eq!(snap.last().unwrap().message, "crumb 44");
    }

    #[test]
    fn test_breadcrumb_serialization_shape() {
        let mut meta = serde_json::Map::new();
        meta.insert("sql".into(), serde_json::json!("SELECT 1"));
        let crumb = Breadcrumb::with_timestamp("query ran", "query", Some(meta), "2026-07-22T00:00:00.000Z".into());
        let v = serde_json::to_value(&crumb).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "message": "query ran",
                "category": "query",
                "metadata": {"sql": "SELECT 1"},
                "timestamp": "2026-07-22T00:00:00.000Z"
            })
        );
    }

    #[test]
    fn test_timestamp_is_iso8601_utc_ms() {
        let ts = now_iso8601_ms();
        // e.g. 2026-07-22T21:03:04.123Z
        assert!(ts.ends_with('Z'));
        assert_eq!(ts.len(), "2026-07-22T21:03:04.123Z".len());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test breadcrumbs` — expected: compile failure (items missing).

- [ ] **Step 3: Implement**

```rust
//! Breadcrumbs: a 40-entry ring buffer serialized into every notice.
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::VecDeque;

const CAPACITY: usize = 40;

#[derive(Clone, Serialize)]
pub struct Breadcrumb {
    pub(crate) message: String,
    pub(crate) category: String,
    pub(crate) metadata: Map<String, Value>,
    pub(crate) timestamp: String,
}

impl Breadcrumb {
    pub fn new(message: &str, category: &str, metadata: Option<Map<String, Value>>) -> Self {
        Self::with_timestamp(message, category, metadata, now_iso8601_ms())
    }

    pub(crate) fn with_timestamp(
        message: &str,
        category: &str,
        metadata: Option<Map<String, Value>>,
        timestamp: String,
    ) -> Self {
        Breadcrumb {
            message: message.to_owned(),
            category: category.to_owned(),
            metadata: metadata.unwrap_or_default(),
            timestamp,
        }
    }
}

pub(crate) struct RingBuffer {
    buf: VecDeque<Breadcrumb>,
}

impl RingBuffer {
    pub(crate) fn new() -> Self {
        RingBuffer { buf: VecDeque::with_capacity(CAPACITY) }
    }

    pub(crate) fn push(&mut self, crumb: Breadcrumb) {
        if self.buf.len() == CAPACITY {
            self.buf.pop_front();
        }
        self.buf.push_back(crumb);
    }

    pub(crate) fn snapshot(&self) -> Vec<Breadcrumb> {
        self.buf.iter().cloned().collect()
    }

    pub(crate) fn clear(&mut self) {
        self.buf.clear();
    }
}

/// UTC timestamp like `2026-07-22T21:03:04.123Z` (ISO8601, millisecond precision).
pub(crate) fn now_iso8601_ms() -> String {
    let ts = jiff::Timestamp::now();
    ts.strftime("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}
```

Add `mod breadcrumbs;` and `pub use crate::breadcrumbs::Breadcrumb;` to `src/lib.rs`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test` — all green. (If `strftime`'s fractional-seconds directive differs in the released jiff, use `format!` on `ts.round(jiff::Unit::Millisecond)` per its docs — the required output shape is what the test asserts.)

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: breadcrumbs ring buffer and timestamp helper"
```

---

### Task 4: Config

**Files:**
- Create: `src/config.rs`
- Modify: `src/lib.rs` (add `mod config;`, `pub use crate::config::{Config, ConfigBuilder};`)

**Interfaces:**
- Consumes: `Error` (Task 1), `Notice` forward-declared as `crate::notice::Notice` — **declare the hook type with a placeholder now**: define `pub type BeforeNotifyHook = dyn Fn(&mut crate::notice::Notice) -> bool + Send + Sync;` and add an empty `pub struct Notice;` in a new stub `src/notice.rs` (`mod notice;` in lib.rs) that Task 6 replaces.
- Produces (consumed by Tasks 5–11):
  - `Config` (fields `pub(crate)`): `api_key: Option<String>`, `env: Option<String>`, `exclude_envs: Vec<String>`, `enabled: Option<bool>`, `endpoint: String`, `root: String`, `hostname: String`, `revision: Option<String>`, `filter_keys: Vec<String>`, `ignore_classes: Vec<String>`, `breadcrumbs_enabled: bool`, `install_panic_hook: bool`, `notice_queue_size: usize`, `connect_timeout: Duration`, `request_timeout: Duration`, `before_notify: Vec<Arc<BeforeNotifyHook>>`
  - `Config::builder() -> ConfigBuilder`; builder setters named exactly per the spec's config table plus `.before_notify(f)` and `.env_source(fn)`; `build(self) -> Result<Config, Error>`
  - `Config::reporting_enabled(&self) -> bool` — `enabled` override, else `env` not in `exclude_envs`

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[test]
    fn test_builder_beats_env_beats_default() {
        let cfg = Config::builder()
            .env_source(|k| (k == "HONEYBADGER_ENV").then(|| "staging".to_string()))
            .api_key("k")
            .build()
            .unwrap();
        assert_eq!(cfg.env.as_deref(), Some("staging")); // env var wins over default

        let cfg = Config::builder()
            .env_source(|k| (k == "HONEYBADGER_ENV").then(|| "staging".to_string()))
            .api_key("k")
            .env("production")
            .build()
            .unwrap();
        assert_eq!(cfg.env.as_deref(), Some("production")); // builder wins over env var
    }

    #[test]
    fn test_api_key_env_var() {
        let cfg = Config::builder()
            .env_source(|k| (k == "HONEYBADGER_API_KEY").then(|| "from-env".to_string()))
            .build()
            .unwrap();
        assert_eq!(cfg.api_key.as_deref(), Some("from-env"));
    }

    #[test]
    fn test_api_key_required_only_when_reporting() {
        // Excluded env: no key needed.
        let cfg = Config::builder().env_source(no_env).env("test").build().unwrap();
        assert!(!cfg.reporting_enabled());
        // Reporting env without key: error.
        let err = Config::builder().env_source(no_env).env("production").build().unwrap_err();
        assert!(matches!(err, crate::Error::MissingApiKey));
    }

    #[test]
    fn test_enabled_overrides_both_directions() {
        let cfg = Config::builder().env_source(no_env).env("test").enabled(true).api_key("k").build().unwrap();
        assert!(cfg.reporting_enabled());
        let cfg = Config::builder().env_source(no_env).env("production").enabled(false).build().unwrap();
        assert!(!cfg.reporting_enabled());
    }

    #[test]
    fn test_endpoint_validation() {
        let err = Config::builder().env_source(no_env).api_key("k").endpoint("ftp://nope").build().unwrap_err();
        assert!(matches!(err, crate::Error::InvalidEndpoint(_)));
    }

    #[test]
    fn test_defaults() {
        let cfg = Config::builder().env_source(no_env).api_key("k").build().unwrap();
        assert_eq!(cfg.endpoint, "https://api.honeybadger.io");
        assert_eq!(cfg.notice_queue_size, 100);
        assert_eq!(cfg.exclude_envs, vec!["development".to_string(), "test".to_string()]);
        assert_eq!(cfg.filter_keys, vec!["password".to_string(), "credit_card".to_string(), "secret".to_string()]);
        assert_eq!(cfg.connect_timeout, Duration::from_secs(2));
        assert_eq!(cfg.request_timeout, Duration::from_secs(5));
        assert!(cfg.breadcrumbs_enabled);
        assert!(cfg.install_panic_hook);
        assert!(!cfg.root.is_empty()); // cwd default
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test config` — expected: compile failure.

- [ ] **Step 3: Implement**

Create stub `src/notice.rs` (replaced in Task 6):

```rust
/// Placeholder; the real Notice lands in the notice-payload task.
pub struct Notice;
```

`src/config.rs`:

```rust
//! Configuration: builder > env var > default (spec "Config" section).
use crate::error::Error;
use crate::notice::Notice;
use std::sync::Arc;
use std::time::Duration;

pub type BeforeNotifyHook = dyn Fn(&mut Notice) -> bool + Send + Sync;
type EnvSource = Box<dyn Fn(&str) -> Option<String>>;

pub struct Config {
    pub(crate) api_key: Option<String>,
    pub(crate) env: Option<String>,
    pub(crate) exclude_envs: Vec<String>,
    pub(crate) enabled: Option<bool>,
    pub(crate) endpoint: String,
    pub(crate) root: String,
    pub(crate) hostname: String,
    pub(crate) revision: Option<String>,
    pub(crate) filter_keys: Vec<String>,
    pub(crate) ignore_classes: Vec<String>,
    pub(crate) breadcrumbs_enabled: bool,
    pub(crate) install_panic_hook: bool,
    pub(crate) notice_queue_size: usize,
    pub(crate) connect_timeout: Duration,
    pub(crate) request_timeout: Duration,
    pub(crate) before_notify: Vec<Arc<BeforeNotifyHook>>,
}

impl Config {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    pub(crate) fn reporting_enabled(&self) -> bool {
        if let Some(enabled) = self.enabled {
            return enabled;
        }
        match &self.env {
            Some(env) => !self.exclude_envs.iter().any(|e| e == env),
            None => true, // unset means "report" (one log::info at init)
        }
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("api_key", &self.api_key.as_deref().map(|_| "<redacted>"))
            .field("env", &self.env)
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .field("hostname", &self.hostname)
            .field("revision", &self.revision)
            .field("hooks", &self.before_notify.len())
            .finish_non_exhaustive()
    }
}

pub struct ConfigBuilder {
    api_key: Option<String>,
    env: Option<String>,
    exclude_envs: Option<Vec<String>>,
    enabled: Option<bool>,
    endpoint: Option<String>,
    root: Option<String>,
    hostname: Option<String>,
    revision: Option<String>,
    filter_keys: Option<Vec<String>>,
    ignore_classes: Option<Vec<String>>,
    breadcrumbs_enabled: Option<bool>,
    install_panic_hook: Option<bool>,
    notice_queue_size: Option<usize>,
    connect_timeout: Option<Duration>,
    request_timeout: Option<Duration>,
    before_notify: Vec<Arc<BeforeNotifyHook>>,
    env_source: EnvSource,
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        ConfigBuilder {
            api_key: None,
            env: None,
            exclude_envs: None,
            enabled: None,
            endpoint: None,
            root: None,
            hostname: None,
            revision: None,
            filter_keys: None,
            ignore_classes: None,
            breadcrumbs_enabled: None,
            install_panic_hook: None,
            notice_queue_size: None,
            connect_timeout: None,
            request_timeout: None,
            before_notify: Vec::new(),
            env_source: Box::new(|key| std::env::var(key).ok()),
        }
    }
}

impl ConfigBuilder {
    pub fn api_key(mut self, v: impl Into<String>) -> Self { self.api_key = Some(v.into()); self }
    pub fn env(mut self, v: impl Into<String>) -> Self { self.env = Some(v.into()); self }
    pub fn exclude_envs<I: IntoIterator<Item = S>, S: Into<String>>(mut self, v: I) -> Self {
        self.exclude_envs = Some(v.into_iter().map(Into::into).collect()); self
    }
    pub fn enabled(mut self, v: bool) -> Self { self.enabled = Some(v); self }
    pub fn endpoint(mut self, v: impl Into<String>) -> Self { self.endpoint = Some(v.into()); self }
    pub fn root(mut self, v: impl Into<String>) -> Self { self.root = Some(v.into()); self }
    pub fn hostname(mut self, v: impl Into<String>) -> Self { self.hostname = Some(v.into()); self }
    pub fn revision(mut self, v: impl Into<String>) -> Self { self.revision = Some(v.into()); self }
    pub fn filter_keys<I: IntoIterator<Item = S>, S: Into<String>>(mut self, v: I) -> Self {
        self.filter_keys = Some(v.into_iter().map(Into::into).collect()); self
    }
    pub fn ignore_classes<I: IntoIterator<Item = S>, S: Into<String>>(mut self, v: I) -> Self {
        self.ignore_classes = Some(v.into_iter().map(Into::into).collect()); self
    }
    pub fn breadcrumbs_enabled(mut self, v: bool) -> Self { self.breadcrumbs_enabled = Some(v); self }
    pub fn install_panic_hook(mut self, v: bool) -> Self { self.install_panic_hook = Some(v); self }
    pub fn notice_queue_size(mut self, v: usize) -> Self { self.notice_queue_size = Some(v); self }
    pub fn connect_timeout(mut self, v: Duration) -> Self { self.connect_timeout = Some(v); self }
    pub fn request_timeout(mut self, v: Duration) -> Self { self.request_timeout = Some(v); self }
    pub fn before_notify<F>(mut self, f: F) -> Self
    where
        F: Fn(&mut Notice) -> bool + Send + Sync + 'static,
    {
        self.before_notify.push(Arc::new(f)); self
    }
    /// Test seam: replaces `std::env::var` (Edition 2024 makes env mutation unsafe; tests inject instead).
    pub fn env_source<F: Fn(&str) -> Option<String> + 'static>(mut self, f: F) -> Self {
        self.env_source = Box::new(f); self
    }

    pub fn build(self) -> Result<Config, Error> {
        let ev = |key: &str| (self.env_source)(key);
        let parse_bool = |s: String| matches!(s.as_str(), "true" | "1" | "yes");

        let endpoint = self
            .endpoint
            .or_else(|| ev("HONEYBADGER_ENDPOINT"))
            .unwrap_or_else(|| "https://api.honeybadger.io".to_string());
        if !(endpoint.starts_with("https://") || endpoint.starts_with("http://")) {
            return Err(Error::InvalidEndpoint(endpoint));
        }

        let config = Config {
            api_key: self.api_key.or_else(|| ev("HONEYBADGER_API_KEY")),
            env: self.env.or_else(|| ev("HONEYBADGER_ENV")),
            exclude_envs: self
                .exclude_envs
                .unwrap_or_else(|| vec!["development".into(), "test".into()]),
            enabled: self.enabled.or_else(|| ev("HONEYBADGER_ENABLED").map(parse_bool)),
            endpoint: endpoint.trim_end_matches('/').to_string(),
            root: self
                .root
                .or_else(|| ev("HONEYBADGER_ROOT"))
                .or_else(|| std::env::current_dir().ok().map(|p| p.to_string_lossy().into_owned()))
                .unwrap_or_default(),
            hostname: self
                .hostname
                .or_else(|| ev("HONEYBADGER_HOSTNAME"))
                .or_else(|| hostname::get().ok().map(|h| h.to_string_lossy().into_owned()))
                .unwrap_or_default(),
            revision: self.revision.or_else(|| ev("HONEYBADGER_REVISION")),
            filter_keys: self
                .filter_keys
                .unwrap_or_else(|| vec!["password".into(), "credit_card".into(), "secret".into()]),
            ignore_classes: self.ignore_classes.unwrap_or_default(),
            breadcrumbs_enabled: self.breadcrumbs_enabled.unwrap_or(true),
            install_panic_hook: self.install_panic_hook.unwrap_or(true),
            notice_queue_size: self.notice_queue_size.unwrap_or(100),
            connect_timeout: self.connect_timeout.unwrap_or(Duration::from_secs(2)),
            request_timeout: self.request_timeout.unwrap_or(Duration::from_secs(5)),
            before_notify: self.before_notify,
        };

        if config.reporting_enabled() && config.api_key.is_none() {
            return Err(Error::MissingApiKey);
        }
        Ok(config)
    }
}
```

Add to `src/lib.rs`: `mod config;`, `mod notice;`, `pub use crate::config::{Config, ConfigBuilder};`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test` — all green.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: config builder with injectable env source and precedence rules"
```

---

### Task 5: Backtrace capture and frame processing

**Files:**
- Create: `src/backtrace.rs` (name the module `bt` internally to avoid colliding with the `backtrace` crate: file `src/bt.rs`, `mod bt;`)

**Interfaces:**
- Produces (consumed by Task 6 payload assembly, Task 11 panic):
  - `pub(crate) struct Frame { pub number: Option<u32>, pub file: Option<String>, pub method: Option<String>, pub source: Option<std::collections::BTreeMap<String, String>> }` (Serialize; `number` serialized as a **string** in the payload — handled in Task 6's assembly, `Frame` itself keeps `u32`)
  - `pub(crate) fn capture(root: &str) -> Vec<Frame>` — capture + resolve + process at the call site
  - `pub(crate) fn map_frame(symbol_name: Option<&str>, file: Option<&std::path::Path>, line: Option<u32>, root: &str) -> Option<Frame>` — pure, testable: returns `None` for internal frames
  - `pub(crate) const MAX_FRAMES: usize = 1_000;`

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_internal_frames_dropped() {
        for name in [
            "honeybadger::client::Client::notify",
            "backtrace::backtrace::trace",
            "std::rt::lang_start",
            "std::panicking::try",
            "core::panicking::panic_fmt",
            "__libc_start_main",
        ] {
            assert!(map_frame(Some(name), None, None, "/app").is_none(), "{name} should be dropped");
        }
    }

    #[test]
    fn test_app_frame_mapped_with_project_root_substitution() {
        let f = map_frame(
            Some("my_app::checkout::charge"),
            Some(Path::new("/app/src/checkout.rs")),
            Some(42),
            "/app",
        )
        .unwrap();
        assert_eq!(f.method.as_deref(), Some("my_app::checkout::charge"));
        assert_eq!(f.file.as_deref(), Some("[PROJECT_ROOT]/src/checkout.rs"));
        assert_eq!(f.number, Some(42));
    }

    #[test]
    fn test_non_root_file_not_substituted_and_no_source() {
        let f = map_frame(
            Some("dep::thing"),
            Some(Path::new("/cargo/registry/dep/lib.rs")),
            Some(7),
            "/app",
        )
        .unwrap();
        assert_eq!(f.file.as_deref(), Some("/cargo/registry/dep/lib.rs"));
        assert!(f.source.is_none());
    }

    #[test]
    fn test_source_excerpt_only_under_root() {
        // Use this very repository as the "project root" and this very file as the frame file.
        let root = env!("CARGO_MANIFEST_DIR");
        let file = Path::new(root).join("src/bt.rs");
        let f = map_frame(Some("honeybadger_test::x"), Some(&file), Some(3), root).unwrap();
        let source = f.source.expect("source excerpt expected for in-root file");
        assert!(source.contains_key("3"));
        assert!(source.len() <= 5); // lineno ± 2
    }

    #[test]
    fn test_capture_returns_frames_capped() {
        let frames = capture(env!("CARGO_MANIFEST_DIR"));
        assert!(frames.len() <= MAX_FRAMES);
        // The capture helper itself must not appear (it's under honeybadger::).
        assert!(frames.iter().all(|f| {
            f.method.as_deref().map(|m| !m.starts_with("honeybadger::bt")).unwrap_or(true)
        }));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test bt` — expected: compile failure.

- [ ] **Step 3: Implement `src/bt.rs`**

```rust
//! Backtrace capture and frame processing (spec "Backtraces" section).
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;

pub(crate) const MAX_FRAMES: usize = 1_000;
const SOURCE_RADIUS: u32 = 2;
const PROJECT_ROOT: &str = "[PROJECT_ROOT]";

const INTERNAL_PREFIXES: &[&str] = &[
    "honeybadger::",
    "backtrace::",
    "std::rt::",
    "std::panicking::",
    "std::panic::",
    "core::panicking::",
    "std::sys::",
    "rust_begin_unwind",
    "__libc_start_main",
    "__rust_try",
    "core::ops::function::FnOnce::call_once",
];

#[derive(Clone, Serialize)]
pub(crate) struct Frame {
    pub(crate) number: Option<u32>,
    pub(crate) file: Option<String>,
    pub(crate) method: Option<String>,
    pub(crate) source: Option<BTreeMap<String, String>>,
}

pub(crate) fn capture(root: &str) -> Vec<Frame> {
    let bt = backtrace::Backtrace::new(); // captures + resolves symbols
    let mut frames = Vec::new();
    for frame in bt.frames() {
        for symbol in frame.symbols() {
            let name = symbol.name().map(|n| n.to_string());
            if let Some(f) = map_frame(name.as_deref(), symbol.filename(), symbol.lineno(), root) {
                frames.push(f);
                if frames.len() == MAX_FRAMES {
                    return frames;
                }
            }
        }
    }
    frames
}

pub(crate) fn map_frame(
    symbol_name: Option<&str>,
    file: Option<&Path>,
    line: Option<u32>,
    root: &str,
) -> Option<Frame> {
    if let Some(name) = symbol_name {
        // Strip the trailing hash (`::h0123abcd`) before matching and reporting.
        let clean = name.rsplit_once("::h").map(|(head, _)| head).unwrap_or(name);
        if INTERNAL_PREFIXES.iter().any(|p| clean.starts_with(p)) {
            return None;
        }
        let in_root = file.map(|f| f.starts_with(root) && !root.is_empty()).unwrap_or(false);
        let source = match (in_root, file, line) {
            (true, Some(f), Some(n)) => read_excerpt(f, n),
            _ => None,
        };
        let file_str = file.map(|f| {
            let s = f.to_string_lossy().into_owned();
            if in_root { s.replacen(root, PROJECT_ROOT, 1) } else { s }
        });
        return Some(Frame { number: line, file: file_str, method: Some(clean.to_owned()), source });
    }
    // Unresolvable frames are kept (address-only) so gaps are visible.
    Some(Frame {
        number: line,
        file: file.map(|f| f.to_string_lossy().into_owned()),
        method: None,
        source: None,
    })
}

fn read_excerpt(file: &Path, lineno: u32) -> Option<BTreeMap<String, String>> {
    let content = std::fs::read_to_string(file).ok()?;
    let start = lineno.saturating_sub(SOURCE_RADIUS).max(1);
    let mut out = BTreeMap::new();
    for (idx, line) in content.lines().enumerate() {
        let n = (idx + 1) as u32;
        if n >= start && n <= lineno + SOURCE_RADIUS {
            out.insert(n.to_string(), line.to_owned());
        }
    }
    if out.is_empty() { None } else { Some(out) }
}
```

Add `mod bt;` to `src/lib.rs`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test` — all green. Note: `test_capture_returns_frames_capped` exercises real symbolication; if the test binary's symbols make the honeybadger-prefix assertion flaky in release CI, scope it with `#[cfg(debug_assertions)]` — but try without first.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: backtrace capture, internal-frame filtering, root-bounded source excerpts"
```

---

### Task 6: Notice, causes, and payload assembly

**Files:**
- Replace: `src/notice.rs` (stub from Task 4)
- Modify: `src/lib.rs` (`pub use crate::notice::Notice;`)

**Interfaces:**
- Consumes: `Frame`/`capture` (Task 5), `Breadcrumb` (Task 3), `Config` (Task 4), `Sanitizer` (Task 2).
- Produces (consumed by Task 9 pipeline, Task 11 panic):
  - `pub struct Notice` — private fields: `class: String`, `message: String`, `causes: Vec<Cause>`, `raw_backtrace: Option<backtrace::Backtrace>` (unresolved; resolution deferred to assembly where `config.root` is known), `frames: Option<Vec<Frame>>` (set when pre-processed, e.g. panic path), `fingerprint: Option<String>`, `tags: Vec<String>`, `context: serde_json::Map<String, Value>`
  - Constructors: `Notice::from_error<E: std::error::Error + ?Sized>(&E) -> Notice` (captures raw backtrace), `Notice::message(class: &str, message: &str) -> Notice` (no backtrace)
  - Consuming builder methods (spec API): `class(self, impl Into<String>)`, `tags<I>(self, I)`, `fingerprint(self, impl Into<String>)`, `context<I: IntoIterator<Item=(K, Value)>>(self, I)`
  - Hook-facing mutators: `set_class`, `set_message`, `set_fingerprint(Option<String>)`, `add_tag`, `set_context(key, value)`
  - Read accessors: `error_class() -> &str`, `error_message() -> &str`, `get_context() -> &Map<String, Value>`, `get_tags() -> &[String]`
  - `pub(crate) fn merge_scope_context(&mut self, scope: Map<String, Value>)` — notice-local keys win
  - `pub(crate) fn assemble(notice: &Notice, config: &Config, breadcrumbs: Option<Vec<Breadcrumb>>, frames: Option<Vec<Frame>>, pid: u32) -> serde_json::Value` — the full wire payload
  - `pub(crate) struct Cause { class: String, message: String }`; `pub(crate) fn first_line_255(&str) -> String`

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fmt;

    #[derive(Debug)]
    struct Inner;
    impl fmt::Display for Inner {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result { write!(f, "inner cause") }
    }
    impl std::error::Error for Inner {}

    #[derive(Debug)]
    struct Outer(Inner);
    impl fmt::Display for Outer {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result { write!(f, "outer failed") }
    }
    impl std::error::Error for Outer {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { Some(&self.0) }
    }

    #[derive(Debug)]
    struct PanickyDisplay;
    impl fmt::Display for PanickyDisplay {
        fn fmt(&self, _: &mut fmt::Formatter) -> fmt::Result { panic!("bad Display") }
    }
    impl std::error::Error for PanickyDisplay {}

    fn test_config() -> crate::Config {
        crate::Config::builder()
            .env_source(|_| None)
            .api_key("k")
            .env("production")
            .root("/app")
            .hostname("web-1")
            .revision("abc123")
            .build()
            .unwrap()
    }

    #[test]
    fn test_from_error_typed_class_and_causes() {
        let n = Notice::from_error(&Outer(Inner));
        assert!(n.error_class().ends_with("Outer"));
        assert_eq!(n.error_message(), "outer failed");
        assert_eq!(n.causes.len(), 1);
        assert_eq!(n.causes[0].class, "inner cause");
        assert_eq!(n.causes[0].message, "inner cause");
    }

    #[test]
    fn test_from_error_dyn_falls_back_to_display() {
        let e: Box<dyn std::error::Error> = Box::new(Outer(Inner));
        let n = Notice::from_error(e.as_ref()); // &dyn Error
        assert_eq!(n.error_class(), "outer failed"); // first Display line, not "dyn ..."
    }

    #[test]
    fn test_panicking_display_is_caught() {
        let n = Notice::from_error(&PanickyDisplay);
        assert_eq!(n.error_message(), "<panic in Display>");
    }

    #[test]
    fn test_merge_scope_context_local_wins() {
        let mut n = Notice::message("X", "y").context([("shared", json!("local")), ("only_local", json!(1))]);
        let mut scope = serde_json::Map::new();
        scope.insert("shared".into(), json!("scope"));
        scope.insert("only_scope".into(), json!(2));
        n.merge_scope_context(scope);
        assert_eq!(n.get_context()["shared"], json!("local"));
        assert_eq!(n.get_context()["only_scope"], json!(2));
        assert_eq!(n.get_context()["only_local"], json!(1));
    }

    #[test]
    fn test_golden_payload() {
        let notice = Notice::message("PaymentError", "card declined")
            .tags(["checkout"])
            .fingerprint("fp-1")
            .context([("user_id", json!(7)), ("request_id", json!("req-9"))]);
        let crumbs = vec![crate::breadcrumbs::Breadcrumb::with_timestamp(
            "clicked pay", "ui", None, "2026-07-22T00:00:00.000Z".into(),
        )];
        let frames = vec![crate::bt::Frame {
            number: Some(42),
            file: Some("[PROJECT_ROOT]/src/main.rs".into()),
            method: Some("my_app::run".into()),
            source: None,
        }];
        let payload = assemble(&notice, &test_config(), Some(crumbs), Some(frames), 12345);
        assert_eq!(
            payload,
            json!({
                "notifier": {
                    "name": "honeybadger-rust",
                    "url": "https://github.com/honeybadger-io/honeybadger-rust",
                    "version": env!("CARGO_PKG_VERSION"),
                    "language": "rust"
                },
                "breadcrumbs": {
                    "enabled": true,
                    "trail": [{"message": "clicked pay", "category": "ui", "metadata": {}, "timestamp": "2026-07-22T00:00:00.000Z"}]
                },
                "error": {
                    "class": "PaymentError",
                    "message": "card declined",
                    "backtrace": [{"number": "42", "file": "[PROJECT_ROOT]/src/main.rs", "method": "my_app::run"}],
                    "fingerprint": "fp-1",
                    "tags": ["checkout"],
                    "causes": []
                },
                "request": {"context": {"user_id": 7, "request_id": "req-9"}},
                "server": {
                    "project_root": "/app",
                    "revision": "abc123",
                    "environment_name": "production",
                    "hostname": "web-1",
                    "pid": 12345
                },
                "correlation_context": {"request_id": "req-9"}
            })
        );
    }

    #[test]
    fn test_payload_omissions() {
        // No env, no revision, no fingerprint, no request_id, no breadcrumbs/backtrace.
        let cfg = crate::Config::builder()
            .env_source(|_| None).api_key("k").enabled(true).root("/app").hostname("h").build().unwrap();
        let payload = assemble(&Notice::message("X", "y"), &cfg, None, None, 1);
        assert_eq!(payload["server"].get("environment_name"), None);
        assert_eq!(payload["server"].get("revision"), None);
        assert_eq!(payload["error"]["fingerprint"], json!(null));
        assert_eq!(payload.get("correlation_context"), None);
        assert_eq!(payload["breadcrumbs"], json!({"enabled": true, "trail": []}));
        assert_eq!(payload["error"]["backtrace"], json!([]));
    }

    #[test]
    fn test_causes_capped_at_five() {
        #[derive(Debug)]
        struct Chain(usize);
        impl fmt::Display for Chain {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result { write!(f, "link {}", self.0) }
        }
        impl std::error::Error for Chain {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                if self.0 < 10 { Some(Box::leak(Box::new(Chain(self.0 + 1)))) } else { None }
            }
        }
        let n = Notice::from_error(&Chain(0));
        assert_eq!(n.causes.len(), 5);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test notice` — expected: compile failure (stub Notice).

- [ ] **Step 3: Implement `src/notice.rs`**

```rust
//! Notice: the error payload and its assembly into the wire format (spec "Notice payload").
use crate::breadcrumbs::Breadcrumb;
use crate::bt::Frame;
use crate::config::Config;
use serde_json::{json, Map, Value};
use std::panic::{catch_unwind, AssertUnwindSafe};

const MAX_CAUSES: usize = 5;
const NOTIFIER_NAME: &str = "honeybadger-rust";
const NOTIFIER_URL: &str = "https://github.com/honeybadger-io/honeybadger-rust";
const VERSION: &str = env!("CARGO_PKG_VERSION");
const DISPLAY_PANIC: &str = "<panic in Display>";

pub(crate) struct Cause {
    pub(crate) class: String,
    pub(crate) message: String,
}

pub struct Notice {
    pub(crate) class: String,
    pub(crate) message: String,
    pub(crate) causes: Vec<Cause>,
    pub(crate) raw_backtrace: Option<backtrace::Backtrace>,
    pub(crate) frames: Option<Vec<Frame>>,
    pub(crate) fingerprint: Option<String>,
    pub(crate) tags: Vec<String>,
    pub(crate) context: Map<String, Value>,
}

impl Notice {
    pub fn from_error<E: std::error::Error + ?Sized>(error: &E) -> Notice {
        let message = safe_display(error);
        let type_name = std::any::type_name::<E>();
        let class = if type_name.starts_with("dyn ") {
            first_line_255(&message)
        } else {
            type_name.to_owned()
        };
        let mut causes = Vec::new();
        let mut source = error.source();
        while let Some(cause) = source {
            if causes.len() == MAX_CAUSES {
                break;
            }
            let msg = safe_display(cause);
            causes.push(Cause { class: first_line_255(&msg), message: msg });
            source = cause.source();
        }
        Notice {
            class,
            message,
            causes,
            raw_backtrace: Some(backtrace::Backtrace::new_unresolved()),
            frames: None,
            fingerprint: None,
            tags: Vec::new(),
            context: Map::new(),
        }
    }

    pub fn message(class: &str, message: &str) -> Notice {
        Notice {
            class: class.to_owned(),
            message: message.to_owned(),
            causes: Vec::new(),
            raw_backtrace: None,
            frames: None,
            fingerprint: None,
            tags: Vec::new(),
            context: Map::new(),
        }
    }

    // Consuming builder methods (spec public API).
    pub fn class(mut self, class: impl Into<String>) -> Self { self.class = class.into(); self }
    pub fn tags<I, S>(mut self, tags: I) -> Self
    where I: IntoIterator<Item = S>, S: Into<String> {
        self.tags.extend(tags.into_iter().map(Into::into)); self
    }
    pub fn fingerprint(mut self, fp: impl Into<String>) -> Self { self.fingerprint = Some(fp.into()); self }
    pub fn context<I, K>(mut self, entries: I) -> Self
    where I: IntoIterator<Item = (K, Value)>, K: Into<String> {
        for (k, v) in entries { self.context.insert(k.into(), v); }
        self
    }

    // Hook-facing mutators.
    pub fn set_class(&mut self, class: impl Into<String>) { self.class = class.into(); }
    pub fn set_message(&mut self, message: impl Into<String>) { self.message = message.into(); }
    pub fn set_fingerprint(&mut self, fp: Option<String>) { self.fingerprint = fp; }
    pub fn add_tag(&mut self, tag: impl Into<String>) { self.tags.push(tag.into()); }
    pub fn set_context(&mut self, key: impl Into<String>, value: impl Into<Value>) {
        self.context.insert(key.into(), value.into());
    }

    // Read accessors.
    pub fn error_class(&self) -> &str { &self.class }
    pub fn error_message(&self) -> &str { &self.message }
    pub fn get_context(&self) -> &Map<String, Value> { &self.context }
    pub fn get_tags(&self) -> &[String] { &self.tags }

    /// Scope context merges UNDER notice-local context (local wins).
    pub(crate) fn merge_scope_context(&mut self, scope: Map<String, Value>) {
        for (k, v) in scope {
            self.context.entry(k).or_insert(v);
        }
    }
}

pub(crate) fn safe_display<E: std::fmt::Display + ?Sized>(value: &E) -> String {
    catch_unwind(AssertUnwindSafe(|| value.to_string())).unwrap_or_else(|_| DISPLAY_PANIC.to_owned())
}

pub(crate) fn first_line_255(s: &str) -> String {
    let line = s.lines().next().unwrap_or_default();
    let mut cut = line.len().min(255);
    while !line.is_char_boundary(cut) {
        cut -= 1;
    }
    line[..cut].to_owned()
}

pub(crate) fn assemble(
    notice: &Notice,
    config: &Config,
    breadcrumbs: Option<Vec<Breadcrumb>>,
    frames: Option<Vec<Frame>>,
    pid: u32,
) -> Value {
    let backtrace: Vec<Value> = frames
        .unwrap_or_default()
        .into_iter()
        .map(|f| {
            let mut obj = Map::new();
            if let Some(n) = f.number { obj.insert("number".into(), json!(n.to_string())); }
            if let Some(file) = f.file { obj.insert("file".into(), json!(file)); }
            if let Some(m) = f.method { obj.insert("method".into(), json!(m)); }
            if let Some(s) = f.source { obj.insert("source".into(), json!(s)); }
            Value::Object(obj)
        })
        .collect();

    let mut server = Map::new();
    server.insert("project_root".into(), json!(config.root));
    if let Some(rev) = &config.revision { server.insert("revision".into(), json!(rev)); }
    if let Some(env) = &config.env { server.insert("environment_name".into(), json!(env)); }
    server.insert("hostname".into(), json!(config.hostname));
    server.insert("pid".into(), json!(pid));

    let mut payload = Map::new();
    payload.insert("notifier".into(), json!({
        "name": NOTIFIER_NAME, "url": NOTIFIER_URL, "version": VERSION, "language": "rust",
    }));
    payload.insert("breadcrumbs".into(), json!({
        "enabled": config.breadcrumbs_enabled,
        "trail": breadcrumbs.unwrap_or_default(),
    }));
    payload.insert("error".into(), json!({
        "class": notice.class,
        "message": notice.message,
        "backtrace": backtrace,
        "fingerprint": notice.fingerprint,
        "tags": notice.tags,
        "causes": notice.causes.iter().map(|c| json!({"class": c.class, "message": c.message})).collect::<Vec<_>>(),
    }));
    payload.insert("request".into(), json!({ "context": notice.context }));
    payload.insert("server".into(), Value::Object(server));
    if let Some(request_id) = notice.context.get("request_id") {
        payload.insert("correlation_context".into(), json!({ "request_id": request_id }));
    }
    Value::Object(payload)
}
```

Update `src/lib.rs`: `pub use crate::notice::Notice;`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test` — all green.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: Notice with source-chain causes, hook mutators, and golden payload assembly"
```

---

### Task 7: Transport

**Files:**
- Create: `src/transport.rs`
- Modify: `src/lib.rs` (`mod transport;`, `pub use crate::transport::{Transport, TransportRequest, RequestKind, TestTransport};`)

**Interfaces:**
- Consumes: `Config` fields (endpoint, api_key, timeouts).
- Produces (consumed by Tasks 8, 9, 11):
  - `pub enum RequestKind { Notices }` (`#[non_exhaustive]`)
  - `pub struct TransportRequest<'a> { pub kind: RequestKind, pub path: &'a str, pub content_type: &'a str, pub body: &'a [u8], pub urgent: bool }` (`#[non_exhaustive]` — construct via `TransportRequest::notices(body, urgent)`)
  - `pub trait Transport: Send + Sync { fn deliver(&self, req: &TransportRequest) -> Result<u16, TransportError>; }`
  - `pub struct TransportError(pub String)`
  - `pub(crate) struct ServerTransport` — `ServerTransport::new(endpoint, api_key, connect_timeout, request_timeout) -> Self`; owns two prebuilt ureq agents (normal 2s/5s, urgent 1s/2s); **compresses nothing** (bodies arrive pre-compressed)
  - `pub(crate) struct NullTransport` — logs at debug, returns `Ok(201)`
  - `pub struct TestTransport` — `TestTransport::new()`, `requests(&self) -> Vec<CapturedRequest>` where `CapturedRequest { pub path: String, pub content_type: String, pub body: Vec<u8>, pub urgent: bool }`; `respond_with(&self, status: u16)` to program the next responses (default 201)
  - `pub(crate) fn compress(body: &[u8]) -> Vec<u8>` (zlib deflate) and `pub(crate) fn user_agent() -> String` (`"Honeybadger Rust {version}"`)

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn inflate(body: &[u8]) -> String {
        let mut out = String::new();
        flate2::read::ZlibDecoder::new(body).read_to_string(&mut out).unwrap();
        out
    }

    #[test]
    fn test_compress_round_trip() {
        let compressed = compress(b"{\"a\":1}");
        assert_eq!(inflate(&compressed), "{\"a\":1}");
    }

    #[test]
    fn test_test_transport_captures_and_responds() {
        let t = TestTransport::new();
        t.respond_with(429);
        let body = compress(b"{}");
        let req = TransportRequest::notices(&body, false);
        assert_eq!(t.deliver(&req).unwrap(), 429);
        assert_eq!(t.deliver(&req).unwrap(), 201); // programmed statuses consumed; default 201
        let captured = t.requests();
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].path, "/v1/notices");
        assert_eq!(inflate(&captured[0].body), "{}");
    }

    #[test]
    fn test_server_transport_headers_and_deflate() {
        let mut server = mockito::Server::new();
        let mock = server
            .mock("POST", "/v1/notices")
            .match_header("X-API-Key", "test-key")
            .match_header("Content-Type", "application/json")
            .match_header("Content-Encoding", "deflate")
            .match_header("User-Agent", user_agent().as_str())
            .with_status(201)
            .create();
        let t = ServerTransport::new(
            server.url(),
            "test-key".into(),
            std::time::Duration::from_secs(2),
            std::time::Duration::from_secs(5),
        );
        let body = compress(b"{\"api\":\"payload\"}");
        let status = t.deliver(&TransportRequest::notices(&body, false)).unwrap();
        assert_eq!(status, 201);
        mock.assert();
    }

    #[test]
    fn test_server_transport_non_2xx_is_ok_status_not_err() {
        let mut server = mockito::Server::new();
        server.mock("POST", "/v1/notices").with_status(429).create();
        let t = ServerTransport::new(server.url(), "k".into(),
            std::time::Duration::from_secs(2), std::time::Duration::from_secs(5));
        let body = compress(b"{}");
        assert_eq!(t.deliver(&TransportRequest::notices(&body, false)).unwrap(), 429);
    }

    #[test]
    fn test_server_transport_connection_refused_is_err() {
        // Port 1 on localhost: nothing listens there.
        let t = ServerTransport::new("http://127.0.0.1:1".into(), "k".into(),
            std::time::Duration::from_millis(200), std::time::Duration::from_millis(200));
        let body = compress(b"{}");
        assert!(t.deliver(&TransportRequest::notices(&body, false)).is_err());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test transport` — expected: compile failure.

- [ ] **Step 3: Implement `src/transport.rs`**

```rust
//! Transport: the HTTP seam (spec "Transport"). Bodies arrive pre-compressed.
use flate2::write::ZlibEncoder;
use flate2::Compression;
use std::io::Write;
use std::sync::Mutex;
use std::time::Duration;

pub(crate) const NOTICES_PATH: &str = "/v1/notices";

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestKind {
    Notices,
}

#[non_exhaustive]
pub struct TransportRequest<'a> {
    pub kind: RequestKind,
    pub path: &'a str,
    pub content_type: &'a str,
    pub body: &'a [u8],
    pub urgent: bool,
}

impl<'a> TransportRequest<'a> {
    pub fn notices(body: &'a [u8], urgent: bool) -> Self {
        TransportRequest {
            kind: RequestKind::Notices,
            path: NOTICES_PATH,
            content_type: "application/json",
            body,
            urgent,
        }
    }
}

#[derive(Debug)]
pub struct TransportError(pub String);

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "transport error: {}", self.0)
    }
}
impl std::error::Error for TransportError {}

pub trait Transport: Send + Sync {
    fn deliver(&self, req: &TransportRequest) -> Result<u16, TransportError>;
}

pub(crate) fn compress(body: &[u8]) -> Vec<u8> {
    let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
    // Writing to a Vec cannot fail; fall back to uncompressed on the impossible path.
    if enc.write_all(body).is_err() {
        return body.to_vec();
    }
    enc.finish().unwrap_or_else(|_| body.to_vec())
}

pub(crate) fn user_agent() -> String {
    format!("Honeybadger Rust {}", env!("CARGO_PKG_VERSION"))
}

// ---------- Server ----------

pub(crate) struct ServerTransport {
    endpoint: String,
    api_key: String,
    agent: ureq::Agent,
    urgent_agent: ureq::Agent,
    user_agent: String,
}

fn build_agent(connect: Duration, total: Duration) -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_connect(Some(connect))
        .timeout_global(Some(total))
        .http_status_as_error(false) // every status returns Ok(response)
        .build()
        .new_agent()
}

impl ServerTransport {
    pub(crate) fn new(endpoint: String, api_key: String, connect: Duration, request: Duration) -> Self {
        ServerTransport {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            api_key,
            agent: build_agent(connect, request),
            urgent_agent: build_agent(Duration::from_secs(1), Duration::from_secs(2)),
            user_agent: user_agent(),
        }
    }
}

impl Transport for ServerTransport {
    fn deliver(&self, req: &TransportRequest) -> Result<u16, TransportError> {
        let url = format!("{}{}", self.endpoint, req.path);
        let agent = if req.urgent { &self.urgent_agent } else { &self.agent };
        agent
            .post(&url)
            .header("X-API-Key", &self.api_key)
            .header("Content-Type", req.content_type)
            .header("Accept", "application/json")
            .header("Content-Encoding", "deflate")
            .header("User-Agent", &self.user_agent)
            .send(req.body)
            .map(|resp| resp.status().as_u16())
            .map_err(|e| TransportError(e.to_string()))
    }
}

// ---------- Null ----------

pub(crate) struct NullTransport;

impl Transport for NullTransport {
    fn deliver(&self, req: &TransportRequest) -> Result<u16, TransportError> {
        log::debug!("honeybadger: reporting disabled; dropping {} bytes to {}", req.body.len(), req.path);
        Ok(201)
    }
}

// ---------- Test ----------

pub struct CapturedRequest {
    pub path: String,
    pub content_type: String,
    pub body: Vec<u8>,
    pub urgent: bool,
}

#[derive(Default)]
pub struct TestTransport {
    requests: Mutex<Vec<CapturedRequest>>,
    responses: Mutex<Vec<u16>>,
}

impl TestTransport {
    pub fn new() -> Self {
        TestTransport::default()
    }

    /// Queue a status for the next delivery (FIFO). Unqueued deliveries return 201.
    pub fn respond_with(&self, status: u16) {
        self.responses.lock().unwrap_or_else(|e| e.into_inner()).push(status);
    }

    pub fn requests(&self) -> Vec<CapturedRequest> {
        let lock = self.requests.lock().unwrap_or_else(|e| e.into_inner());
        lock.iter()
            .map(|r| CapturedRequest {
                path: r.path.clone(),
                content_type: r.content_type.clone(),
                body: r.body.clone(),
                urgent: r.urgent,
            })
            .collect()
    }
}

impl Transport for TestTransport {
    fn deliver(&self, req: &TransportRequest) -> Result<u16, TransportError> {
        self.requests.lock().unwrap_or_else(|e| e.into_inner()).push(CapturedRequest {
            path: req.path.to_owned(),
            content_type: req.content_type.to_owned(),
            body: req.body.to_vec(),
            urgent: req.urgent,
        });
        let mut responses = self.responses.lock().unwrap_or_else(|e| e.into_inner());
        Ok(if responses.is_empty() { 201 } else { responses.remove(0) })
    }
}
```

Add to `src/lib.rs`: `mod transport;` and `pub use crate::transport::{RequestKind, TestTransport, Transport, TransportError, TransportRequest};`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test` — all green. (ureq 3 API drift note from Global Constraints applies to `build_agent` and `.send(...)`.)

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: Transport trait with Server (ureq/deflate), Null, and Test implementations"
```

---

### Task 8: Worker

**Files:**
- Create: `src/worker.rs`
- Modify: `src/lib.rs` (`mod worker;`)

**Interfaces:**
- Consumes: `Transport`, `TransportRequest`, `compress` (Task 7).
- Produces (consumed by Task 9):
  - `pub(crate) struct WorkerHandle`: `try_enqueue(&self, payload: Vec<u8>) -> bool` (false = dropped: full or worker gone; caller logs), `flush(&self, timeout: Duration) -> bool`, `shutdown(&self, timeout: Duration)`
  - `pub(crate) fn spawn(transport: Arc<dyn Transport>, queue_size: usize) -> std::io::Result<WorkerHandle>`
  - `pub(crate) fn throttle_interval(n: u32) -> Duration` (pure, saturated)
  - Test-only knob: `pub(crate) fn spawn_with_intervals(transport, queue_size, suspend_interval: Duration) -> std::io::Result<WorkerHandle>` — production `spawn` passes 3600s.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{TestTransport, compress};
    use std::sync::Arc;
    use std::time::Duration;

    fn payload() -> Vec<u8> {
        compress(b"{}")
    }

    #[test]
    fn test_throttle_interval_shape_and_saturation() {
        assert_eq!(throttle_interval(0), Duration::ZERO);
        assert!(throttle_interval(10) > Duration::ZERO);
        assert!(throttle_interval(10) < throttle_interval(50));
        assert_eq!(throttle_interval(10_000), Duration::from_secs(300)); // saturated, no panic
        assert_eq!(throttle_interval(u32::MAX), Duration::from_secs(300));
    }

    #[test]
    fn test_delivers_and_flush_barrier() {
        let transport = Arc::new(TestTransport::new());
        let w = spawn(transport.clone(), 10).unwrap();
        for _ in 0..3 {
            assert!(w.try_enqueue(payload()));
        }
        assert!(w.flush(Duration::from_secs(5)));
        assert_eq!(transport.requests().len(), 3);
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_queue_overflow_drops() {
        let transport = Arc::new(TestTransport::new());
        // Suspend delivery by pre-programming a 402 on the FIRST send, so the worker
        // enters suspension and stops consuming; then overfill the queue.
        transport.respond_with(402);
        let w = spawn_with_intervals(transport.clone(), 2, Duration::from_secs(30)).unwrap();
        assert!(w.try_enqueue(payload())); // consumed, triggers suspension
        std::thread::sleep(Duration::from_millis(200)); // let suspension start
        let mut accepted = 0;
        for _ in 0..10 {
            if w.try_enqueue(payload()) {
                accepted += 1;
            }
        }
        assert!(accepted <= 2, "bounded queue must reject overflow (accepted {accepted})");
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_suspend_on_403_drops_queue_but_flush_and_shutdown_work() {
        let transport = Arc::new(TestTransport::new());
        transport.respond_with(403);
        let w = spawn_with_intervals(transport.clone(), 10, Duration::from_secs(30)).unwrap();
        w.try_enqueue(payload()); // 403 → suspended
        std::thread::sleep(Duration::from_millis(200));
        w.try_enqueue(payload()); // lands in queue, will be dropped by suspension drain
        assert!(w.flush(Duration::from_secs(2)), "flush must ack while suspended");
        assert_eq!(transport.requests().len(), 1, "no delivery while suspended");
        w.shutdown(Duration::from_secs(2)); // must return promptly despite 30s suspension
    }

    #[test]
    fn test_throttle_429_slows_but_continues_and_recovers() {
        let transport = Arc::new(TestTransport::new());
        transport.respond_with(429);
        let w = spawn(transport.clone(), 10).unwrap();
        w.try_enqueue(payload()); // 429 → throttle n=1 (~0.05s pause)
        w.try_enqueue(payload()); // 201 → n back to 0
        w.try_enqueue(payload());
        assert!(w.flush(Duration::from_secs(10)));
        assert_eq!(transport.requests().len(), 3);
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_enqueue_after_shutdown_returns_false() {
        let transport = Arc::new(TestTransport::new());
        let w = spawn(transport, 10).unwrap();
        w.shutdown(Duration::from_secs(5));
        assert!(!w.try_enqueue(payload()));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test worker` — expected: compile failure.

- [ ] **Step 3: Implement `src/worker.rs`**

```rust
//! The delivery worker: dedicated OS thread, bounded notice channel + unbounded
//! control channel, throttle/suspend semantics (spec "Delivery architecture").
use crate::transport::{Transport, TransportRequest};
use crossbeam_channel::{bounded, unbounded, Receiver, RecvTimeoutError, Sender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const MAX_THROTTLE_EXP: i32 = 150;
const MAX_THROTTLE_INTERVAL: Duration = Duration::from_secs(300);
const SUSPEND_INTERVAL: Duration = Duration::from_secs(3600);

pub(crate) enum Control {
    Flush(Sender<bool>),
    Shutdown(Sender<()>),
}

pub(crate) struct WorkerHandle {
    notices: Sender<Vec<u8>>,
    control: Sender<Control>,
    join: Mutex<Option<JoinHandle<()>>>,
}

pub(crate) fn throttle_interval(n: u32) -> Duration {
    if n == 0 {
        return Duration::ZERO;
    }
    let exp = (n as i64).min(MAX_THROTTLE_EXP as i64) as i32;
    let secs = 1.05f64.powi(exp) - 1.0;
    if !secs.is_finite() || secs <= 0.0 {
        return Duration::ZERO;
    }
    MAX_THROTTLE_INTERVAL.min(Duration::from_secs_f64(secs.min(300.0)))
}

pub(crate) fn spawn(transport: Arc<dyn Transport>, queue_size: usize) -> std::io::Result<WorkerHandle> {
    spawn_with_intervals(transport, queue_size, SUSPEND_INTERVAL)
}

pub(crate) fn spawn_with_intervals(
    transport: Arc<dyn Transport>,
    queue_size: usize,
    suspend_interval: Duration,
) -> std::io::Result<WorkerHandle> {
    let (notice_tx, notice_rx) = bounded(queue_size);
    let (control_tx, control_rx) = unbounded();
    let join = std::thread::Builder::new()
        .name("honeybadger-worker".into())
        .spawn(move || Worker { transport, notices: notice_rx, control: control_rx, throttle: 0, suspend_interval }.run())?;
    Ok(WorkerHandle { notices: notice_tx, control: control_tx, join: Mutex::new(Some(join)) })
}

impl WorkerHandle {
    /// Returns false if the payload was dropped (queue full or worker gone).
    pub(crate) fn try_enqueue(&self, payload: Vec<u8>) -> bool {
        match self.notices.try_send(payload) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => false,
        }
    }

    pub(crate) fn flush(&self, timeout: Duration) -> bool {
        let (ack_tx, ack_rx) = bounded(1);
        if self.control.send(Control::Flush(ack_tx)).is_err() {
            return false;
        }
        ack_rx.recv_timeout(timeout).unwrap_or(false)
    }

    pub(crate) fn shutdown(&self, timeout: Duration) {
        let (ack_tx, ack_rx) = bounded(1);
        if self.control.send(Control::Shutdown(ack_tx)).is_err() {
            return;
        }
        // Worker acks right before exiting; bounded wait, then join (instant) or detach.
        if ack_rx.recv_timeout(timeout).is_ok() {
            if let Some(handle) = self.join.lock().unwrap_or_else(|e| e.into_inner()).take() {
                let _ = handle.join();
            }
        } else {
            log::warn!("honeybadger: worker did not stop within {timeout:?}; detaching");
            // Dropping the JoinHandle detaches; the thread exits after its current send.
            self.join.lock().unwrap_or_else(|e| e.into_inner()).take();
        }
    }
}

enum SendOutcome {
    Continue,
    Suspend,
}

struct Worker {
    transport: Arc<dyn Transport>,
    notices: Receiver<Vec<u8>>,
    control: Receiver<Control>,
    throttle: u32,
    suspend_interval: Duration,
}

impl Worker {
    fn run(mut self) {
        loop {
            crossbeam_channel::select! {
                recv(self.control) -> msg => match msg {
                    Ok(control) => if self.handle_control(control) { return; },
                    Err(_) => return, // all handles dropped
                },
                recv(self.notices) -> msg => match msg {
                    Ok(payload) => {
                        match self.send_one(&payload) {
                            SendOutcome::Suspend => if self.suspended_wait() { return; },
                            SendOutcome::Continue => if self.throttle_pause() { return; },
                        }
                    }
                    Err(_) => return,
                },
            }
        }
    }

    /// Returns true when the worker should exit.
    fn handle_control(&mut self, control: Control) -> bool {
        match control {
            Control::Flush(ack) => {
                self.drain_and_send();
                let _ = ack.send(true);
                false
            }
            Control::Shutdown(ack) => {
                if self.throttle == 0 {
                    self.drain_and_send();
                } else {
                    let dropped = self.drain_and_drop();
                    if dropped > 0 {
                        log::warn!("honeybadger: dropping {dropped} queued notices at shutdown (throttled)");
                    }
                }
                let _ = ack.send(());
                true
            }
        }
    }

    /// Barrier semantics: everything already in the notice channel is processed.
    fn drain_and_send(&mut self) {
        while let Ok(payload) = self.notices.try_recv() {
            if matches!(self.send_one(&payload), SendOutcome::Suspend) {
                let dropped = self.drain_and_drop();
                log::warn!("honeybadger: suspended during flush; dropped {dropped} queued notices");
                return;
            }
        }
    }

    fn drain_and_drop(&mut self) -> usize {
        let mut count = 0;
        while self.notices.try_recv().is_ok() {
            count += 1;
        }
        count
    }

    fn send_one(&mut self, payload: &[u8]) -> SendOutcome {
        let req = TransportRequest::notices(payload, false);
        match self.transport.deliver(&req) {
            Ok(status) if (200..300).contains(&status) => {
                self.throttle = self.throttle.saturating_sub(1);
                SendOutcome::Continue
            }
            Ok(429) | Ok(503) => {
                self.throttle = self.throttle.saturating_add(1);
                log::debug!("honeybadger: throttled (n={})", self.throttle);
                SendOutcome::Continue
            }
            Ok(402) => {
                log::warn!("honeybadger: payment required; suspending delivery for {:?}", self.suspend_interval);
                SendOutcome::Suspend
            }
            Ok(403) => {
                log::warn!("honeybadger: unauthorized (bad API key or inactive account); suspending delivery for {:?}", self.suspend_interval);
                SendOutcome::Suspend
            }
            Ok(413) => {
                log::warn!("honeybadger: payload too large; notice dropped");
                SendOutcome::Continue
            }
            Ok(status) => {
                log::warn!("honeybadger: unexpected API status {status}; notice dropped");
                SendOutcome::Continue
            }
            Err(e) => {
                log::warn!("honeybadger: delivery failed: {e}");
                SendOutcome::Continue
            }
        }
    }

    /// Interruptible throttle pause. Returns true when the worker should exit.
    fn throttle_pause(&mut self) -> bool {
        let pause = throttle_interval(self.throttle);
        if pause.is_zero() {
            return false;
        }
        match self.control.recv_timeout(pause) {
            Ok(control) => self.handle_control(control),
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => true,
        }
    }

    /// Suspension: drop the queue, wait out the interval servicing control.
    /// Returns true when the worker should exit.
    fn suspended_wait(&mut self) -> bool {
        let dropped = self.drain_and_drop();
        if dropped > 0 {
            log::warn!("honeybadger: dropped {dropped} queued notices (suspended)");
        }
        let deadline = Instant::now() + self.suspend_interval;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                self.throttle = 0; // reset on resume
                self.drain_and_drop(); // anything accepted while suspended is stale
                return false;
            }
            match self.control.recv_timeout(remaining) {
                Ok(Control::Flush(ack)) => {
                    self.drain_and_drop();
                    let _ = ack.send(true); // queue empty by definition while suspended
                }
                Ok(Control::Shutdown(ack)) => {
                    self.drain_and_drop();
                    let _ = ack.send(());
                    return true;
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return true,
            }
        }
    }
}
```

Add `mod worker;` to `src/lib.rs`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test worker` — expected: 6 pass (timing-sensitive tests use generous margins). Then `cargo test` — all green.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: delivery worker with throttle, suspension, flush barrier, bounded shutdown"
```

---

### Task 9: Client and the notify pipeline

**Files:**
- Create: `src/client.rs`
- Modify: `src/lib.rs` (`mod client;`, `pub use crate::client::{Client, ClientBuilder};`)

**Interfaces:**
- Consumes: everything from Tasks 2–8.
- Produces (consumed by Tasks 10–11):
  - `pub struct Client` (cheap `Clone` via `Arc`): `Client::new(config: Config) -> Result<Client, Error>`; `Client::builder(config) -> ClientBuilder` with `.transport(Arc<dyn Transport>)` and `.build() -> Result<Client, Error>`
  - Methods: `notify<E: std::error::Error + ?Sized>(&self, &E)`, `notify_notice(&self, Notice)`, `context<I: IntoIterator<Item=(K, Value)>, K: Into<String>>(&self, I)` (Null value removes the key), `clear_context(&self)`, `add_breadcrumb(&self, message: &str, category: &str, metadata: Option<Map<String, Value>>)`, `flush(&self, Duration) -> bool`, `shutdown(&self, Duration)`
  - `pub(crate) fn deliver_now(&self, notice: Notice) -> ()` — full pipeline, then **urgent synchronous** transport call bypassing the worker (panic path; Task 11)
  - `pub(crate) fn wants_panic_hook(&self) -> bool`
  - `pub(crate) const MAX_PAYLOAD_BYTES: usize = 1_048_576;`

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::TestTransport;
    use serde_json::json;
    use std::io::Read;
    use std::sync::Arc;
    use std::time::Duration;

    fn test_client(transport: Arc<TestTransport>) -> Client {
        test_client_with(transport, |b| b)
    }

    fn test_client_with(
        transport: Arc<TestTransport>,
        f: impl FnOnce(crate::ConfigBuilder) -> crate::ConfigBuilder,
    ) -> Client {
        let builder = crate::Config::builder()
            .env_source(|_| None)
            .api_key("k")
            .env("production")
            .root("/app")
            .hostname("h");
        let config = f(builder).build().unwrap();
        Client::builder(config).transport(transport).build().unwrap()
    }

    fn delivered(transport: &TestTransport) -> Vec<serde_json::Value> {
        transport
            .requests()
            .iter()
            .map(|r| {
                let mut s = String::new();
                flate2::read::ZlibDecoder::new(&r.body[..]).read_to_string(&mut s).unwrap();
                serde_json::from_str(&s).unwrap()
            })
            .collect()
    }

    #[test]
    fn test_notify_delivers_payload_with_scope_context_and_breadcrumbs() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.context([("user_id", json!(7))]);
        client.add_breadcrumb("step one", "custom", None);
        client.notify(&std::io::Error::other("boom"));
        assert!(client.flush(Duration::from_secs(5)));
        let payloads = delivered(&transport);
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0]["request"]["context"]["user_id"], json!(7));
        assert_eq!(payloads[0]["breadcrumbs"]["trail"][0]["message"], json!("step one"));
        assert_eq!(payloads[0]["error"]["message"], json!("boom"));
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_context_null_removes_key() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.context([("a", json!(1)), ("b", json!(2))]);
        client.context([("a", serde_json::Value::Null)]);
        client.notify_notice(crate::Notice::message("X", "y"));
        client.flush(Duration::from_secs(5));
        let payloads = delivered(&transport);
        assert_eq!(payloads[0]["request"]["context"].get("a"), None);
        assert_eq!(payloads[0]["request"]["context"]["b"], json!(2));
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_before_notify_mutates_and_halts_and_panics_are_caught() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client_with(transport.clone(), |b| {
            b.before_notify(|n| { n.add_tag("hooked"); true })
                .before_notify(|_| panic!("bad hook"))          // caught, treated as pass
                .before_notify(|n| n.error_class() != "Halted") // halts "Halted" notices
        });
        client.notify_notice(crate::Notice::message("Halted", "no"));
        client.notify_notice(crate::Notice::message("Kept", "yes"));
        client.flush(Duration::from_secs(5));
        let payloads = delivered(&transport);
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0]["error"]["class"], json!("Kept"));
        assert_eq!(payloads[0]["error"]["tags"], json!(["hooked"]));
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_ignore_classes_checked_before_and_after_hooks() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client_with(transport.clone(), |b| {
            b.ignore_classes(["Ignored"])
                .before_notify(|n| { if n.error_class() == "MakeIgnored" { n.set_class("Ignored"); } true })
        });
        client.notify_notice(crate::Notice::message("Ignored", "pre"));       // dropped pre-hook
        client.notify_notice(crate::Notice::message("MakeIgnored", "post")); // dropped post-hook
        client.notify_notice(crate::Notice::message("Kept", "k"));
        client.flush(Duration::from_secs(5));
        assert_eq!(delivered(&transport).len(), 1);
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_sanitization_covers_hook_introduced_secrets() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client_with(transport.clone(), |b| {
            b.before_notify(|n| { n.set_context("password", "hunter2"); true })
        });
        client.notify_notice(crate::Notice::message("X", "y"));
        client.flush(Duration::from_secs(5));
        let payloads = delivered(&transport);
        assert_eq!(payloads[0]["request"]["context"]["password"], json!("[FILTERED]"));
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_oversized_payload_dropped() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        let big = "x".repeat(MAX_PAYLOAD_BYTES); // sanitizer truncates strings at 64KB,
        // so build the oversize from many keys instead:
        let mut notice = crate::Notice::message("Big", "b");
        for i in 0..40 {
            notice.set_context(format!("k{i}"), json!("y".repeat(60_000)));
        }
        drop(big);
        client.notify_notice(notice);
        client.flush(Duration::from_secs(5));
        assert_eq!(delivered(&transport).len(), 0, "oversized payload must be dropped");
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_deliver_now_bypasses_worker_and_is_urgent() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.deliver_now(crate::Notice::message("panic", "argh"));
        // No flush needed: delivery was synchronous.
        let reqs = transport.requests();
        assert_eq!(reqs.len(), 1);
        assert!(reqs[0].urgent);
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_null_transport_when_env_excluded_no_api_key_needed() {
        let config = crate::Config::builder().env_source(|_| None).env("test").build().unwrap();
        let client = Client::new(config).unwrap(); // no api key, no panic, Null transport
        client.notify_notice(crate::Notice::message("X", "y"));
        assert!(client.flush(Duration::from_secs(5)));
        client.shutdown(Duration::from_secs(5));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test client` — expected: compile failure.

- [ ] **Step 3: Implement `src/client.rs`**

```rust
//! Client: shared state + the notify pipeline (spec "Notify pipeline").
use crate::breadcrumbs::{Breadcrumb, RingBuffer};
use crate::config::Config;
use crate::error::Error;
use crate::notice::{assemble, Notice};
use crate::sanitizer::Sanitizer;
use crate::transport::{NullTransport, ServerTransport, Transport, TransportRequest, compress};
use crate::worker::WorkerHandle;
use serde_json::{Map, Value};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(crate) const MAX_PAYLOAD_BYTES: usize = 1_048_576;

struct Inner {
    config: Config,
    sanitizer: Sanitizer,
    context: Mutex<Map<String, Value>>,
    breadcrumbs: Mutex<RingBuffer>,
    transport: Arc<dyn Transport>,
    worker: WorkerHandle,
}

#[derive(Clone)]
pub struct Client(Arc<Inner>);

pub struct ClientBuilder {
    config: Config,
    transport: Option<Arc<dyn Transport>>,
}

impl Client {
    pub fn new(config: Config) -> Result<Client, Error> {
        Client::builder(config).build()
    }

    pub fn builder(config: Config) -> ClientBuilder {
        ClientBuilder { config, transport: None }
    }

    pub fn notify<E: std::error::Error + ?Sized>(&self, error: &E) {
        self.notify_notice(Notice::from_error(error));
    }

    pub fn notify_notice(&self, notice: Notice) {
        if let Some(payload) = self.run_pipeline(notice) {
            if !self.0.worker.try_enqueue(payload) {
                log::warn!("honeybadger: notice dropped (queue full or worker stopped)");
            }
        }
    }

    /// Panic path: full pipeline, then synchronous urgent delivery bypassing the worker.
    pub(crate) fn deliver_now(&self, notice: Notice) {
        if let Some(payload) = self.run_pipeline(notice) {
            let req = TransportRequest::notices(&payload, true);
            match self.0.transport.deliver(&req) {
                Ok(status) if (200..300).contains(&status) => {}
                Ok(status) => log::warn!("honeybadger: urgent delivery got status {status}"),
                Err(e) => log::warn!("honeybadger: urgent delivery failed: {e}"),
            }
        }
    }

    /// Spec pipeline steps 1–6. Returns the compressed wire payload, or None if dropped.
    fn run_pipeline(&self, mut notice: Notice) -> Option<Vec<u8>> {
        let inner = &*self.0;

        // 1. Assembly inputs: scope context (local wins), breadcrumbs, backtrace frames.
        let scope = inner.context.lock().unwrap_or_else(|e| e.into_inner()).clone();
        notice.merge_scope_context(scope);
        let breadcrumbs = if inner.config.breadcrumbs_enabled && notice.frames.is_none() || inner.config.breadcrumbs_enabled {
            Some(inner.breadcrumbs.lock().unwrap_or_else(|e| e.into_inner()).snapshot())
        } else {
            None
        };
        let frames = match (notice.frames.take(), notice.raw_backtrace.take()) {
            (Some(frames), _) => Some(frames), // pre-processed (panic path)
            (None, Some(mut raw)) => {
                raw.resolve();
                Some(crate::bt::process_resolved(&raw, &inner.config.root))
            }
            (None, None) => None,
        };

        // 2. Ignore check (cheapest rejection first).
        if inner.config.ignore_classes.iter().any(|c| c == notice.error_class()) {
            return None;
        }

        // 3. before_notify hooks; panics caught and treated as pass.
        for hook in &inner.config.before_notify {
            let hook = hook.clone();
            let keep = catch_unwind(AssertUnwindSafe(|| hook(&mut notice))).unwrap_or_else(|_| {
                log::warn!("honeybadger: before_notify hook panicked; continuing");
                true
            });
            if !keep {
                return None;
            }
        }

        // 4. Ignore recheck (hooks may have changed the class).
        if inner.config.ignore_classes.iter().any(|c| c == notice.error_class()) {
            return None;
        }

        // 5. Sanitize last, so hook-introduced data is covered.
        let mut context_value = Value::Object(std::mem::take(&mut notice.context));
        inner.sanitizer.sanitize(&mut context_value);
        if let Value::Object(map) = context_value {
            notice.context = map;
        }
        let breadcrumbs = breadcrumbs.map(|crumbs| {
            crumbs
                .into_iter()
                .map(|mut crumb| {
                    let mut meta = Value::Object(std::mem::take(&mut crumb.metadata));
                    inner.sanitizer.sanitize_shallow(&mut meta);
                    if let Value::Object(m) = meta {
                        crumb.metadata = m;
                    }
                    crumb
                })
                .collect::<Vec<Breadcrumb>>()
        });

        // 6. Serialize + size cap + compress.
        let payload = assemble(&notice, &inner.config, breadcrumbs, frames, std::process::id());
        let bytes = match serde_json::to_vec(&payload) {
            Ok(b) => b,
            Err(e) => {
                log::warn!("honeybadger: failed to serialize notice: {e}");
                return None;
            }
        };
        if bytes.len() > MAX_PAYLOAD_BYTES {
            log::warn!("honeybadger: notice payload {} bytes exceeds 1 MiB cap; dropped", bytes.len());
            return None;
        }
        Some(compress(&bytes))
    }

    pub fn context<I, K>(&self, entries: I)
    where
        I: IntoIterator<Item = (K, Value)>,
        K: Into<String>,
    {
        let mut ctx = self.0.context.lock().unwrap_or_else(|e| e.into_inner());
        for (k, v) in entries {
            let key = k.into();
            if v.is_null() {
                ctx.remove(&key);
            } else {
                ctx.insert(key, v);
            }
        }
    }

    pub fn clear_context(&self) {
        self.0.context.lock().unwrap_or_else(|e| e.into_inner()).clear();
        self.0.breadcrumbs.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }

    pub fn add_breadcrumb(&self, message: &str, category: &str, metadata: Option<Map<String, Value>>) {
        if !self.0.config.breadcrumbs_enabled {
            return;
        }
        self.0
            .breadcrumbs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Breadcrumb::new(message, category, metadata));
    }

    pub fn flush(&self, timeout: Duration) -> bool {
        self.0.worker.flush(timeout)
    }

    pub fn shutdown(&self, timeout: Duration) {
        self.0.worker.shutdown(timeout);
    }

    pub(crate) fn wants_panic_hook(&self) -> bool {
        self.0.config.install_panic_hook
    }
}

impl ClientBuilder {
    pub fn transport(mut self, transport: Arc<dyn Transport>) -> Self {
        self.transport = Some(transport);
        self
    }

    pub fn build(self) -> Result<Client, Error> {
        let config = self.config;
        let transport: Arc<dyn Transport> = match self.transport {
            Some(t) => t,
            None if config.reporting_enabled() => {
                let api_key = config.api_key.clone().ok_or(Error::MissingApiKey)?;
                Arc::new(ServerTransport::new(
                    config.endpoint.clone(),
                    api_key,
                    config.connect_timeout,
                    config.request_timeout,
                ))
            }
            None => {
                if config.env.is_none() {
                    log::info!("honeybadger: env not set; reporting enabled by default");
                }
                Arc::new(NullTransport)
            }
        };
        let worker = crate::worker::spawn(transport.clone(), config.notice_queue_size)
            .map_err(Error::WorkerSpawn)?;
        let sanitizer = Sanitizer::new(config.filter_keys.iter());
        Ok(Client(Arc::new(Inner {
            config,
            sanitizer,
            context: Mutex::new(Map::new()),
            breadcrumbs: Mutex::new(RingBuffer::new()),
            transport,
            worker,
        })))
    }
}
```

Also add to `src/bt.rs` (used above — factors the resolved-backtrace walk out of `capture`):

```rust
pub(crate) fn process_resolved(bt: &backtrace::Backtrace, root: &str) -> Vec<Frame> {
    let mut frames = Vec::new();
    for frame in bt.frames() {
        for symbol in frame.symbols() {
            let name = symbol.name().map(|n| n.to_string());
            if let Some(f) = map_frame(name.as_deref(), symbol.filename(), symbol.lineno(), root) {
                frames.push(f);
                if frames.len() == MAX_FRAMES {
                    return frames;
                }
            }
        }
    }
    frames
}
```

and change `capture` to `backtrace::Backtrace::new()` followed by `process_resolved(&bt, root)` (delete its now-duplicated loop). Fix the breadcrumbs conditional in pipeline step 1 to simply:

```rust
        let breadcrumbs = inner.config.breadcrumbs_enabled.then(|| {
            inner.breadcrumbs.lock().unwrap_or_else(|e| e.into_inner()).snapshot()
        });
```

(the double-condition line above is a typo hazard — use this form). `Breadcrumb.metadata` needs `pub(crate)` visibility (already is, per Task 3).

Add to `src/lib.rs`: `mod client;`, `pub use crate::client::{Client, ClientBuilder};`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test` — all green.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: Client with full notify pipeline (ignore/hooks/sanitize/cap) and urgent path"
```

---

### Task 10: Global facade and Guard

**Files:**
- Create: `src/global.rs`
- Modify: `src/lib.rs` (`mod global;`, re-export free functions + `Guard`)

**Interfaces:**
- Consumes: `Client` (Task 9); `crate::panic_hook::{register, deregister}` — **stub these now** in a new `src/panic_hook.rs` (`pub(crate) fn register(_: crate::Client) {}`, `pub(crate) fn deregister() {}`), replaced in Task 11.
- Produces (public API):
  - `pub fn init(config: Config) -> Result<Guard, Error>`
  - `#[must_use] pub struct Guard` — Drop: deregister panic client, flush(5s), shutdown, reset global to Uninitialized
  - Free functions mirroring `Client`: `notify<E: Error + ?Sized>(&E)`, `notify_notice(Notice)`, `context(...)`, `clear_context()`, `add_breadcrumb(...)`, `flush(Duration) -> bool` — all no-ops (with `log::debug`) when uninitialized
  - `pub(crate) fn global_client() -> Option<Client>` (Task 11's dispatcher uses `panic_hook`'s own registration, not this)

- [ ] **Step 1: Write the failing tests**

Global state forces serial execution — put these in one test fn to avoid cross-test interference:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn config() -> crate::Config {
        crate::Config::builder().env_source(|_| None).env("test").build().unwrap()
    }

    #[test]
    fn test_init_lifecycle() {
        // Free functions before init: no panic, no effect.
        crate::notify_notice(crate::Notice::message("X", "y"));
        assert!(!crate::flush(Duration::from_millis(100)));

        // Init claims the slot; double init fails; guard drop releases it.
        let guard = crate::init(config()).unwrap();
        assert!(global_client().is_some());
        assert!(matches!(crate::init(config()).unwrap_err(), crate::Error::AlreadyInitialized));
        crate::notify_notice(crate::Notice::message("X", "y"));
        assert!(crate::flush(Duration::from_secs(5)));
        drop(guard);
        assert!(global_client().is_none());

        // Re-init after drop is supported.
        let guard2 = crate::init(config()).unwrap();
        assert!(global_client().is_some());
        drop(guard2);
        assert!(global_client().is_none());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test global` — expected: compile failure.

- [ ] **Step 3: Implement**

`src/panic_hook.rs` (stub — Task 11 replaces):

```rust
//! Panic dispatcher (stub until the panic task). Registration is a no-op.
pub(crate) fn register(_client: crate::Client) {}
pub(crate) fn deregister() {}
```

`src/global.rs`:

```rust
//! The global facade: one process-wide Client behind init/Guard (spec "Client, init,
//! and shutdown lifecycle").
use crate::client::Client;
use crate::config::Config;
use crate::error::Error;
use crate::notice::Notice;
use serde_json::{Map, Value};
use std::sync::Mutex;
use std::time::Duration;

static GLOBAL: Mutex<Option<Client>> = Mutex::new(None);
const GUARD_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

/// Keeps the global client alive; dropping it flushes and shuts reporting down.
#[must_use = "dropping the Guard immediately shuts Honeybadger reporting down — bind it (e.g. `let _guard = honeybadger::init(...)?;`)"]
pub struct Guard {
    _priv: (),
}

pub fn init(config: Config) -> Result<Guard, Error> {
    let mut slot = GLOBAL.lock().unwrap_or_else(|e| e.into_inner());
    if slot.is_some() {
        return Err(Error::AlreadyInitialized);
    }
    let client = Client::new(config)?;
    if client.wants_panic_hook() {
        crate::panic_hook::register(client.clone());
    }
    *slot = Some(client);
    Ok(Guard { _priv: () })
}

impl Drop for Guard {
    fn drop(&mut self) {
        crate::panic_hook::deregister();
        let client = GLOBAL.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(client) = client {
            client.flush(GUARD_FLUSH_TIMEOUT);
            client.shutdown(GUARD_FLUSH_TIMEOUT);
        }
    }
}

pub(crate) fn global_client() -> Option<Client> {
    GLOBAL.lock().unwrap_or_else(|e| e.into_inner()).clone()
}

fn with_client(f: impl FnOnce(&Client)) {
    match global_client() {
        Some(client) => f(&client),
        None => log::debug!("honeybadger: not initialized; call honeybadger::init first"),
    }
}

pub fn notify<E: std::error::Error + ?Sized>(error: &E) {
    with_client(|c| c.notify(error));
}

pub fn notify_notice(notice: Notice) {
    with_client(|c| c.notify_notice(notice));
}

pub fn context<I, K>(entries: I)
where
    I: IntoIterator<Item = (K, Value)>,
    K: Into<String>,
{
    with_client(|c| c.context(entries));
}

pub fn clear_context() {
    with_client(|c| c.clear_context());
}

pub fn add_breadcrumb(message: &str, category: &str, metadata: Option<Map<String, Value>>) {
    with_client(|c| c.add_breadcrumb(message, category, metadata));
}

pub fn flush(timeout: Duration) -> bool {
    global_client().map(|c| c.flush(timeout)).unwrap_or(false)
}
```

`src/lib.rs` additions: `mod global; mod panic_hook;` and `pub use crate::global::{add_breadcrumb, clear_context, context, flush, init, notify, notify_notice, Guard};`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test` — all green.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: global facade with atomic init/Guard lifecycle"
```

---

### Task 11: Panic dispatcher

**Files:**
- Replace: `src/panic_hook.rs` (stub from Task 10)
- Create: `tests/panic_hook.rs`, `examples/panic_fixture.rs`, `tests/fixtures/abort_fixture/{Cargo.toml,src/main.rs}`

**Interfaces:**
- Consumes: `Client::deliver_now` (Task 9), `Notice`, `bt::capture`.
- Produces: real `register(client)` / `deregister()`; behavior per spec "Panic hook": permanent dispatcher, recursion guard, catch_unwind, urgent synchronous delivery, chain to previous hook.

- [ ] **Step 1: Replace `src/panic_hook.rs`**

```rust
//! Permanent panic dispatcher (spec "Panic hook"): installed once, never uninstalled;
//! Guard drop only deregisters the client.
use crate::bt::Frame;
use crate::client::Client;
use crate::notice::Notice;
use std::cell::Cell;
use std::panic::{catch_unwind, AssertUnwindSafe, PanicHookInfo};
use std::sync::{Once, RwLock};

static INSTALL: Once = Once::new();
static PANIC_CLIENT: RwLock<Option<Client>> = RwLock::new(None);

thread_local! {
    static IN_HOOK: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn register(client: Client) {
    INSTALL.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| dispatch(info, previous.as_ref())));
    });
    *PANIC_CLIENT.write().unwrap_or_else(|e| e.into_inner()) = Some(client);
}

pub(crate) fn deregister() {
    *PANIC_CLIENT.write().unwrap_or_else(|e| e.into_inner()) = None;
}

fn dispatch(info: &PanicHookInfo<'_>, previous: &(dyn Fn(&PanicHookInfo<'_>) + Send + Sync)) {
    let reentered = IN_HOOK.with(|flag| flag.replace(true));
    if !reentered {
        let client = PANIC_CLIENT.read().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some(client) = client {
            let _ = catch_unwind(AssertUnwindSafe(|| report(&client, info)));
        }
        IN_HOOK.with(|flag| flag.set(false));
    }
    previous(info);
}

fn report(client: &Client, info: &PanicHookInfo<'_>) {
    let message = if let Some(s) = info.payload().downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = info.payload().downcast_ref::<String>() {
        s.clone()
    } else {
        "Box<dyn Any>".to_owned()
    };

    let mut notice = Notice::message("panic", &message);
    // Location as the top frame, then the captured backtrace below it.
    let mut frames = Vec::new();
    if let Some(location) = info.location() {
        frames.push(Frame {
            number: Some(location.line()),
            file: Some(location.file().to_owned()),
            method: Some("panic".to_owned()),
            source: None,
        });
    }
    frames.extend(client.capture_frames());
    notice.frames = Some(frames);
    client.deliver_now(notice);
}
```

Add to `src/client.rs` (small accessor the dispatcher needs; root lives in config):

```rust
    pub(crate) fn capture_frames(&self) -> Vec<crate::bt::Frame> {
        crate::bt::capture(&self.0.config.root)
    }
```

- [ ] **Step 2: Write the fixture example `examples/panic_fixture.rs`**

Reads the mock endpoint from env, initializes, panics. Used by both integration tests and (copied) by the abort fixture.

```rust
//! Test fixture: initializes Honeybadger against HONEYBADGER_ENDPOINT and panics.
fn main() {
    let config = honeybadger::Config::builder()
        .env("fixture")
        .exclude_envs(Vec::<String>::new())
        .api_key("fixture-key")
        .build()
        .expect("config");
    let _guard = honeybadger::init(config).expect("init");
    honeybadger::add_breadcrumb("about to panic", "custom", None);
    panic!("fixture panicked on purpose");
}
```

(The fixture reads `HONEYBADGER_ENDPOINT` via the default env source — the test sets it on the child process.)

- [ ] **Step 3: Write the abort fixture crate**

`tests/fixtures/abort_fixture/Cargo.toml`:

```toml
[package]
name = "abort_fixture"
version = "0.0.0"
edition = "2024"
publish = false

[workspace]

[dependencies]
honeybadger = { path = "../../.." }

[profile.dev]
panic = "abort"
```

`tests/fixtures/abort_fixture/src/main.rs`: identical `main` to `examples/panic_fixture.rs` (repeat the code — same body, `panic!("abort fixture panicked on purpose")` as the message).

- [ ] **Step 4: Write the integration tests `tests/panic_hook.rs`**

```rust
//! End-to-end panic-hook tests against fixture processes and a mockito server.
use std::io::Read;
use std::process::Command;
use std::time::Duration;

fn spawn_fixture(cmd: &mut Command, endpoint: &str) -> std::process::Output {
    cmd.env("HONEYBADGER_ENDPOINT", endpoint)
        .env_remove("HONEYBADGER_ENV")
        .output()
        .expect("fixture ran")
}

fn assert_panic_notice_received(server: &mut mockito::Server, mock: &mockito::Mock) {
    mock.assert(); // exactly one POST /v1/notices arrived before child exit
    let _ = server; // (request body assertions happen via match_request below)
}

#[test]
fn test_unwind_panic_reports_before_exit() {
    let mut server = mockito::Server::new();
    let mock = server
        .mock("POST", "/v1/notices")
        .match_request(|req| {
            let mut body = String::new();
            flate2::read::ZlibDecoder::new(req.body().unwrap().as_slice())
                .read_to_string(&mut body)
                .unwrap();
            body.contains("\"class\":\"panic\"")
                && body.contains("fixture panicked on purpose")
                && body.contains("about to panic") // breadcrumb survived
        })
        .with_status(201)
        .expect(1)
        .create();

    let out = spawn_fixture(
        Command::new(env!("CARGO")).args(["run", "--quiet", "--example", "panic_fixture"]),
        &server.url(),
    );
    assert!(!out.status.success(), "fixture must exit nonzero after panic");
    assert_panic_notice_received(&mut server, &mock);
}

#[test]
fn test_abort_panic_reports_before_exit() {
    let mut server = mockito::Server::new();
    let mock = server
        .mock("POST", "/v1/notices")
        .match_request(|req| {
            let mut body = String::new();
            flate2::read::ZlibDecoder::new(req.body().unwrap().as_slice())
                .read_to_string(&mut body)
                .unwrap();
            body.contains("\"class\":\"panic\"")
        })
        .with_status(201)
        .expect(1)
        .create();

    let manifest = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/abort_fixture/Cargo.toml");
    let out = spawn_fixture(
        Command::new(env!("CARGO")).args(["run", "--quiet", "--manifest-path", manifest]),
        &server.url(),
    );
    assert!(!out.status.success());
    assert_panic_notice_received(&mut server, &mock);
}

#[test]
fn test_hook_recursion_and_chaining_do_not_abort_unwind_build() {
    // In-process: register via init, panic inside catch_unwind, assert we survive
    // and the previous hook still ran.
    use std::sync::atomic::{AtomicBool, Ordering};
    static PREV_RAN: AtomicBool = AtomicBool::new(false);
    std::panic::set_hook(Box::new(|_| PREV_RAN.store(true, Ordering::SeqCst)));

    let config = honeybadger::Config::builder()
        .env("fixture")
        .exclude_envs(Vec::<String>::new())
        .api_key("k")
        .endpoint("http://127.0.0.1:1") // urgent delivery fails fast; must not panic/loop
        .connect_timeout(Duration::from_millis(100))
        .request_timeout(Duration::from_millis(100))
        .build()
        .unwrap();
    let guard = honeybadger::init(config).unwrap();

    let result = std::panic::catch_unwind(|| panic!("in-process panic"));
    assert!(result.is_err());
    assert!(PREV_RAN.load(Ordering::SeqCst), "previous hook must still run");
    drop(guard);
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test` — all green (the fixture tests compile the fixture on first run; allow time). If `mockito`'s `match_request` body accessor differs in the released version, adapt per its docs — the assertion (inflate body, check class/message/breadcrumb) is the requirement.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: permanent panic dispatcher with urgent delivery and abort-safe fixtures"
```

---

### Task 12: Examples, crate docs, README, CI

**Files:**
- Create: `examples/notify.rs`, `README.md`, `.github/workflows/ci.yml`, `LICENSE`
- Modify: `src/lib.rs` (crate-level docs)

**Interfaces:** consumes the finished public API; produces nothing further.

- [ ] **Step 1: Write `examples/notify.rs`**

```rust
//! Reporting a handled error with context, breadcrumbs, and a custom notice.
use serde_json::json;

#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error("failed to load configuration")]
    ConfigLoad(#[from] std::io::Error),
}

fn main() {
    let _guard = honeybadger::init(
        honeybadger::Config::builder()
            .api_key("your-project-api-key")
            .env("production")
            .build()
            .expect("honeybadger config"),
    )
    .expect("honeybadger init");

    honeybadger::context([("user_id", json!(123))]);
    honeybadger::add_breadcrumb("loading config", "app", None);

    if let Err(e) = load_config() {
        honeybadger::notify(&e);
    }

    honeybadger::notify_notice(
        honeybadger::Notice::message("BillingAlert", "manual notice example")
            .tags(["billing"])
            .fingerprint("billing-alerts"),
    );
} // _guard drop flushes before exit

fn load_config() -> Result<(), AppError> {
    std::fs::read_to_string("/does/not/exist")?;
    Ok(())
}
```

Note: `thiserror` is already a dependency of the crate, so examples may use it.

- [ ] **Step 2: Write crate-level docs in `src/lib.rs`**

Replace the placeholder module doc with:

```rust
//! The official [Honeybadger](https://www.honeybadger.io) error-tracking SDK for Rust.
//!
//! # Quick start
//!
//! ```rust,no_run
//! fn main() {
//!     let _guard = honeybadger::init(
//!         honeybadger::Config::builder()
//!             .api_key("your-project-api-key") // or HONEYBADGER_API_KEY
//!             .env("production")
//!             .build()
//!             .unwrap(),
//!     )
//!     .unwrap();
//!
//!     if let Err(e) = std::fs::read_to_string("/missing") {
//!         honeybadger::notify(&e);
//!     }
//! } // guard drop flushes pending notices and stops the worker
//! ```
//!
//! Any error implementing [`std::error::Error`] can be reported; its `source()` chain
//! becomes the Honeybadger cause list, and a backtrace is captured at the `notify`
//! call site. Panics are reported automatically (disable with
//! `Config::builder().install_panic_hook(false)`).
//!
//! The SDK never panics and never blocks your app on network I/O: `notify` enqueues to
//! a background worker thread (bounded queue, rate-limit aware). It works in any app —
//! tokio, async-std, or plain sync Rust — because it never touches an async runtime.
```

- [ ] **Step 3: Write `README.md`**

```markdown
# Honeybadger for Rust

The official [Honeybadger](https://www.honeybadger.io) error-tracking SDK for Rust.

## Installation

```toml
[dependencies]
honeybadger = "0.1"
```

## Usage

```rust
fn main() {
    let _guard = honeybadger::init(
        honeybadger::Config::builder()
            .api_key("your-project-api-key") // or HONEYBADGER_API_KEY
            .env("production")
            .build()
            .unwrap(),
    )
    .unwrap();

    honeybadger::context([("user_id", serde_json::json!(123))]);

    if let Err(e) = do_work() {
        honeybadger::notify(&e);
    }
}
```

- Reports any `std::error::Error` (with its `source()` chain and a backtrace).
- Reports panics automatically.
- Breadcrumbs, tags, fingerprints, and `before_notify` hooks are supported — see the
  [crate docs](https://docs.rs/honeybadger) and `examples/`.
- Non-blocking: notices are delivered by a background worker with rate-limit handling.
- Runtime-agnostic: no async runtime required (or embedded).

Configuration is env-var friendly: `HONEYBADGER_API_KEY`, `HONEYBADGER_ENV`,
`HONEYBADGER_REVISION`, `HONEYBADGER_ROOT`, `HONEYBADGER_HOSTNAME`,
`HONEYBADGER_ENDPOINT`, `HONEYBADGER_ENABLED`.

Development and test environments don't report by default (`exclude_envs`).

## License

MIT
```

- [ ] **Step 4: Write `.github/workflows/ci.yml`** (and an MIT `LICENSE` file with the standard MIT text, copyright "2026 Honeybadger Industries LLC")

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy, rustfmt
      - run: cargo fmt --check
      - run: cargo clippy --all-targets -- -D warnings
      - run: cargo build --examples
      - run: cargo test
  msrv:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@1.85
      - run: cargo check --all-targets
```

- [ ] **Step 5: Full verification**

Run: `cargo fmt && cargo clippy --all-targets -- -D warnings && cargo build --examples && cargo test`
Expected: everything green. Fix clippy findings mechanically; stop and flag anything that would change public API.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "docs: examples, crate docs, README, CI workflow"
```

---

## Post-plan follow-ups (not tasks)

- crates.io name outreach to fussybeaver (Ben, in parallel; publishing blocked on it).
- Move the repo to the honeybadger-io org; flip the CI branch name if the default branch differs.
- Phase 2 planning (Insights events) once Phase 1 ships.
