# Honeybadger for Rust

The official [Honeybadger](https://www.honeybadger.io) error-tracking SDK for Rust.

## Installation

```toml
[dependencies]
honeybadger = "0.1"
serde_json = "1"  # context values are serde_json::Value
```

`serde_json` is a direct dependency for you too: context values are `serde_json::Value`,
so calling `honeybadger::context([("user_id", json!(123))])` needs the `json!` macro in
your own `Cargo.toml`.

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

    if let Err(e) = do_work() {
        honeybadger::notify(&e);
    }
}
```

- Reports any `std::error::Error` (with its `source()` chain and a backtrace).
- Reports panics automatically, including under `panic = "abort"`.
- Breadcrumbs, tags, fingerprints, and `before_notify` hooks are supported — see the
  [crate docs](https://docs.rs/honeybadger) and `examples/`.
- No network I/O on your thread: delivery happens on a background worker with
  rate-limit handling.
- Runtime-agnostic: no async runtime required (or embedded).

## Context

Attach data to an individual error by putting it on the notice:

```rust
use serde_json::json;

honeybadger::notify_notice(
    honeybadger::Notice::from_error(&e)
        .context([("user_id", json!(123)), ("request_id", json!(request_id))]),
);
```

There is also `honeybadger::context(...)`, which sets context for **every** later
notice. It is process-wide — not per request, not per thread, not per task — so in a
concurrent server one request overwrites another's values and an error can be reported
against the wrong user:

```text
request A:  context([("user_id", 1)])
request B:  context([("user_id", 2)])     // overwrites A
request A:  notify(&err)                  // reported against user 2
```

`clear_context()` has the same reach: it clears the shared process-wide state, including
breadcrumbs, not the calling thread's.

Reserve the global functions for facts that really are process-wide (release channel,
region, worker identity) or for programs handling one unit of work at a time — a CLI, a
cron job, a serialized consumer. Everything request-shaped belongs on the notice.

Per-request scoping, where `context()` follows the current request automatically, comes
with the planned `tracing` layer and tower/axum middleware.

Configuration is env-var friendly: `HONEYBADGER_API_KEY`, `HONEYBADGER_ENV`,
`HONEYBADGER_REVISION`, `HONEYBADGER_ROOT`, `HONEYBADGER_HOSTNAME`,
`HONEYBADGER_ENDPOINT`, `HONEYBADGER_ENABLED`.

Development and test environments don't report by default (`exclude_envs`).

## License

MIT
