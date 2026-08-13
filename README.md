# Honeybadger for Rust

The official [Honeybadger](https://www.honeybadger.io) error-tracking SDK for Rust.

## Installation

```toml
[dependencies]
honeybadger = "0.5"
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
notice. Without an active scope (see below) it is process-wide — not per request, not
per thread, not per task — so in a concurrent server one request overwrites another's
values and an error can be reported against the wrong user:

```text
request A:  context([("user_id", 1)])
request B:  context([("user_id", 2)])     // overwrites A
request A:  notify(&err)                  // reported against user 2
```

`clear_context()` has the same reach: without a scope it clears the shared
process-wide context, breadcrumb trail, event context, and request id — not just
the calling thread's own.

Reserve the global functions, unscoped, for facts that really are process-wide
(release channel, region, worker identity) or for programs handling one unit of work
at a time — a CLI, a cron job, a serialized consumer. Everything request-shaped
either belongs on the notice, as above, or inside a scope.

### Request-scoped context

With the `tokio` feature enabled, `honeybadger::scope(...)` gives `context()`,
`add_breadcrumb()`, `event_context()`, and `request_id()` a per-request home, so what
one request writes is invisible to every other:

```toml
[dependencies]
honeybadger = { version = "0.5", features = ["tokio"] }
```

```rust
use serde_json::json;

async fn handle_request(user_id: u64) {
    honeybadger::scope(async {
        honeybadger::request_id(format!("req-{user_id}"));
        honeybadger::context([("user_id", json!(user_id))]);

        // ... do the request's work; any notice or event reported in here
        // carries only this request's data, even with other requests running
        // concurrently ...
    })
    .await;
}
```

A scope sits on top of the process-wide state rather than replacing it, and the four
stores do not all resolve the same way:

- `context()` and `event_context()` **merge**: a notice or event reported inside a
  scope carries the process-wide entries with the request's own on top, and the
  request wins a key collision. A `version` set once at boot still reaches every
  scoped notice.
- The breadcrumb trail is **replaced**: a scoped notice carries that request's crumbs
  alone, never the process-wide trail, even when the request recorded none — merging
  trails is the cross-request contamination scoping exists to remove.
- `request_id()` is the request's own when it set one, and the process-wide id
  otherwise.

The scope does not cross a `tokio::spawn`, `tokio::task::spawn_blocking`, or
`std::thread::spawn` boundary on its own — capture it with
`honeybadger::ScopeHandle::current()` and re-enter it on the other side with
`ScopeHandle::enter`/`enter_sync`. See the
[`scope` docs](https://docs.rs/honeybadger) (built with `--features tokio`) for the
full explanation, including the one case no API can fix — a `spawn` inside a
third-party dependency — and `examples/scoped_request.rs` for a complete, runnable
walkthrough:

```sh
cargo run --features tokio --example scoped_request
```

The example runs in the `development` environment, which `exclude_envs` excludes from
reporting by default, so it exercises the whole assembly pipeline against the null
transport and sends nothing over the network.

Configuration is env-var friendly: `HONEYBADGER_API_KEY`, `HONEYBADGER_ENV`,
`HONEYBADGER_REVISION`, `HONEYBADGER_ROOT`, `HONEYBADGER_HOSTNAME`,
`HONEYBADGER_ENDPOINT`, `HONEYBADGER_ENABLED`.

Development and test environments don't report by default (`exclude_envs`).

## Insights events

Beyond errors, the SDK sends structured events to
[Honeybadger Insights](https://www.honeybadger.io/insights/):

```rust
use serde_json::json;

honeybadger::event("user.created", json!({ "user_id": 7, "plan": "pro" }));

// The event_type can live in the payload instead:
honeybadger::event_value(json!({ "event_type": "job.finished", "ms": 91 }));
```

Events batch in the background and go out when the first of three triggers fires:
1000 events, 30 seconds, or 4.5 MB. The worker thread starts on your first `event()`
call, so a program that only reports errors never pays for it. `honeybadger::flush()`
covers events and notices together, within one timeout.

The payload is a `serde_json::Value`, not `impl Serialize`. That is deliberate:
passing a struct would send every field it happens to carry, including ones nobody
enumerated, so a struct is converted explicitly at the call site:

```rust
honeybadger::event("user.created", serde_json::to_value(&user)?);
```

Because every field in an event is written by hand, `filter_keys` redaction does **not**
apply to events the way it does to notice context. Depth capping, string truncation,
and the 100 kB per-event ceiling still do; an oversized event is logged and dropped.

### Correlation and sampling

```rust
honeybadger::request_id("req-9");                        // correlates notices and events
honeybadger::event_context([("service", json!("checkout"))]); // added to every later event
```

`request_id` also drives sampling: events sharing a request id share one decision, so a
sampled request keeps all of its events or none. Like `context`, without an active
scope both the event context and the request-id slot are **process-wide** — in a
concurrent server, either wrap the request in `scope()` (see above) or put
`request_id` in the event payload instead, where it travels with the event and cannot
be clobbered.

### Configuration

| Option | Env | Default |
| --- | --- | --- |
| `events_enabled` | `HONEYBADGER_EVENTS_ENABLED` | `true` |
| `events_batch_size` | `HONEYBADGER_EVENTS_BATCH_SIZE` | `1000` |
| `events_flush_interval` | `HONEYBADGER_EVENTS_FLUSH_INTERVAL` (seconds) | `30s` |
| `events_queue_size` | `HONEYBADGER_EVENTS_QUEUE_SIZE` | `10000` |
| `events_max_retries` | `HONEYBADGER_EVENTS_MAX_RETRIES` | `3` |
| `events_sample_rate` | `HONEYBADGER_EVENTS_SAMPLE_RATE` | `100` |
| `events_attach_hostname` | `HONEYBADGER_EVENTS_ATTACH_HOSTNAME` | `true` |
| `events_attach_environment` | `HONEYBADGER_EVENTS_ATTACH_ENVIRONMENT` | `true` |
| `before_event` | — | none |

`events_queue_size` bounds everything outstanding at once — queued, batching, and
awaiting retry — rather than just the channel. Past that limit the oldest retained
batch is shed first, so a rate-limited endpoint cannot stall the pipeline.

`before_event` hooks mirror `before_notify`: they run in registration order against
the assembled event, may mutate it freely, and returning `false` drops it.

See `examples/events.rs` for a runnable version.

## License

MIT
