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
