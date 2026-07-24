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
