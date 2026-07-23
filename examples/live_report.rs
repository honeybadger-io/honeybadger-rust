//! Live smoke test: reports a real error to the Honeybadger service.
//!
//! The API key is read from the environment — it is never in this file. Run with:
//!
//! ```sh
//! HONEYBADGER_API_KEY=hbp_xxx RUST_LOG=honeybadger=debug \
//!     cargo run --example live_report
//! ```
//!
//! A delivery failure (bad key, network) is logged at `warn` by the worker, which is why
//! the example installs a logger. On success the worker is silent, so the example prints
//! its own confirmation after `flush` reports the delivery barrier was reached.
use serde_json::json;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
enum SmokeTestError {
    #[error("simulated failure charging order {order_id}")]
    ChargeFailed {
        order_id: u64,
        #[source]
        source: std::io::Error,
    },
}

fn main() {
    // Surfaces the worker's debug/warn lines (throttle, suspend, delivery failures).
    env_logger::init();

    if std::env::var("HONEYBADGER_API_KEY").is_err() {
        eprintln!(
            "set HONEYBADGER_API_KEY in the environment first, e.g.\n  \
             HONEYBADGER_API_KEY=hbp_xxx cargo run --example live_report"
        );
        std::process::exit(1);
    }

    let _guard = honeybadger::init(
        honeybadger::Config::builder()
            // api_key is intentionally omitted: it comes from HONEYBADGER_API_KEY.
            .env("production")
            .build()
            .expect("honeybadger config"),
    )
    .expect("honeybadger init");

    honeybadger::add_breadcrumb("starting checkout", "app", None);

    let error = SmokeTestError::ChargeFailed {
        order_id: 4242,
        source: std::io::Error::new(std::io::ErrorKind::TimedOut, "payment gateway timed out"),
    };

    // Request-shaped data belongs on the notice (see the crate docs on process-wide
    // context), and tags/fingerprint make this smoke test easy to find in the UI.
    honeybadger::notify_notice(
        honeybadger::Notice::from_error(&error)
            .tags(["smoke-test", "honeybadger-rust"])
            .fingerprint("honeybadger-rust-live-report")
            .context([
                ("order_id", json!(4242)),
                ("request_id", json!("smoke-test-req-1")),
                ("sdk", json!("honeybadger-rust")),
            ]),
    );

    // Block until the worker has attempted delivery (fire-and-forget otherwise).
    let delivered = honeybadger::flush(Duration::from_secs(10));
    if delivered {
        println!(
            "flush reached the delivery barrier — check your project's faults for\n  \
             tag \"smoke-test\" / fingerprint \"honeybadger-rust-live-report\"."
        );
    } else {
        eprintln!("flush timed out before the worker drained; see the log output above.");
        std::process::exit(1);
    }
    // Dropping `_guard` flushes again and stops the worker.
}
