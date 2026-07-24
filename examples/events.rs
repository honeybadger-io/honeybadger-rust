//! Sending Insights events.
//!
//! Run with a real key to see them arrive:
//!   HONEYBADGER_API_KEY=... cargo run --example events
//!
//! The key and environment both come from the environment, so
//! `HONEYBADGER_ENV=test cargo run --example events` exercises the whole
//! pipeline offline against the null transport.
use serde_json::json;
use std::time::Duration;

fn main() {
    let _guard = honeybadger::init(honeybadger::Config::builder().build().expect("config"))
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
