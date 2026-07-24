//! The official [Honeybadger](https://www.honeybadger.io) error-tracking SDK for Rust.
//!
//! # Quick start
//!
//! ```rust,no_run
//! let _guard = honeybadger::init(
//!     honeybadger::Config::builder()
//!         .api_key("your-project-api-key") // or HONEYBADGER_API_KEY
//!         .env("production")
//!         .build()
//!         .unwrap(),
//! )
//! .unwrap();
//!
//! if let Err(e) = std::fs::read_to_string("/missing") {
//!     honeybadger::notify(&e);
//! }
//! // dropping `_guard` flushes pending notices and stops the worker
//! ```
//!
//! Any error implementing [`std::error::Error`] can be reported; its `source()` chain
//! becomes the Honeybadger cause list, and a backtrace is captured at the `notify`
//! call site. Panics are reported automatically (disable with
//! `Config::builder().install_panic_hook(false)`).
//!
//! The SDK performs no network I/O on your thread: `notify` assembles the notice and
//! hands it to a background worker (bounded queue, rate-limit aware). It works in any
//! app — tokio, async-std, or plain sync Rust — because it never touches an async
//! runtime.
//!
//! # Context is process-wide
//!
//! **[`context`] and [`add_breadcrumb`] write to one store shared by the whole process.
//! They are not request-scoped, and not thread- or task-local.** In a concurrent server
//! this is a data-attribution hazard:
//!
//! ```text
//! request A:  context([("user_id", 1)])
//! request B:  context([("user_id", 2)])     // overwrites A's value
//! request A:  notify(&err)                  // reported against user 2
//! ```
//!
//! Either request calling [`clear_context`] also wipes the other's state. Attaching one
//! user's data to another user's error is a privacy problem, not just a confusing graph.
//!
//! Use the global functions only for data that is genuinely process-wide (release
//! channel, region, worker identity) or for programs that handle one unit of work at a
//! time — a CLI, a cron job, a single-threaded consumer.
//!
//! **For anything belonging to a request, put it on the notice:**
//!
//! ```rust,no_run
//! # fn handle(err: &std::io::Error, user_id: u64, request_id: &str) {
//! use serde_json::json;
//!
//! honeybadger::notify_notice(
//!     honeybadger::Notice::from_error(err)
//!         .context([("user_id", json!(user_id)), ("request_id", json!(request_id))]),
//! );
//! # }
//! ```
//!
//! Notice-local context is carried with the notice from creation to delivery, so it
//! cannot be overwritten by a concurrent request. Where both are set, notice-local keys
//! win over the process-wide ones.
//!
//! Automatic per-request scoping — where `context()` resolves to the current request
//! rather than the process — arrives with the planned `tracing` layer and tower/axum
//! middleware. Until then, the rule above is the whole story.
//!
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
//!
//! # Panics
//!
//! The SDK does not panic, and it contains panics from the code you give it: a
//! `Display` or `source()` impl, a `before_notify` hook, or a custom [`Transport`] may
//! panic without taking your program down. Each is wrapped in
//! [`catch_unwind`](std::panic::catch_unwind).
//!
//! That containment depends on unwinding. A crate built with `panic = "abort"` cannot
//! catch anything — the process aborts at the panic site, before any handler runs. This
//! is a property of the panic strategy rather than of this SDK, and it applies equally
//! to every crate in the build. Reporting still works there: the panic hook runs before
//! the abort and delivers the notice synchronously.

#![warn(missing_docs)]

mod breadcrumbs;
mod bt;
mod client;
mod config;
mod drops;
mod error;
mod event;
mod events_worker;
mod global;
mod notice;
mod panic_hook;
mod sanitizer;
mod transport;
mod worker;

pub use crate::breadcrumbs::Breadcrumb;
pub use crate::client::{Client, ClientBuilder};
pub use crate::config::{Config, ConfigBuilder};
pub use crate::error::Error;
pub use crate::global::{
    Guard, add_breadcrumb, clear_context, clear_event_context, clear_request_id, context, event,
    event_context, event_value, flush, init, notify, notify_notice, request_id,
};
pub use crate::notice::Notice;
pub use crate::transport::{
    CapturedRequest, RequestKind, TestTransport, Transport, TransportError, TransportRequest,
};
