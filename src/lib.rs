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
//! The SDK never panics and never blocks your app on network I/O: `notify` enqueues to
//! a background worker thread (bounded queue, rate-limit aware). It works in any app —
//! tokio, async-std, or plain sync Rust — because it never touches an async runtime.

#![warn(missing_docs)]

mod breadcrumbs;
mod bt;
mod client;
mod config;
mod error;
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
    Guard, add_breadcrumb, clear_context, context, flush, init, notify, notify_notice,
};
pub use crate::notice::Notice;
pub use crate::transport::{
    CapturedRequest, RequestKind, TestTransport, Transport, TransportError, TransportRequest,
};
