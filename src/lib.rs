//! The official Honeybadger error-tracking SDK for Rust. (Docs land in the final task.)

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
