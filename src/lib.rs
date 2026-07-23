//! The official Honeybadger error-tracking SDK for Rust. (Docs land in the final task.)

mod breadcrumbs;
mod config;
mod error;
mod notice;
mod sanitizer;

pub use crate::breadcrumbs::Breadcrumb;
pub use crate::config::{Config, ConfigBuilder};
pub use crate::error::Error;
