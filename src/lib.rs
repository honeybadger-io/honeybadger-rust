//! The official Honeybadger error-tracking SDK for Rust. (Docs land in the final task.)

mod breadcrumbs;
mod error;
mod sanitizer;

pub use crate::breadcrumbs::Breadcrumb;
pub use crate::error::Error;
