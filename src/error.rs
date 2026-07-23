//! Errors returned by the SDK's fallible surfaces (`init`, `Client::new`, `Config::build`).
use thiserror::Error;

/// Errors returned by [`crate::init`] and [`crate::Client::new`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// A [`crate::Guard`] is already outstanding. Drop it before initializing again.
    #[error(
        "Honeybadger is already initialized; drop the previous Guard before calling init again"
    )]
    AlreadyInitialized,

    /// Reporting is on and the transport resolved to the Honeybadger service, but no
    /// API key was configured.
    #[error(
        "an API key is required to report to the Honeybadger service (set HONEYBADGER_API_KEY or Config::builder().api_key(...))"
    )]
    MissingApiKey,

    /// The configured endpoint is not a usable `http://` or `https://` URL.
    #[error("invalid Honeybadger endpoint URL: {0}")]
    InvalidEndpoint(String),

    /// The OS refused to spawn the background delivery thread.
    #[error("failed to spawn the honeybadger worker thread")]
    WorkerSpawn(#[source] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        assert!(
            Error::AlreadyInitialized
                .to_string()
                .contains("already initialized")
        );
        assert!(
            Error::InvalidEndpoint("ftp://x".into())
                .to_string()
                .contains("ftp://x")
        );
    }
}
