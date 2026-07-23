//! Errors returned by the SDK's fallible surfaces (`init`, `Client::new`, `Config::build`).
use thiserror::Error;

/// Errors returned by [`crate::init`] and [`crate::Client::new`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error(
        "Honeybadger is already initialized; drop the previous Guard before calling init again"
    )]
    AlreadyInitialized,

    #[error(
        "an API key is required to report to the Honeybadger service (set HONEYBADGER_API_KEY or Config::builder().api_key(...))"
    )]
    MissingApiKey,

    #[error("invalid Honeybadger endpoint URL: {0}")]
    InvalidEndpoint(String),

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
