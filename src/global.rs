//! The global facade: one process-wide Client behind init/Guard (spec "Client, init,
//! and shutdown lifecycle").
use crate::client::Client;
use crate::config::Config;
use crate::error::Error;
use crate::notice::Notice;
use serde_json::{Map, Value};
use std::sync::Mutex;
use std::time::Duration;

static GLOBAL: Mutex<Option<Client>> = Mutex::new(None);
const GUARD_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

/// Keeps the global client alive; dropping it flushes and shuts reporting down.
#[derive(Debug)]
#[must_use = "dropping the Guard immediately shuts Honeybadger reporting down — bind it (e.g. `let _guard = honeybadger::init(...)?;`)"]
pub struct Guard {
    _priv: (),
}

/// Initializes the process-wide client and returns its [`Guard`].
///
/// Bind the guard for the lifetime of your program — dropping it flushes pending
/// notices and stops reporting:
///
/// ```rust,no_run
/// let _guard = honeybadger::init(
///     honeybadger::Config::builder().api_key("...").build().unwrap(),
/// ).unwrap();
/// ```
///
/// # Errors
///
/// [`Error::AlreadyInitialized`] if a guard is still outstanding, plus any error from
/// [`crate::Client::new`].
pub fn init(config: Config) -> Result<Guard, Error> {
    let mut slot = GLOBAL.lock().unwrap_or_else(|e| e.into_inner());
    if slot.is_some() {
        return Err(Error::AlreadyInitialized);
    }
    let client = Client::new(config)?;
    if client.wants_panic_hook() {
        crate::panic_hook::register(client.clone());
    }
    *slot = Some(client);
    Ok(Guard { _priv: () })
}

impl Drop for Guard {
    fn drop(&mut self) {
        crate::panic_hook::deregister();
        let client = GLOBAL.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(client) = client {
            client.flush(GUARD_FLUSH_TIMEOUT);
            client.shutdown(GUARD_FLUSH_TIMEOUT);
        }
    }
}

pub(crate) fn global_client() -> Option<Client> {
    GLOBAL.lock().unwrap_or_else(|e| e.into_inner()).clone()
}

fn with_client(f: impl FnOnce(&Client)) {
    match global_client() {
        Some(client) => f(&client),
        None => log::debug!("honeybadger: not initialized; call honeybadger::init first"),
    }
}

/// Reports an error through the global client. A no-op (logged at debug) before
/// [`init`]. See [`crate::Client::notify`].
pub fn notify<E: std::error::Error + ?Sized>(error: &E) {
    with_client(|c| c.notify(error));
}

/// Reports a hand-built notice through the global client. A no-op before [`init`].
pub fn notify_notice(notice: Notice) {
    with_client(|c| c.notify_notice(notice));
}

/// Merges context into the global client. A no-op before [`init`]. See
/// [`crate::Client::context`].
pub fn context<I, K>(entries: I)
where
    I: IntoIterator<Item = (K, Value)>,
    K: Into<String>,
{
    with_client(|c| c.context(entries));
}

/// Clears the global client's context and breadcrumb trail. A no-op before [`init`].
pub fn clear_context() {
    with_client(|c| c.clear_context());
}

/// Records a breadcrumb on the global client. A no-op before [`init`].
pub fn add_breadcrumb(message: &str, category: &str, metadata: Option<Map<String, Value>>) {
    with_client(|c| c.add_breadcrumb(message, category, metadata));
}

/// Flushes the global client, returning `false` if it is uninitialized or the timeout
/// expired first.
pub fn flush(timeout: Duration) -> bool {
    global_client().map(|c| c.flush(timeout)).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn config() -> crate::Config {
        crate::Config::builder()
            .env_source(|_| None)
            .env("test")
            .build()
            .unwrap()
    }

    #[test]
    fn test_init_lifecycle() {
        // Free functions before init: no panic, no effect.
        crate::notify_notice(crate::Notice::message("X", "y"));
        assert!(!crate::flush(Duration::from_millis(100)));

        // Init claims the slot; double init fails; guard drop releases it.
        let guard = crate::init(config()).unwrap();
        assert!(global_client().is_some());
        assert!(matches!(
            crate::init(config()).unwrap_err(),
            crate::Error::AlreadyInitialized
        ));
        crate::notify_notice(crate::Notice::message("X", "y"));
        assert!(crate::flush(Duration::from_secs(5)));
        drop(guard);
        assert!(global_client().is_none());

        // Re-init after drop is supported.
        let guard2 = crate::init(config()).unwrap();
        assert!(global_client().is_some());
        drop(guard2);
        assert!(global_client().is_none());
    }
}
