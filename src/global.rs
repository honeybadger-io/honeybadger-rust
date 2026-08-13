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
const GUARD_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Keeps the global client alive; dropping it shuts reporting down, delivering
/// whatever is still queued first.
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
            // Shutdown already drains and delivers the queue on both pipelines,
            // so a flush first bought nothing and paid a second full timeout for
            // it — against an unreachable endpoint that doubled exit latency.
            // It also undid a deliberate choice: shutdown discards when the
            // worker is throttled, and flushing first forced one more attempt
            // against an endpoint that had just asked us to stop.
            client.shutdown(GUARD_SHUTDOWN_TIMEOUT);
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

/// Merges key/value pairs into the context attached to every later notice. A no-op
/// before [`init`].
///
/// **Without an active scope, this store is shared by every thread and every task; it
/// is not request-scoped.** In a concurrent server, one request's values overwrite
/// another's, and a notice can be reported against the wrong user. Use it only for
/// process-wide facts, and put request data on the notice instead — see
/// [the crate docs](crate#context-is-process-wide). Inside `scope()` (requires the
/// `tokio` feature) this call writes to that request's own context instead.
///
/// Setting a key to [`serde_json::Value::Null`] removes it.
pub fn context<I, K>(entries: I)
where
    I: IntoIterator<Item = (K, Value)>,
    K: Into<String>,
{
    with_client(|c| c.context(entries));
}

/// Clears the context, breadcrumb trail, event context, and request id. A no-op
/// before [`init`].
///
/// **Without an active scope, this resets state shared by the entire process, not
/// the calling thread's or task's.** Calling it from a request handler discards
/// whatever every other in-flight request has accumulated, so it belongs between
/// units of work in a program that handles one at a time — not in a concurrent
/// server. Inside `scope()` (requires the `tokio` feature) this clears only that
/// request's own scope, leaving every other request's state — and the process-wide
/// base — untouched.
pub fn clear_context() {
    with_client(|c| c.clear_context());
}

/// Records a breadcrumb in the trail. A no-op before [`init`].
///
/// Without an active scope the trail is process-wide, and under concurrency
/// crumbs from unrelated requests interleave and evict one another — treat it
/// as a process-level log. Inside `scope()` (requires the `tokio` feature) the
/// trail belongs to that request alone.
pub fn add_breadcrumb(message: &str, category: &str, metadata: Option<Map<String, Value>>) {
    with_client(|c| c.add_breadcrumb(message, category, metadata));
}

/// Sends an Insights event through the global client. A no-op before [`init`].
///
/// `event_type` always wins over any `event_type` key in `payload`, and the
/// payload must be a JSON object. See [`crate::Client::event`].
///
/// ```rust,no_run
/// use serde_json::json;
/// honeybadger::event("user.created", json!({ "user_id": 7, "plan": "pro" }));
/// ```
pub fn event(event_type: &str, payload: Value) {
    with_client(|c| c.event(event_type, payload));
}

/// Sends an Insights event whose `event_type` is already in the payload.
/// A no-op before [`init`].
pub fn event_value(payload: Value) {
    with_client(|c| c.event_value(payload));
}

/// Merges key/value pairs into the event context attached to every later event.
/// A no-op before [`init`].
///
/// **Without an active scope, this store is shared by the whole process and is
/// not request-scoped, exactly like [`context`];** see
/// [the crate docs](crate#context-is-process-wide). Inside `scope()` (requires
/// the `tokio` feature) this call writes to that request's own event context
/// instead. Setting a key to [`serde_json::Value::Null`] removes it.
pub fn event_context<I, K>(entries: I)
where
    I: IntoIterator<Item = (K, Value)>,
    K: Into<String>,
{
    with_client(|c| c.event_context(entries));
}

/// Clears the **process-wide** event context, leaving notice context untouched.
/// A no-op before [`init`].
pub fn clear_event_context() {
    with_client(|c| c.clear_event_context());
}

/// Sets the request id correlating notices with events and driving deterministic
/// sampling. A no-op before [`init`].
///
/// **Without an active scope, this slot carries the same hazard as [`context`]:**
/// under concurrency one request's id overwrites another's, so an event can be
/// attributed and sampled as the wrong request. In a concurrent server either
/// wrap the request in `scope()` (requires the `tokio` feature), which gives
/// this call its own request-local slot, or, without the feature, put
/// `request_id` in the event payload instead. See [`crate::Client::request_id`].
pub fn request_id(id: impl Into<String>) {
    with_client(|c| c.request_id(id));
}

/// Clears the process-wide request id. A no-op before [`init`].
pub fn clear_request_id() {
    with_client(|c| c.clear_request_id());
}

/// Flushes the global client, returning `false` if it is uninitialized or the timeout
/// expired first.
pub fn flush(timeout: Duration) -> bool {
    global_client().map(|c| c.flush(timeout)).unwrap_or(false)
}

/// Runs `f` with a fresh request scope. See [`crate::Client::context`] for what
/// becomes request-local. Requires the `tokio` feature.
///
/// # The scope does not cross a thread or task boundary
///
/// It lives in a task-local that is only visible for the duration of `f`'s own
/// poll. [`tokio::spawn`], [`tokio::task::spawn_blocking`], and
/// [`std::thread::spawn`] all start their work with **no scope active**, so
/// anything it records — [`crate::context`], [`crate::add_breadcrumb`],
/// [`crate::event_context`], [`crate::request_id`] — falls back to the
/// process-wide store documented on those functions, exactly as if `scope()`
/// had never been entered.
///
/// Carry the scope across explicitly by capturing it with
/// [`crate::current_scope`] and re-entering it with [`crate::in_scope`] (async)
/// or [`crate::in_scope_sync`] (a `spawn_blocking` closure or a
/// `std::thread::spawn` body):
///
/// ```rust,no_run
/// # #[cfg(feature = "tokio")]
/// # async fn handler() {
/// let scope = honeybadger::current_scope();
/// tokio::task::spawn_blocking(move || {
///     honeybadger::in_scope_sync(scope, || {
///         // a synchronous database driver, still recorded against this request:
///         honeybadger::add_breadcrumb("query ran", "query", None);
///     })
/// })
/// .await
/// .unwrap();
/// # }
/// ```
///
/// **One hole no API closes:** a `spawn` performed *inside a third-party
/// dependency* cannot be wrapped by you, so those notices fall back to
/// process-global state. There is no workaround short of the dependency
/// cooperating.
#[cfg(feature = "tokio")]
pub async fn scope<F: std::future::Future>(f: F) -> F::Output {
    crate::scope::scope(f).await
}

/// [`scope`] for synchronous code. Requires the `tokio` feature.
#[cfg(feature = "tokio")]
pub fn sync_scope<T>(f: impl FnOnce() -> T) -> T {
    crate::scope::sync_scope(f)
}

/// Captures the current request scope. Requires the `tokio` feature.
#[cfg(feature = "tokio")]
pub fn current_scope() -> crate::scope::ScopeHandle {
    crate::scope::current_scope()
}

/// Runs `f` inside a captured scope. Requires the `tokio` feature.
#[cfg(feature = "tokio")]
pub async fn in_scope<F: std::future::Future>(
    handle: crate::scope::ScopeHandle,
    f: F,
) -> F::Output {
    crate::scope::in_scope(handle, f).await
}

/// [`in_scope`] for synchronous work. Requires the `tokio` feature.
#[cfg(feature = "tokio")]
pub fn in_scope_sync<T>(handle: crate::scope::ScopeHandle, f: impl FnOnce() -> T) -> T {
    crate::scope::in_scope_sync(handle, f)
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

    #[test]
    fn test_event_functions_are_no_ops_before_init() {
        // Must not panic, must not spawn anything.
        crate::event("t", serde_json::json!({ "a": 1 }));
        crate::event_value(serde_json::json!({ "event_type": "t" }));
        crate::event_context([("k", serde_json::json!(1))]);
        crate::request_id("req-1");
        crate::clear_request_id();
        crate::clear_event_context();
    }
}
