//! Ambient state: the four stores that answer "what was happening?".
//!
//! They come in two layers: one process-global set per client, and a per-request
//! `Overlay` read on top of it. Both live here so the resolution rule has one
//! home.
use crate::breadcrumbs::RingBuffer;
use serde_json::{Map, Value};
#[cfg(feature = "tokio")]
use std::future::Future;
use std::sync::{Arc, Mutex};

/// A client's process-global ambient state.
pub(crate) struct Scope {
    pub(crate) context: Mutex<Map<String, Value>>,
    pub(crate) breadcrumbs: Mutex<RingBuffer>,
    pub(crate) event_context: Mutex<Map<String, Value>>,
    pub(crate) request_id: Mutex<Option<String>>,
}

impl Scope {
    pub(crate) fn new() -> Self {
        Scope {
            context: Mutex::new(Map::new()),
            breadcrumbs: Mutex::new(RingBuffer::new()),
            event_context: Mutex::new(Map::new()),
            request_id: Mutex::new(None),
        }
    }
}

/// One request's own ambient state, shared by every client via the task-local.
///
/// An overlay is *not* a copy of a client's global state. Reads merge the
/// client's own global beneath it, so two clients with different global context
/// each keep their own base while sharing one request's state — which is what
/// makes the same request reportable to two projects.
pub(crate) struct Overlay {
    pub(crate) context: Mutex<Map<String, Value>>,
    pub(crate) breadcrumbs: Mutex<RingBuffer>,
    pub(crate) event_context: Mutex<Map<String, Value>>,
    pub(crate) request_id: Mutex<Option<String>>,
}

#[cfg(feature = "tokio")]
impl Overlay {
    /// A fresh overlay seeded from the enclosing one, if any.
    ///
    /// Context, event context, and request id are inherited so a nested scope
    /// keeps the request it belongs to. Breadcrumbs are not: inheriting a trail
    /// is exactly the cross-request mixing this exists to remove.
    fn seeded_from_current() -> Overlay {
        // One lock at a time: each clone is bound on its own so the parent's
        // guard is released before the next is taken, rather than holding all
        // three for the length of one `let` while two maps are deep-cloned.
        let (context, event_context, request_id) = match current_overlay() {
            Some(parent) => {
                let context = parent
                    .context
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                let event_context = parent
                    .event_context
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                let request_id = parent
                    .request_id
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                (context, event_context, request_id)
            }
            None => (Map::new(), Map::new(), None),
        };
        Overlay {
            context: Mutex::new(context),
            breadcrumbs: Mutex::new(RingBuffer::new()),
            event_context: Mutex::new(event_context),
            request_id: Mutex::new(request_id),
        }
    }
}

#[cfg(feature = "tokio")]
tokio::task_local! {
    static CURRENT: Arc<Overlay>;
}

/// The overlay for the current request, or `None` when no scope is active.
///
/// On every notify and event path, so it must never panic. `try_with` returns
/// `Err(AccessError)` rather than panicking when no value is set — including
/// outside a tokio runtime entirely, and on a thread with no task.
pub(crate) fn current_overlay() -> Option<Arc<Overlay>> {
    #[cfg(feature = "tokio")]
    {
        if let Ok(overlay) = CURRENT.try_with(Arc::clone) {
            return Some(overlay);
        }
    }
    None
}

/// The client's global entries with the overlay's on top, overlay winning.
pub(crate) fn merged_context(
    global: &Mutex<Map<String, Value>>,
    overlay: Option<&Mutex<Map<String, Value>>>,
) -> Map<String, Value> {
    let mut merged = global.lock().unwrap_or_else(|e| e.into_inner()).clone();
    if let Some(overlay) = overlay {
        for (k, v) in overlay.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            merged.insert(k.clone(), v.clone());
        }
    }
    merged
}

/// Runs `f` with a fresh request scope, seeded from the enclosing one.
///
/// This module is private, so nothing here is rendered: the caller-facing docs
/// for this and the four functions below live on their `pub use`-reexported
/// wrappers in `src/global.rs`, which is what rustdoc shows and what
/// `RUSTDOCFLAGS=-D warnings` lint-checks. Keep behaviour notes for maintainers
/// here and the substance there — one canonical copy, so the two cannot drift.
#[cfg(feature = "tokio")]
pub async fn scope<F: Future>(f: F) -> F::Output {
    CURRENT
        .scope(Arc::new(Overlay::seeded_from_current()), f)
        .await
}

/// [`scope`] for synchronous code — a blocking handler, or the body of a
/// `spawn_blocking` closure.
#[cfg(feature = "tokio")]
pub fn sync_scope<T>(f: impl FnOnce() -> T) -> T {
    CURRENT.sync_scope(Arc::new(Overlay::seeded_from_current()), f)
}

/// A captured request scope, for carrying across a thread or task boundary.
///
/// Obtained from [`crate::current_scope`] and handed to [`crate::in_scope`] or
/// [`crate::in_scope_sync`]. Cheap to clone — it shares the overlay rather than
/// copying it, so state recorded in the spawned work reaches the original request.
#[cfg(feature = "tokio")]
#[derive(Clone)]
pub struct ScopeHandle(Arc<Overlay>);

/// Opaque on purpose. `ScopeHandle` implements `Debug` because a public type
/// held in user structs should, but the overlay behind it holds the request's
/// own context and breadcrumb metadata — user data that `filter_keys` redacts on
/// the way out and that a derived `Debug` would print verbatim into a log.
#[cfg(feature = "tokio")]
impl std::fmt::Debug for ScopeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopeHandle").finish_non_exhaustive()
    }
}

/// Captures the current request scope, or a fresh empty one when none is active.
/// Total by design; the caller-facing caveats are on the `global.rs` wrapper.
#[cfg(feature = "tokio")]
pub fn current_scope() -> ScopeHandle {
    ScopeHandle(match current_overlay() {
        Some(overlay) => overlay,
        None => Arc::new(Overlay::seeded_from_current()),
    })
}

/// Runs `f` inside a captured scope. The remedy for `tokio::spawn` losing it.
#[cfg(feature = "tokio")]
pub async fn in_scope<F: Future>(handle: ScopeHandle, f: F) -> F::Output {
    CURRENT.scope(handle.0, f).await
}

/// [`in_scope`] for synchronous work — a `spawn_blocking` closure or a
/// `std::thread::spawn` body.
#[cfg(feature = "tokio")]
pub fn in_scope_sync<T>(handle: ScopeHandle, f: impl FnOnce() -> T) -> T {
    CURRENT.sync_scope(handle.0, f)
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use super::*;
    use serde_json::json;

    fn set(map: &Mutex<Map<String, Value>>, k: &str, v: Value) {
        map.lock().unwrap().insert(k.into(), v);
    }

    #[tokio::test]
    async fn test_overlay_is_absent_outside_a_scope() {
        assert!(
            current_overlay().is_none(),
            "no scope means no overlay, so every read falls back to the client's global"
        );
    }

    #[tokio::test]
    async fn test_overlay_is_visible_inside_a_scope() {
        crate::scope::scope(async {
            assert!(current_overlay().is_some());
        })
        .await;
        assert!(
            current_overlay().is_none(),
            "the overlay must not outlive the scope"
        );
    }

    #[tokio::test]
    async fn test_concurrent_scopes_do_not_see_each_other() {
        // The reported bug, reduced: two requests in flight, each writing its
        // own state, neither seeing the other's.
        let a = crate::scope::scope(async {
            let o = current_overlay().unwrap();
            set(&o.context, "who", json!("a"));
            tokio::task::yield_now().await; // force interleaving
            o.context.lock().unwrap().get("who").cloned()
        });
        let b = crate::scope::scope(async {
            let o = current_overlay().unwrap();
            set(&o.context, "who", json!("b"));
            tokio::task::yield_now().await;
            o.context.lock().unwrap().get("who").cloned()
        });
        let (ra, rb) = tokio::join!(a, b);
        assert_eq!(ra, Some(json!("a")));
        assert_eq!(rb, Some(json!("b")));
    }

    #[tokio::test]
    async fn test_merge_puts_global_underneath_and_overlay_on_top() {
        let global = Mutex::new(Map::new());
        set(&global, "version", json!("1.4.2"));
        set(&global, "shared", json!("from-global"));
        let overlay = Mutex::new(Map::new());
        set(&overlay, "user_id", json!(42));
        set(&overlay, "shared", json!("from-overlay"));

        let merged = merged_context(&global, Some(&overlay));
        assert_eq!(merged["version"], json!("1.4.2"), "global base survives");
        assert_eq!(merged["user_id"], json!(42), "overlay contributes");
        assert_eq!(
            merged["shared"],
            json!("from-overlay"),
            "overlay wins a collision"
        );
    }

    #[tokio::test]
    async fn test_merge_with_no_overlay_is_the_global_alone() {
        let global = Mutex::new(Map::new());
        set(&global, "version", json!("1.4.2"));
        assert_eq!(merged_context(&global, None)["version"], json!("1.4.2"));
    }

    #[tokio::test]
    async fn test_nested_scope_inherits_context_but_not_breadcrumbs() {
        crate::scope::scope(async {
            let outer = current_overlay().unwrap();
            set(&outer.context, "outer", json!(true));
            outer
                .breadcrumbs
                .lock()
                .unwrap()
                .push(crate::breadcrumbs::Breadcrumb::new("outer", "custom", None));

            crate::scope::scope(async {
                let inner = current_overlay().unwrap();
                assert_eq!(
                    inner.context.lock().unwrap().get("outer"),
                    Some(&json!(true)),
                    "a nested scope inherits the enclosing context"
                );
                assert!(
                    inner.breadcrumbs.lock().unwrap().snapshot().is_empty(),
                    "but starts a clean trail — inheriting one reintroduces the mixing"
                );
            })
            .await;
        })
        .await;
    }

    #[test]
    fn test_sync_scope_isolates_and_restores() {
        assert!(current_overlay().is_none());
        crate::scope::sync_scope(|| {
            let o = current_overlay().unwrap();
            set(&o.context, "who", json!("sync"));
            assert_eq!(o.context.lock().unwrap().get("who"), Some(&json!("sync")));
        });
        assert!(current_overlay().is_none(), "restored on exit");
    }

    #[tokio::test]
    async fn test_a_captured_scope_survives_tokio_spawn() {
        let seen = crate::scope::scope(async {
            let o = current_overlay().unwrap();
            set(&o.context, "who", json!("parent"));
            let handle = crate::scope::current_scope();
            tokio::spawn(crate::scope::in_scope(handle, async {
                current_overlay()
                    .unwrap()
                    .context
                    .lock()
                    .unwrap()
                    .get("who")
                    .cloned()
            }))
            .await
            .unwrap()
        })
        .await;
        assert_eq!(seen, Some(json!("parent")));
    }

    #[tokio::test]
    async fn test_a_captured_scope_survives_spawn_blocking() {
        // spawn_blocking is the common case: synchronous database drivers run
        // there, and "query ran" is the canonical breadcrumb.
        let seen = crate::scope::scope(async {
            let o = current_overlay().unwrap();
            set(&o.context, "who", json!("parent"));
            let handle = crate::scope::current_scope();
            tokio::task::spawn_blocking(move || {
                crate::scope::in_scope_sync(handle, || {
                    current_overlay()
                        .unwrap()
                        .context
                        .lock()
                        .unwrap()
                        .get("who")
                        .cloned()
                })
            })
            .await
            .unwrap()
        })
        .await;
        assert_eq!(seen, Some(json!("parent")));
    }

    #[tokio::test]
    async fn test_a_captured_scope_writes_back_to_the_same_overlay() {
        // The handle shares the overlay rather than copying it, so a crumb
        // recorded in a spawned task reaches the parent request's notice.
        crate::scope::scope(async {
            let handle = crate::scope::current_scope();
            tokio::spawn(crate::scope::in_scope(handle, async {
                current_overlay()
                    .unwrap()
                    .breadcrumbs
                    .lock()
                    .unwrap()
                    .push(crate::breadcrumbs::Breadcrumb::new("child", "custom", None));
            }))
            .await
            .unwrap();
            let crumbs = current_overlay()
                .unwrap()
                .breadcrumbs
                .lock()
                .unwrap()
                .snapshot();
            assert_eq!(crumbs.len(), 1);
            assert_eq!(crumbs[0].message, "child");
        })
        .await;
    }

    #[tokio::test]
    async fn test_capturing_outside_a_scope_yields_a_usable_fresh_scope() {
        // current_scope() must not panic or return an Option — an unscoped
        // caller gets a new empty scope, so the API is total.
        assert!(current_overlay().is_none());
        let handle = crate::scope::current_scope();
        crate::scope::in_scope(handle, async {
            assert!(current_overlay().is_some());
        })
        .await;
    }
}
