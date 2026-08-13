//! Ambient state: the four stores that answer "what was happening?".
//!
//! They come in two layers: one process-global set per client, and a per-request
//! `Overlay` read on top of it. Both live here so the resolution rule has one
//! home.
//!
//! The public scope API lives here too and is re-exported at the crate root, so
//! the doc comments below are the copy rustdoc renders and
//! `RUSTDOCFLAGS=-D warnings` lint-checks. There is deliberately no second copy
//! anywhere to drift from.
//!
//! The module is called `request_scope` rather than `scope` so that nothing
//! collides with the re-exported `scope` function: a `crate::scope` or bare
//! `scope` intra-doc link would otherwise be ambiguous between the module and
//! the function. Ordinary `cargo doc` happens to resolve it silently while the
//! module is private, but `cargo doc --document-private-items` documents the
//! module and then the same link is a hard error under
//! `RUSTDOCFLAGS=-D warnings`. Renaming removes the collision outright; the
//! module name is private and never appears in a caller's path.
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

/// Runs `f` with a fresh request scope. Requires the `tokio` feature.
///
/// [`context`](crate::context), [`add_breadcrumb`](crate::add_breadcrumb),
/// [`event_context`](crate::event_context), and [`request_id`](crate::request_id)
/// called inside `f` write to this request's own state instead of the
/// process-wide store, and concurrent scopes never see each other's values.
///
/// # What a scoped read sees
///
/// The two halves are not symmetrical, which matters if you set process-wide
/// facts at boot:
///
/// - [`context`](crate::context) and [`event_context`](crate::event_context)
///   **merge**: a notice or event reported inside a scope carries the client's
///   process-wide entries with this request's own on top, and the request wins a
///   key collision. A `version` set once at startup still reaches every scoped
///   notice.
/// - The breadcrumb trail is **replaced**: a scoped notice carries this
///   request's crumbs alone, never the process-wide trail — even when the
///   request recorded none — because merging trails is the cross-request
///   contamination scoping exists to remove.
/// - [`request_id`](crate::request_id) is this request's own when it set one,
///   and the process-wide id otherwise.
///
/// A nested scope starts from the enclosing scope's context, event context, and
/// request id, and with an empty breadcrumb trail.
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
/// [`ScopeHandle::current`] and re-entering it with [`ScopeHandle::enter`]
/// (async) or [`ScopeHandle::enter_sync`] (a `spawn_blocking` closure or a
/// `std::thread::spawn` body):
///
/// ```rust,no_run
/// # #[cfg(feature = "tokio")]
/// # async fn handler() {
/// let scope = honeybadger::ScopeHandle::current();
/// tokio::task::spawn_blocking(move || {
///     scope.enter_sync(|| {
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
pub async fn scope<F: Future>(f: F) -> F::Output {
    CURRENT
        .scope(Arc::new(Overlay::seeded_from_current()), f)
        .await
}

/// [`scope`] for synchronous code — a blocking request handler, or the body of a
/// `spawn_blocking` closure. Requires the `tokio` feature.
///
/// Identical to [`scope`] in every other respect: a fresh scope for the duration
/// of `f`, the same merge rules for what a read inside it sees, and the
/// process-wide state restored when `f` returns. The thread-boundary rule applies
/// here too — work `f` hands to another thread starts with no scope unless you
/// carry it across with [`ScopeHandle::current`] and
/// [`ScopeHandle::enter_sync`].
#[cfg(feature = "tokio")]
pub fn scope_sync<T>(f: impl FnOnce() -> T) -> T {
    CURRENT.sync_scope(Arc::new(Overlay::seeded_from_current()), f)
}

/// A captured request scope, for carrying across a thread or task boundary.
/// Requires the `tokio` feature.
///
/// Obtained from [`ScopeHandle::current`] (or [`ScopeHandle::try_current`]) and
/// re-entered with [`ScopeHandle::enter`] or [`ScopeHandle::enter_sync`], each of
/// which consumes it so the work it is handed to can own it. Cheap to clone — it
/// shares the request's state rather than copying it, so anything recorded through
/// any clone reaches the request that captured it, and cloning is what you do to
/// enter the same scope twice.
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

#[cfg(feature = "tokio")]
impl ScopeHandle {
    /// Captures the current request scope, **or a fresh empty one when no scope
    /// is active**.
    ///
    /// Cheap: the handle shares the request's state rather than copying it, so
    /// anything recorded through it reaches the request that captured it.
    /// Re-enter it with [`enter`](Self::enter) (async) or
    /// [`enter_sync`](Self::enter_sync) (a `spawn_blocking` closure or a
    /// `std::thread::spawn` body).
    ///
    /// Total by design: an unscoped caller gets a usable handle rather than an
    /// `Option` to unwrap on a path that is often error handling already. **The
    /// cost is that such a handle is detached.** A handle captured when no scope
    /// was active — or captured inside a scope and re-entered after that scope
    /// has exited — carries an overlay of its own. Context and breadcrumbs
    /// recorded through it are visible for exactly as long as the handle is
    /// entered: a notice or event reported *inside* that work does carry them.
    /// What they never reach is the request that spawned the work, which is
    /// reading a different overlay, so anything the child recorded is missing
    /// from the parent's notices. Note that this is a change in failure mode:
    /// without a scope anywhere, an `add_breadcrumb` from a `spawn_blocking`
    /// child landed in the process-wide trail, which the parent's notice then
    /// read, so it *did* arrive. Capture inside the scope whose state you want,
    /// and use the handle while that scope is still running.
    ///
    /// Use [`try_current`](Self::try_current) where you would rather detect that
    /// case than record into an overlay the surrounding request cannot see.
    pub fn current() -> ScopeHandle {
        ScopeHandle(match current_overlay() {
            Some(overlay) => overlay,
            None => Arc::new(Overlay::seeded_from_current()),
        })
    }

    /// Captures the current request scope, or `None` when no scope is active.
    ///
    /// The fallible counterpart to [`current`](Self::current), which is total and
    /// hands back a fresh empty scope instead. Prefer this one when "no scope
    /// here" is a condition worth acting on rather than papering over: a handle
    /// captured outside a scope carries an overlay of its own, so what the
    /// spawned work records through it stays with that work and never reaches
    /// the notices reported by the code that spawned it. Reach for it in library
    /// or middleware code that can fall back to putting the data on the notice,
    /// log a warning, or assert during development that the request really was
    /// wrapped in [`scope`].
    ///
    /// Prefer [`current`](Self::current) in application code that is inside a
    /// scope by construction, where an `Option` only adds an unwrap to a path
    /// that is often error handling already.
    pub fn try_current() -> Option<ScopeHandle> {
        current_overlay().map(ScopeHandle)
    }

    /// Runs `f` inside this captured scope — the remedy for [`tokio::spawn`]
    /// starting with none.
    ///
    /// The handle shares the request's state rather than copying it, so context,
    /// breadcrumbs, event context, and request id recorded inside `f` belong to
    /// the request that captured the handle: they are visible both to notices
    /// reported inside `f` and to later ones reported back in the original
    /// request. A scope already active on this task is replaced for the duration
    /// of `f`, not merged with the handle's.
    ///
    /// Consumes the handle, so the returned future owns it and can be handed
    /// straight to [`tokio::spawn`], which requires `'static`:
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "tokio")]
    /// # async fn handler() {
    /// let scope = honeybadger::ScopeHandle::current();
    /// tokio::spawn(scope.enter(async {
    ///     // still recorded against the request that captured `scope`:
    ///     honeybadger::add_breadcrumb("cache warmed", "custom", None);
    /// }))
    /// .await
    /// .unwrap();
    /// # }
    /// ```
    ///
    /// To re-enter the same scope more than once, clone the handle — it is an
    /// [`Arc`] newtype, so a clone is one atomic increment and every clone shares
    /// the one request's state:
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "tokio")]
    /// # async fn handler() {
    /// let scope = honeybadger::ScopeHandle::current();
    /// for shard in 0..3 {
    ///     tokio::spawn(scope.clone().enter(async move {
    ///         honeybadger::add_breadcrumb(&format!("shard {shard} queried"), "query", None);
    ///     }))
    ///     .await
    ///     .unwrap();
    /// }
    /// # }
    /// ```
    ///
    /// See [`scope`] for the `spawn_blocking` counterpart.
    pub async fn enter<F: Future>(self, f: F) -> F::Output {
        CURRENT.scope(self.0, f).await
    }

    /// [`enter`](Self::enter) for synchronous work — a
    /// [`tokio::task::spawn_blocking`] closure or a [`std::thread::spawn`] body.
    ///
    /// The common case, and the reason the capture API exists: synchronous
    /// database drivers run on `spawn_blocking`, and "query ran" is the canonical
    /// breadcrumb. Same share-not-copy semantics as [`enter`](Self::enter), and
    /// likewise consumes the handle — clone it to use it again; see [`scope`] for
    /// the worked example.
    pub fn enter_sync<T>(self, f: impl FnOnce() -> T) -> T {
        CURRENT.sync_scope(self.0, f)
    }
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
        scope(async {
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
        let a = scope(async {
            let o = current_overlay().unwrap();
            set(&o.context, "who", json!("a"));
            tokio::task::yield_now().await; // force interleaving
            o.context.lock().unwrap().get("who").cloned()
        });
        let b = scope(async {
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
        scope(async {
            let outer = current_overlay().unwrap();
            set(&outer.context, "outer", json!(true));
            outer
                .breadcrumbs
                .lock()
                .unwrap()
                .push(crate::breadcrumbs::Breadcrumb::new("outer", "custom", None));

            scope(async {
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
    fn test_scope_sync_isolates_and_restores() {
        assert!(current_overlay().is_none());
        scope_sync(|| {
            let o = current_overlay().unwrap();
            set(&o.context, "who", json!("sync"));
            assert_eq!(o.context.lock().unwrap().get("who"), Some(&json!("sync")));
        });
        assert!(current_overlay().is_none(), "restored on exit");
    }

    #[tokio::test]
    async fn test_a_captured_scope_survives_tokio_spawn() {
        let seen = scope(async {
            let o = current_overlay().unwrap();
            set(&o.context, "who", json!("parent"));
            let handle = ScopeHandle::current();
            // `enter` consumes the handle, so the future owns it and goes
            // straight to spawn — the idiom users should copy.
            tokio::spawn(handle.enter(async {
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
        let seen = scope(async {
            let o = current_overlay().unwrap();
            set(&o.context, "who", json!("parent"));
            let handle = ScopeHandle::current();
            tokio::task::spawn_blocking(move || {
                handle.enter_sync(|| {
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
        scope(async {
            let handle = ScopeHandle::current();
            tokio::spawn(handle.enter(async {
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
        // ScopeHandle::current() must not panic or return an Option — an
        // unscoped caller gets a new empty scope, so the API is total.
        assert!(current_overlay().is_none());
        let handle = ScopeHandle::current();
        handle
            .enter(async {
                let o = current_overlay().expect("the detached overlay is installed");
                // And it is readable while entered — a notice reported in here
                // does carry what was recorded through the handle. What no
                // notice outside this work sees is the same state, because
                // outside there is no overlay to read it from.
                set(&o.context, "who", json!("detached"));
                assert_eq!(
                    o.context.lock().unwrap().get("who"),
                    Some(&json!("detached"))
                );
            })
            .await;
        assert!(current_overlay().is_none(), "and it is gone again on exit");
    }

    #[tokio::test]
    async fn test_try_current_reports_whether_a_scope_is_active() {
        // The detectable counterpart to current(): outside a scope there is
        // nothing to capture, and a caller that would otherwise record into a
        // detached overlay its own request cannot read can see that.
        assert!(ScopeHandle::try_current().is_none());
        scope(async {
            assert!(ScopeHandle::try_current().is_some());
        })
        .await;
        assert!(ScopeHandle::try_current().is_none());
    }

    #[tokio::test]
    async fn test_try_current_captures_the_same_overlay_as_current() {
        // Same sharing semantics — a write through the handle reaches the scope.
        scope(async {
            let handle = ScopeHandle::try_current().unwrap();
            tokio::spawn(handle.enter(async {
                set(&current_overlay().unwrap().context, "who", json!("child"));
            }))
            .await
            .unwrap();
            assert_eq!(
                current_overlay()
                    .unwrap()
                    .context
                    .lock()
                    .unwrap()
                    .get("who"),
                Some(&json!("child"))
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_a_cloned_handle_enters_the_same_scope_again() {
        // `enter`/`enter_sync` consume the handle so spawned work can own it;
        // cloning is how you enter the same scope twice, and every clone shares
        // the one overlay.
        scope(async {
            let handle = ScopeHandle::current();
            for i in 0..3 {
                handle
                    .clone()
                    .enter(async move {
                        current_overlay().unwrap().breadcrumbs.lock().unwrap().push(
                            crate::breadcrumbs::Breadcrumb::new(
                                &format!("crumb {i}"),
                                "custom",
                                None,
                            ),
                        );
                    })
                    .await;
                handle
                    .clone()
                    .enter_sync(|| assert!(current_overlay().is_some()));
            }
            assert_eq!(
                current_overlay()
                    .unwrap()
                    .breadcrumbs
                    .lock()
                    .unwrap()
                    .snapshot()
                    .len(),
                3
            );
        })
        .await;
    }
}
