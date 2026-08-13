# Request-Scoped Context Implementation Plan

> **Historical note — read before following any path or name below.** This plan
> records the work as planned and executed, and is deliberately left as written.
> The implementation was reshaped afterwards: the module it calls `src/scope.rs`
> is now `src/request_scope.rs`, `sync_scope` is now `scope_sync`, and
> `current_scope` / `in_scope` / `in_scope_sync` are now the `ScopeHandle`
> methods `current` / `enter` / `enter_sync`, joined by `try_current`. See
> decision 5 of
> [the design spec](../specs/2026-07-31-request-scoped-context-design.md) for the
> current shape.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `context`, `breadcrumbs`, `event_context`, and `request_id` request-scoped in concurrent servers, so a notice carries only its own request's state.

**Architecture:** Each `Client` keeps its four ambient stores as a global base (`Inner.global: Arc<Scope>`). A tokio task-local carries one request's `Overlay`. Reads merge the client's global beneath the overlay with the overlay winning; writes go to the overlay when one is active and to the global otherwise. Breadcrumbs never merge — an active overlay's trail is used alone. With the `tokio` feature off or no overlay active, every path resolves to the client's global and behaviour is identical to today.

**Tech Stack:** Rust 2024 edition, MSRV 1.88, `tokio` (optional, `default-features = false`, `features = ["rt"]`), `serde_json`, `crossbeam-channel`.

**Spec:** `docs/superpowers/specs/2026-07-31-request-scoped-context-design.md`

## Global Constraints

- MSRV is **1.88**; `rust-version` in `Cargo.toml` and the `toolchain:` pin in the ci.yml `msrv` job must stay in step.
- `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`, and `cargo doc --no-deps` (with `RUSTDOCFLAGS: -D warnings`) must all pass. Every public item needs a doc comment.
- cargo is at `~/.cargo/bin` and is **not** on the non-interactive PATH. Run `export PATH="$HOME/.cargo/bin:$PATH"` in every shell.
- The default build must gain **no** new dependency. `tokio` is optional and additive only.
- `current_overlay()` sits on every notify and event path and **must never panic**. `tokio::task_local!`'s `try_with` returns `Err(AccessError)` rather than panicking when no value is set, including outside a runtime — that is the property this relies on.
- Breadcrumbs cap at 40 entries (`CAPACITY` in `src/breadcrumbs.rs`).
- Do not push. Commit locally only.

## File Structure

| File | Responsibility |
| --- | --- |
| `src/scope.rs` | **New.** `Scope` (a client's global base), `Overlay` (one request's state), `ScopeHandle`, the task-local, and the read/write helpers. All scope mechanics live here. |
| `src/client.rs` | Modified. `Inner` holds `global: Arc<Scope>`; the 12 store access points route through the helpers. |
| `src/global.rs` | Modified. Adds the `scope` / `sync_scope` / `current_scope` / `in_scope` / `in_scope_sync` free functions. |
| `src/lib.rs` | Modified. Declares `mod scope` and re-exports `ScopeHandle` plus the free functions. |
| `src/breadcrumbs.rs` | Modified. `RingBuffer::new` allocates lazily. |
| `Cargo.toml` | Modified. `[features]` and the optional `tokio` dependency. |
| `.github/workflows/ci.yml` | Modified. `--no-default-features` check and a `--features tokio` test run. |

Scope mechanics go in their own module rather than into `client.rs` because `client.rs` is already ~1000 lines and the scope logic has one clear responsibility with a small interface (two helpers plus the public wrappers).

---

### Task 1: Lazy breadcrumb allocation

Standalone and independently valuable: `RingBuffer::new` currently reserves 40 slots, which is right once per client and wrong once per in-flight request. Doing it first means the later per-request buffers are cheap from the moment they exist.

**Files:**
- Modify: `src/breadcrumbs.rs:47-51`
- Test: `src/breadcrumbs.rs` (inline `mod tests`)

**Interfaces:**
- Consumes: nothing.
- Produces: `RingBuffer::new()` unchanged in signature; allocates nothing until first push.

- [ ] **Step 1: Write the failing test**

Add to the `mod tests` block in `src/breadcrumbs.rs`:

```rust
    #[test]
    fn test_ring_buffer_allocates_nothing_until_used() {
        // One buffer per client is cheap; one per in-flight request is not. At
        // high concurrency a 40-slot reservation per request costs megabytes
        // that most requests never use, and all of it is waste when
        // breadcrumbs are disabled.
        let buf = RingBuffer::new();
        assert_eq!(buf.buf.capacity(), 0, "an unused trail must not reserve");

        let mut buf = RingBuffer::new();
        buf.push(Breadcrumb::new("first", "custom", None));
        assert_eq!(buf.snapshot().len(), 1, "still usable once pushed to");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib breadcrumbs::tests::test_ring_buffer_allocates_nothing_until_used`
Expected: FAIL — `assertion left == right failed`, left: `40`, right: `0`.

- [ ] **Step 3: Write minimal implementation**

Replace `RingBuffer::new` in `src/breadcrumbs.rs`:

```rust
    /// An empty trail. Deliberately allocation-free: one of these exists per
    /// in-flight request scope, and most requests record no breadcrumbs at all.
    /// Growth is bounded by `CAPACITY` in `push`.
    pub(crate) fn new() -> Self {
        RingBuffer {
            buf: VecDeque::new(),
        }
    }
```

- [ ] **Step 4: Run the full suite**

Run: `cargo test --lib breadcrumbs`
Expected: PASS, including the existing `test_ring_buffer_drops_oldest_beyond_capacity`, which proves the 40-entry cap still holds without the pre-reservation.

- [ ] **Step 5: Commit**

```bash
cargo clippy --all-targets -- -D warnings && cargo fmt --check
git add src/breadcrumbs.rs
git commit -m "perf: allocate breadcrumb trails lazily

One RingBuffer per client is cheap. Request-scoped trails put one per
in-flight request, where a 40-slot reservation costs 4-6 KiB apiece that
most requests never use, and all of it is waste when breadcrumbs are
disabled. The 40-entry cap is enforced in push and is unchanged."
```

---

### Task 2: Extract `Scope` with the global implementation only

Pure refactor. No new dependency, no behaviour change, no new public API. The existing suite passing unmodified **is** the proof. If the tokio work in Task 3 turns out awkward, this task still stands on its own.

**Files:**
- Create: `src/scope.rs`
- Modify: `src/lib.rs` (add `mod scope;`)
- Modify: `src/client.rs` — `Inner` (lines ~40-53), and the 12 store access points at ~134, ~142, ~253, ~273, ~278, ~302, ~395, ~478, ~494, ~511, ~516, ~521

**Interfaces:**
- Consumes: `RingBuffer` from Task 1.
- Produces:
  - `pub(crate) struct Scope` with `pub(crate) context: Mutex<Map<String, Value>>`, `breadcrumbs: Mutex<RingBuffer>`, `event_context: Mutex<Map<String, Value>>`, `request_id: Mutex<Option<String>>`
  - `pub(crate) fn Scope::new() -> Scope`
  - `Inner.global: Arc<Scope>` replacing the four former fields

- [ ] **Step 1: Create the module**

Create `src/scope.rs`:

```rust
//! Ambient state: the four stores that answer "what was happening?".
//!
//! Today there is exactly one set of them per client, process-wide. Task 3 adds
//! a per-request overlay in front; this module is where both live so the
//! resolution rule has one home.
use crate::breadcrumbs::RingBuffer;
use serde_json::{Map, Value};
use std::sync::Mutex;

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
```

- [ ] **Step 2: Declare the module**

In `src/lib.rs`, add `mod scope;` alongside the other `mod` declarations (they are private module declarations near the top of the file, before the `pub use` block).

- [ ] **Step 3: Replace the four fields on `Inner`**

In `src/client.rs`, delete these four lines from `struct Inner`:

```rust
    context: Mutex<Map<String, Value>>,
    breadcrumbs: Mutex<RingBuffer>,
    event_context: Mutex<Map<String, Value>>,
    request_id: Mutex<Option<String>>,
```

and add:

```rust
    /// This client's process-global ambient state. Task 3 puts a per-request
    /// overlay in front of it.
    global: Arc<crate::scope::Scope>,
```

- [ ] **Step 4: Fix the constructor**

In `ClientBuilder::build`, replace the four initializers:

```rust
            context: Mutex::new(Map::new()),
            breadcrumbs: Mutex::new(RingBuffer::new()),
            event_context: Mutex::new(Map::new()),
            request_id: Mutex::new(None),
```

with:

```rust
            global: Arc::new(crate::scope::Scope::new()),
```

- [ ] **Step 5: Route all 12 access points**

Every `self.0.<store>` and `inner.<store>` becomes `self.0.global.<store>` / `inner.global.<store>`. The complete list, with the current expression on the left:

| Location | Change |
| --- | --- |
| `run_pipeline`, reading context | `inner.context.lock()` → `inner.global.context.lock()` |
| `run_pipeline`, reading breadcrumbs | `inner.breadcrumbs.lock()` → `inner.global.breadcrumbs.lock()` |
| `context()` | `self.0.context.lock()` → `self.0.global.context.lock()` |
| `clear_context()`, context | `self.0.context.lock()` → `self.0.global.context.lock()` |
| `clear_context()`, breadcrumbs | `self.0.breadcrumbs.lock()` → `self.0.global.breadcrumbs.lock()` |
| `add_breadcrumb()` | `self.0.breadcrumbs.lock()` → `self.0.global.breadcrumbs.lock()` |
| `enqueue_event()` | `inner.event_context.lock()` → `inner.global.event_context.lock()` |
| `event_context()` | `self.0.event_context.lock()` → `self.0.global.event_context.lock()` |
| `clear_event_context()` | `self.0.event_context.lock()` → `self.0.global.event_context.lock()` |
| `request_id()` | `self.0.request_id.lock()` → `self.0.global.request_id.lock()` |
| `clear_request_id()` | `self.0.request_id.lock()` → `self.0.global.request_id.lock()` |
| `current_request_id()` | `self.0.request_id.lock()` → `self.0.global.request_id.lock()` |

Do not change any other line. If `RingBuffer` or `Mutex` become unused imports in `client.rs`, remove them; clippy will tell you.

- [ ] **Step 6: Run the whole suite**

Run: `cargo test`
Expected: PASS, all four result lines, with **no test file modified**. That is the proof this task changed nothing observable. If any test needed editing, the refactor was wrong — revert and redo.

- [ ] **Step 7: Commit**

```bash
cargo clippy --all-targets -- -D warnings && cargo fmt --check
git add src/scope.rs src/lib.rs src/client.rs
git commit -m "refactor: lift the four ambient stores into a Scope type

context, breadcrumbs, event_context, and request_id move from four fields
on Inner into one Scope, held as global: Arc<Scope>. No behaviour change
and no new dependency — the existing suite passes unmodified, which is the
point of doing this separately from the per-request overlay that follows."
```

---

### Task 3: The task-local overlay

Adds the `tokio` feature, the `Overlay`, the resolution helpers, and `scope()` / `sync_scope()`. This is where behaviour changes.

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/scope.rs`
- Modify: `src/client.rs` (route the 12 points through the helpers)
- Modify: `src/global.rs` (free functions)
- Modify: `src/lib.rs` (re-exports)
- Test: `src/scope.rs` (inline `mod tests`)

**Interfaces:**
- Consumes: `Scope` from Task 2.
- Produces:
  - `pub(crate) struct Overlay` — same four fields as `Scope`
  - `pub(crate) fn current_overlay() -> Option<Arc<Overlay>>`
  - `pub(crate) fn merged_context(global: &Mutex<Map<String, Value>>, overlay: Option<&Mutex<Map<String, Value>>>) -> Map<String, Value>`
  - `pub async fn scope<F: Future>(f: F) -> F::Output`
  - `pub fn sync_scope<T>(f: impl FnOnce() -> T) -> T`

- [ ] **Step 1: Add the feature and dependency**

In `Cargo.toml`, add after the `[dependencies]` block's existing entries:

```toml
tokio = { version = "1", default-features = false, features = ["rt"], optional = true }
```

and add a new section before `[dependencies]`:

```toml
[features]
# Request-scoped context via a tokio task-local. Named for the dependency it
# pulls in rather than the capability, so the cost is visible at the call site.
# tokio's `rt` feature is `rt = []` — it adds no transitive crates.
tokio = ["dep:tokio"]
```

Also add `tokio` with `features = ["rt", "macros"]` to `[dev-dependencies]` so tests can use `#[tokio::test]`:

```toml
# `rt-multi-thread` is required by `#[tokio::test(flavor = "multi_thread")]`,
# which the concurrency test in step 8 uses — without it that attribute fails
# to compile.
tokio = { version = "1", default-features = false, features = ["rt", "rt-multi-thread", "macros"] }
```

- [ ] **Step 2: Write the failing tests**

Add to `src/scope.rs`:

```rust
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
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test --features tokio --lib scope`
Expected: FAIL to compile — `cannot find function current_overlay`, `cannot find function merged_context`, `cannot find function scope`.

- [ ] **Step 4: Implement the overlay and helpers**

Add to `src/scope.rs`:

```rust
use std::future::Future;
use std::sync::Arc;

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

impl Overlay {
    /// A fresh overlay seeded from the enclosing one, if any.
    ///
    /// Context, event context, and request id are inherited so a nested scope
    /// keeps the request it belongs to. Breadcrumbs are not: inheriting a trail
    /// is exactly the cross-request mixing this exists to remove.
    fn seeded_from_current() -> Overlay {
        let (context, event_context, request_id) = match current_overlay() {
            Some(parent) => (
                parent.context.lock().unwrap_or_else(|e| e.into_inner()).clone(),
                parent
                    .event_context
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone(),
                parent
                    .request_id
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone(),
            ),
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

/// Runs `f` with a fresh request scope.
///
/// Ambient state written inside — [`crate::context`], [`crate::add_breadcrumb`],
/// [`crate::request_id`], [`crate::event_context`] — belongs to this scope and is
/// invisible to concurrent ones.
///
/// **The scope does not cross a thread or task boundary.** It lives in a
/// thread-local that tokio installs for the duration of each poll, so
/// `tokio::spawn`, `tokio::task::spawn_blocking`, and `std::thread::spawn` all
/// start with no scope and fall back to process-global state. Carry it across
/// explicitly with [`crate::current_scope`] and [`crate::in_scope`].
#[cfg(feature = "tokio")]
pub async fn scope<F: Future>(f: F) -> F::Output {
    CURRENT.scope(Arc::new(Overlay::seeded_from_current()), f).await
}

/// [`scope`] for synchronous code — a blocking handler, or the body of a
/// `spawn_blocking` closure.
#[cfg(feature = "tokio")]
pub fn sync_scope<T>(f: impl FnOnce() -> T) -> T {
    CURRENT.sync_scope(Arc::new(Overlay::seeded_from_current()), f)
}
```

- [ ] **Step 5: Run the scope tests**

Run: `cargo test --features tokio --lib scope`
Expected: PASS, 7 tests.

- [ ] **Step 6: Route the client's reads and writes through the overlay**

In `src/client.rs`, add a private helper on `impl Client`:

```rust
    /// The overlay for the current request, if a scope is active.
    fn overlay(&self) -> Option<Arc<crate::scope::Overlay>> {
        crate::scope::current_overlay()
    }
```

Then change the 12 points. Writes prefer the overlay; reads merge. Replace `context()` with:

```rust
    pub fn context<I, K>(&self, entries: I)
    where
        I: IntoIterator<Item = (K, Value)>,
        K: Into<String>,
    {
        let overlay = self.overlay();
        let store = match &overlay {
            Some(o) => &o.context,
            None => &self.0.global.context,
        };
        let mut ctx = store.lock().unwrap_or_else(|e| e.into_inner());
        for (k, v) in entries {
            let key = k.into();
            if v.is_null() {
                ctx.remove(&key);
            } else {
                ctx.insert(key, v);
            }
        }
    }
```

Apply the identical `overlay`-or-`global` selection to `event_context()`, `clear_event_context()`, `request_id()`, `clear_request_id()`, and `add_breadcrumb()`.

`clear_context()` clears the overlay when one is active and the global otherwise — it must not clear both, so a request clearing its own context cannot erase the application's:

```rust
    pub fn clear_context(&self) {
        match self.overlay() {
            Some(o) => {
                o.context.lock().unwrap_or_else(|e| e.into_inner()).clear();
                o.breadcrumbs.lock().unwrap_or_else(|e| e.into_inner()).clear();
                o.event_context.lock().unwrap_or_else(|e| e.into_inner()).clear();
                *o.request_id.lock().unwrap_or_else(|e| e.into_inner()) = None;
            }
            None => {
                let g = &self.0.global;
                g.context.lock().unwrap_or_else(|e| e.into_inner()).clear();
                g.breadcrumbs.lock().unwrap_or_else(|e| e.into_inner()).clear();
                g.event_context.lock().unwrap_or_else(|e| e.into_inner()).clear();
                *g.request_id.lock().unwrap_or_else(|e| e.into_inner()) = None;
            }
        }
    }
```

The three reads merge. In `run_pipeline`:

```rust
        let overlay = self.overlay();
        let scope_context = crate::scope::merged_context(
            &inner.global.context,
            overlay.as_ref().map(|o| &o.context),
        );
        notice.merge_scope_context(scope_context);
        let request_id_fallback = self.current_request_id();
        let breadcrumbs = inner.config.breadcrumbs_enabled.then(|| {
            // Never merged: an active overlay's trail is used alone, because
            // merging the global trail reintroduces the cross-request mixing.
            match &overlay {
                Some(o) => o.breadcrumbs.lock().unwrap_or_else(|e| e.into_inner()).snapshot(),
                None => inner
                    .global
                    .breadcrumbs
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .snapshot(),
            }
        });
```

In `enqueue_event`, replace the `event_context` read:

```rust
        let overlay = self.overlay();
        let scope = crate::scope::merged_context(
            &inner.global.event_context,
            overlay.as_ref().map(|o| &o.event_context),
        );
```

And `current_request_id` prefers the overlay:

```rust
    pub(crate) fn current_request_id(&self) -> Option<String> {
        if let Some(o) = self.overlay() {
            return o.request_id.lock().unwrap_or_else(|e| e.into_inner()).clone();
        }
        self.0
            .global
            .request_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
```

- [ ] **Step 7: Add the facade and re-exports**

In `src/global.rs`, add:

```rust
/// Runs `f` with a fresh request scope. See [`crate::Client::context`] for what
/// becomes request-local. Requires the `tokio` feature.
#[cfg(feature = "tokio")]
pub async fn scope<F: std::future::Future>(f: F) -> F::Output {
    crate::scope::scope(f).await
}

/// [`scope`] for synchronous code. Requires the `tokio` feature.
#[cfg(feature = "tokio")]
pub fn sync_scope<T>(f: impl FnOnce() -> T) -> T {
    crate::scope::sync_scope(f)
}
```

In `src/lib.rs`, extend the `pub use crate::global::{...}` list with `scope` and `sync_scope`, gated:

```rust
#[cfg(feature = "tokio")]
pub use crate::global::{scope, sync_scope};
```

- [ ] **Step 8: Write the end-to-end contamination and isolation tests**

Add to the `mod tests` block in `src/client.rs`:

```rust
    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_requests_do_not_share_state() {
        // The bug this feature exists to fix. Before scoping, each notice
        // carried whatever the global 40-entry trail happened to hold.
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());

        let mut handles = Vec::new();
        for i in 0..8 {
            let client = client.clone();
            handles.push(tokio::spawn(crate::scope::scope(async move {
                client.request_id(format!("req-{i}"));
                client.context([("who", json!(i))]);
                client.add_breadcrumb(&format!("crumb-{i}"), "custom", None);
                tokio::task::yield_now().await;
                client.notify_notice(crate::Notice::message("Boom", "x"));
            })));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert!(client.flush(Duration::from_secs(5)));

        for notice in delivered(&transport) {
            let who = notice["request"]["context"]["who"].as_i64().unwrap();
            let crumbs = notice["breadcrumbs"]["trail"].as_array().unwrap();
            assert_eq!(crumbs.len(), 1, "exactly this request's own crumb");
            assert_eq!(crumbs[0]["message"], json!(format!("crumb-{who}")));
            // The request-id slot lands in `correlation_context`, not
            // `request.context` — see `assemble` in src/notice.rs.
            assert_eq!(
                notice["correlation_context"]["request_id"],
                json!(format!("req-{who}")),
                "request_id must match the same request as the context"
            );
        }
        client.shutdown(Duration::from_secs(5));
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_an_unscoped_spawn_does_poison_later_scopes() {
        // The failure mode behind shipping a capture API: a child with no scope
        // writes to the client's global store, which every later scope merges
        // beneath itself. Left unchecked, one request's context persists into
        // all subsequent ones.
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());

        crate::scope::scope(async {
            let c = client.clone();
            // Deliberately NOT wrapped in in_scope — this is the hazard.
            tokio::spawn(async move { c.context([("leaked", json!(true))]) })
                .await
                .unwrap();
        })
        .await;

        let leaked = crate::scope::scope(async {
            client.notify_notice(crate::Notice::message("Boom", "x"));
            assert!(client.flush(Duration::from_secs(5)));
            delivered(&transport)[0]["request"]["context"]
                .get("leaked")
                .cloned()
        })
        .await;
        assert_eq!(
            leaked,
            Some(json!(true)),
            "documents the known hazard: an unscoped write lands in the global \
             base and every later scope merges it. Task 4's capture API is the \
             remedy; if this ever returns None the docs must be updated."
        );
        client.shutdown(Duration::from_secs(5));
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_global_context_set_after_scope_entry_is_visible() {
        // The property a snapshot-based design would have lost.
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        crate::scope::scope(async {
            client.context([("in_scope", json!(1))]);
            // A different clone writes globally, from outside any scope.
            let outside = client.clone();
            std::thread::spawn(move || outside.context([("late", json!(2))]))
                .join()
                .unwrap();
            client.notify_notice(crate::Notice::message("Boom", "x"));
            assert!(client.flush(Duration::from_secs(5)));
            let ctx = &delivered(&transport)[0]["request"]["context"];
            assert_eq!(ctx["in_scope"], json!(1));
            assert_eq!(ctx["late"], json!(2), "merged at read time, not copied");
        })
        .await;
        client.shutdown(Duration::from_secs(5));
    }
```

- [ ] **Step 9: Run both feature configurations**

Run: `cargo test --features tokio`
Expected: PASS.

Run: `cargo test`
Expected: PASS — the default build still compiles and behaves as before, with the `#[cfg(feature = "tokio")]` tests skipped.

- [ ] **Step 10: Commit**

```bash
cargo clippy --all-targets --features tokio -- -D warnings
cargo clippy --all-targets -- -D warnings
cargo fmt --check
git add Cargo.toml Cargo.lock src/scope.rs src/client.rs src/global.rs src/lib.rs
git commit -m "feat: request-scoped context via a tokio task-local overlay

A task-local carries one request's Overlay; each client merges its own
global base beneath it, overlay winning. Breadcrumbs are never merged — an
active overlay's trail is used alone, since merging the global trail
reintroduces the cross-request mixing this removes.

Merging rather than snapshotting is what lets two clients share one
request while keeping their own bases, and it also keeps global context set
after scope entry visible, which a snapshot would have lost.

Behind an optional tokio feature; tokio's rt feature is rt = [] and adds no
transitive crates. With the feature off or no scope active, every path
resolves to the client's global and behaviour is unchanged."
```

---

### Task 4: Scope capture across thread and task boundaries

**Files:**
- Modify: `src/scope.rs`
- Modify: `src/global.rs`, `src/lib.rs`
- Test: `src/scope.rs`

**Interfaces:**
- Consumes: `Overlay`, `current_overlay`, `CURRENT` from Task 3.
- Produces:
  - `pub struct ScopeHandle` (opaque, `Clone`)
  - `pub fn current_scope() -> ScopeHandle`
  - `pub async fn in_scope<F: Future>(handle: ScopeHandle, f: F) -> F::Output`
  - `pub fn in_scope_sync<T>(handle: ScopeHandle, f: impl FnOnce() -> T) -> T`

- [ ] **Step 1: Write the failing tests**

Add to the `mod tests` block in `src/scope.rs`:

```rust
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
            let crumbs = current_overlay().unwrap().breadcrumbs.lock().unwrap().snapshot();
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --features tokio --lib scope`
Expected: FAIL to compile — `cannot find function current_scope`, `cannot find function in_scope`, `cannot find function in_scope_sync`.

- [ ] **Step 3: Implement capture**

Add to `src/scope.rs`:

```rust
/// A captured request scope, for carrying across a thread or task boundary.
///
/// Obtained from [`current_scope`] and handed to [`in_scope`] or
/// [`in_scope_sync`]. Cheap to clone — it shares the overlay rather than copying
/// it, so state recorded in the spawned work reaches the original request.
#[cfg(feature = "tokio")]
#[derive(Clone)]
pub struct ScopeHandle(Arc<Overlay>);

/// Captures the current request scope, or a fresh empty one when none is active.
///
/// Total by design: an unscoped caller gets a usable handle rather than an
/// `Option` to unwrap on a path that is often error handling already.
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
```

- [ ] **Step 4: Run the tests**

Run: `cargo test --features tokio --lib scope`
Expected: PASS, 11 tests.

- [ ] **Step 5: Re-export**

In `src/global.rs`, add thin wrappers mirroring `scope`/`sync_scope`:

```rust
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
```

In `src/lib.rs`, extend the gated re-export:

```rust
#[cfg(feature = "tokio")]
pub use crate::global::{current_scope, in_scope, in_scope_sync, scope, sync_scope};
#[cfg(feature = "tokio")]
pub use crate::scope::ScopeHandle;
```

- [ ] **Step 6: Verify both configurations**

Run: `cargo test --features tokio && cargo test`
Expected: PASS both.

- [ ] **Step 7: Commit**

```bash
cargo clippy --all-targets --features tokio -- -D warnings
cargo clippy --all-targets -- -D warnings
cargo fmt --check
git add src/scope.rs src/global.rs src/lib.rs
git commit -m "feat: capture a scope to carry it across a thread boundary

The task-local lives in a thread-local that tokio installs per poll, so the
scope is lost by tokio::spawn and equally by spawn_blocking and
std::thread::spawn — where synchronous database drivers run, and where
'query ran' is the canonical breadcrumb.

The fallback is not merely lossy: an unscoped child writes to the client's
global store, which every later overlay merges beneath itself, so one
request's context persists into all subsequent ones.

ScopeHandle shares the overlay rather than copying it, so state recorded in
spawned work reaches the original request's notice. current_scope() is total
— an unscoped caller gets a fresh usable scope rather than an Option."
```

---

### Task 5: Documentation and CI feature matrix

**Files:**
- Modify: `src/client.rs` — rustdoc on `context`, `clear_context`, `add_breadcrumb`, `event_context`, `request_id`
- Modify: `README.md` — the Context section
- Modify: `src/lib.rs` — crate-level docs
- Modify: `.github/workflows/ci.yml`
- Create: `examples/scoped_request.rs`

**Interfaces:**
- Consumes: everything from Tasks 3 and 4.
- Produces: no new API.

- [ ] **Step 1: Rewrite the hazard warnings**

Each of the five doc comments currently states the store is process-wide without qualification. Rewrite each so the hazard is tied to its real condition. For `add_breadcrumb`, replace the existing warning paragraph with:

```rust
    /// Without an active scope the trail is process-wide, and under concurrency
    /// crumbs from unrelated requests interleave and evict one another — treat it
    /// as a process-level log. Inside [`crate::scope`] (requires the `tokio`
    /// feature) the trail belongs to that request alone.
```

Apply the same treatment to `context`, `clear_context`, `event_context`, and `request_id`: keep the process-wide warning as the no-scope case, and point at `scope()` as the remedy. `request_id`'s existing advice to "put `request_id` in the event payload instead" stays as the guidance for anyone not using the feature.

- [ ] **Step 2: Document the thread-boundary rule where it will be read**

The `scope()` rustdoc written in Task 3 already names `tokio::spawn`, `spawn_blocking`, and `std::thread::spawn`. Extend it with the residual limitation, which no API can fix:

```rust
/// A `spawn` performed *inside a dependency* cannot be wrapped by you, so those
/// notices fall back to process-global state. There is no workaround short of
/// the dependency cooperating.
```

- [ ] **Step 3: Add the runnable example**

Create `examples/scoped_request.rs`:

```rust
//! Request-scoped context. Run with: cargo run --features tokio --example scoped_request
use serde_json::json;
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let config = honeybadger::Config::builder()
        .env("development")
        .exclude_envs(Vec::<String>::new())
        .api_key("example-key")
        .build()
        .expect("config");
    let _guard = honeybadger::init(config).expect("init");

    // Process-wide, set once at boot: every notice gets these.
    honeybadger::context([("version", json!("1.4.2"))]);

    let mut requests = Vec::new();
    for id in 0..3 {
        requests.push(tokio::spawn(honeybadger::scope(async move {
            honeybadger::request_id(format!("req-{id}"));
            honeybadger::context([("user_id", json!(id))]);
            honeybadger::add_breadcrumb("query ran", "query", None);

            // Crossing a thread boundary needs the scope carried explicitly.
            let scope = honeybadger::current_scope();
            tokio::task::spawn_blocking(move || {
                honeybadger::in_scope_sync(scope, || {
                    honeybadger::add_breadcrumb("blocking work", "custom", None);
                })
            })
            .await
            .expect("blocking task");

            honeybadger::notify_notice(honeybadger::Notice::message(
                "Example",
                &format!("from request {id}"),
            ));
        })));
    }
    for r in requests {
        r.await.expect("request");
    }

    println!("three requests reported, each with only its own two breadcrumbs");
}
```

- [ ] **Step 4: Gate the example on the feature**

The example calls `honeybadger::scope`, which does not exist without the feature,
so `cargo build --examples` on the default build would fail to compile it. Add to
`Cargo.toml` **before** building:

```toml
[[example]]
name = "scoped_request"
required-features = ["tokio"]
```

- [ ] **Step 5: Verify the example compiles and runs**

Run: `cargo run --features tokio --example scoped_request`
Expected: prints the final line. Reporting goes to the null transport because `env` is `development` with `exclude_envs` emptied — no network required.

Run: `cargo build --examples`
Expected: PASS, with `scoped_request` skipped rather than attempted — that is what the `required-features` stanza buys.

- [ ] **Step 6: Add the CI feature matrix**

In `.github/workflows/ci.yml`, add two `run` steps to the `test` job after the existing `cargo test`:

```yaml
      # The default build must never gain a dependency or lose behaviour.
      - run: cargo test --no-default-features
      - run: cargo test --features tokio
```

And in the `lint` job, after the existing clippy line:

```yaml
      - run: cargo clippy --all-targets --features tokio -- -D warnings
```

- [ ] **Step 7: Full verification**

```bash
cargo test && cargo test --features tokio && cargo test --no-default-features
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --features tokio -- -D warnings
cargo fmt --check
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
cargo +1.88 check --all-targets
```

Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add src/client.rs src/lib.rs README.md examples/scoped_request.rs Cargo.toml .github/workflows/ci.yml
git commit -m "docs: scope the process-wide warnings to their real condition

The five hazard warnings said the stores are process-wide without
qualification. That is now only true with no active scope, so each says so
and points at scope() as the remedy.

scope()'s own docs name spawn_blocking and std::thread::spawn alongside
tokio::spawn — understating this as 'spawn does not propagate' was the
first draft's mistake — and record the one hole no API closes: a spawn
inside a dependency cannot be wrapped by the caller.

CI gains --no-default-features and --features tokio runs so the default
build cannot silently break and the feature cannot silently rot."
```

---

## Self-Review

**Spec coverage.** Every spec section maps to a task: the problem statement drives Task 3's isolation test; decisions 1-3 are Task 3's mechanism; decision 4 (overlay + merge + nesting + `clear_context`) is Task 3 steps 4 and 6; decision 5 (capture) is Task 4; decision 6 (per-client bases) is verified by Task 3's `test_global_context_set_after_scope_entry_is_visible` and the merge tests; decision 7 (lazy allocation) is Task 1. The spec's testing list is covered, with one deliberate deviation noted below. Commit sequence matches the spec's four commits, plus Task 1 split out first because it is independently valuable and makes the per-request buffers cheap before they exist.

**Deviation from the spec's test list.** The spec asks for a test that "two clients with different global context, one active overlay: each notice carries its own client's base plus the shared overlay." Task 3 covers the mechanism through `merged_context` unit tests and the late-global-write test rather than building two full clients, because `test_client` wires a whole worker per client and the merge behaviour is what actually matters. If the implementer wants the literal two-client test, add it to Task 3 step 8 — it needs two `TestTransport`s and two `Client`s sharing one `scope()`.

**Type consistency.** `Overlay`, `Scope`, `ScopeHandle`, `current_overlay`, `merged_context`, `seeded_from_current`, `scope`, `sync_scope`, `current_scope`, `in_scope`, `in_scope_sync` are used identically everywhere they appear. `Inner.global` is `Arc<Scope>` in Task 2 and read as `self.0.global.<store>` in Tasks 2 and 3. `ScopeHandle` wraps `Arc<Overlay>` in Task 4 and is consumed by value in `in_scope`/`in_scope_sync`, matching the `#[derive(Clone)]` that lets a caller reuse one.

**Known risk the implementer should expect.** Task 3 step 6 touches twelve call sites by hand. The compiler catches a missed `global`, but it cannot catch a site that reads the global where it should read the overlay — that shows up as a *passing* build with a failing isolation test. If `test_concurrent_requests_do_not_share_state` fails, suspect a read site still going straight to `inner.global`.
