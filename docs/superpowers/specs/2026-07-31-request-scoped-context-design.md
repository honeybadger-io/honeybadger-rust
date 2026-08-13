# Request-scoped context

Status: designed 2026-07-31, revised after review 2026-08-12, not yet
implemented. Delivers the "scope contract" reserved by the Phase 1 spec.
Independent of Phase 3, and the load-bearing half of it.

## Problem

Four stores are process-global: `context`, `breadcrumbs`, `event_context`, and
`request_id`. In a program that handles one unit of work at a time — a CLI, a
cron job, a serialized consumer — that is correct. In a concurrent server it is
wrong in three escalating ways.

**Breadcrumbs are unusable as designed, with no workaround.** The ring buffer
holds 40 entries process-wide and is snapshotted *in full* into every notice. At
concurrency N, roughly `40/N` of the crumbs in a notice are the request's own;
the rest belong to unrelated requests, and the request's own may be evicted
before its error even fires. Every other store has an explicit escape —
`Notice::context` for context, the payload for event fields — that travels with
the item and cannot be clobbered, and the docs already point at them.
`add_breadcrumb` has no such escape, because a breadcrumb's whole purpose is to
be recorded from deep in a call stack that holds no request object.

**Foreign crumbs carry foreign data.** Those interleaved entries bring other
requests' `message` and `metadata` into a notice. That is the privacy edge, not
merely noise.

**`request_id` cannot do its job.** It exists to correlate one request's notices
and events, and a concurrent server overwrites it between the two.

None of this is news: the Phase 1 spec documented the hazard from day one and
reserved the semantics, so that fixing it would read as "an advertised feature,
not silent breakage". This is that refinement.

## Decisions

**1. The scope is ambient, not passed.** A breadcrumb is recorded by a database
layer or an HTTP client that has no access to a request object. An explicit
`scope.add_breadcrumb(...)` threaded through user code would defeat the feature
it is meant to fix.

**2. A tokio task-local carries it.** Thread-locals were rejected in Phase 1 and
that still holds: async tasks migrate between worker threads, so a thread-local
scope is silently wrong in exactly the applications that matter. Of the
remaining carriers:

- `tokio::task_local!` follows a task across worker threads, is readable from
  *synchronous* code inside that task — which `add_breadcrumb` requires — and
  needs tokio's `rt` feature, which is `rt = []` and pulls in no transitive
  crates. One crate, no cascade.
- A `tracing` span carrier is runtime-agnostic, but stores data in span
  extensions, which live in `tracing-subscriber`'s `Registry`. It would require
  users to install a subscriber *and* register our layer, and would silently do
  nothing otherwise — the worst failure mode for a feature whose entire job is
  correctness.

Sentry and OpenTelemetry both keep the mechanism in core and ship the tracing
bridge as a separate integration crate. This follows that shape. Choosing tokio
now does not foreclose a tracing carrier in Phase 3: it is an additional
implementation behind one internal lookup, and by then the tracing dependency is
earned by the rest of the phase.

Accepted cost: async-std and smol users get no scoping.

**3. All four stores, not breadcrumbs alone.** The mechanism is the expensive
part and it is paid for either way; the other three are wiring. It also retires
the privacy hazard and makes `request_id` usable for its stated purpose.

**4. The task-local carries an overlay, not a whole scope.** Reads merge the
client's own global stores *beneath* the request's overlay, with the overlay
winning; writes go to the overlay when one is active and to the client's global
otherwise. Breadcrumbs are the exception in both directions: when an overlay is
active its trail is used *alone*, never merged, because merging the global trail
reintroduces the contamination being removed.

An earlier draft had the task-local hold a complete `Scope` snapshotted from the
global one. Review killed it: `scope()` is a free function with no client, so
with two clients it is ambiguous whose global is snapshotted, and whichever loses
would silently ship the other client's context to a different project.
Snapshotting also cannot see context set on the global *after* the scope opened.
The overlay resolves both — each client contributes its own base at read time.

Nesting follows: a nested scope seeds a fresh overlay from the enclosing
overlay's `context`, `event_context`, and `request_id`, and starts an empty
breadcrumb trail. Global is not consulted at seed time; it is merged at read
time, so it is never copied and never stale.

`clear_context` clears the *overlay*, not the client's global. Reads then fall
back to showing the global base again, which is correct: a request clearing its
own context has not un-set the application's deploy revision.

**5. The scope must be capturable, so it can be carried across a thread or task
boundary.** This reverses an earlier decision to ship without it.

`tokio::task_local!` stores its value in a thread-local that
`TaskLocalFuture::poll` installs for the duration of each poll. The scope is
therefore visible only on the thread currently polling — which means it is lost
by `tokio::spawn`, **and equally by `tokio::task::spawn_blocking`,
`std::thread::spawn`, and any thread-pool handoff.** That is a far wider hole
than "spawn does not propagate": synchronous database drivers run on
`spawn_blocking`, and "query ran" is the canonical breadcrumb.

Worse, the fallback is not merely lossy. A child with no overlay *writes to the
client's global stores*, and reads merge global beneath every later overlay — so
one request's `context`, `request_id`, or breadcrumbs persist into every
subsequent request. That is the cross-request contamination this work exists to
remove, made permanent rather than transient.

```rust
pub struct ScopeHandle(/* Arc<Overlay> */);   // Clone: one atomic increment

impl ScopeHandle {
    pub fn current() -> ScopeHandle;                            // total: fresh empty one if unscoped
    pub fn try_current() -> Option<ScopeHandle>;                // None if no scope active
    pub async fn enter<F: Future>(self, f: F) -> F::Output;
    pub fn enter_sync<T>(self, f: impl FnOnce() -> T) -> T;     // spawn_blocking, threads
}
```

**Shipped shape (reshaped before publication).** The three free functions above
were originally `current_scope()` / `in_scope(h, f)` / `in_scope_sync(h, f)`.
They became methods on `ScopeHandle` — the reasoning in this decision is
unchanged; only the surface moved. Three names came off a crowded crate root, and
the operation is discoverable from the type the caller is already holding.
`try_current()` was added because `current()` is deliberately total, which left
callers unable to *detect* the detached-handle case: a handle captured with no
scope active carries an overlay of its own, so what the spawned work records
through it is read by that work's own notices and never by the notices of the
code that spawned it.

`enter`/`enter_sync` take `self` by value, which matters for the one operation
this API exists for. A `&self` receiver makes the future returned by `enter`
borrow the handle, so it is not `'static` and cannot be handed to `tokio::spawn`
at all — every headline call site would need an `async move { … .await }`
wrapper. By value, `tokio::spawn(scope.enter(async { … }))` compiles as written.
The cost is a visible `.clone()` when one handle is entered more than once, which
on an `Arc` newtype is a single atomic increment — a better trade than a wrapper
on the common path.

Residual limitation, documented not solved: a `spawn` **inside a dependency**
cannot be wrapped by the caller. Those notices land unscoped. The write-path
corruption is what makes this worth a warning rather than a shrug, and it is the
strongest argument for the Phase 3 tracing carrier, which propagates through
`tracing`'s own instrumentation without the caller wrapping anything.

**6. Two clients share the request overlay, and keep their own global base.**
Sharing the overlay is right: one HTTP request reported to two projects is one
request described twice, and should carry the same `request_id` and breadcrumbs.
The Ruby gem shares ambient context across agents by default for the same reason,
with isolation as the opt-in (`local_context: true`).

The analogy is only partial, and the earlier draft leaned on it too hard: a Ruby
`Agent` holds *config*, not a context base, so it has no per-agent global to
lose. Each of our `Client`s does (`src/client.rs`, `Inner`), which is precisely
why decision 4 merges rather than snapshots.

**7. Scoped breadcrumb storage allocates lazily.** `RingBuffer::new` uses
`VecDeque::with_capacity(40)` (`src/breadcrumbs.rs:47-50`), which is correct once
per client and wrong once per in-flight request: it reserves roughly 4–6 KiB per
concurrent request even when the request records no breadcrumbs, or when
`breadcrumbs_enabled` is false. The overlay's buffer starts empty and grows to
the same 40-entry cap.

## Design

### Types

```rust
/// The four ambient stores. One per client, holding the process-global base.
struct Scope { /* context, breadcrumbs, event_context, request_id */ }

/// One request's own state, shared by every client via the task-local.
struct Overlay { /* same four fields; breadcrumbs allocate lazily */ }

pub struct ScopeHandle(Arc<Overlay>);
```

`Inner` keeps `global: Arc<Scope>` — the four fields it holds today, lifted into
one type.

### Resolution

```rust
#[cfg(feature = "tokio")]
tokio::task_local! {
    static CURRENT: Arc<Overlay>;
}

fn current_overlay() -> Option<Arc<Overlay>> {
    #[cfg(feature = "tokio")]
    if let Ok(o) = CURRENT.try_with(Arc::clone) {
        return Some(o);
    }
    None
}
```

`try_with` returns `Err(AccessError)` rather than panicking when no value is set,
including outside a runtime entirely and on a thread with no task — verified
against tokio 1.53.1. `current_overlay()` is on every notify and event path and
must never panic, so this is load-bearing.

Reads take the overlay when present and merge the client's global beneath it;
writes take the overlay when present and the client's global otherwise. The 12
existing store access points in `client.rs` route through those two helpers.

With no feature and no overlay, every path resolves to the client's global store
and behaviour is unchanged — which is what keeps CLIs, cron jobs, and the
existing test suite working untouched.

### Public API

```rust
pub async fn scope<F: Future>(f: F) -> F::Output;
pub fn scope_sync<T>(f: impl FnOnce() -> T) -> T;
```

Both shapes are needed because `tokio::task_local!` provides `scope` and
`sync_scope` separately, and blocking handlers exist. (Ours is named `scope_sync`
rather than mirroring tokio's `sync_scope`, so that the four public names read as
two pairs with one suffix: `scope`/`scope_sync` and `enter`/`enter_sync`.) Plus
the `ScopeHandle` capture methods from decision 5. Feature-gated:

```toml
[features]
tokio = ["dep:tokio"]

[dependencies]
tokio = { version = "1", default-features = false, features = ["rt"], optional = true }
```

Without the feature `scope()` does not exist. A compile error is the point: it is
why this carrier was chosen over one that no-ops silently.

### Usage

```rust
honeybadger::scope(async {
    honeybadger::request_id(&id);
    honeybadger::context([("user_id", json!(42))]);

    // Crossing a thread boundary requires carrying the scope explicitly.
    let scope = honeybadger::ScopeHandle::current();
    tokio::task::spawn_blocking(move || {
        scope.enter_sync(|| {
            honeybadger::add_breadcrumb("query ran", "query", None);
            run_query()
        })
    }).await?;

    handle(req).await
}).await
```

With `context([("version", json!("1.4.2"))])` set at boot, a notice from inside
that scope carries `version` (merged from global), `user_id` (overlay), and only
this request's crumbs.

### Documentation

The rustdoc on `context`, `clear_context`, `add_breadcrumb`, `event_context`, and
`request_id` currently warns that the store is process-wide. Each is rewritten so
the hazard is scoped to its real condition — no active scope — and each gains the
`scope()` remedy. The README's context section and the crate docs get the same
treatment.

`scope()`'s own docs must state the thread-boundary rule from decision 5
explicitly, name `spawn_blocking` and `std::thread::spawn` alongside
`tokio::spawn`, and show the capture pattern. Understating this as "spawn does
not propagate" is what the first draft got wrong.

## Testing

- **The regression test for the reported problem:** N concurrent scopes, each
  recording its own crumbs, context, and `request_id`; assert every notice
  carries only its own and none of its neighbours'. This is the test that would
  fail today.
- **The contamination test:** inside a scope, spawn a task *without* capturing,
  have it write context, then open a fresh scope and assert the write **did**
  leak into it. The leak is real and deliberate, exactly as decision 5 describes:
  a child with no overlay writes to the client's global base, and every later
  overlay merges that base beneath itself. The test therefore pins the failure
  mode rather than a fix for it — capturing a `ScopeHandle` is the remedy — and
  if it ever stops leaking, the hazard documentation must change with it.
- Merge semantics: a scope sees the client's global context, does not see global
  breadcrumbs, and overlay keys win over global keys.
- Global writes *after* a scope opens are visible inside it — the property
  snapshotting would have lost.
- Two clients with different global context, one active overlay: each notice
  carries its own client's base plus the shared overlay.
- Nesting: an inner scope inherits the outer's context and `request_id`, and
  starts a clean trail.
- Capture: `ScopeHandle::enter` and `enter_sync` restore the overlay across
  `tokio::spawn` and `spawn_blocking`, and a cloned handle re-enters the same
  overlay.
- `clear_context` inside a scope leaves the global base intact.
- Fallback: with no scope active, all four stores resolve to the client's global.
  The existing suite covers this by construction and must pass unmodified — that
  is the back-compatibility proof.
- Breadcrumb storage allocates nothing until the first crumb.
- CI gains a `--no-default-features` check so the default build cannot silently
  break, and runs the suite with `--features tokio`.

## Commit sequence

This records the sequence as planned and executed, so it names the API as it
stood at each step; the surface was reshaped afterwards, per decision 5.

1. Extract `Scope` and route the 12 access points through the read/write helpers,
   with only the global implementation. No behaviour change, no new dependency;
   the existing suite is the proof.
2. Add the `tokio` feature, the task-local overlay, `scope()` / `sync_scope()`,
   merge semantics, and the concurrency and contamination tests.
3. Add `current_scope` / `in_scope` / `in_scope_sync` and their tests.
4. Rewrite the hazard documentation and add the CI feature matrix.

## Out of scope

- A tower/axum middleware. Framework integration belongs with Phase 3.
- The `tracing` carrier — decision 2. Note that it is the only real answer to the
  dependency-`spawn` hole in decision 5.
- async-std and smol support.
- Per-client overlay isolation, Ruby's `local_context: true` — additive later.
