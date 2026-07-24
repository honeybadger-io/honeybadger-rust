# Honeybadger Rust SDK — Phase 2: Insights Events

**Status:** approved design, ready for implementation planning
**Date:** 2026-07-24
**Phase covered:** Phase 2 (Insights events). Extends, and does not replace,
`2026-07-22-honeybadger-rust-sdk-design.md`, which remains authoritative for
everything in Phase 1.

Phase 1 (error notices) shipped in v0.5.0. This document specifies the events
pipeline: a manual `event()` API, a batching delivery worker, and the
`POST /v1/events` NDJSON transport.

## Reference implementations

Three existing clients were read in full before this design. They disagree in
places, and where they do, the disagreement is recorded in the decision log
rather than resolved silently.

| Client | Role |
| --- | --- |
| honeybadger-ruby 6.9.1 | wire-protocol reference; the only one that compresses event bodies |
| honeybadger-elixir | the batching/retry model, and the manual-vs-instrumented filtering split |
| honeybadger-go 0.9.0 | closest structural analogue: no runtime, explicit concurrency, single owner thread |

Wire-format facts below are from the published API documentation, not inferred
from client source.

## Scope

**In scope.** A manual events API, event context, deterministic sampling,
`before_event` hooks, the batching worker, the NDJSON transport, and
`events_*` configuration.

**Out of scope**, deliberately:

- Automatic instrumentation. A `tracing` layer or `log` bridge is Phase 3, where
  it can be designed properly and likely as its own crate. Elixir's telemetry
  integrations are the model, and they are a large surface of their own.
- Ruby's `events.ignore` / `events.ignore_only` machinery. Its entire default
  list is Rails telemetry noise; with manual-only events there is nothing to
  filter, and a `before_event` hook returning `false` covers the case. Revisit
  when Phase 3 starts capturing events nobody wrote by hand.
- The API's `defaults` query parameter, which is redundant with
  `events_attach_hostname` / `events_attach_environment`.
- Go's `Sync: true` mode.

## Task zero: a Phase 1 correction

`Client::notify_notice` logs one `log::warn!` per dropped notice when the queue
is full. The queue only fills during an error storm — exactly when the
application is already unhealthy — so a 10,000-notice storm becomes 10,000 log
lines. Ruby and Go both consolidate instead.

Phase 2 makes this materially worse: the events queue is deeper and events
arrive at far higher volume than errors.

**Fix, landing first in the Phase 2 plan:** a shared drop counter in
`src/drops.rs`, one atomic per pipeline, consumed by both workers. The summary
is emitted on the next successful delivery and again at shutdown, rate-limited
to at most once per 60 seconds. No third timer thread, and the message appears
where a reader will connect it to the storm that caused it.

Events removed by sampling are **not** drops and are not counted.

A second Phase 1 defect, found in review, lands in the same task: a panicking
`before_notify` hook currently triggers our own panic hook and delivers an
urgent notice before `catch_unwind` regains control. See "Panics in user code
must not self-report" below; the guard covers both pipelines.

## Public API

```rust
// Free functions (global facade)
pub fn event(event_type: &str, payload: Value);
pub fn event_value(payload: Value);
pub fn event_context<I, K>(entries: I) where I: IntoIterator<Item = (K, Value)>, K: Into<String>;
pub fn clear_event_context();
pub fn request_id(id: impl Into<String>);
pub fn clear_request_id();

// Client methods mirror every one of the above.
```

**The payload is a `serde_json::Value`, not `impl Serialize`.** This is
deliberate and load-bearing, and it matches every reference client: Go accepts
only `map[string]any` and makes callers convert (its own `slog` and `zerolog`
adapters decode into a map before calling `Event`), while Ruby takes a Hash and
Elixir a map. `Value` is Rust's equivalent of `map[string]any`, and `json!` is
its ergonomic literal.

```rust
honeybadger::event("payment.failed", json!({ "amount": 42, "code": "declined" }));
honeybadger::event_value(json!({ "event_type": "job.finished", "ms": 91 }));

// A struct is converted explicitly, at the call site:
honeybadger::event("user.created", serde_json::to_value(&user)?);
```

Accepting `impl Serialize` would let `event("user.created", &user)` serialize a
whole struct — every field it happens to carry, including ones nobody
enumerated. That is the single vector by which an event can contain a field its
author never considered, and it is what would force `filter_keys` redaction onto
the events path (decision 5). Requiring an explicit `to_value` keeps the
conversion visible where a reader can see it, at the cost of one obvious call.

It also removes a failure mode: a `Value` cannot fail or panic during
serialization, so no user `Serialize` implementation ever runs inside `event()`.

A payload that is not a JSON **object** — `json!(42)`, a string, an array — is
logged and dropped. An Insights event is a set of fields.

`event()` and `event_value()` return `()`. They are fire-and-forget, matching
`notify`. Go returns `error`, but in async mode it is always `nil` except when a
hook errors, which is not information a caller can act on.

### Scope contract

Event context is a **separate store** from notice context, matching all three
reference clients. It carries the same process-wide hazard Phase 1 documents for
`context()`, and its documentation must say so in the same terms.

`request_id` is **neither store**. It is a dedicated slot, following Ruby, whose
context manager holds `request_id` separately from both context stores. Without
it users would set the same value twice and would silently lose per-request
sampling determinism if they set only one.

**It does not change Phase 1's notice behavior.** Merged notice context remains
the authoritative source of `correlation_context.request_id`
(`src/notice.rs:273`, covered by `test_golden_payload`); the slot is only a
**fallback**, consulted when the merged context has no `request_id` key. A call
like `Notice::message(..).context([("request_id", ..)])` must keep working
exactly as it does today.

**The slot is process-wide and carries the same hazard as `context()`.** Naming
it `request_id` describes what users put in it, not a scoping guarantee the SDK
can make. If request A sets `A`, request B sets `B`, and A then emits an event,
A's event is both attributed *and sampled* as B — worse than no correlation,
because it is silently wrong in two ways at once, and either request clearing
the slot erases the other's value. Its documentation must carry the Phase 1
hazard warning verbatim, and must direct concurrent servers to put `request_id`
in the event payload instead, where it travels with the event and cannot be
clobbered. The slot is for programs that handle one unit of work at a time — a
CLI, a cron job, a serialized consumer. Genuine per-request scoping arrives with
the Phase 3 `tracing` layer.

## Changes to Phase 1 behavior

Four, all intentional — plus the panic-suppression guard folded into task zero,
which fixes existing `before_notify` behavior rather than changing it by design:

1. **Drop logging is consolidated** (task zero, above).
2. **`clear_context()` also clears event context and the request id.** Its
   existing documentation calls it "the whole accumulated diagnostic scope," so
   this matches the stated intent, and it keeps a request handler from needing
   three separate clear calls. `clear_event_context()` remains for the narrow
   case.
3. **`flush(timeout)` covers both pipelines**, as the Phase 1 spec promised it
   would from day one. It sends a flush control to both workers and waits for
   both acknowledgements against a single shared deadline, concurrently, so the
   timeout stays the number the caller passed rather than doubling.
   `Guard::drop` flushes both, then shuts down both.

   **`Client::shutdown` is also public and must not silently lose a partial
   batch.** With 500 events accumulated and no count, byte, or time trigger yet
   fired, an implementation obeying only the worker's stated rules could exit and
   discard all 500. Shutdown therefore force-cuts the current batch and drains it
   along with any retained retries, within its deadline — the same barrier
   semantics `flush` has. What it cannot finish inside the deadline is dropped
   and counted, exactly as Phase 1 does when shutting down while throttled.
4. **`CapturedRequest` gains a `kind: RequestKind` field and becomes
   `#[non_exhaustive]`**, so tests assert on pipeline rather than string-matching
   paths, and later additions are free. A contained breaking change, cheap at
   0.5.0 and expensive after 1.0.

## Event pipeline

Ordered. Steps mirror the Phase 1 notify pipeline where the reasoning carries
over.

1. **Guard.** No-op if the client is uninitialized or `events_enabled` is false.
2. **Check shape.** Not a JSON object → warn, drop. No serialization runs here:
   the payload arrives as a `Value`, so there is no user code to fail or panic.
3. **Merge**, with the caller's payload winning over event context.
4. **Set `event_type`** unconditionally, so it always reflects the argument.
   (`event_value()` skips this step; the caller owns the field.)
5. **Set `ts` if absent**, ISO 8601 UTC with millisecond precision — the format
   `breadcrumbs::now_iso8601_ms` already produces.
6. **Set `request_id` if absent** and the slot is populated.
7. **Set `hostname` / `environment` if absent**, per config.

   Steps 6–7 are insert-if-absent, giving the precedence
   `payload > event_context > {request_id, hostname, environment}` — Ruby's
   precedence exactly.
8. **`before_event` hooks**, in registration order. Returning `false` drops the
   event. A panicking hook is caught, logged, and treated as pass, matching
   `before_notify`. See the panic-suppression note below.
9. **Validate**, after hooks, because hooks can remove or corrupt the reserved
   fields inserted above. Require a non-empty string `event_type`, reasserting
   the argument's value for `event()` since the API promises it always wins; drop
   the event otherwise. A `request_id` that is not a string is left in the
   payload but ignored for sampling, which needs a string to hash. This matters
   more than it looks: an invalid event provokes a 422, and per the failure
   matrix a 422 discards **the entire batch**, so one malformed event can destroy
   999 valid ones.
10. **Sampling** (below). After validation, so it can rely on `request_id`'s
    type. Ruby's order.
11. **Structural sanitizing** — depth cap 20, string truncation at 65,536 bytes,
    UTF-8 boundary safety. **No `filter_keys` redaction**; see the decision log.
    Last, so hook-introduced data is covered, which is the Phase 1 reasoning.
12. **Render to one JSON line.** Over `MAX_EVENT_BYTES` (102,400) → warn naming
    the `event_type`, drop.
13. **Enqueue.** On failure, increment the drop counter. Never blocks.

Steps 2–12 run on the caller's thread, mirroring Phase 1's serialize-before-
enqueue. This bounds queue memory predictably, keeps work off the worker, and
enforces the per-event size limit where a useful message can still be logged.

### Panics in user code must not self-report

`catch_unwind` does not stop Rust's panic hook from running — the hook fires at
the panic site, *before* unwinding reaches the catch. So with our own dispatcher
installed, a panicking `before_event` hook synchronously delivers an urgent panic
notice, on the caller's thread, before `event()` regains control. That adds the
urgent HTTP timeout to what should be a fire-and-forget call and reports a panic
the SDK deliberately contained.

Taking `Value` rather than `impl Serialize` already removes the other vector
here: no user `Serialize` implementation runs inside `event()`, so hooks are the
only callback left to guard on the events path.

**This is a live Phase 1 bug, not a new one:** `before_notify` hooks are already
caught this way in `src/client.rs:142`, with the same consequence. The fix is a
thread-local suppression guard set around every expected-callback `catch_unwind`
and honored by our dispatcher, which continues to chain to external hooks so
non-Honeybadger panic handling is unaffected. It lands alongside task zero, and
covers both pipelines.

### Sampling

```
rate >= 100                        -> keep (short-circuit, no work)
rate <= 0                          -> drop
request_id present (and a string)  -> crc32(request_id) % 100 < rate
otherwise                          -> (seed + counter.fetch_add(1)) % 100 < rate
```

CRC32 comes from `flate2`, already a dependency. The counter path replaces
Ruby's and Elixir's random draw: it needs no `rand` dependency and yields an
exact rate rather than a probabilistic one. Determinism on `request_id` means
every event in a request shares one fate, which is the property that makes
sampled Insights data still tell a coherent per-request story.

**The counter must be seeded per process**, with `crc32(pid ‖ process start
time) % 100`. A counter starting at zero always keeps its first event, because
`0 % 100 < rate` holds for any positive rate — so at a 1% rate, a thousand
short-lived processes each emitting a single event would keep all thousand
instead of roughly ten. That is exactly the CLI and cron shape this SDK is meant
to serve, and it would silently inflate low-rate sampling by two orders of
magnitude. Seeding costs nothing and removes the bias.

Note also what "exact" does and does not mean: the rate is exact over complete
100-event cycles of the fallback stream alone. Events carrying a `request_id`
take the CRC path and are not counted by it, so a workload mixing both paths
gets each path's rate independently rather than one exact global rate.

No per-event sample-rate override in Phase 2. Ruby's `_hb.sample_rate` exists
mainly to let its auto-instrumentation exempt itself; with manual events there is
no such caller.

## Delivery architecture

**A second dedicated OS thread**, `honeybadger-events`, with its own bounded
event channel and its own unbounded control channel, mirroring `worker.rs`.

Rejected: multiplexing onto the notices thread. An events batch approaches
5 MB and uploads synchronously on the worker, so sharing would let a slow events
POST stall error delivery — the SDK's primary job. The failure domains differ
too: events can be suspended on 402/403 while notices keep flowing.

Rejected: a generic worker abstraction over both. Batching plus flush timer plus
retry queue versus single-payload fire-and-forget is enough divergence that the
shared skeleton would be mostly branches. Only the genuinely common pieces are
shared — the throttle curve, suspension, and the drop counter.

**Spawned lazily**, on the first `event()` call. Go spawns its events goroutine
at package init, so importing the library costs a goroutine whether or not you
send events; Ruby initializes lazily, and that is the better behavior. An
unspawned events pipeline flushes as a no-op returning success — a flush must
never spawn a worker in order to flush it.

**Not `Once`.** `Once` cannot prevent a worker from being created *after*
shutdown: one `Client` clone can call `shutdown()` before any event while
another clone then enters `event()` and spawns a worker nobody will ever flush
or stop, leaking a thread and silently swallowing events. The same race exists
between a clone captured just before `Guard::drop` and the drop itself. Use a
mutex-guarded lifecycle instead:

```
NotStarted --event()--> Running --shutdown()--> Stopped
     |                     |
     +---spawn fails-------+--> Failed   (event() becomes a permanent no-op)
```

Spawning is permitted only from `NotStarted`. `shutdown()` transitions to
`Stopped` **before** inspecting the worker, so a concurrent `event()` either
observes `NotStarted` and is refused, or observes `Running` and is enqueued
ahead of the drain. `Stopped` and `Failed` are terminal.

**Fork.** After the worker has spawned, `fork()` leaves the child holding
channel state but no worker thread: events would be accepted until the orphaned
channel filled, and flush would wait out its full timeout for an acknowledgement
that can never arrive. The events worker records the PID at spawn and compares
it on enqueue; on mismatch it logs once and returns to `NotStarted` so the next
`event()` respawns. The notices worker inherits the same problem from Phase 1
and is not lazily spawned, so for it the rule is documentation: **initialize
after forking.** Pre-fork initialization is unsupported for notices.

### Batch triggers

Three, not the two the reference clients use:

| Trigger | Threshold |
| --- | --- |
| Count | `events_batch_size` (1000) |
| Time | `events_flush_interval` (30s) since the last send |
| Bytes | `BATCH_BYTE_LIMIT` (4,500,000) accumulated |

The byte trigger is the one the other clients miss. With a 1000-event batch and
a 100 kB per-event ceiling, a worst-case batch is 100 MB against a documented
5 MB request limit. Cutting on the way in means a batch is never split or
dropped after the fact.

**The time deadline belongs to the current batch, not to the last send.** It
starts on the empty-to-nonempty transition — the moment the first event of a
batch arrives — and is cleared when the batch is cut. "Since the last send" is
wrong twice over: after an idle period the deadline has already expired, so the
very first event flushes as a batch of one; and if retry attempts count as
sends, they reset the deadline repeatedly and starve a partial batch that is
accumulating behind them. Retry and backoff timing is tracked separately and
never touches this deadline. Elixir gets this right with a `timeout_started_at`
sentinel armed by the first push; Ruby does not, and pays for it with a partial
batch waiting between one and two full intervals.

The timer is `recv_timeout` against that deadline — no separate timer thread
(Ruby) and no ticker (Go).

### Retry queue

A `VecDeque` of already-compressed batches, oldest first, each carrying its
attempt count and the number of events it holds. Compressing once at cut time
means retries reuse the bytes. Head-of-line blocking is intentional and
preserves ordering, matching Go.

**One budget covers everything outstanding.** `events_queue_size` bounds the
channel *and* the events held in the current batch, the in-flight batch, and the
retry queue — one total, the way Go counts its `queueSize`. Bounding only the
channel would be no bound at all: during an outage the worker keeps draining the
bounded channel into an unbounded `VecDeque` of multi-megabyte compressed
buffers, converting a capped queue into unbounded memory growth precisely when
the process is already struggling.

When admitting an event would exceed the budget, the **oldest retained batch is
dropped first** and counted, before refusing new events. Shedding the stalest
data keeps the pipeline live: it is also what breaks the head-of-line deadlock
described in the failure matrix below.

### Failure matrix

The table is **total over every `Ok(u16)` a `Transport` can return**, not just
the statuses the API documents. `Transport` is public and user-implementable, so
a custom implementation or an unhandled redirect can produce a 1xx, 3xx, or
out-of-range value; leaving those undefined would leave the worker in an
undefined state.

| Response | Action |
| --- | --- |
| 2xx | pop batch; decay throttle (`saturating_sub(1)`) |
| 402 | suspend 1h; drop everything outstanding |
| 403 | suspend 1h; drop everything outstanding |
| 429 | throttle; retain batch; **no attempt burned** |
| 503 | throttle; retain batch; **attempt burned** |
| other 4xx (400, 404, 413, 422, …) | drop batch immediately, zero retries |
| other 5xx (500, 502, 504, …) | retry; `attempts += 1` |
| anything else (1xx, 3xx, ≥ 600) | log the unexpected status; drop batch |
| transport error | retry; `attempts += 1` |

`events_max_retries` counts retries **after** the initial attempt, so the
default of 3 means a batch is attempted 4 times in total before being dropped
with a log naming the attempt count.

**429 and 503 are not the same signal, and Go conflating them is a bug we should
not inherit.** A 429 is the service telling us to slow down, and discarding data
for obeying it would be perverse — so it burns no retry budget. A 503 is a
server error that carries no such instruction, and a permanently unhealthy
endpoint must not be able to pin a batch at the head of the queue forever, so it
burns budget like any other 5xx.

That leaves one liveness hole, which the budget in the retry queue section
closes: because a throttled batch burns nothing, a permanently rate-limited
endpoint could otherwise hold one batch at the head indefinitely while every
newer event queues behind it and the channel fills and sheds. Drop-oldest on
budget overflow evicts that stalled head, so the pipeline keeps making progress
and the data lost is the stalest rather than the freshest. A pipeline that stays
rate-limited long enough will therefore shed old batches by design; that is
stated here so it is not discovered later as a surprise.

**Suspension (402/403) drops everything outstanding** — the retry queue, the
in-flight batch, and the partially accumulated current batch — matching what the
Phase 1 notices worker already does on suspend. Nothing accumulates across a
one-hour suspension, because none of it would still be worth sending.

The retry budget is spent only where retrying can help. Elixir and Go both burn
their whole budget on batches the server has already called unprocessable, and
Go additionally hammers the API forever on an invalid API key because it never
distinguishes 402/403 from a transient failure. The throttle and suspend
behavior is inherited from the Phase 1 notices worker unchanged.

**Accepted risk:** a batch that was delivered but whose response timed out will
be retried and duplicated. Notices deduplicate into faults, so Phase 1 is
unaffected; Insights data is counted, so duplicates skew counts. Judged worth it
against losing 1000 events to a single connection reset. Ruby's no-retry model
is the alternative and was considered.

## Transport

Additive. `RequestKind` is already `#[non_exhaustive]`, so this is not a
breaking trait change — the Phase 1 request descriptor existed for exactly this.

```rust
pub enum RequestKind { Notices, Events }

impl TransportRequest<'_> {
    pub fn events(body: &[u8]) -> Self;  // urgent is always false
}
```

### Wire format

```
POST {endpoint}/v1/events
X-API-Key: {api_key}
Content-Type: application/x-ndjson
Accept: application/json
Content-Encoding: deflate
User-Agent: Honeybadger Rust {version}

deflate(lines.join("\n"))
```

Documented limits: each event under 102,400 bytes, total request under 5 MB.
Documented statuses: 201 success, 403, 413, 422, 429, 500.

`application/x-ndjson`, not Ruby's `application/json` — a wart both newer
clients corrected. Bodies are deflated, which only Ruby does; the API accepts
it, and on a multi-megabyte batch it is a real bandwidth saving.

Events are flat objects with no envelope, one per line:

```json
{"event_type":"user.created","ts":"2026-07-24T18:03:12.456Z","request_id":"req-9","hostname":"web-1","environment":"production","user_id":7}
```

## Config

Builder > environment variable > default, as Phase 1.

| Option | Env | Default | Purpose |
| --- | --- | --- | --- |
| `events_enabled` | `HONEYBADGER_EVENTS_ENABLED` | `true` | master switch; when false, `event()` never spawns the worker |
| `events_batch_size` | `HONEYBADGER_EVENTS_BATCH_SIZE` | `1000` | flush trigger by count |
| `events_flush_interval` | `HONEYBADGER_EVENTS_FLUSH_INTERVAL` | `30s` | flush trigger by time |
| `events_queue_size` | `HONEYBADGER_EVENTS_QUEUE_SIZE` | `10_000` | bounded channel capacity |
| `events_max_retries` | `HONEYBADGER_EVENTS_MAX_RETRIES` | `3` | attempts per batch on retryable failures |
| `events_sample_rate` | `HONEYBADGER_EVENTS_SAMPLE_RATE` | `100` | 0–100, deterministic per request id |
| `events_attach_hostname` | `HONEYBADGER_EVENTS_ATTACH_HOSTNAME` | `true` | adds `hostname` to every event |
| `events_attach_environment` | `HONEYBADGER_EVENTS_ATTACH_ENVIRONMENT` | `true` | adds `environment` to every event |
| `before_event` | — | none | hooks, mirroring `before_notify` |

`endpoint`, `api_key`, `connect_timeout`, and `request_timeout` stay shared
across pipelines rather than being duplicated, which is also what Go does.

**Validation happens in `ConfigBuilder::build`**, returning an error rather than
degrading silently. `events_flush_interval` must be greater than zero — a zero
deadline turns `recv_timeout` into a busy loop that spins a core forever.
`events_batch_size` and `events_queue_size` must be at least 1. Booleans parse
as Phase 1's do (`true`/`1`/`yes`). Numeric environment values that fail to
parse are an error, not a silent fallback to the default. `events_sample_rate`
is clamped to 0–100 rather than rejected, since 0 and 100 are both meaningful
and a value outside the range has an obvious intent.

Two naming notes. The flush trigger is `events_flush_interval`, **not** Ruby's
`events.timeout`, which would collide confusingly with the existing
`request_timeout` (an HTTP timeout). And `events_queue_size` follows the
`notice_queue_size` precedent set in Phase 1.

The queue default is Elixir's 10,000 rather than Ruby and Go's 100,000: at
roughly a kilobyte per serialized event, the larger number claims a hundred
megabytes of headroom by default, which is not a library's decision to make.

## Error-handling guarantee

Unchanged from Phase 1: the SDK never panics, never blocks the caller, and never
propagates an error out of a reporting call. Every new failure path resolves to
a log line and a drop — non-object payloads, oversized events, panicking
`Serialize` impls, panicking hooks, panicking custom transports, full queues.

One case is genuinely new. If the notices worker fails to spawn, `init()` returns
`Error::WorkerSpawn`. The events worker spawns lazily inside `event()`, which has
no error channel and should not grow one, so a spawn failure there logs once and
makes `event()` a permanent no-op. Errors are the primary job and their failure
is worth surfacing; Insights degrading must not take an application's error
reporting down with it.

## Module layout

New:

- `src/event.rs` — assembly, sampling decision, line serialization
- `src/events_worker.rs` — batching worker, retry queue, failure matrix
- `src/drops.rs` — shared drop counters (task zero)

Modified: `client.rs`, `config.rs`, `global.rs`, `transport.rs`, `worker.rs`,
`lib.rs`.

`client.rs` is already 582 lines. Event assembly lives in `event.rs` and
`Client::event` stays thin orchestration, the way `notify_notice` is today.

## Testing strategy

Phase 1's structure: inline `#[cfg(test)]` units per module, `tests/` only where
process-global state forces a separate binary, `TestTransport` as the seam.

**Assembly** — merge ordering and the full precedence chain; `event_type` always
winning; `ts` inserted only when absent; non-object payloads rejected;
`request_id`, hostname, and environment attachment; sanitizing applied without
key redaction; the 102,400-byte cap; a golden payload test mirroring
`test_golden_payload`. Post-hook validation gets its own cases: a hook that
deletes `event_type` drops the event, a hook that rewrites it is overridden for
`event()`, and a non-string `request_id` is retained in the payload but skipped
for sampling.

**Sampling** — a rate of 100 short-circuits without touching the counter; a rate
of 0 drops everything; the same `request_id` always reaches the same verdict;
different ids spread across the range; the counter path yields an exact rate
over a full cycle. Plus the regression the seed exists to prevent: **two clients
with different seeds must not both keep their first event at a 1% rate.**

**Worker** — each of the three batch triggers, with the flush interval injected
short the way `spawn_with_intervals` already injects suspension; one test per
row of the failure matrix, including the catch-all for an unexpected status. The
ones that matter most are where we diverge from the references: **429 throttles
without burning a retry attempt, 503 burns one**, and **413/422 are dropped on
the first response**. Then the invariants review surfaced — the deadline starts
on the first event of a batch rather than the last send, so an idle worker does
not flush a batch of one; the outstanding-events budget spans channel, current
batch, and retry queue, and **drop-oldest evicts a stalled head batch so a
permanently throttled endpoint cannot deadlock the pipeline**; suspension
discards the partial batch too; and shutdown force-cuts a partial batch rather
than losing it. Plus queue overflow counting, the flush barrier, and a panicking
transport not killing the worker.

**Lifecycle** — the race the state machine exists to close: `shutdown()` on one
clone followed by `event()` on another must **not** spawn a worker. Also lazy
spawn happening exactly once under concurrent first calls, `events_enabled =
false` never spawning, a spawn failure making `event()` a permanent no-op, and
`event()` before `init()` as a no-op.

**Panic containment** — a panicking `before_event` hook must be contained
**without** emitting a panic notice, and `event()` must return promptly rather
than paying the urgent HTTP timeout. The equivalent `before_notify` case is a
Phase 1 regression test.

**Config** — zero `events_flush_interval`, zero batch size, and zero queue size
are rejected by `build()`; sample rates outside 0–100 clamp; unparseable numeric
environment values error rather than falling back.

**Integration** — a mockito test asserting content type, content encoding, and
that the inflated body is N lines of valid JSON; an all-pipeline flush test
proving one call covers both workers inside a single timeout budget; concurrent
`event()` calls during a flush neither deadlocking nor losing acknowledgements.

Fork behavior is specified but not covered by an automated test: forking under
`cargo test` is hostile to the harness. The PID check is unit-tested by
injecting the recorded PID rather than by actually forking.

## Decision log

1. **Manual events only in Phase 2.** A `tracing` layer is where Rust users
   would get the most automatic value, but it roughly doubles the phase and
   deserves its own design. Phase 3.
2. **Two free functions taking `serde_json::Value`, not `impl Serialize`.**
   `Value` is Rust's `map[string]any`, which is exactly what Go accepts and all
   three clients effectively require. An earlier draft took `impl Serialize` so
   `&my_struct` would work; that was dropped **at Ben's direction on
   2026-07-24** once it became clear it was the one thing that could put
   unenumerated fields into an event, and therefore the thing forcing redaction
   onto the events path. Explicit `serde_json::to_value` keeps the conversion
   visible at the call site.
3. **Separate event context plus a shared `request_id` slot.** All three clients
   separate the stores; only Ruby also hoists `request_id` out of both, and it
   is right to. Without the shared slot, per-request sampling determinism
   silently breaks when a user sets the value in only one place.
4. **Targeted retry.** Retry transport errors and 5xx; drop 4xx immediately;
   throttle without burning an attempt; suspend on 402/403. Strictly better than
   Go, which spends its budget uniformly and never backs off a bad key.
   Duplicate risk accepted and documented.
5. **No `filter_keys` redaction on events** — upheld, but only because of
   decision 2. All three clients decline to redact manual events; Elixir redacts
   telemetry-sourced events but explicitly not `Honeybadger.event/1` payloads.
   The rationale is that every field in an event was written down by its author,
   so silent redaction of a legitimately named field would corrupt analytics with
   no error.

   Codex challenged this and was right to: the rationale collapses the moment the
   API can serialize a whole struct, since `&user` carries whatever fields `User`
   happens to have. Rather than adding redaction to compensate, **Ben chose to
   remove struct serialization** (decision 2), which restores the premise
   directly instead of papering over its failure. Every remaining route into an
   event — `json!` literals, `event_context` entries, `before_event` hooks — is
   something the developer typed key by key.

   Revisit in Phase 3, when auto-instrumentation begins capturing fields nobody
   chose; that is the point at which redaction genuinely earns its place.
6. **Byte-based batch cutting**, absent from all three references, because
   `batch_size × per-event limit` exceeds the documented request limit by 20×.
7. **Lazy worker spawn**, following Ruby. Go's unconditional spawn is a cost
   levied on every program that merely imports the library.
8. **Deadline-based timer** rather than a timer thread or ticker; fixes Ruby's
   one-to-two-interval flush latency.
9. **Counter-based sampling fallback** instead of a `rand` dependency; gives an
   exact rate rather than a probabilistic one.
10. **`events_flush_interval`, not `events_timeout`**, to avoid colliding with
    the existing HTTP `request_timeout`. Ruby has this exact collision.
11. **Queue default 10,000**, Elixir's, not Ruby and Go's 100,000.
12. **Deflate event bodies**, which only Ruby does, and
    `application/x-ndjson`, which only Ruby doesn't.
13. **Drop-log consolidation lands as task zero** rather than a separate
    pre-Phase-2 commit — same mechanism both pipelines need, easier to review
    beside the worker that motivates it. **Confirmed by Ben 2026-07-24.**
14. **Rate limiting throttles rather than suspends**, revising the Phase 1
    spec's sketch of "separate worker with suspend-on-throttle semantics." That
    sketch described Ruby, whose events worker suspends for a full hour on a
    single 429 or 503. Go and Elixir both wait 60 seconds instead, and Ruby is
    the outlier. Reusing the Phase 1 notices curve — **`1.05^n − 1` seconds**,
    capped at 300s, decaying on success (`src/worker.rs:25-35`) — is
    self-correcting and keeps one throttle implementation shared across both
    pipelines. Note the `− 1`: the first pause is about 50 milliseconds, not a
    second, and the curve only becomes material after dozens of consecutive
    rate-limit responses. The events worker reuses `throttle_interval` unchanged;
    no Phase 1 timing changes. Suspension is reserved for 402 and 403, where an
    hour is right because nothing will change until a human acts.

## Review revisions

Codex review, 2026-07-24, 14 findings against the first draft. Thirteen accepted
and folded in above; one referred back to Ben because it contradicts a decision
he had already made.

**Contradicted shipped Phase 1 code:**

- The throttle curve is `1.05^n − 1`, not `1.05^n` — the first pause is ~50 ms,
  not a second. Decision 14 corrected; no Phase 1 timing changes.
- `correlation_context.request_id` is derived from merged notice context and has
  a golden test. The first draft read as though the new slot replaced that
  source, silently breaking `Notice::message(..).context([("request_id", ..)])`.
  Notice context is now stated as authoritative, the slot as a fallback.

**Liveness and lifecycle, all P1:**

- `Once` cannot prevent spawn-after-shutdown. Replaced with a mutex-guarded
  `NotStarted/Running/Stopped/Failed` lifecycle.
- The retry queue was unbounded: only the channel was capped, so an outage moved
  events into an unbounded collection of multi-megabyte buffers. One budget now
  spans channel, current batch, in-flight batch, and retry queue, shedding the
  oldest retained batch first.
- Because a throttled batch burned no retry budget, a permanently rate-limited
  endpoint could pin one batch at the head forever while everything newer queued
  behind it. 503 now burns budget (it is a server error, not a rate-limit
  instruction), and drop-oldest evicts a stalled 429-retained head.

**Correctness gaps:**

- `catch_unwind` does not stop the panic hook from firing first, so a panicking
  hook or `Serialize` impl self-reported an urgent notice on the caller's
  thread. **This is a live Phase 1 bug**; the suppression guard joins task zero.
- The flush deadline ran "since the last send," which flushes a batch of one
  after any idle period and lets retries starve an accumulating batch. It now
  belongs to the current batch, armed on the first event.
- The failure matrix was not total over `u16`. Catch-all row added.
- No post-hook validation: a hook could delete `event_type`, and one invalid
  event provokes a 422 that discards the entire batch of up to 1000.
- The sampling counter started at zero, so `0 % 100 < rate` always kept the first
  event — a thousand short-lived CLI processes at a 1% rate would keep all
  thousand. Counter is now seeded per process.
- `Client::shutdown` could lose a partial batch no trigger had cut. It now
  force-cuts and drains.
- `events_flush_interval = 0` would busy-loop `recv_timeout`. Config validation
  added.
- Fork left a completed `Once` with no worker thread. PID check for the events
  worker; documented "initialize after forking" for notices.

**Referred back, and resolved by removing the cause.** The reviewer argued
`filter_keys` should redact manual events after all, because "hand-constructed"
overstated the control a caller has: `event("user.created", &user)` serializes a
whole struct including fields nobody enumerated, and the by-value
`impl Serialize` signature actively encouraged that.

Ben's call was to delete the capability rather than add redaction to compensate:
the payload is now a `serde_json::Value`, so a struct must be converted with an
explicit `serde_json::to_value` at the call site. This matches Go, which accepts
only `map[string]any` for the same reason, and it restores decision 5's premise
directly. Decision 5 stands, now resting on an API that cannot violate it.

Two things fell out of that change for free: no user `Serialize` implementation
runs inside `event()` any more, so hooks are the only callback the panic
suppression guard has to cover on the events path, and one drop path
disappears from the pipeline.
