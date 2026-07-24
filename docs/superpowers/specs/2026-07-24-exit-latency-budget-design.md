# Exit-latency budget

Status: designed 2026-07-24, not yet implemented. Amends the Phase 1 spec's
"Client, init, and shutdown lifecycle" and "Panic hook" sections.

## Problem

The SDK can delay process exit far longer than any single configured timeout
suggests, because every stage of shutdown claims a full timeout of its own and
the stages run in sequence.

Measured against an endpoint that completes the TCP connect and then never
answers — the shape of a network partition or a hung load balancer, and the case
every timeout exists for:

| scenario | added exit latency |
| --- | --- |
| panic, nothing else in flight | 5.4s |
| panic with one notice and one event in flight | 15.3s |
| clean `main` return with one notice and one event in flight | 10.2s |

The 15.3s decomposes into four independent waits:

```
panic hook: synchronous urgent send   ≤5s   (Client::deliver_now)
Guard::drop: flush                    ≤5s   (GUARD_FLUSH_TIMEOUT)
Guard::drop: events worker shutdown   ≤5s   (Client::shutdown)
Guard::drop: notices worker shutdown  ≤5s   (Client::shutdown)
```

Two observations reframe this:

- **Most of it is not the panic path.** Only the first ~5s is. The rest is
  `Guard::drop`, which every program runs, so a clean exit pays ~10s for nothing
  when the endpoint is unreachable. This was read as a panic problem and is not
  one.
- **`Client::shutdown` already contradicts its own documentation.** It says
  "giving queued work up to `timeout` to drain" but passes that timeout to the
  events worker and then again to the notices worker, so `shutdown(5s)` can take
  10s. A reader has no reason to expect that.

## Decisions

**1. One budget, subdivided — not per-stage timeouts multiplied.** The SDK owns
a single answer to "how long may Honeybadger delay exit", and the stages of
shutdown divide it. Considered and rejected: fixing only the panic path, which
leaves the larger and more general ~10s in place, including on clean exits.

**2. `shutdown_timeout` is configurable, default 5s.** Exit latency is exactly
the setting that differs by program shape: a CLI wants a few hundred
milliseconds, a long-running server does not care about 5s. Builder-only, no
environment variable — the values that matter most here are sub-second, and every
other numeric env var in this SDK is whole seconds
(`HONEYBADGER_EVENTS_FLUSH_INTERVAL`), so a millisecond-valued env var would
either be inconsistent or unable to express the useful range.

**3. The panic send stays outside the budget, additive to it.** The panic hook
cannot know whether an exit is coming: a panic caught by a thread pool in a
long-lived server must not spend the process's exit budget, and there is no way
to distinguish that at hook time. Making the two share one budget would require
the panicking thread to record its elapsed time in shared mutable state for
`Guard::drop` to deduct — extra machinery in the one code path where machinery is
least safe. Worst case therefore becomes panic + exit, not one or the other.

**4. Shutdown keeps a floor even when flush exhausts the budget.** Each worker
shutdown is guaranteed at least `SHUTDOWN_FLOOR` (100ms) to send its `Shutdown`
control message and receive the ack. That ack is what makes a worker run
`report_final()` and emit the `dropped N notices` / `dropped N events` summary —
the single log line that says data was lost, during precisely the incident that
lost it. Passing a literal zero would exit marginally faster and discard that
line exactly when it matters, and would detach both threads rather than joining
them.

**5. The panic path remains direct and synchronous.** Unchanged from Phase 1, and
its reasoning still holds: a full queue must not drop the crash report, and
worker throttling must not delay it. Routing the panic notice through the async
worker would be a way to remove the blocking entirely, and is rejected for those
two reasons.

## Design

### Config

```rust
pub fn shutdown_timeout(mut self, v: Duration) -> Self;   // default 5s
```

Resolved in `Config::build` like any other field, with no env fallback. Validated
non-zero, consistent with `events_flush_interval`: a zero budget would make
`Guard::drop` detach both workers on every exit, which is a footgun rather than a
useful setting. A caller wanting that can bind no `Guard` at all.

### `Client::shutdown` becomes a true total

`shutdown(timeout)` treats `timeout` as the budget for stopping *both* workers,
matching what its documentation already claims:

```
deadline = now + timeout

events worker:  shutdown( ((deadline - now) - SHUTDOWN_FLOOR).max(SHUTDOWN_FLOOR) )
notices worker: shutdown(  (deadline - now)                  .max(SHUTDOWN_FLOOR) )
```

The events worker is deliberately not offered the whole budget: one
`SHUTDOWN_FLOOR` is withheld so the notices worker cannot be starved by it.
Worst case is `timeout`, not `2 × timeout`.

This is a public semantic change. It is free before publication and would not be
after, and it turns a documented promise into a true one.

### `Guard::drop` subdivides one deadline

```
budget   = config.shutdown_timeout
deadline = now + budget

flush(   (deadline - now).saturating_sub(SHUTDOWN_RESERVE) )
shutdown( (deadline - now).max(SHUTDOWN_RESERVE) )
```

`SHUTDOWN_RESERVE` = 2 × `SHUTDOWN_FLOOR` = 200ms, the minimum `Client::shutdown`
needs to stop two workers cleanly. If flush uses everything it is allowed,
shutdown still receives its reserve and the total lands at `budget`. If flush
returns early — the common case, since flush returns as soon as both pipelines
acknowledge — shutdown inherits the slack.

`flush()` itself needs no change: it already starts both pipelines before waiting
on either and bounds the wait with one deadline.

### Resulting worst cases

| scenario | before | after (default 5s) | after (`shutdown_timeout(500ms)`) |
| --- | --- | --- | --- |
| panic with traffic in flight | 15.3s | ~10s | ~5.5s |
| clean exit with traffic in flight | 10.2s | ~5s | ~0.5s |
| panic, nothing in flight | 5.4s | ~5s | ~5s |

The panic-only row barely moves, because it is the urgent send rather than
shutdown. Lowering it further means lowering `request_timeout`, which
`urgent_budget` already derives from.

## Testing

- **Unit, `client.rs`:** with both workers live against a transport that blocks
  past the budget, `shutdown(d)` returns within `d` — the regression test for the
  `2 × timeout` behavior.
- **Unit, `client.rs`:** when flush has consumed the whole budget, both workers
  still stop and the drop counter still reports, proving the floor is doing its
  job rather than being decorative.
- **Unit, `config.rs`:** default is 5s; a zero value is rejected.
- **Integration, `tests/panic_hook.rs`:** a fixture panicking with a notice and
  an event in flight, against a listener that accepts and never answers, asserting
  measured exit latency stays inside `panic budget + shutdown_timeout` with
  margin. This is the measurement from the Problem section, promoted to a test so
  the 15.3s cannot come back unobserved.

A test asserting wall-clock latency is ordinarily a smell. Here the wall clock is
the specification, so the assertion is generous — an upper bound with headroom for
CI scheduling, checking the budget is enforced at all rather than pinning a
number.

## Commit sequence

1. `Client::shutdown` as a true total across both workers, with the floor.
   Independently reviewable, and a bug fix against its own documentation.
2. `shutdown_timeout` config plus the `Guard::drop` subdivision, with the
   integration test.

## Out of scope

- Reducing what the panic path does before the network: symbolication and
  source-excerpt file I/O both run on the panicking thread inside `run_pipeline`,
  and neither has been measured. A real avenue for cutting crash latency, and a
  separate piece of work.
- Making the panic path asynchronous — see decision 5.
- Any change to `install_panic_hook` defaulting to true.
