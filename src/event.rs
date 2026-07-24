//! Event assembly and sampling for the Insights pipeline.
use crate::breadcrumbs::now_iso8601_ms;
use crate::config::Config;
use crate::sanitizer::Sanitizer;
use flate2::Crc;
use serde_json::{Map, Value};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};

/// Per-event ceiling documented by the Insights API (100 kB).
pub(crate) const MAX_EVENT_BYTES: usize = 102_400;

/// Sampling decision, deterministic per request where a request id exists.
///
/// Events sharing a `request_id` share one fate, so a sampled request tells a
/// coherent story rather than a randomly punctured one. Events without a
/// request id fall back to a counter, which needs no `rand` dependency and
/// gives an exact rate over a complete cycle.
pub(crate) struct Sampler {
    rate: u8,
    counter: AtomicU64,
}

impl Sampler {
    pub(crate) fn new(rate: u8) -> Self {
        Sampler::with_seed(rate, process_seed())
    }

    pub(crate) fn with_seed(rate: u8, seed: u64) -> Self {
        Sampler {
            rate: rate.min(100),
            counter: AtomicU64::new(seed),
        }
    }

    pub(crate) fn keep(&self, request_id: Option<&str>) -> bool {
        if self.rate >= 100 {
            return true;
        }
        if self.rate == 0 {
            return false;
        }
        match request_id {
            Some(id) => u64::from(crc32(id.as_bytes()) % 100) < u64::from(self.rate),
            None => self.counter.fetch_add(1, Ordering::Relaxed) % 100 < u64::from(self.rate),
        }
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = Crc::new();
    crc.update(bytes);
    crc.sum()
}

/// Distinct per process, so a fleet of short-lived processes does not all make
/// the same decision for its first event.
fn process_seed() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let mut crc = Crc::new();
    crc.update(&std::process::id().to_le_bytes());
    crc.update(&nanos.to_le_bytes());
    u64::from(crc.sum() % 100)
}

/// Builds the NDJSON line for one event, or `None` if it was dropped.
///
/// `event_type` is `Some` for `event()` — where the argument always wins — and
/// `None` for `event_value()`, where the caller owns the field.
#[allow(clippy::too_many_arguments)]
pub(crate) fn assemble(
    event_type: Option<&str>,
    payload: Value,
    scope: &Map<String, Value>,
    request_id: Option<&str>,
    config: &Config,
    sanitizer: &Sanitizer,
    sampler: &Sampler,
) -> Option<String> {
    // 2. Shape. No user code runs here: the payload is already a Value.
    let Value::Object(fields) = payload else {
        log::warn!("honeybadger: event payload must be a JSON object; dropped");
        return None;
    };

    // 3. Merge, caller's payload winning over event context.
    let mut event = scope.clone();
    event.extend(fields);

    // 4-7. Injected fields. event_type is unconditional; the rest fill gaps.
    if let Some(t) = event_type {
        event.insert("event_type".into(), Value::String(t.to_owned()));
    }
    event
        .entry("ts")
        .or_insert_with(|| Value::String(now_iso8601_ms()));
    if let Some(id) = request_id {
        event
            .entry("request_id")
            .or_insert_with(|| Value::String(id.to_owned()));
    }
    if config.events_attach_hostname && !config.hostname.is_empty() {
        event
            .entry("hostname")
            .or_insert_with(|| Value::String(config.hostname.clone()));
    }
    if config.events_attach_environment
        && let Some(env) = &config.env
    {
        event
            .entry("environment")
            .or_insert_with(|| Value::String(env.clone()));
    }

    // 8. Hooks. Panics are caught and treated as pass; the guard stops our own
    //    panic hook from reporting a panic we are containing.
    for hook in &config.before_event {
        let hook = hook.clone();
        let keep = {
            let _suppressed = crate::panic_hook::suppress_reporting();
            catch_unwind(AssertUnwindSafe(|| hook(&mut event))).unwrap_or_else(|_| {
                log::warn!("honeybadger: before_event hook panicked; continuing");
                true
            })
        };
        if !keep {
            return None;
        }
    }

    // 9. Validate after hooks. An invalid event provokes a 422, and a 422
    //    discards the whole batch — one bad event must not destroy 999 good ones.
    match event.get("event_type").and_then(Value::as_str) {
        Some(t) if !t.is_empty() => {}
        _ => {
            log::warn!("honeybadger: event has no non-empty string event_type; dropped");
            return None;
        }
    }

    // The argument always wins, so reassert it after the hooks have run: a hook
    // may enrich an event but must not redirect it to a different type. This
    // runs after validation, so a hook that *deletes* event_type still drops the
    // event rather than having it quietly restored.
    if let Some(t) = event_type {
        event.insert("event_type".into(), Value::String(t.to_owned()));
    }

    // 10. Sampling. Only a string request_id can be hashed.
    let sampling_id = event.get("request_id").and_then(Value::as_str);
    if !sampler.keep(sampling_id) {
        return None;
    }

    // 11. Structural sanitizing, last, so hook-introduced data is covered.
    //     Deliberately no filter_keys redaction; see the spec's decision 5.
    let mut value = Value::Object(event);
    sanitizer.sanitize_structural(&mut value);

    // 12. Render and enforce the per-event ceiling.
    let line = match serde_json::to_string(&value) {
        Ok(line) => line,
        Err(e) => {
            log::warn!("honeybadger: failed to serialize event: {e}");
            return None;
        }
    };
    if line.len() > MAX_EVENT_BYTES {
        let event_type = value
            .get("event_type")
            .and_then(Value::as_str)
            .unwrap_or("<unknown>");
        log::warn!(
            "honeybadger: event {event_type} is {} bytes, over the {MAX_EVENT_BYTES}-byte limit; dropped",
            line.len()
        );
        return None;
    }
    Some(line)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sanitizer::Sanitizer;
    use serde_json::{Map, Value, json};

    fn cfg() -> crate::Config {
        crate::Config::builder()
            .env_source(|_| None)
            .api_key("k")
            .env("production")
            .hostname("web-1")
            .build()
            .unwrap()
    }

    fn assemble_default(event_type: Option<&str>, payload: Value) -> Option<Value> {
        assemble_with(event_type, payload, &Map::new(), None, &cfg())
    }

    fn assemble_with(
        event_type: Option<&str>,
        payload: Value,
        scope: &Map<String, Value>,
        request_id: Option<&str>,
        config: &crate::Config,
    ) -> Option<Value> {
        let sanitizer = Sanitizer::new(config.filter_keys.iter());
        let sampler = Sampler::with_seed(100, 0);
        assemble(
            event_type, payload, scope, request_id, config, &sanitizer, &sampler,
        )
        .map(|line| serde_json::from_str(&line).expect("a valid JSON line"))
    }

    #[test]
    fn test_golden_event() {
        let mut scope = Map::new();
        scope.insert("tenant".into(), json!("acme"));
        let out = assemble_with(
            Some("user.created"),
            json!({ "user_id": 7 }),
            &scope,
            Some("req-9"),
            &cfg(),
        )
        .unwrap();

        assert_eq!(out["event_type"], json!("user.created"));
        assert_eq!(out["user_id"], json!(7));
        assert_eq!(out["tenant"], json!("acme"));
        assert_eq!(out["request_id"], json!("req-9"));
        assert_eq!(out["hostname"], json!("web-1"));
        assert_eq!(out["environment"], json!("production"));
        assert!(
            out["ts"].as_str().unwrap().ends_with('Z'),
            "ts is ISO 8601 UTC"
        );
    }

    #[test]
    fn test_precedence_payload_beats_scope_beats_injected() {
        let mut scope = Map::new();
        scope.insert("shared".into(), json!("scope"));
        scope.insert("hostname".into(), json!("from-scope"));
        let out = assemble_with(
            Some("t"),
            json!({ "shared": "payload" }),
            &scope,
            Some("req-1"),
            &cfg(),
        )
        .unwrap();
        assert_eq!(out["shared"], json!("payload"));
        assert_eq!(out["hostname"], json!("from-scope"));
    }

    #[test]
    fn test_event_type_argument_always_wins_and_ts_is_kept() {
        let out = assemble_default(
            Some("real"),
            json!({ "event_type": "fake", "ts": "2020-01-01T00:00:00.000Z" }),
        )
        .unwrap();
        assert_eq!(out["event_type"], json!("real"));
        assert_eq!(out["ts"], json!("2020-01-01T00:00:00.000Z"));
    }

    #[test]
    fn test_non_object_payloads_are_dropped() {
        assert!(assemble_default(Some("t"), json!(42)).is_none());
        assert!(assemble_default(Some("t"), json!("a string")).is_none());
        assert!(assemble_default(Some("t"), json!([1, 2])).is_none());
        assert!(assemble_default(Some("t"), Value::Null).is_none());
    }

    #[test]
    fn test_event_value_requires_event_type_in_the_payload() {
        assert!(assemble_default(None, json!({ "a": 1 })).is_none());
        assert!(assemble_default(None, json!({ "event_type": "", "a": 1 })).is_none());
        assert!(assemble_default(None, json!({ "event_type": 42 })).is_none());
        assert!(assemble_default(None, json!({ "event_type": "ok" })).is_some());
    }

    #[test]
    fn test_hooks_mutate_drop_and_are_validated_after() {
        let config = crate::Config::builder()
            .env_source(|_| None)
            .api_key("k")
            .before_event(|e| {
                e.insert("hooked".into(), json!(true));
                true
            })
            .before_event(|e| e.get("event_type") != Some(&json!("halt")))
            .before_event(|e| {
                if e.get("event_type") == Some(&json!("sabotage")) {
                    e.remove("event_type");
                }
                true
            })
            .build()
            .unwrap();

        let kept = assemble_with(Some("keep"), json!({}), &Map::new(), None, &config).unwrap();
        assert_eq!(kept["hooked"], json!(true));

        assert!(
            assemble_with(Some("halt"), json!({}), &Map::new(), None, &config).is_none(),
            "a hook returning false drops the event"
        );
        assert!(
            assemble_with(Some("sabotage"), json!({}), &Map::new(), None, &config).is_none(),
            "validation runs after hooks: a deleted event_type drops the event"
        );
    }

    #[test]
    fn test_a_hook_cannot_redirect_an_event_to_another_type() {
        // The API promises the `event()` argument always wins, so it is
        // reasserted after hooks run — a hook may enrich an event but must not
        // silently turn a payment.failed into a payment.succeeded.
        let config = crate::Config::builder()
            .env_source(|_| None)
            .api_key("k")
            .before_event(|e| {
                e.insert("event_type".into(), json!("payment.succeeded"));
                true
            })
            .build()
            .unwrap();

        let out = assemble_with(
            Some("payment.failed"),
            json!({}),
            &Map::new(),
            None,
            &config,
        )
        .unwrap();
        assert_eq!(out["event_type"], json!("payment.failed"));

        // event_value() has no argument to reassert, so there the hook owns the
        // field and its rewrite stands.
        let out = assemble_with(
            None,
            json!({ "event_type": "payment.failed" }),
            &Map::new(),
            None,
            &config,
        )
        .unwrap();
        assert_eq!(out["event_type"], json!("payment.succeeded"));
    }

    #[test]
    fn test_panicking_hook_is_caught_and_treated_as_pass() {
        let config = crate::Config::builder()
            .env_source(|_| None)
            .api_key("k")
            .before_event(|_| panic!("bad hook"))
            .build()
            .unwrap();
        assert!(assemble_with(Some("t"), json!({}), &Map::new(), None, &config).is_some());
    }

    #[test]
    fn test_sanitizing_applies_but_filter_keys_do_not() {
        let config = crate::Config::builder()
            .env_source(|_| None)
            .api_key("k")
            .filter_keys(["password"])
            .build()
            .unwrap();
        let out = assemble_with(
            Some("t"),
            json!({ "password": "hunter2", "long": "y".repeat(70_000) }),
            &Map::new(),
            None,
            &config,
        )
        .unwrap();
        assert_eq!(
            out["password"],
            json!("hunter2"),
            "events are not key-redacted; every field was written by hand"
        );
        assert!(
            out["long"].as_str().unwrap().ends_with("[TRUNCATED]"),
            "structural sanitizing still applies"
        );
    }

    #[test]
    fn test_oversized_events_are_dropped() {
        // The sanitizer truncates single strings at 64 KiB, so exceed the
        // 100 kB event limit with many keys instead of one huge value.
        let mut payload = Map::new();
        for i in 0..40 {
            payload.insert(format!("k{i}"), json!("y".repeat(60_000)));
        }
        assert!(assemble_default(Some("big"), Value::Object(payload)).is_none());
    }

    #[test]
    fn test_sampling_drops_and_non_string_request_id_is_kept_but_unsampled() {
        let config = cfg();
        let sanitizer = Sanitizer::new(config.filter_keys.iter());
        let none = Sampler::with_seed(0, 0);
        assert!(
            assemble(
                Some("t"),
                json!({}),
                &Map::new(),
                None,
                &config,
                &sanitizer,
                &none
            )
            .is_none()
        );

        // A non-string request_id survives into the payload but must not be
        // hashed for sampling; at rate 0 nothing is kept either way, so use a
        // full-rate sampler and simply assert the field round-trips.
        let all = Sampler::with_seed(100, 0);
        let line = assemble(
            Some("t"),
            json!({ "request_id": 12345 }),
            &Map::new(),
            None,
            &config,
            &sanitizer,
            &all,
        )
        .unwrap();
        let out: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(out["request_id"], json!(12345));
    }

    #[test]
    fn test_full_and_zero_rates_short_circuit() {
        let all = Sampler::with_seed(100, 0);
        let none = Sampler::with_seed(0, 0);
        for _ in 0..10 {
            assert!(all.keep(None));
            assert!(all.keep(Some("req-1")));
            assert!(!none.keep(None));
            assert!(!none.keep(Some("req-1")));
        }
    }

    #[test]
    fn test_request_id_sampling_is_deterministic() {
        let s = Sampler::with_seed(50, 0);
        let first = s.keep(Some("req-abc"));
        for _ in 0..50 {
            assert_eq!(s.keep(Some("req-abc")), first, "same id, same fate");
        }
        // Different ids must not all land on the same side.
        let ids: Vec<bool> = (0..200)
            .map(|i| s.keep(Some(&format!("req-{i}"))))
            .collect();
        assert!(ids.iter().any(|k| *k) && ids.iter().any(|k| !*k));
    }

    #[test]
    fn test_counter_fallback_hits_the_rate_over_a_full_cycle() {
        let s = Sampler::with_seed(25, 0);
        let kept = (0..100).filter(|_| s.keep(None)).count();
        assert_eq!(kept, 25, "exact over a complete cycle");
    }

    #[test]
    fn test_seed_prevents_every_process_keeping_its_first_event() {
        // The bug this seed exists to prevent: an unseeded counter starts at 0,
        // and 0 % 100 < rate holds for any positive rate, so every short-lived
        // process would keep its first event regardless of the sample rate.
        let unseeded = Sampler::with_seed(1, 0);
        let seeded = Sampler::with_seed(1, 50);
        assert!(unseeded.keep(None), "counter at 0 keeps");
        assert!(!seeded.keep(None), "a different seed must not");
    }
}
