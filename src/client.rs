//! Client: shared state + the notify pipeline (spec "Notify pipeline").
use crate::breadcrumbs::{Breadcrumb, RingBuffer};
use crate::config::Config;
use crate::drops::DropCounter;
use crate::error::Error;
use crate::event::{Sampler, assemble as assemble_event};
use crate::events_worker::{EventsConfig, EventsWorkerHandle};
use crate::notice::{Notice, assemble};
use crate::sanitizer::Sanitizer;
use crate::transport::{
    NullTransport, ServerTransport, Transport, TransportError, TransportRequest, compress,
};
use crate::worker::WorkerHandle;
use serde_json::{Map, Value};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Serialized-JSON ceiling, matching the Reporting API's documented maximum. Measured
/// before compression: the service applies the limit to the decompressed body.
pub(crate) const MAX_PAYLOAD_BYTES: usize = 262_144;

/// Events-worker lifecycle. `Once` cannot express this: a worker must never be
/// created after shutdown, which means the state has to be checked and changed
/// under one lock.
enum EventsState {
    NotStarted,
    Running(EventsWorkerHandle),
    Stopped,
    Failed,
}

struct Inner {
    config: Config,
    sanitizer: Sanitizer,
    context: Mutex<Map<String, Value>>,
    breadcrumbs: Mutex<RingBuffer>,
    transport: Arc<dyn Transport>,
    worker: WorkerHandle,
    notice_drops: Arc<DropCounter>,
    event_context: Mutex<Map<String, Value>>,
    request_id: Mutex<Option<String>>,
    events: Mutex<EventsState>,
    event_drops: Arc<DropCounter>,
    sampler: Sampler,
}

/// A configured Honeybadger reporter. Cheap to clone; all clones share one worker.
#[derive(Clone)]
pub struct Client(Arc<Inner>);

/// Builder for [`Client`], used to supply a custom [`Transport`].
pub struct ClientBuilder {
    config: Config,
    transport: Option<Arc<dyn Transport>>,
}

impl Client {
    /// Builds a client and starts its delivery thread.
    ///
    /// # Errors
    ///
    /// [`Error::MissingApiKey`] when reporting is enabled but no key was configured, and
    /// [`Error::WorkerSpawn`] if the delivery thread cannot start.
    pub fn new(config: Config) -> Result<Client, Error> {
        Client::builder(config).build()
    }

    /// Starts a builder, for injecting a custom [`Transport`].
    pub fn builder(config: Config) -> ClientBuilder {
        ClientBuilder {
            config,
            transport: None,
        }
    }

    /// Reports an error, capturing a backtrace at this call site.
    ///
    /// Fire-and-forget, and **no network I/O happens on your thread** — but the call is
    /// not free. Assembly is synchronous: symbolicating the backtrace, reading source
    /// excerpts for in-project frames, running `before_notify` hooks, serializing, and
    /// compressing all happen here. Only delivery is handed to the background worker.
    /// Symbolication dominates, and it is not cheap on a large binary.
    ///
    /// Every failure path — full queue, oversized payload, network error — is reported
    /// through the `log` facade rather than by panicking or blocking.
    pub fn notify<E: std::error::Error + ?Sized>(&self, error: &E) {
        self.notify_notice(Notice::from_error(error));
    }

    /// Reports a notice built by hand. See [`Notice::from_error`] and
    /// [`Notice::message`].
    pub fn notify_notice(&self, notice: Notice) {
        if let Some(payload) = self.run_pipeline(notice)
            && !self.0.worker.try_enqueue(payload)
        {
            self.0.notice_drops.record();
        }
    }

    /// Panic path: full pipeline, then synchronous urgent delivery bypassing the worker.
    pub(crate) fn deliver_now(&self, notice: Notice) {
        if let Some(payload) = self.run_pipeline(notice) {
            let req = TransportRequest::notices(&payload, true);
            // `Transport` is user-implementable: a panicking impl must not escape into
            // the panic hook that called us.
            let result = catch_unwind(AssertUnwindSafe(|| self.0.transport.deliver(&req)))
                .unwrap_or_else(|_| Err(TransportError("transport panicked".into())));
            match result {
                Ok(status) if (200..300).contains(&status) => {}
                Ok(status) => log::warn!("honeybadger: urgent delivery got status {status}"),
                Err(e) => log::warn!("honeybadger: urgent delivery failed: {e}"),
            }
        }
    }

    pub(crate) fn capture_frames(&self) -> Vec<crate::bt::Frame> {
        crate::bt::capture(&self.0.config.root)
    }

    /// Spec pipeline steps 1–6. Returns the compressed wire payload, or None if dropped.
    fn run_pipeline(&self, mut notice: Notice) -> Option<Vec<u8>> {
        let inner = &*self.0;

        // 1. Assembly inputs: scope context (local wins), breadcrumbs, backtrace frames.
        let scope = inner
            .context
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        notice.merge_scope_context(scope);
        let request_id_fallback = self.current_request_id();
        let breadcrumbs = inner.config.breadcrumbs_enabled.then(|| {
            inner
                .breadcrumbs
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .snapshot()
        });
        let frames = match (notice.frames.take(), notice.raw_backtrace.take()) {
            (Some(frames), _) => Some(frames), // pre-processed (panic path)
            (None, Some(mut raw)) => {
                raw.resolve();
                Some(crate::bt::process_resolved(&raw, &inner.config.root))
            }
            (None, None) => None,
        };

        // 2. Ignore check (cheapest rejection first).
        if inner
            .config
            .ignore_classes
            .iter()
            .any(|c| c == notice.error_class())
        {
            return None;
        }

        // 3. before_notify hooks; panics caught and treated as pass. The guard
        //    stops our own panic hook from reporting a panic we are containing.
        for hook in &inner.config.before_notify {
            let hook = hook.clone();
            let keep = {
                let _suppressed = crate::panic_hook::suppress_reporting();
                catch_unwind(AssertUnwindSafe(|| hook(&mut notice))).unwrap_or_else(|_| {
                    log::warn!("honeybadger: before_notify hook panicked; continuing");
                    true
                })
            };
            if !keep {
                return None;
            }
        }

        // 4. Ignore recheck (hooks may have changed the class).
        if inner
            .config
            .ignore_classes
            .iter()
            .any(|c| c == notice.error_class())
        {
            return None;
        }

        // 5. Sanitize last, so hook-introduced data is covered.
        let mut context_value = Value::Object(std::mem::take(&mut notice.context));
        inner.sanitizer.sanitize(&mut context_value);
        if let Value::Object(map) = context_value {
            notice.context = map;
        }
        let breadcrumbs = breadcrumbs.map(|crumbs| {
            crumbs
                .into_iter()
                .map(|mut crumb| {
                    let mut meta = Value::Object(std::mem::take(&mut crumb.metadata));
                    inner.sanitizer.sanitize_shallow(&mut meta);
                    if let Value::Object(m) = meta {
                        crumb.metadata = m;
                    }
                    crumb
                })
                .collect::<Vec<Breadcrumb>>()
        });

        // 6. Serialize + size cap + compress.
        let payload = assemble(
            &notice,
            &inner.config,
            breadcrumbs,
            frames,
            std::process::id(),
            request_id_fallback.as_deref(),
        );
        let bytes = match serde_json::to_vec(&payload) {
            Ok(b) => b,
            Err(e) => {
                log::warn!("honeybadger: failed to serialize notice: {e}");
                return None;
            }
        };
        if bytes.len() > MAX_PAYLOAD_BYTES {
            log::warn!(
                "honeybadger: notice payload {} bytes exceeds the {MAX_PAYLOAD_BYTES}-byte cap; dropped",
                bytes.len()
            );
            return None;
        }
        Some(compress(&bytes))
    }

    /// Merges key/value pairs into this client's context, attached to every later
    /// notice. Setting a key to [`serde_json::Value::Null`] removes it.
    ///
    /// **Shared by every thread and task using this client — it is not request-scoped.**
    /// Concurrent requests overwrite each other here, which can attribute one user's
    /// error to another. Use it for process-wide facts; put request data on the notice
    /// via [`Notice::context`], which travels with the notice and cannot be clobbered.
    /// See [the crate docs](crate#context-is-process-wide).
    ///
    /// Notice-local context wins on key collisions.
    pub fn context<I, K>(&self, entries: I)
    where
        I: IntoIterator<Item = (K, Value)>,
        K: Into<String>,
    {
        let mut ctx = self.0.context.lock().unwrap_or_else(|e| e.into_inner());
        for (k, v) in entries {
            let key = k.into();
            if v.is_null() {
                ctx.remove(&key);
            } else {
                ctx.insert(key, v);
            }
        }
    }

    /// Clears this client's context, its breadcrumb trail, its event context, and its
    /// request id — the whole accumulated diagnostic scope.
    ///
    /// The scope is shared process-wide, so this discards what every other in-flight
    /// caller has accumulated too. It suits programs that handle one unit of work at a
    /// time (a CLI, a cron job, a serialized queue consumer); calling it from a
    /// concurrent request handler will erase other requests' state.
    pub fn clear_context(&self) {
        self.0
            .context
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        self.0
            .breadcrumbs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        self.clear_event_context();
        self.clear_request_id();
    }

    /// Records a breadcrumb. The most recent 40 are attached to each notice; older ones
    /// fall off. A no-op when breadcrumbs are disabled in the config.
    ///
    /// The trail is shared process-wide, not per request: under concurrency, crumbs from
    /// unrelated requests interleave and evict one another. Treat it as a process-level
    /// log rather than a request timeline.
    pub fn add_breadcrumb(
        &self,
        message: &str,
        category: &str,
        metadata: Option<Map<String, Value>>,
    ) {
        if !self.0.config.breadcrumbs_enabled {
            return;
        }
        self.0
            .breadcrumbs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Breadcrumb::new(message, category, metadata));
    }

    /// Blocks until everything enqueued so far on **both** pipelines has been
    /// attempted, or `timeout` expires. Returns whether the barrier completed.
    ///
    /// Both flushes start before either is waited on, so the timeout is the
    /// number you passed rather than twice it. An events pipeline that was
    /// never started flushes as a no-op success.
    ///
    /// `false` means some of it did not go out: the timeout expired, or a batch
    /// is being retried and everything behind it is waiting its turn. It is not
    /// an error — delivery continues — but it is not a delivery receipt either.
    pub fn flush(&self, timeout: Duration) -> bool {
        // `None` means the timeout is not representable as an instant — the SDK
        // must not panic on a caller's `Duration::MAX`, so wait without one.
        let deadline = Instant::now().checked_add(timeout);
        let notices = self.0.worker.flush_begin();
        let events = {
            let state = self.0.events.lock().unwrap_or_else(|e| e.into_inner());
            match &*state {
                EventsState::Running(handle) => handle.flush_begin(),
                _ => None, // never spawn a worker in order to flush it
            }
        };

        let wait = |rx: crossbeam_channel::Receiver<bool>| match deadline {
            Some(deadline) => rx.recv_deadline(deadline).unwrap_or(false),
            None => rx.recv().unwrap_or(false),
        };
        let notices_ok = notices.map(&wait).unwrap_or(false);
        let events_ok = events.map(&wait).unwrap_or(true);
        notices_ok && events_ok
    }

    /// Stops both delivery threads, giving queued work up to `timeout` to
    /// drain. The client accepts nothing further afterwards.
    pub fn shutdown(&self, timeout: Duration) {
        // Mark Stopped before touching the worker, so a concurrent event() can
        // never spawn one behind our back.
        let previous = {
            let mut state = self.0.events.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::replace(&mut *state, EventsState::Stopped)
        };
        if let EventsState::Running(handle) = previous {
            handle.shutdown(timeout);
        }
        self.0.worker.shutdown(timeout);
    }

    /// Sends an Insights event. `event_type` always wins over any `event_type`
    /// key in `payload`.
    ///
    /// The payload must be a JSON **object**; anything else is logged and
    /// dropped. Fire-and-forget: assembly happens on your thread, delivery does
    /// not. To send a struct, convert it explicitly with
    /// [`serde_json::to_value`] — that conversion is deliberately visible,
    /// because it is the one way an event can carry a field nobody enumerated.
    pub fn event(&self, event_type: &str, payload: Value) {
        self.enqueue_event(Some(event_type), payload);
    }

    /// Sends an Insights event whose `event_type` is already in the payload.
    /// An event without a non-empty string `event_type` is dropped.
    pub fn event_value(&self, payload: Value) {
        self.enqueue_event(None, payload);
    }

    fn enqueue_event(&self, event_type: Option<&str>, payload: Value) {
        let inner = &*self.0;
        if !inner.config.events_enabled {
            return;
        }
        let scope = inner
            .event_context
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let request_id = self.current_request_id();
        let Some(line) = assemble_event(
            event_type,
            payload,
            &scope,
            request_id.as_deref(),
            &inner.config,
            &inner.sanitizer,
            &inner.sampler,
        ) else {
            return;
        };
        let enqueued = self
            .with_events_worker(|handle| handle.try_enqueue(line))
            .unwrap_or(false);
        if !enqueued {
            inner.event_drops.record();
        }
    }

    /// Runs `f` against the events worker, spawning it if this is the first
    /// event. Returns None when no worker exists or may be created.
    fn with_events_worker<R>(&self, f: impl FnOnce(&EventsWorkerHandle) -> R) -> Option<R> {
        let inner = &*self.0;
        let mut state = inner.events.lock().unwrap_or_else(|e| e.into_inner());

        // A forked child inherits channel state but no thread; enqueueing into
        // it would silently discard events and a flush would wait out its whole
        // timeout for an acknowledgement that can never arrive.
        if let EventsState::Running(handle) = &*state
            && handle.pid != std::process::id()
        {
            log::warn!("honeybadger: events worker did not survive fork; restarting");
            *state = EventsState::NotStarted;
        }

        if matches!(*state, EventsState::NotStarted) {
            let cfg = EventsConfig {
                batch_size: inner.config.events_batch_size,
                flush_interval: inner.config.events_flush_interval,
                queue_size: inner.config.events_queue_size,
                max_retries: inner.config.events_max_retries,
                suspend_interval: Duration::from_secs(3600),
            };
            match crate::events_worker::spawn(
                inner.transport.clone(),
                cfg,
                inner.event_drops.clone(),
            ) {
                Ok(handle) => *state = EventsState::Running(handle),
                Err(e) => {
                    log::error!(
                        "honeybadger: could not start the events worker; events are disabled for this process: {e}"
                    );
                    *state = EventsState::Failed;
                }
            }
        }

        match &*state {
            EventsState::Running(handle) => Some(f(handle)),
            _ => None,
        }
    }

    /// Merges key/value pairs into this client's **event** context, attached to
    /// every later event. Setting a key to [`serde_json::Value::Null`] removes it.
    ///
    /// **Shared by every thread and task using this client — it is not
    /// request-scoped.** Concurrent requests overwrite each other here. Use it
    /// for process-wide facts and put per-request data in the event payload,
    /// where it travels with the event and cannot be clobbered.
    pub fn event_context<I, K>(&self, entries: I)
    where
        I: IntoIterator<Item = (K, Value)>,
        K: Into<String>,
    {
        let mut ctx = self
            .0
            .event_context
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for (k, v) in entries {
            let key = k.into();
            if v.is_null() {
                ctx.remove(&key);
            } else {
                ctx.insert(key, v);
            }
        }
    }

    /// Clears this client's event context, leaving notice context untouched.
    pub fn clear_event_context(&self) {
        self.0
            .event_context
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    /// Sets the request id correlating this client's notices and events, and
    /// driving deterministic sampling — every event sharing an id shares one
    /// sampling decision.
    ///
    /// **This slot is process-wide, exactly like [`Client::context`].** The name
    /// describes what you put in it, not a scoping guarantee. Under concurrency
    /// one request's id overwrites another's, so an event can be attributed
    /// *and sampled* as the wrong request. Use it in programs that handle one
    /// unit of work at a time — a CLI, a cron job, a serialized consumer — and
    /// in a concurrent server put `request_id` in the event payload instead.
    pub fn request_id(&self, id: impl Into<String>) {
        *self.0.request_id.lock().unwrap_or_else(|e| e.into_inner()) = Some(id.into());
    }

    /// Clears the request id set by [`Client::request_id`].
    pub fn clear_request_id(&self) {
        *self.0.request_id.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    pub(crate) fn current_request_id(&self) -> Option<String> {
        self.0
            .request_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub(crate) fn wants_panic_hook(&self) -> bool {
        self.0.config.install_panic_hook
    }
}

impl ClientBuilder {
    /// Supplies the transport, replacing the one that would be chosen from the config.
    /// A client built this way needs no API key.
    pub fn transport(mut self, transport: Arc<dyn Transport>) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Resolves the transport and starts the delivery thread.
    ///
    /// # Errors
    ///
    /// [`Error::MissingApiKey`] if reporting is enabled, no transport was supplied, and
    /// no API key is configured; [`Error::WorkerSpawn`] if the thread cannot start.
    pub fn build(self) -> Result<Client, Error> {
        let config = self.config;
        let transport: Arc<dyn Transport> = match self.transport {
            Some(t) => t,
            None if config.reporting_enabled() => {
                let api_key = config.api_key.clone().ok_or(Error::MissingApiKey)?;
                Arc::new(ServerTransport::new(
                    config.endpoint.clone(),
                    api_key,
                    config.connect_timeout,
                    config.request_timeout,
                ))
            }
            None => {
                if config.env.is_none() {
                    log::info!("honeybadger: env not set; reporting enabled by default");
                }
                Arc::new(NullTransport)
            }
        };
        let notice_drops = Arc::new(DropCounter::new("notices"));
        let worker = crate::worker::spawn(
            transport.clone(),
            config.notice_queue_size,
            notice_drops.clone(),
        )
        .map_err(Error::WorkerSpawn)?;
        let sanitizer = Sanitizer::new(config.filter_keys.iter());
        let sampler = Sampler::new(config.events_sample_rate);
        Ok(Client(Arc::new(Inner {
            config,
            sanitizer,
            context: Mutex::new(Map::new()),
            breadcrumbs: Mutex::new(RingBuffer::new()),
            transport,
            worker,
            notice_drops,
            event_context: Mutex::new(Map::new()),
            request_id: Mutex::new(None),
            events: Mutex::new(EventsState::NotStarted),
            event_drops: Arc::new(DropCounter::new("events")),
            sampler,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::TestTransport;
    use serde_json::json;
    use std::io::Read;
    use std::sync::Arc;
    use std::time::Duration;

    fn test_client(transport: Arc<TestTransport>) -> Client {
        test_client_with(transport, |b| b)
    }

    fn test_client_with(
        transport: Arc<TestTransport>,
        f: impl FnOnce(crate::ConfigBuilder) -> crate::ConfigBuilder,
    ) -> Client {
        let builder = crate::Config::builder()
            .env_source(|_| None)
            .api_key("k")
            .env("production")
            .root("/app")
            .hostname("h");
        let config = f(builder).build().unwrap();
        Client::builder(config)
            .transport(transport)
            .build()
            .unwrap()
    }

    fn delivered(transport: &TestTransport) -> Vec<serde_json::Value> {
        transport
            .requests()
            .iter()
            .filter(|r| r.kind == crate::RequestKind::Notices)
            .map(|r| {
                let mut s = String::new();
                flate2::read::ZlibDecoder::new(&r.body[..])
                    .read_to_string(&mut s)
                    .unwrap();
                serde_json::from_str(&s).unwrap()
            })
            .collect()
    }

    fn events_delivered(transport: &TestTransport) -> Vec<serde_json::Value> {
        transport
            .requests()
            .iter()
            .filter(|r| r.kind == crate::RequestKind::Events)
            .flat_map(|r| {
                let mut s = String::new();
                flate2::read::ZlibDecoder::new(&r.body[..])
                    .read_to_string(&mut s)
                    .unwrap();
                s.lines()
                    .map(|l| serde_json::from_str(l).unwrap())
                    .collect::<Vec<serde_json::Value>>()
            })
            .collect()
    }

    #[test]
    fn test_event_delivers_with_context_and_request_id() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.event_context([("tenant", json!("acme"))]);
        client.request_id("req-9");
        client.event("user.created", json!({ "user_id": 7 }));
        assert!(client.flush(Duration::from_secs(5)));

        let events = events_delivered(&transport);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["event_type"], json!("user.created"));
        assert_eq!(events[0]["user_id"], json!(7));
        assert_eq!(events[0]["tenant"], json!("acme"));
        assert_eq!(events[0]["request_id"], json!("req-9"));
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_event_value_and_batching_share_one_request() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        for i in 0..5 {
            client.event_value(json!({ "event_type": "tick", "n": i }));
        }
        assert!(client.flush(Duration::from_secs(5)));
        assert_eq!(events_delivered(&transport).len(), 5);
        assert_eq!(
            transport
                .requests()
                .iter()
                .filter(|r| r.kind == crate::RequestKind::Events)
                .count(),
            1,
            "a flush cuts one batch, not five requests"
        );
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_flush_covers_both_pipelines_in_one_timeout() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.notify_notice(crate::Notice::message("X", "y"));
        client.event("t", json!({}));
        assert!(client.flush(Duration::from_secs(5)));
        assert_eq!(delivered(&transport).len(), 1, "the notice went out");
        assert_eq!(events_delivered(&transport).len(), 1, "the event went out");
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_flush_succeeds_when_the_events_worker_was_never_spawned() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.notify_notice(crate::Notice::message("X", "y"));
        assert!(
            client.flush(Duration::from_secs(5)),
            "an unspawned events pipeline must flush as a no-op success"
        );
        assert!(
            transport
                .requests()
                .iter()
                .all(|r| r.kind == crate::RequestKind::Notices),
            "flushing must never spawn the worker it is flushing"
        );
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_event_after_shutdown_never_spawns_a_worker() {
        // The race Once cannot close: one clone shuts down, another then calls
        // event() and would otherwise spawn a worker nobody will ever stop.
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        let clone = client.clone();
        client.shutdown(Duration::from_secs(5));
        clone.event("t", json!({}));
        assert!(
            events_delivered(&transport).is_empty(),
            "no events pipeline may be created after shutdown"
        );
    }

    #[test]
    fn test_a_worker_that_did_not_survive_fork_is_replaced() {
        // Forking under the test harness is hostile, so inject the recorded PID
        // instead: a child inherits the channel but not the thread, so without
        // this check events would vanish and flush would wait out its whole
        // timeout for an acknowledgement that can never arrive.
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.event("before.fork", json!({}));
        assert!(client.flush(Duration::from_secs(5)));

        {
            let mut state = client.0.events.lock().unwrap();
            match &mut *state {
                EventsState::Running(handle) => handle.pid = handle.pid.wrapping_add(1),
                _ => panic!("the first event must have started a worker"),
            }
        }

        client.event("after.fork", json!({}));
        assert!(client.flush(Duration::from_secs(5)));
        {
            let state = client.0.events.lock().unwrap();
            match &*state {
                EventsState::Running(handle) => assert_eq!(
                    handle.pid,
                    std::process::id(),
                    "a replacement worker must record the live PID"
                ),
                _ => panic!("the PID mismatch must respawn, not disable the pipeline"),
            }
        }
        let types: Vec<serde_json::Value> = events_delivered(&transport)
            .iter()
            .map(|e| e["event_type"].clone())
            .collect();
        assert_eq!(types, vec![json!("before.fork"), json!("after.fork")]);
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_events_disabled_never_spawns() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client_with(transport.clone(), |b| b.events_enabled(false));
        client.event("t", json!({}));
        assert!(client.flush(Duration::from_secs(5)));
        assert!(events_delivered(&transport).is_empty());
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_concurrent_events_during_a_flush_neither_deadlock_nor_lose_acks() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        let mut threads = Vec::new();
        for t in 0..4 {
            let client = client.clone();
            threads.push(std::thread::spawn(move || {
                for i in 0..50 {
                    client.event("load", json!({ "t": t, "i": i }));
                }
            }));
        }
        // Flush repeatedly while producers are still running. Each must return
        // within its own timeout rather than blocking behind the producers.
        for _ in 0..5 {
            assert!(
                client.flush(Duration::from_secs(10)),
                "a flush must acknowledge even under concurrent production"
            );
        }
        for t in threads {
            t.join().unwrap();
        }
        assert!(client.flush(Duration::from_secs(10)));
        assert_eq!(
            events_delivered(&transport).len(),
            200,
            "no event may be lost when flushes interleave with producers"
        );
        client.shutdown(Duration::from_secs(10));
    }

    #[test]
    fn test_flush_with_an_unrepresentable_timeout_does_not_panic() {
        // `Instant::now() + Duration::MAX` panics, and the SDK promises never to
        // panic on a caller's input.
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.event("t", json!({}));
        assert!(client.flush(Duration::MAX));
        assert_eq!(events_delivered(&transport).len(), 1);
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_request_id_slot_correlates_notices_with_events() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.request_id("req-42");
        client.notify_notice(crate::Notice::message("X", "y"));
        client.event("t", json!({}));
        assert!(client.flush(Duration::from_secs(5)));

        assert_eq!(
            delivered(&transport)[0]["correlation_context"]["request_id"],
            json!("req-42")
        );
        assert_eq!(
            events_delivered(&transport)[0]["request_id"],
            json!("req-42")
        );
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_clear_context_clears_the_whole_scope() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.context([("a", json!(1))]);
        client.event_context([("b", json!(2))]);
        client.request_id("req-1");
        client.clear_context();

        client.event("t", json!({}));
        assert!(client.flush(Duration::from_secs(5)));
        let events = events_delivered(&transport);
        assert_eq!(events[0].get("b"), None, "event context cleared");
        assert_eq!(events[0].get("request_id"), None, "request id cleared");
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_notify_delivers_payload_with_scope_context_and_breadcrumbs() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.context([("user_id", json!(7))]);
        client.add_breadcrumb("step one", "custom", None);
        client.notify(&std::io::Error::other("boom"));
        assert!(client.flush(Duration::from_secs(5)));
        let payloads = delivered(&transport);
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0]["request"]["context"]["user_id"], json!(7));
        assert_eq!(
            payloads[0]["breadcrumbs"]["trail"][0]["message"],
            json!("step one")
        );
        assert_eq!(payloads[0]["error"]["message"], json!("boom"));
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_context_null_removes_key() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.context([("a", json!(1)), ("b", json!(2))]);
        client.context([("a", serde_json::Value::Null)]);
        client.notify_notice(crate::Notice::message("X", "y"));
        client.flush(Duration::from_secs(5));
        let payloads = delivered(&transport);
        assert_eq!(payloads[0]["request"]["context"].get("a"), None);
        assert_eq!(payloads[0]["request"]["context"]["b"], json!(2));
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_before_notify_mutates_and_halts_and_panics_are_caught() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client_with(transport.clone(), |b| {
            b.before_notify(|n| {
                n.add_tag("hooked");
                true
            })
            .before_notify(|_| panic!("bad hook")) // caught, treated as pass
            .before_notify(|n| n.error_class() != "Halted") // halts "Halted" notices
        });
        client.notify_notice(crate::Notice::message("Halted", "no"));
        client.notify_notice(crate::Notice::message("Kept", "yes"));
        client.flush(Duration::from_secs(5));
        let payloads = delivered(&transport);
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0]["error"]["class"], json!("Kept"));
        assert_eq!(payloads[0]["error"]["tags"], json!(["hooked"]));
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_ignore_classes_checked_before_and_after_hooks() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client_with(transport.clone(), |b| {
            b.ignore_classes(["Ignored"]).before_notify(|n| {
                if n.error_class() == "MakeIgnored" {
                    n.set_class("Ignored");
                }
                true
            })
        });
        client.notify_notice(crate::Notice::message("Ignored", "pre")); // dropped pre-hook
        client.notify_notice(crate::Notice::message("MakeIgnored", "post")); // dropped post-hook
        client.notify_notice(crate::Notice::message("Kept", "k"));
        client.flush(Duration::from_secs(5));
        assert_eq!(delivered(&transport).len(), 1);
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_sanitization_covers_hook_introduced_secrets() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client_with(transport.clone(), |b| {
            b.before_notify(|n| {
                n.set_context("password", "hunter2");
                true
            })
        });
        client.notify_notice(crate::Notice::message("X", "y"));
        client.flush(Duration::from_secs(5));
        let payloads = delivered(&transport);
        assert_eq!(
            payloads[0]["request"]["context"]["password"],
            json!("[FILTERED]")
        );
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_oversized_payload_dropped() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        // The sanitizer truncates individual strings at 64 KB, so build the oversize
        // payload from many keys instead of one giant string.
        let mut notice = crate::Notice::message("Big", "b");
        for i in 0..10 {
            notice.set_context(format!("k{i}"), json!("y".repeat(60_000)));
        }
        client.notify_notice(notice);
        client.flush(Duration::from_secs(5));
        assert_eq!(
            delivered(&transport).len(),
            0,
            "oversized payload must be dropped"
        );

        // A payload comfortably under the cap still goes out: the check is a ceiling,
        // not a blanket rejection of large-ish context.
        let mut notice = crate::Notice::message("Chunky", "b");
        notice.set_context("k", json!("y".repeat(60_000)));
        client.notify_notice(notice);
        client.flush(Duration::from_secs(5));
        assert_eq!(delivered(&transport).len(), 1);
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_deliver_now_bypasses_worker_and_is_urgent() {
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.deliver_now(crate::Notice::message("panic", "argh"));
        // No flush needed: delivery was synchronous.
        let reqs = transport.requests();
        assert_eq!(reqs.len(), 1);
        assert!(reqs[0].urgent);
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_api_key_required_only_for_the_server_transport() {
        fn reporting_config_without_key() -> crate::Config {
            crate::Config::builder()
                .env_source(|_| None)
                .env("production")
                .build()
                .unwrap()
        }

        // No key + no custom transport → ServerTransport is unbuildable.
        match Client::new(reporting_config_without_key()) {
            Err(Error::MissingApiKey) => {}
            Err(e) => panic!("expected MissingApiKey, got {e}"),
            Ok(_) => panic!("expected MissingApiKey, got a Client"),
        }

        // No key + a caller-supplied transport → fine, no credentials involved.
        let transport = Arc::new(TestTransport::new());
        let client = Client::builder(reporting_config_without_key())
            .transport(transport.clone())
            .build()
            .expect("a custom transport needs no API key");
        client.notify_notice(crate::Notice::message("X", "y"));
        client.flush(Duration::from_secs(5));
        assert_eq!(delivered(&transport).len(), 1);
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_panicking_transport_does_not_escape_the_urgent_path() {
        struct Panicky;
        impl crate::Transport for Panicky {
            fn deliver(
                &self,
                _req: &crate::TransportRequest,
            ) -> Result<u16, crate::TransportError> {
                panic!("urgent transport blew up");
            }
        }
        let config = crate::Config::builder()
            .env_source(|_| None)
            .api_key("k")
            .env("production")
            .build()
            .unwrap();
        let client = Client::builder(config)
            .transport(Arc::new(Panicky))
            .build()
            .unwrap();
        // deliver_now runs inside the panic hook; it must swallow the transport panic.
        client.deliver_now(crate::Notice::message("panic", "argh"));
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_queue_full_drops_are_counted_not_logged_individually() {
        // Suspend the worker with a 402 so it stops consuming, then overfill.
        let transport = Arc::new(TestTransport::new());
        transport.respond_with(402);
        let client = test_client_with(transport.clone(), |b| b.notice_queue_size(2));
        client.notify_notice(crate::Notice::message("X", "y")); // consumed -> suspends
        std::thread::sleep(Duration::from_millis(200));
        for _ in 0..20 {
            client.notify_notice(crate::Notice::message("X", "y"));
        }
        assert!(
            client.0.notice_drops.pending() > 0,
            "overflow must be accumulated in the counter"
        );
        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_null_transport_when_env_excluded_no_api_key_needed() {
        let config = crate::Config::builder()
            .env_source(|_| None)
            .env("test")
            .build()
            .unwrap();
        let client = Client::new(config).unwrap(); // no api key, no panic, Null transport
        client.notify_notice(crate::Notice::message("X", "y"));
        assert!(client.flush(Duration::from_secs(5)));
        client.shutdown(Duration::from_secs(5));
    }
}
