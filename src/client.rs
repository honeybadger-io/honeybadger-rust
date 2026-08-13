//! Client: shared state + the notify pipeline (spec "Notify pipeline").
use crate::breadcrumbs::Breadcrumb;
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

/// Least time a worker is given to stop, even when the shutdown budget is spent.
///
/// Not a delivery allowance — a worker blocked on a socket will not answer within
/// it either way. It exists so a stage that *would* acknowledge immediately is
/// never handed a literal zero and detached for no reason.
const SHUTDOWN_FLOOR: Duration = Duration::from_millis(100);

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
    /// This client's process-global ambient state. Task 3 puts a per-request
    /// overlay in front of it.
    global: Arc<crate::scope::Scope>,
    transport: Arc<dyn Transport>,
    worker: WorkerHandle,
    notice_drops: Arc<DropCounter>,
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
            match result.map(|resp| resp.status) {
                Ok(status) if (200..300).contains(&status) => {}
                Ok(status) => log::warn!("honeybadger: urgent delivery got status {status}"),
                Err(e) => log::warn!("honeybadger: urgent delivery failed: {e}"),
            }
        }
    }

    pub(crate) fn capture_frames(&self) -> Vec<crate::bt::Frame> {
        crate::bt::capture(&self.0.config.root)
    }

    /// The overlay for the current request, if a scope is active.
    fn overlay(&self) -> Option<Arc<crate::scope::Overlay>> {
        crate::scope::current_overlay()
    }

    /// Spec pipeline steps 1–6. Returns the compressed wire payload, or None if dropped.
    fn run_pipeline(&self, mut notice: Notice) -> Option<Vec<u8>> {
        let inner = &*self.0;

        // 1. Assembly inputs: scope context (local wins), breadcrumbs, backtrace frames.
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
                Some(o) => o
                    .breadcrumbs
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .snapshot(),
                None => inner
                    .global
                    .breadcrumbs
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .snapshot(),
            }
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
    /// **Without an active scope, this is shared by every thread and task using this
    /// client — it is not request-scoped.** Concurrent requests overwrite each other
    /// here, which can attribute one user's error to another. Use it for process-wide
    /// facts; put request data on the notice via [`Notice::context`], which travels
    /// with the notice and cannot be clobbered. Inside `scope()` (requires the `tokio`
    /// feature) this call writes to that request's own context instead — see
    /// [the crate docs](crate#context-is-process-wide).
    ///
    /// Notice-local context wins on key collisions.
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

    /// Clears this client's context, its breadcrumb trail, its event context, and its
    /// request id — the whole accumulated diagnostic scope.
    ///
    /// Without an active scope, this state is shared process-wide, so this discards
    /// what every other in-flight caller has accumulated too. It suits programs that
    /// handle one unit of work at a time (a CLI, a cron job, a serialized queue
    /// consumer); calling it from a concurrent request handler will erase other
    /// requests' state. Inside `scope()` (requires the `tokio` feature) this clears
    /// only that request's own scope, leaving every other request's state — and the
    /// process-wide base — untouched.
    pub fn clear_context(&self) {
        match self.overlay() {
            Some(o) => {
                o.context.lock().unwrap_or_else(|e| e.into_inner()).clear();
                o.breadcrumbs
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clear();
                o.event_context
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clear();
                *o.request_id.lock().unwrap_or_else(|e| e.into_inner()) = None;
            }
            None => {
                let g = &self.0.global;
                g.context.lock().unwrap_or_else(|e| e.into_inner()).clear();
                g.breadcrumbs
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clear();
                g.event_context
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clear();
                *g.request_id.lock().unwrap_or_else(|e| e.into_inner()) = None;
            }
        }
    }

    /// Records a breadcrumb. The most recent 40 are attached to each notice; older ones
    /// fall off. A no-op when breadcrumbs are disabled in the config.
    ///
    /// Without an active scope the trail is process-wide, and under concurrency
    /// crumbs from unrelated requests interleave and evict one another — treat it
    /// as a process-level log. Inside `scope()` (requires the `tokio` feature) the
    /// trail belongs to that request alone.
    pub fn add_breadcrumb(
        &self,
        message: &str,
        category: &str,
        metadata: Option<Map<String, Value>>,
    ) {
        if !self.0.config.breadcrumbs_enabled {
            return;
        }
        let overlay = self.overlay();
        let store = match &overlay {
            Some(o) => &o.breadcrumbs,
            None => &self.0.global.breadcrumbs,
        };
        store
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
    /// drain **in total**. The client accepts nothing further afterwards.
    ///
    /// The two pipelines share one budget rather than each receiving `timeout`,
    /// so this bounds process-exit latency at the number you passed.
    pub fn shutdown(&self, timeout: Duration) {
        // `None` means the deadline is not representable — a caller's
        // `Duration::MAX` must not panic here, so fall back to the full timeout
        // for each stage rather than computing a remainder.
        let deadline = Instant::now().checked_add(timeout);
        let remaining =
            || deadline.map_or(timeout, |d| d.saturating_duration_since(Instant::now()));

        // Mark Stopped before touching the worker, so a concurrent event() can
        // never spawn one behind our back.
        let previous = {
            let mut state = self.0.events.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::replace(&mut *state, EventsState::Stopped)
        };
        if let EventsState::Running(handle) = previous {
            // Withhold one floor so a slow events worker cannot starve the
            // notices worker of the time it needs to stop cleanly.
            handle.shutdown(
                remaining()
                    .saturating_sub(SHUTDOWN_FLOOR)
                    .max(SHUTDOWN_FLOOR),
            );
        }
        self.0.worker.shutdown(remaining().max(SHUTDOWN_FLOOR));
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
        let overlay = self.overlay();
        let scope = crate::scope::merged_context(
            &inner.global.event_context,
            overlay.as_ref().map(|o| &o.event_context),
        );
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
    /// **Without an active scope, this is shared by every thread and task using
    /// this client — it is not request-scoped.** Concurrent requests overwrite
    /// each other here. Use it for process-wide facts and put per-request data
    /// in the event payload, where it travels with the event and cannot be
    /// clobbered. Inside `scope()` (requires the `tokio` feature) this call
    /// writes to that request's own event context instead.
    pub fn event_context<I, K>(&self, entries: I)
    where
        I: IntoIterator<Item = (K, Value)>,
        K: Into<String>,
    {
        let overlay = self.overlay();
        let store = match &overlay {
            Some(o) => &o.event_context,
            None => &self.0.global.event_context,
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

    /// Clears this client's event context, leaving notice context untouched.
    pub fn clear_event_context(&self) {
        let overlay = self.overlay();
        let store = match &overlay {
            Some(o) => &o.event_context,
            None => &self.0.global.event_context,
        };
        store.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }

    /// Sets the request id correlating this client's notices and events, and
    /// driving deterministic sampling — every event sharing an id shares one
    /// sampling decision.
    ///
    /// **Without an active scope, this slot is process-wide, exactly like
    /// [`Client::context`].** The name describes what you put in it, not a
    /// scoping guarantee: under concurrency one request's id overwrites
    /// another's, so an event can be attributed *and sampled* as the wrong
    /// request. Use it in programs that handle one unit of work at a time — a
    /// CLI, a cron job, a serialized consumer — and in a concurrent server
    /// either wrap the request in `scope()` (requires the `tokio` feature),
    /// which gives this call its own request-local slot, or, without the
    /// feature, put `request_id` in the event payload instead.
    pub fn request_id(&self, id: impl Into<String>) {
        let overlay = self.overlay();
        let store = match &overlay {
            Some(o) => &o.request_id,
            None => &self.0.global.request_id,
        };
        *store.lock().unwrap_or_else(|e| e.into_inner()) = Some(id.into());
    }

    /// Clears the request id set by [`Client::request_id`].
    pub fn clear_request_id(&self) {
        let overlay = self.overlay();
        let store = match &overlay {
            Some(o) => &o.request_id,
            None => &self.0.global.request_id,
        };
        *store.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    pub(crate) fn current_request_id(&self) -> Option<String> {
        if let Some(o) = self.overlay() {
            return o
                .request_id
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
        }
        self.0
            .global
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
            global: Arc::new(crate::scope::Scope::new()),
            transport,
            worker,
            notice_drops,
            events: Mutex::new(EventsState::NotStarted),
            event_drops: Arc::new(DropCounter::new("events")),
            sampler,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{TestTransport, TransportResponse};
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

    #[test]
    fn test_shutdown_budget_is_shared_by_both_workers() {
        // Regression: `shutdown(d)` handed `d` to the events worker and then `d`
        // again to the notices worker, so the documented "up to `timeout`" could
        // take twice that. A transport that never returns in time pins both.
        struct Slow;
        impl Transport for Slow {
            fn deliver(
                &self,
                _req: &TransportRequest,
            ) -> Result<TransportResponse, TransportError> {
                std::thread::sleep(Duration::from_secs(10));
                Ok(201.into())
            }
        }
        let config = crate::Config::builder()
            .env_source(|_| None)
            .api_key("k")
            .env("production")
            .build()
            .unwrap();
        let client = Client::builder(config)
            .transport(Arc::new(Slow))
            .build()
            .unwrap();

        client.event("t", json!({})); // starts the events worker
        client.notify_notice(crate::Notice::message("X", "y"));
        std::thread::sleep(Duration::from_millis(150)); // both pick up work

        let started = Instant::now();
        client.shutdown(Duration::from_millis(800));
        assert!(
            started.elapsed() < Duration::from_millis(1300),
            "shutdown(800ms) must bound both workers together, not give each 800ms (took {:?})",
            started.elapsed()
        );
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

        let notices = delivered(&transport);
        assert_eq!(
            notices.len(),
            8,
            "all 8 notices must arrive, or the per-notice assertions below run vacuously"
        );
        let mut whos = Vec::new();
        for notice in &notices {
            let who = notice["request"]["context"]["who"].as_i64().unwrap();
            whos.push(who);
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
        whos.sort_unstable();
        whos.dedup();
        assert_eq!(whos.len(), 8, "all 8 `who` values must be distinct");
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

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_breadcrumbs_are_selected_not_merged_when_scoped() {
        // The rule the Task 3 commit singles out: an active overlay's trail is
        // used ALONE. If the read ever merged global beneath overlay, this
        // global crumb would leak into a scoped notice's trail.
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.add_breadcrumb("global-crumb", "custom", None);

        crate::scope::scope(async {
            client.add_breadcrumb("scoped-crumb", "custom", None);
            client.notify_notice(crate::Notice::message("Boom", "x"));
            assert!(client.flush(Duration::from_secs(5)));
        })
        .await;

        let notices = delivered(&transport);
        assert_eq!(notices.len(), 1);
        let crumbs = notices[0]["breadcrumbs"]["trail"].as_array().unwrap();
        assert_eq!(
            crumbs.len(),
            1,
            "the scope's own trail alone, not merged with the global trail"
        );
        assert_eq!(crumbs[0]["message"], json!("scoped-crumb"));
        client.shutdown(Duration::from_secs(5));
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_clear_context_does_not_erase_global_context_while_scoped() {
        // The stated safety property, for all four sub-stores clear_context
        // touches: a request clearing its own context/breadcrumbs/
        // event_context/request_id cannot erase the application's
        // process-wide versions of any of them. A leak confined to just one
        // of the three non-context stores must fail this test.
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.context([("global_key", json!("g"))]);
        client.add_breadcrumb("global-crumb", "custom", None);
        client.event_context([("global_event_key", json!("ge"))]);
        client.request_id("req-global");

        crate::scope::scope(async {
            client.context([("scoped_key", json!("s"))]);
            client.add_breadcrumb("scoped-crumb", "custom", None);
            client.event_context([("scoped_event_key", json!("se"))]);
            client.request_id("req-scoped");
            client.clear_context();
        })
        .await;

        client.notify_notice(crate::Notice::message("Boom", "x"));
        client.event("after.scope", json!({}));
        assert!(client.flush(Duration::from_secs(5)));

        let notice = &delivered(&transport)[0];
        let ctx = &notice["request"]["context"];
        assert_eq!(
            ctx["global_key"],
            json!("g"),
            "clear_context inside a scope must not touch the global context"
        );
        assert!(
            ctx.get("scoped_key").is_none(),
            "the scope's own context was cleared"
        );

        let crumbs = notice["breadcrumbs"]["trail"].as_array().unwrap();
        assert_eq!(
            crumbs.len(),
            1,
            "the global breadcrumb trail must survive a scoped clear_context"
        );
        assert_eq!(crumbs[0]["message"], json!("global-crumb"));

        assert_eq!(
            notice["correlation_context"]["request_id"],
            json!("req-global"),
            "the global request_id must survive a scoped clear_context"
        );

        let events = events_delivered(&transport);
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0]["global_event_key"],
            json!("ge"),
            "the global event_context must survive a scoped clear_context"
        );
        assert!(events[0].get("scoped_event_key").is_none());

        client.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_unscoped_clear_context_does_clear_the_global() {
        // Converse of the above: with no scope active, clear_context still
        // clears the global — the earlier behaviour is unchanged.
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.context([("global_key", json!("g"))]);
        client.clear_context();
        client.notify_notice(crate::Notice::message("Boom", "x"));
        assert!(client.flush(Duration::from_secs(5)));
        let ctx = &delivered(&transport)[0]["request"]["context"];
        assert!(ctx.get("global_key").is_none());
        client.shutdown(Duration::from_secs(5));
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

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_event_context_write_merge_and_request_id_are_overlay_routed() {
        // Covers the write, the merged read, and request_id routing in one
        // test: a scoped event_context() write must land in the overlay, the
        // global base must still be merged beneath it at read time, and the
        // overlay wins a key collision.
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.event_context([("tenant", json!("global")), ("shared", json!("global"))]);

        crate::scope::scope(async {
            client.event_context([("shared", json!("overlay"))]);
            client.request_id("req-scoped");
            client.event("user.created", json!({ "user_id": 7 }));
            assert!(client.flush(Duration::from_secs(5)));
        })
        .await;

        // A second, unscoped event proves the scoped write above landed in
        // the overlay rather than the global: if it had mutated the global,
        // "shared" would still read "overlay" here, and request_id (never
        // written globally) would still read "req-scoped".
        client.event("second.event", json!({}));
        assert!(client.flush(Duration::from_secs(5)));

        let events = events_delivered(&transport);
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0]["tenant"],
            json!("global"),
            "the global base is still merged beneath the overlay"
        );
        assert_eq!(
            events[0]["shared"],
            json!("overlay"),
            "the overlay wins a key collision"
        );
        assert_eq!(events[0]["request_id"], json!("req-scoped"));

        assert_eq!(
            events[1]["shared"],
            json!("global"),
            "the scoped write must not have mutated the global event context"
        );
        assert!(
            events[1].get("request_id").is_none(),
            "the scoped request_id must not have leaked into the global slot"
        );
        client.shutdown(Duration::from_secs(5));
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_clear_event_context_does_not_erase_the_global_while_scoped() {
        // clear_event_context() had zero coverage: neither its overlay-vs-global
        // routing nor the safety property (a scoped clear must not erase the
        // global) was exercised anywhere in the suite.
        let transport = Arc::new(TestTransport::new());
        let client = test_client(transport.clone());
        client.event_context([("global_key", json!("g"))]);

        crate::scope::scope(async {
            client.event_context([("overlay_key", json!("o"))]);
            client.clear_event_context();
            client.event("scoped.event", json!({}));
            assert!(client.flush(Duration::from_secs(5)));
        })
        .await;

        client.event("unscoped.event", json!({}));
        assert!(client.flush(Duration::from_secs(5)));

        let events = events_delivered(&transport);
        assert_eq!(events.len(), 2);
        assert!(
            events[0].get("overlay_key").is_none(),
            "the scoped clear_event_context must have cleared the overlay's own entry"
        );
        assert_eq!(
            events[1]["global_key"],
            json!("g"),
            "a scoped clear_event_context must not erase the global event context"
        );
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
            ) -> Result<crate::TransportResponse, crate::TransportError> {
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
