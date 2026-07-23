//! Client: shared state + the notify pipeline (spec "Notify pipeline").
use crate::breadcrumbs::{Breadcrumb, RingBuffer};
use crate::config::Config;
use crate::error::Error;
use crate::notice::{Notice, assemble};
use crate::sanitizer::Sanitizer;
use crate::transport::{
    NullTransport, ServerTransport, Transport, TransportError, TransportRequest, compress,
};
use crate::worker::WorkerHandle;
use serde_json::{Map, Value};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Serialized-JSON ceiling, matching the Reporting API's documented maximum. Measured
/// before compression: the service applies the limit to the decompressed body.
pub(crate) const MAX_PAYLOAD_BYTES: usize = 262_144;

struct Inner {
    config: Config,
    sanitizer: Sanitizer,
    context: Mutex<Map<String, Value>>,
    breadcrumbs: Mutex<RingBuffer>,
    transport: Arc<dyn Transport>,
    worker: WorkerHandle,
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
    /// Fire-and-forget: the notice is queued for the delivery thread and this returns
    /// immediately. Every failure path — full queue, oversized payload, network error —
    /// is reported through the `log` facade, never by panicking or blocking.
    pub fn notify<E: std::error::Error + ?Sized>(&self, error: &E) {
        self.notify_notice(Notice::from_error(error));
    }

    /// Reports a notice built by hand. See [`Notice::from_error`] and
    /// [`Notice::message`].
    pub fn notify_notice(&self, notice: Notice) {
        if let Some(payload) = self.run_pipeline(notice) {
            if !self.0.worker.try_enqueue(payload) {
                log::warn!("honeybadger: notice dropped (queue full or worker stopped)");
            }
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

        // 3. before_notify hooks; panics caught and treated as pass.
        for hook in &inner.config.before_notify {
            let hook = hook.clone();
            let keep = catch_unwind(AssertUnwindSafe(|| hook(&mut notice))).unwrap_or_else(|_| {
                log::warn!("honeybadger: before_notify hook panicked; continuing");
                true
            });
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

    /// Merges key/value pairs into the client-wide context attached to every later
    /// notice. Setting a key to [`serde_json::Value::Null`] removes it.
    ///
    /// Notice-local context set via [`Notice::context`] wins on key collisions.
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

    /// Clears the client-wide context **and** the breadcrumb trail — the whole
    /// accumulated diagnostic scope. Useful between requests or jobs on a reused thread.
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
    }

    /// Records a breadcrumb. The most recent 40 are attached to each notice; older ones
    /// fall off. A no-op when breadcrumbs are disabled in the config.
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

    /// Blocks until every notice queued so far has been attempted, or `timeout` expires.
    /// Returns whether the barrier completed in time.
    pub fn flush(&self, timeout: Duration) -> bool {
        self.0.worker.flush(timeout)
    }

    /// Stops the delivery thread, giving queued notices up to `timeout` to drain. The
    /// client accepts no further notices afterwards.
    pub fn shutdown(&self, timeout: Duration) {
        self.0.worker.shutdown(timeout);
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
        let worker = crate::worker::spawn(transport.clone(), config.notice_queue_size)
            .map_err(Error::WorkerSpawn)?;
        let sanitizer = Sanitizer::new(config.filter_keys.iter());
        Ok(Client(Arc::new(Inner {
            config,
            sanitizer,
            context: Mutex::new(Map::new()),
            breadcrumbs: Mutex::new(RingBuffer::new()),
            transport,
            worker,
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
            .map(|r| {
                let mut s = String::new();
                flate2::read::ZlibDecoder::new(&r.body[..])
                    .read_to_string(&mut s)
                    .unwrap();
                serde_json::from_str(&s).unwrap()
            })
            .collect()
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
