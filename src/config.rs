//! Configuration: builder > env var > default (spec "Config" section).
use crate::error::Error;
use crate::notice::Notice;
use std::sync::Arc;
use std::time::Duration;

/// A hook run against every notice before delivery; returning `false` drops it.
pub type BeforeNotifyHook = dyn Fn(&mut Notice) -> bool + Send + Sync;

/// A hook run against every event before delivery; returning `false` drops it.
/// Receives the fully assembled event object and may mutate it freely.
pub type BeforeEventHook =
    dyn Fn(&mut serde_json::Map<String, serde_json::Value>) -> bool + Send + Sync;
type EnvSource = Box<dyn Fn(&str) -> Option<String>>;

/// Resolved SDK configuration. Build one with [`Config::builder`].
///
/// Every field follows the same precedence: an explicit builder call wins, then the
/// matching `HONEYBADGER_*` environment variable, then the default.
pub struct Config {
    pub(crate) api_key: Option<String>,
    pub(crate) env: Option<String>,
    pub(crate) exclude_envs: Vec<String>,
    pub(crate) enabled: Option<bool>,
    pub(crate) endpoint: String,
    pub(crate) root: String,
    pub(crate) hostname: String,
    pub(crate) revision: Option<String>,
    pub(crate) filter_keys: Vec<String>,
    pub(crate) ignore_classes: Vec<String>,
    pub(crate) breadcrumbs_enabled: bool,
    pub(crate) install_panic_hook: bool,
    pub(crate) notice_queue_size: usize,
    pub(crate) connect_timeout: Duration,
    pub(crate) request_timeout: Duration,
    pub(crate) before_notify: Vec<Arc<BeforeNotifyHook>>,
    pub(crate) events_enabled: bool,
    pub(crate) events_batch_size: usize,
    pub(crate) events_flush_interval: Duration,
    pub(crate) events_queue_size: usize,
    pub(crate) events_max_retries: u32,
    pub(crate) events_sample_rate: u8,
    pub(crate) events_attach_hostname: bool,
    pub(crate) events_attach_environment: bool,
    pub(crate) before_event: Vec<Arc<BeforeEventHook>>,
}

impl Config {
    /// Starts a new configuration builder.
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    pub(crate) fn reporting_enabled(&self) -> bool {
        if let Some(enabled) = self.enabled {
            return enabled;
        }
        match &self.env {
            Some(env) => !self.exclude_envs.iter().any(|e| e == env),
            None => true, // unset means "report" (one log::info at init)
        }
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("api_key", &self.api_key.as_deref().map(|_| "<redacted>"))
            .field("env", &self.env)
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .field("hostname", &self.hostname)
            .field("revision", &self.revision)
            .field("hooks", &self.before_notify.len())
            .field("events_enabled", &self.events_enabled)
            .field("events_batch_size", &self.events_batch_size)
            .field("events_flush_interval", &self.events_flush_interval)
            .field("events_queue_size", &self.events_queue_size)
            .field("events_max_retries", &self.events_max_retries)
            .field("events_sample_rate", &self.events_sample_rate)
            .field("events_attach_hostname", &self.events_attach_hostname)
            .field("events_attach_environment", &self.events_attach_environment)
            .field("event_hooks", &self.before_event.len())
            .finish_non_exhaustive()
    }
}

/// Builder for [`Config`]. Every setter is optional; see each one for its default and
/// its environment-variable fallback.
pub struct ConfigBuilder {
    api_key: Option<String>,
    env: Option<String>,
    exclude_envs: Option<Vec<String>>,
    enabled: Option<bool>,
    endpoint: Option<String>,
    root: Option<String>,
    hostname: Option<String>,
    revision: Option<String>,
    filter_keys: Option<Vec<String>>,
    ignore_classes: Option<Vec<String>>,
    breadcrumbs_enabled: Option<bool>,
    install_panic_hook: Option<bool>,
    notice_queue_size: Option<usize>,
    connect_timeout: Option<Duration>,
    request_timeout: Option<Duration>,
    before_notify: Vec<Arc<BeforeNotifyHook>>,
    events_enabled: Option<bool>,
    events_batch_size: Option<usize>,
    events_flush_interval: Option<Duration>,
    events_queue_size: Option<usize>,
    events_max_retries: Option<u32>,
    events_sample_rate: Option<u8>,
    events_attach_hostname: Option<bool>,
    events_attach_environment: Option<bool>,
    before_event: Vec<Arc<BeforeEventHook>>,
    env_source: EnvSource,
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        ConfigBuilder {
            api_key: None,
            env: None,
            exclude_envs: None,
            enabled: None,
            endpoint: None,
            root: None,
            hostname: None,
            revision: None,
            filter_keys: None,
            ignore_classes: None,
            breadcrumbs_enabled: None,
            install_panic_hook: None,
            notice_queue_size: None,
            connect_timeout: None,
            request_timeout: None,
            before_notify: Vec::new(),
            events_enabled: None,
            events_batch_size: None,
            events_flush_interval: None,
            events_queue_size: None,
            events_max_retries: None,
            events_sample_rate: None,
            events_attach_hostname: None,
            events_attach_environment: None,
            before_event: Vec::new(),
            env_source: Box::new(|key| std::env::var(key).ok()),
        }
    }
}

impl ConfigBuilder {
    /// Project API key. Env: `HONEYBADGER_API_KEY`. Required only when notices are
    /// actually sent to the Honeybadger service — an excluded environment, or a
    /// caller-supplied [`crate::Transport`], needs no key.
    pub fn api_key(mut self, v: impl Into<String>) -> Self {
        self.api_key = Some(v.into());
        self
    }
    /// Environment name reported with each notice, e.g. `"production"`. Env:
    /// `HONEYBADGER_ENV`. Also drives [`ConfigBuilder::exclude_envs`].
    pub fn env(mut self, v: impl Into<String>) -> Self {
        self.env = Some(v.into());
        self
    }
    /// Environments that do not report. Default: `["development", "test"]`. In an
    /// excluded environment the SDK initializes and accepts notices but discards them.
    pub fn exclude_envs<I: IntoIterator<Item = S>, S: Into<String>>(mut self, v: I) -> Self {
        self.exclude_envs = Some(v.into_iter().map(Into::into).collect());
        self
    }
    /// Forces reporting on or off, overriding the [`ConfigBuilder::exclude_envs`]
    /// decision in both directions. Env: `HONEYBADGER_ENABLED` (`true`/`1`/`yes`).
    pub fn enabled(mut self, v: bool) -> Self {
        self.enabled = Some(v);
        self
    }
    /// Base URL of the Honeybadger API. Default: `https://api.honeybadger.io`. Env:
    /// `HONEYBADGER_ENDPOINT`. Set this for the EU region, a proxy, or a test server.
    pub fn endpoint(mut self, v: impl Into<String>) -> Self {
        self.endpoint = Some(v.into());
        self
    }
    /// Project root. Paths beneath it are reported as `[PROJECT_ROOT]/…`, and only
    /// files under it get source excerpts. Default: the current directory. Env:
    /// `HONEYBADGER_ROOT`.
    pub fn root(mut self, v: impl Into<String>) -> Self {
        self.root = Some(v.into());
        self
    }
    /// Hostname reported with each notice. Default: the system hostname. Env:
    /// `HONEYBADGER_HOSTNAME`.
    pub fn hostname(mut self, v: impl Into<String>) -> Self {
        self.hostname = Some(v.into());
        self
    }
    /// Deploy revision (commit SHA) used to attribute errors to a release. Env:
    /// `HONEYBADGER_REVISION`.
    pub fn revision(mut self, v: impl Into<String>) -> Self {
        self.revision = Some(v.into());
        self
    }
    /// Keys whose values are replaced with `"[FILTERED]"` in context and breadcrumb
    /// metadata, matched case-insensitively. Default:
    /// `["password", "credit_card", "secret"]`. Filtering runs last, so it also covers
    /// data added by [`ConfigBuilder::before_notify`] hooks.
    pub fn filter_keys<I: IntoIterator<Item = S>, S: Into<String>>(mut self, v: I) -> Self {
        self.filter_keys = Some(v.into_iter().map(Into::into).collect());
        self
    }
    /// Error classes to drop without reporting, matched exactly against
    /// [`crate::Notice::error_class`]. Checked both before and after hooks run.
    pub fn ignore_classes<I: IntoIterator<Item = S>, S: Into<String>>(mut self, v: I) -> Self {
        self.ignore_classes = Some(v.into_iter().map(Into::into).collect());
        self
    }
    /// Whether breadcrumbs are collected and attached to notices. Default: `true`.
    /// When `false`, [`crate::add_breadcrumb`] becomes a no-op.
    pub fn breadcrumbs_enabled(mut self, v: bool) -> Self {
        self.breadcrumbs_enabled = Some(v);
        self
    }
    /// Whether [`crate::init`] reports panics. Default: `true`. The dispatcher chains
    /// to whatever hook was already installed, so existing panic handling still runs.
    pub fn install_panic_hook(mut self, v: bool) -> Self {
        self.install_panic_hook = Some(v);
        self
    }
    /// Capacity of the queue feeding the delivery thread. Default: `100`. Notices
    /// offered to a full queue are dropped with a `log::warn` rather than blocking the
    /// caller.
    pub fn notice_queue_size(mut self, v: usize) -> Self {
        self.notice_queue_size = Some(v);
        self
    }
    /// TCP connect timeout for delivery. Default: 2s. (Panic notices use a fixed,
    /// shorter timeout: the process is already on its way out.)
    pub fn connect_timeout(mut self, v: Duration) -> Self {
        self.connect_timeout = Some(v);
        self
    }
    /// Total timeout for one delivery request. Default: 5s.
    pub fn request_timeout(mut self, v: Duration) -> Self {
        self.request_timeout = Some(v);
        self
    }
    /// Registers a hook run against every notice just before delivery, in registration
    /// order. Returning `false` drops the notice. Hooks may mutate the notice freely
    /// (add tags, rewrite the class, attach context).
    ///
    /// A panicking hook is caught, logged, and treated as `true` — one bad hook must not
    /// silence error reporting. (Unwinding builds only; `panic = "abort"` aborts the
    /// process before any handler runs.)
    pub fn before_notify<F>(mut self, f: F) -> Self
    where
        F: Fn(&mut Notice) -> bool + Send + Sync + 'static,
    {
        self.before_notify.push(Arc::new(f));
        self
    }
    /// Whether the Insights events pipeline is active. Default: `true`. Env:
    /// `HONEYBADGER_EVENTS_ENABLED`. When false, [`crate::event`] is a no-op and
    /// the events worker thread is never spawned.
    pub fn events_enabled(mut self, v: bool) -> Self {
        self.events_enabled = Some(v);
        self
    }

    /// Events per batch before one is cut and sent. Default: `1000`. Env:
    /// `HONEYBADGER_EVENTS_BATCH_SIZE`.
    pub fn events_batch_size(mut self, v: usize) -> Self {
        self.events_batch_size = Some(v);
        self
    }

    /// How long a partially filled batch waits before being sent anyway.
    /// Default: 30s. Env: `HONEYBADGER_EVENTS_FLUSH_INTERVAL`, **in seconds**.
    pub fn events_flush_interval(mut self, v: Duration) -> Self {
        self.events_flush_interval = Some(v);
        self
    }

    /// Total events allowed outstanding — queued, batching, and awaiting retry
    /// combined. Default: `10_000`. Env: `HONEYBADGER_EVENTS_QUEUE_SIZE`.
    /// Beyond this the oldest retained batch is dropped first.
    pub fn events_queue_size(mut self, v: usize) -> Self {
        self.events_queue_size = Some(v);
        self
    }

    /// Retries **after** the initial attempt for a batch that failed
    /// retryably. Default: `3`, so four attempts in total. Env:
    /// `HONEYBADGER_EVENTS_MAX_RETRIES`.
    pub fn events_max_retries(mut self, v: u32) -> Self {
        self.events_max_retries = Some(v);
        self
    }

    /// Percentage of events to keep, 0–100, clamped. Default: `100`. Env:
    /// `HONEYBADGER_EVENTS_SAMPLE_RATE`. Events sharing a `request_id` share one
    /// sampling decision, so a sampled request keeps all of its events or none.
    pub fn events_sample_rate(mut self, v: u8) -> Self {
        self.events_sample_rate = Some(v);
        self
    }

    /// Adds `hostname` to every event. Default: `true`. Env:
    /// `HONEYBADGER_EVENTS_ATTACH_HOSTNAME`.
    pub fn events_attach_hostname(mut self, v: bool) -> Self {
        self.events_attach_hostname = Some(v);
        self
    }

    /// Adds `environment` to every event. Default: `true`. Env:
    /// `HONEYBADGER_EVENTS_ATTACH_ENVIRONMENT`.
    pub fn events_attach_environment(mut self, v: bool) -> Self {
        self.events_attach_environment = Some(v);
        self
    }

    /// Registers a hook run against every event just before delivery, in
    /// registration order. Returning `false` drops the event. A panicking hook
    /// is caught, logged, and treated as `true`.
    ///
    /// Hooks run *before* validation, so a hook that deletes `event_type` drops
    /// the event rather than producing a malformed one.
    pub fn before_event<F>(mut self, f: F) -> Self
    where
        F: Fn(&mut serde_json::Map<String, serde_json::Value>) -> bool + Send + Sync + 'static,
    {
        self.before_event.push(Arc::new(f));
        self
    }

    /// Test seam: replaces `std::env::var` (Edition 2024 makes env mutation unsafe; tests inject instead).
    pub fn env_source<F: Fn(&str) -> Option<String> + 'static>(mut self, f: F) -> Self {
        self.env_source = Box::new(f);
        self
    }

    /// Resolves builder values, environment variables, and defaults into a [`Config`].
    ///
    /// # Errors
    ///
    /// [`Error::InvalidEndpoint`] if the endpoint is not an `http://` or `https://` URL.
    /// The API key is *not* checked here — that happens when the transport is resolved
    /// in [`crate::Client::new`].
    pub fn build(self) -> Result<Config, Error> {
        let ev = |key: &str| (self.env_source)(key);
        let parse_bool = |s: String| matches!(s.as_str(), "true" | "1" | "yes");

        let endpoint = self
            .endpoint
            .or_else(|| ev("HONEYBADGER_ENDPOINT"))
            .unwrap_or_else(|| "https://api.honeybadger.io".to_string());
        if !(endpoint.starts_with("https://") || endpoint.starts_with("http://")) {
            return Err(Error::InvalidEndpoint(endpoint));
        }

        // Builder beats environment, so an environment value is only read — and
        // only validated — when the builder left that option unset. Otherwise a
        // stray `HONEYBADGER_EVENTS_BATCH_SIZE=lots` would defeat an explicit
        // `.events_batch_size(50)`, inverting the documented precedence.
        let parse_num = |key: &str, builder_set: bool| -> Result<Option<u64>, Error> {
            if builder_set {
                return Ok(None);
            }
            match ev(key) {
                None => Ok(None),
                Some(raw) => raw.parse::<u64>().map(Some).map_err(|_| {
                    Error::InvalidConfig(format!(
                        "{key} must be a non-negative integer, got {raw:?}"
                    ))
                }),
            }
        };
        // Narrowing must not wrap: `4294967296 as u32` is 0, which would silently
        // turn "lots of retries" into "no retries".
        fn narrow<T: TryFrom<u64>>(key: &str, n: u64) -> Result<T, Error> {
            T::try_from(n).map_err(|_| {
                Error::InvalidConfig(format!("{key} is out of range for this platform: {n}"))
            })
        }

        let env_batch = parse_num(
            "HONEYBADGER_EVENTS_BATCH_SIZE",
            self.events_batch_size.is_some(),
        )?;
        let env_interval = parse_num(
            "HONEYBADGER_EVENTS_FLUSH_INTERVAL",
            self.events_flush_interval.is_some(),
        )?;
        let env_queue = parse_num(
            "HONEYBADGER_EVENTS_QUEUE_SIZE",
            self.events_queue_size.is_some(),
        )?;
        let env_retries = parse_num(
            "HONEYBADGER_EVENTS_MAX_RETRIES",
            self.events_max_retries.is_some(),
        )?;
        let env_rate = parse_num(
            "HONEYBADGER_EVENTS_SAMPLE_RATE",
            self.events_sample_rate.is_some(),
        )?;

        let events_batch_size = match (self.events_batch_size, env_batch) {
            (Some(v), _) => v,
            (None, Some(n)) => narrow("HONEYBADGER_EVENTS_BATCH_SIZE", n)?,
            (None, None) => 1000,
        };
        let events_queue_size = match (self.events_queue_size, env_queue) {
            (Some(v), _) => v,
            (None, Some(n)) => narrow("HONEYBADGER_EVENTS_QUEUE_SIZE", n)?,
            (None, None) => 10_000,
        };
        let events_max_retries = match (self.events_max_retries, env_retries) {
            (Some(v), _) => v,
            (None, Some(n)) => narrow("HONEYBADGER_EVENTS_MAX_RETRIES", n)?,
            (None, None) => 3,
        };

        let config = Config {
            api_key: self.api_key.or_else(|| ev("HONEYBADGER_API_KEY")),
            env: self.env.or_else(|| ev("HONEYBADGER_ENV")),
            exclude_envs: self
                .exclude_envs
                .unwrap_or_else(|| vec!["development".into(), "test".into()]),
            enabled: self
                .enabled
                .or_else(|| ev("HONEYBADGER_ENABLED").map(parse_bool)),
            endpoint: endpoint.trim_end_matches('/').to_string(),
            root: self
                .root
                .or_else(|| ev("HONEYBADGER_ROOT"))
                .or_else(|| {
                    std::env::current_dir()
                        .ok()
                        .map(|p| p.to_string_lossy().into_owned())
                })
                .unwrap_or_default(),
            hostname: self
                .hostname
                .or_else(|| ev("HONEYBADGER_HOSTNAME"))
                .or_else(|| {
                    hostname::get()
                        .ok()
                        .map(|h| h.to_string_lossy().into_owned())
                })
                .unwrap_or_default(),
            revision: self.revision.or_else(|| ev("HONEYBADGER_REVISION")),
            filter_keys: self
                .filter_keys
                .unwrap_or_else(|| vec!["password".into(), "credit_card".into(), "secret".into()]),
            ignore_classes: self.ignore_classes.unwrap_or_default(),
            breadcrumbs_enabled: self.breadcrumbs_enabled.unwrap_or(true),
            install_panic_hook: self.install_panic_hook.unwrap_or(true),
            notice_queue_size: self.notice_queue_size.unwrap_or(100),
            connect_timeout: self.connect_timeout.unwrap_or(Duration::from_secs(2)),
            request_timeout: self.request_timeout.unwrap_or(Duration::from_secs(5)),
            before_notify: self.before_notify,
            events_enabled: self
                .events_enabled
                .or_else(|| ev("HONEYBADGER_EVENTS_ENABLED").map(parse_bool))
                .unwrap_or(true),
            events_batch_size,
            events_flush_interval: self
                .events_flush_interval
                .or(env_interval.map(Duration::from_secs))
                .unwrap_or(Duration::from_secs(30)),
            events_queue_size,
            events_max_retries,
            // Clamped before narrowing, so the cast cannot wrap.
            events_sample_rate: self
                .events_sample_rate
                .or(env_rate.map(|n| n.min(100) as u8))
                .unwrap_or(100)
                .min(100),
            events_attach_hostname: self
                .events_attach_hostname
                .or_else(|| ev("HONEYBADGER_EVENTS_ATTACH_HOSTNAME").map(parse_bool))
                .unwrap_or(true),
            events_attach_environment: self
                .events_attach_environment
                .or_else(|| ev("HONEYBADGER_EVENTS_ATTACH_ENVIRONMENT").map(parse_bool))
                .unwrap_or(true),
            before_event: self.before_event,
        };

        if config.events_flush_interval.is_zero() {
            return Err(Error::InvalidConfig(
                "events_flush_interval must be greater than zero".into(),
            ));
        }
        if config.events_batch_size == 0 {
            return Err(Error::InvalidConfig(
                "events_batch_size must be at least 1".into(),
            ));
        }
        if config.events_queue_size == 0 {
            return Err(Error::InvalidConfig(
                "events_queue_size must be at least 1".into(),
            ));
        }

        // The API key is NOT validated here: it is required only when the resolved
        // transport is `Server` (spec "Config"), and the transport isn't chosen until
        // `ClientBuilder::build` — a caller supplying their own `Transport` needs no key.
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[test]
    fn test_builder_beats_env_beats_default() {
        let cfg = Config::builder()
            .env_source(|k| (k == "HONEYBADGER_ENV").then(|| "staging".to_string()))
            .api_key("k")
            .build()
            .unwrap();
        assert_eq!(cfg.env.as_deref(), Some("staging")); // env var wins over default

        let cfg = Config::builder()
            .env_source(|k| (k == "HONEYBADGER_ENV").then(|| "staging".to_string()))
            .api_key("k")
            .env("production")
            .build()
            .unwrap();
        assert_eq!(cfg.env.as_deref(), Some("production")); // builder wins over env var
    }

    #[test]
    fn test_api_key_env_var() {
        let cfg = Config::builder()
            .env_source(|k| (k == "HONEYBADGER_API_KEY").then(|| "from-env".to_string()))
            .build()
            .unwrap();
        assert_eq!(cfg.api_key.as_deref(), Some("from-env"));
    }

    #[test]
    fn test_api_key_not_validated_at_config_build() {
        // Excluded env: no key needed.
        let cfg = Config::builder()
            .env_source(no_env)
            .env("test")
            .build()
            .unwrap();
        assert!(!cfg.reporting_enabled());
        // A reporting env without a key still builds: the key is required only once
        // the transport resolves to `Server` (see the Client tests).
        let cfg = Config::builder()
            .env_source(no_env)
            .env("production")
            .build()
            .unwrap();
        assert!(cfg.reporting_enabled());
        assert!(cfg.api_key.is_none());
    }

    #[test]
    fn test_enabled_overrides_both_directions() {
        let cfg = Config::builder()
            .env_source(no_env)
            .env("test")
            .enabled(true)
            .api_key("k")
            .build()
            .unwrap();
        assert!(cfg.reporting_enabled());
        let cfg = Config::builder()
            .env_source(no_env)
            .env("production")
            .enabled(false)
            .build()
            .unwrap();
        assert!(!cfg.reporting_enabled());
    }

    #[test]
    fn test_endpoint_validation() {
        let err = Config::builder()
            .env_source(no_env)
            .api_key("k")
            .endpoint("ftp://nope")
            .build()
            .unwrap_err();
        assert!(matches!(err, crate::Error::InvalidEndpoint(_)));
    }

    #[test]
    fn test_defaults() {
        let cfg = Config::builder()
            .env_source(no_env)
            .api_key("k")
            .build()
            .unwrap();
        assert_eq!(cfg.endpoint, "https://api.honeybadger.io");
        assert_eq!(cfg.notice_queue_size, 100);
        assert_eq!(
            cfg.exclude_envs,
            vec!["development".to_string(), "test".to_string()]
        );
        assert_eq!(
            cfg.filter_keys,
            vec![
                "password".to_string(),
                "credit_card".to_string(),
                "secret".to_string()
            ]
        );
        assert_eq!(cfg.connect_timeout, Duration::from_secs(2));
        assert_eq!(cfg.request_timeout, Duration::from_secs(5));
        assert!(cfg.breadcrumbs_enabled);
        assert!(cfg.install_panic_hook);
        assert!(!cfg.root.is_empty()); // cwd default
    }

    #[test]
    fn test_events_defaults() {
        let cfg = Config::builder()
            .env_source(no_env)
            .api_key("k")
            .build()
            .unwrap();
        assert!(cfg.events_enabled);
        assert_eq!(cfg.events_batch_size, 1000);
        assert_eq!(cfg.events_flush_interval, Duration::from_secs(30));
        assert_eq!(cfg.events_queue_size, 10_000);
        assert_eq!(cfg.events_max_retries, 3);
        assert_eq!(cfg.events_sample_rate, 100);
        assert!(cfg.events_attach_hostname);
        assert!(cfg.events_attach_environment);
    }

    #[test]
    fn test_events_env_vars() {
        let cfg = Config::builder()
            .env_source(|k| match k {
                "HONEYBADGER_EVENTS_BATCH_SIZE" => Some("50".into()),
                "HONEYBADGER_EVENTS_FLUSH_INTERVAL" => Some("5".into()),
                "HONEYBADGER_EVENTS_ENABLED" => Some("false".into()),
                _ => None,
            })
            .api_key("k")
            .build()
            .unwrap();
        assert_eq!(cfg.events_batch_size, 50);
        assert_eq!(cfg.events_flush_interval, Duration::from_secs(5));
        assert!(!cfg.events_enabled);
    }

    #[test]
    fn test_zero_interval_and_sizes_are_rejected() {
        // A zero flush interval would turn recv_timeout into a busy loop.
        for build in [
            || {
                Config::builder()
                    .env_source(no_env)
                    .api_key("k")
                    .events_flush_interval(Duration::ZERO)
                    .build()
            },
            || {
                Config::builder()
                    .env_source(no_env)
                    .api_key("k")
                    .events_batch_size(0)
                    .build()
            },
            || {
                Config::builder()
                    .env_source(no_env)
                    .api_key("k")
                    .events_queue_size(0)
                    .build()
            },
        ] {
            assert!(
                matches!(build().unwrap_err(), crate::Error::InvalidConfig(_)),
                "invalid events settings must be rejected at build time"
            );
        }
    }

    #[test]
    fn test_builder_beats_an_unparseable_env_var() {
        // Regression: every numeric env var was parsed eagerly, so a stray
        // value failed the build even when the builder had already overridden
        // it — inverting the documented builder > env > default precedence.
        let cfg = Config::builder()
            .env_source(|k| (k == "HONEYBADGER_EVENTS_BATCH_SIZE").then(|| "lots".to_string()))
            .api_key("k")
            .events_batch_size(50)
            .build()
            .expect("an explicit builder value wins over a broken env var");
        assert_eq!(cfg.events_batch_size, 50);
    }

    #[test]
    fn test_out_of_range_env_numbers_error_rather_than_wrapping() {
        // `4294967296 as u32` is 0, which would silently disable retries.
        let err = Config::builder()
            .env_source(|k| {
                (k == "HONEYBADGER_EVENTS_MAX_RETRIES").then(|| "4294967296".to_string())
            })
            .api_key("k")
            .build()
            .unwrap_err();
        assert!(matches!(err, crate::Error::InvalidConfig(_)));
    }

    #[test]
    fn test_sample_rate_clamps_and_bad_numbers_error() {
        let cfg = Config::builder()
            .env_source(no_env)
            .api_key("k")
            .events_sample_rate(250)
            .build()
            .unwrap();
        assert_eq!(cfg.events_sample_rate, 100, "out-of-range rate clamps");

        let err = Config::builder()
            .env_source(|k| (k == "HONEYBADGER_EVENTS_BATCH_SIZE").then(|| "lots".to_string()))
            .api_key("k")
            .build()
            .unwrap_err();
        assert!(matches!(err, crate::Error::InvalidConfig(_)));
    }
}
