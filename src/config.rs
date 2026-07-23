//! Configuration: builder > env var > default (spec "Config" section).
use crate::error::Error;
use crate::notice::Notice;
use std::sync::Arc;
use std::time::Duration;

/// A hook run against every notice before delivery; returning `false` drops it.
pub type BeforeNotifyHook = dyn Fn(&mut Notice) -> bool + Send + Sync;
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
    /// silence error reporting.
    pub fn before_notify<F>(mut self, f: F) -> Self
    where
        F: Fn(&mut Notice) -> bool + Send + Sync + 'static,
    {
        self.before_notify.push(Arc::new(f));
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
        };

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
}
