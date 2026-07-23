//! Configuration: builder > env var > default (spec "Config" section).
use crate::error::Error;
use crate::notice::Notice;
use std::sync::Arc;
use std::time::Duration;

/// A hook run against every notice before delivery; returning `false` drops it.
pub type BeforeNotifyHook = dyn Fn(&mut Notice) -> bool + Send + Sync;
type EnvSource = Box<dyn Fn(&str) -> Option<String>>;

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
    pub fn api_key(mut self, v: impl Into<String>) -> Self {
        self.api_key = Some(v.into());
        self
    }
    pub fn env(mut self, v: impl Into<String>) -> Self {
        self.env = Some(v.into());
        self
    }
    pub fn exclude_envs<I: IntoIterator<Item = S>, S: Into<String>>(mut self, v: I) -> Self {
        self.exclude_envs = Some(v.into_iter().map(Into::into).collect());
        self
    }
    pub fn enabled(mut self, v: bool) -> Self {
        self.enabled = Some(v);
        self
    }
    pub fn endpoint(mut self, v: impl Into<String>) -> Self {
        self.endpoint = Some(v.into());
        self
    }
    pub fn root(mut self, v: impl Into<String>) -> Self {
        self.root = Some(v.into());
        self
    }
    pub fn hostname(mut self, v: impl Into<String>) -> Self {
        self.hostname = Some(v.into());
        self
    }
    pub fn revision(mut self, v: impl Into<String>) -> Self {
        self.revision = Some(v.into());
        self
    }
    pub fn filter_keys<I: IntoIterator<Item = S>, S: Into<String>>(mut self, v: I) -> Self {
        self.filter_keys = Some(v.into_iter().map(Into::into).collect());
        self
    }
    pub fn ignore_classes<I: IntoIterator<Item = S>, S: Into<String>>(mut self, v: I) -> Self {
        self.ignore_classes = Some(v.into_iter().map(Into::into).collect());
        self
    }
    pub fn breadcrumbs_enabled(mut self, v: bool) -> Self {
        self.breadcrumbs_enabled = Some(v);
        self
    }
    pub fn install_panic_hook(mut self, v: bool) -> Self {
        self.install_panic_hook = Some(v);
        self
    }
    pub fn notice_queue_size(mut self, v: usize) -> Self {
        self.notice_queue_size = Some(v);
        self
    }
    pub fn connect_timeout(mut self, v: Duration) -> Self {
        self.connect_timeout = Some(v);
        self
    }
    pub fn request_timeout(mut self, v: Duration) -> Self {
        self.request_timeout = Some(v);
        self
    }
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

        if config.reporting_enabled() && config.api_key.is_none() {
            return Err(Error::MissingApiKey);
        }
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
    fn test_api_key_required_only_when_reporting() {
        // Excluded env: no key needed.
        let cfg = Config::builder()
            .env_source(no_env)
            .env("test")
            .build()
            .unwrap();
        assert!(!cfg.reporting_enabled());
        // Reporting env without key: error.
        let err = Config::builder()
            .env_source(no_env)
            .env("production")
            .build()
            .unwrap_err();
        assert!(matches!(err, crate::Error::MissingApiKey));
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
