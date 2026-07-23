//! Notice: the error payload and its assembly into the wire format (spec "Notice payload").
use crate::breadcrumbs::Breadcrumb;
use crate::bt::Frame;
use crate::config::Config;
use serde_json::{Map, Value, json};
use std::panic::{AssertUnwindSafe, catch_unwind};

const MAX_CAUSES: usize = 5;
const NOTIFIER_NAME: &str = "honeybadger-rust";
const NOTIFIER_URL: &str = "https://github.com/honeybadger-io/honeybadger-rust";
const VERSION: &str = env!("CARGO_PKG_VERSION");
const DISPLAY_PANIC: &str = "<panic in Display>";

pub(crate) struct Cause {
    pub(crate) class: String,
    pub(crate) message: String,
}

/// A single error report: what happened, plus the metadata sent alongside it.
pub struct Notice {
    pub(crate) class: String,
    pub(crate) message: String,
    pub(crate) causes: Vec<Cause>,
    pub(crate) raw_backtrace: Option<backtrace::Backtrace>,
    pub(crate) frames: Option<Vec<Frame>>,
    pub(crate) fingerprint: Option<String>,
    pub(crate) tags: Vec<String>,
    pub(crate) context: Map<String, Value>,
}

impl Notice {
    pub fn from_error<E: std::error::Error + ?Sized>(error: &E) -> Notice {
        let message = safe_display(error);
        let type_name = std::any::type_name::<E>();
        let class = if type_name.starts_with("dyn ") {
            first_line_255(&message)
        } else {
            type_name.to_owned()
        };
        let mut causes = Vec::new();
        let mut source = error.source();
        while let Some(cause) = source {
            if causes.len() == MAX_CAUSES {
                break;
            }
            let msg = safe_display(cause);
            causes.push(Cause {
                class: first_line_255(&msg),
                message: msg,
            });
            source = cause.source();
        }
        Notice {
            class,
            message,
            causes,
            raw_backtrace: Some(backtrace::Backtrace::new_unresolved()),
            frames: None,
            fingerprint: None,
            tags: Vec::new(),
            context: Map::new(),
        }
    }

    pub fn message(class: &str, message: &str) -> Notice {
        Notice {
            class: class.to_owned(),
            message: message.to_owned(),
            causes: Vec::new(),
            raw_backtrace: None,
            frames: None,
            fingerprint: None,
            tags: Vec::new(),
            context: Map::new(),
        }
    }

    // Consuming builder methods (spec public API).
    pub fn class(mut self, class: impl Into<String>) -> Self {
        self.class = class.into();
        self
    }
    pub fn tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tags.extend(tags.into_iter().map(Into::into));
        self
    }
    pub fn fingerprint(mut self, fp: impl Into<String>) -> Self {
        self.fingerprint = Some(fp.into());
        self
    }
    pub fn context<I, K>(mut self, entries: I) -> Self
    where
        I: IntoIterator<Item = (K, Value)>,
        K: Into<String>,
    {
        for (k, v) in entries {
            self.context.insert(k.into(), v);
        }
        self
    }

    // Hook-facing mutators.
    pub fn set_class(&mut self, class: impl Into<String>) {
        self.class = class.into();
    }
    pub fn set_message(&mut self, message: impl Into<String>) {
        self.message = message.into();
    }
    pub fn set_fingerprint(&mut self, fp: Option<String>) {
        self.fingerprint = fp;
    }
    pub fn add_tag(&mut self, tag: impl Into<String>) {
        self.tags.push(tag.into());
    }
    pub fn set_context(&mut self, key: impl Into<String>, value: impl Into<Value>) {
        self.context.insert(key.into(), value.into());
    }

    // Read accessors.
    pub fn error_class(&self) -> &str {
        &self.class
    }
    pub fn error_message(&self) -> &str {
        &self.message
    }
    pub fn get_context(&self) -> &Map<String, Value> {
        &self.context
    }
    pub fn get_tags(&self) -> &[String] {
        &self.tags
    }

    /// Scope context merges UNDER notice-local context (local wins).
    pub(crate) fn merge_scope_context(&mut self, scope: Map<String, Value>) {
        for (k, v) in scope {
            self.context.entry(k).or_insert(v);
        }
    }
}

pub(crate) fn safe_display<E: std::fmt::Display + ?Sized>(value: &E) -> String {
    catch_unwind(AssertUnwindSafe(|| value.to_string()))
        .unwrap_or_else(|_| DISPLAY_PANIC.to_owned())
}

pub(crate) fn first_line_255(s: &str) -> String {
    let line = s.lines().next().unwrap_or_default();
    let mut cut = line.len().min(255);
    while !line.is_char_boundary(cut) {
        cut -= 1;
    }
    line[..cut].to_owned()
}

pub(crate) fn assemble(
    notice: &Notice,
    config: &Config,
    breadcrumbs: Option<Vec<Breadcrumb>>,
    frames: Option<Vec<Frame>>,
    pid: u32,
) -> Value {
    let backtrace: Vec<Value> = frames
        .unwrap_or_default()
        .into_iter()
        .map(|f| {
            let mut obj = Map::new();
            if let Some(n) = f.number {
                obj.insert("number".into(), json!(n.to_string()));
            }
            if let Some(file) = f.file {
                obj.insert("file".into(), json!(file));
            }
            if let Some(m) = f.method {
                obj.insert("method".into(), json!(m));
            }
            if let Some(s) = f.source {
                obj.insert("source".into(), json!(s));
            }
            Value::Object(obj)
        })
        .collect();

    let mut server = Map::new();
    server.insert("project_root".into(), json!(config.root));
    if let Some(rev) = &config.revision {
        server.insert("revision".into(), json!(rev));
    }
    if let Some(env) = &config.env {
        server.insert("environment_name".into(), json!(env));
    }
    server.insert("hostname".into(), json!(config.hostname));
    server.insert("pid".into(), json!(pid));

    let mut payload = Map::new();
    payload.insert(
        "notifier".into(),
        json!({
            "name": NOTIFIER_NAME, "url": NOTIFIER_URL, "version": VERSION, "language": "rust",
        }),
    );
    payload.insert(
        "breadcrumbs".into(),
        json!({
            "enabled": config.breadcrumbs_enabled,
            "trail": breadcrumbs.unwrap_or_default(),
        }),
    );
    payload.insert(
        "error".into(),
        json!({
            "class": notice.class,
            "message": notice.message,
            "backtrace": backtrace,
            "fingerprint": notice.fingerprint,
            "tags": notice.tags,
            "causes": notice.causes.iter().map(|c| json!({"class": c.class, "message": c.message})).collect::<Vec<_>>(),
        }),
    );
    payload.insert("request".into(), json!({ "context": notice.context }));
    payload.insert("server".into(), Value::Object(server));
    if let Some(request_id) = notice.context.get("request_id") {
        payload.insert(
            "correlation_context".into(),
            json!({ "request_id": request_id }),
        );
    }
    Value::Object(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fmt;

    #[derive(Debug)]
    struct Inner;
    impl fmt::Display for Inner {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "inner cause")
        }
    }
    impl std::error::Error for Inner {}

    #[derive(Debug)]
    struct Outer(Inner);
    impl fmt::Display for Outer {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "outer failed")
        }
    }
    impl std::error::Error for Outer {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[derive(Debug)]
    struct PanickyDisplay;
    impl fmt::Display for PanickyDisplay {
        fn fmt(&self, _: &mut fmt::Formatter) -> fmt::Result {
            panic!("bad Display")
        }
    }
    impl std::error::Error for PanickyDisplay {}

    fn test_config() -> crate::Config {
        crate::Config::builder()
            .env_source(|_| None)
            .api_key("k")
            .env("production")
            .root("/app")
            .hostname("web-1")
            .revision("abc123")
            .build()
            .unwrap()
    }

    #[test]
    fn test_from_error_typed_class_and_causes() {
        let n = Notice::from_error(&Outer(Inner));
        assert!(n.error_class().ends_with("Outer"));
        assert_eq!(n.error_message(), "outer failed");
        assert_eq!(n.causes.len(), 1);
        assert_eq!(n.causes[0].class, "inner cause");
        assert_eq!(n.causes[0].message, "inner cause");
    }

    #[test]
    fn test_from_error_dyn_falls_back_to_display() {
        let e: Box<dyn std::error::Error> = Box::new(Outer(Inner));
        let n = Notice::from_error(e.as_ref()); // &dyn Error
        assert_eq!(n.error_class(), "outer failed"); // first Display line, not "dyn ..."
    }

    #[test]
    fn test_panicking_display_is_caught() {
        let n = Notice::from_error(&PanickyDisplay);
        assert_eq!(n.error_message(), "<panic in Display>");
    }

    #[test]
    fn test_merge_scope_context_local_wins() {
        let mut n = Notice::message("X", "y")
            .context([("shared", json!("local")), ("only_local", json!(1))]);
        let mut scope = serde_json::Map::new();
        scope.insert("shared".into(), json!("scope"));
        scope.insert("only_scope".into(), json!(2));
        n.merge_scope_context(scope);
        assert_eq!(n.get_context()["shared"], json!("local"));
        assert_eq!(n.get_context()["only_scope"], json!(2));
        assert_eq!(n.get_context()["only_local"], json!(1));
    }

    #[test]
    fn test_golden_payload() {
        let notice = Notice::message("PaymentError", "card declined")
            .tags(["checkout"])
            .fingerprint("fp-1")
            .context([("user_id", json!(7)), ("request_id", json!("req-9"))]);
        let crumbs = vec![crate::breadcrumbs::Breadcrumb::with_timestamp(
            "clicked pay",
            "ui",
            None,
            "2026-07-22T00:00:00.000Z".into(),
        )];
        let frames = vec![crate::bt::Frame {
            number: Some(42),
            file: Some("[PROJECT_ROOT]/src/main.rs".into()),
            method: Some("my_app::run".into()),
            source: None,
        }];
        let payload = assemble(&notice, &test_config(), Some(crumbs), Some(frames), 12345);
        assert_eq!(
            payload,
            json!({
                "notifier": {
                    "name": "honeybadger-rust",
                    "url": "https://github.com/honeybadger-io/honeybadger-rust",
                    "version": env!("CARGO_PKG_VERSION"),
                    "language": "rust"
                },
                "breadcrumbs": {
                    "enabled": true,
                    "trail": [{"message": "clicked pay", "category": "ui", "metadata": {}, "timestamp": "2026-07-22T00:00:00.000Z"}]
                },
                "error": {
                    "class": "PaymentError",
                    "message": "card declined",
                    "backtrace": [{"number": "42", "file": "[PROJECT_ROOT]/src/main.rs", "method": "my_app::run"}],
                    "fingerprint": "fp-1",
                    "tags": ["checkout"],
                    "causes": []
                },
                "request": {"context": {"user_id": 7, "request_id": "req-9"}},
                "server": {
                    "project_root": "/app",
                    "revision": "abc123",
                    "environment_name": "production",
                    "hostname": "web-1",
                    "pid": 12345
                },
                "correlation_context": {"request_id": "req-9"}
            })
        );
    }

    #[test]
    fn test_payload_omissions() {
        // No env, no revision, no fingerprint, no request_id, no breadcrumbs/backtrace.
        let cfg = crate::Config::builder()
            .env_source(|_| None)
            .api_key("k")
            .enabled(true)
            .root("/app")
            .hostname("h")
            .build()
            .unwrap();
        let payload = assemble(&Notice::message("X", "y"), &cfg, None, None, 1);
        assert_eq!(payload["server"].get("environment_name"), None);
        assert_eq!(payload["server"].get("revision"), None);
        assert_eq!(payload["error"]["fingerprint"], json!(null));
        assert_eq!(payload.get("correlation_context"), None);
        assert_eq!(payload["breadcrumbs"], json!({"enabled": true, "trail": []}));
        assert_eq!(payload["error"]["backtrace"], json!([]));
    }

    #[test]
    fn test_causes_capped_at_five() {
        #[derive(Debug)]
        struct Chain(usize);
        impl fmt::Display for Chain {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "link {}", self.0)
            }
        }
        impl std::error::Error for Chain {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                if self.0 < 10 {
                    Some(Box::leak(Box::new(Chain(self.0 + 1))))
                } else {
                    None
                }
            }
        }
        let n = Notice::from_error(&Chain(0));
        assert_eq!(n.causes.len(), 5);
    }
}
