//! Request scoping through the public facade.
//!
//! The unit tests in `src/` reach the scope functions crate-internally, so the
//! `pub use`d surface a user actually calls — `honeybadger::init` plus the free
//! functions — was only ever exercised by `no_run` doctests and an example that
//! is built but never run. This runs it end to end against an HTTP server.
//!
//! Its own binary on purpose: `init` claims a process-wide slot that the unit
//! test for the init lifecycle also claims, and unit tests share one process.
#![cfg(feature = "tokio")]

use serde_json::json;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[tokio::test]
async fn test_the_public_facade_reports_scoped_state() {
    let mut server = mockito::Server::new_async().await;
    let bodies: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
    let captured = bodies.clone();
    let mock = server
        .mock("POST", "/v1/notices")
        .match_request(move |req| {
            let mut json = String::new();
            flate2::read::ZlibDecoder::new(req.body().unwrap().as_slice())
                .read_to_string(&mut json)
                .unwrap();
            captured
                .lock()
                .unwrap()
                .push(serde_json::from_str(&json).unwrap());
            true
        })
        .with_status(201)
        .expect(1)
        .create_async()
        .await;

    let _guard = honeybadger::init(
        honeybadger::Config::builder()
            .env_source(|_| None)
            .api_key("test-key")
            .env("production")
            .endpoint(server.url())
            .install_panic_hook(false)
            .build()
            .unwrap(),
    )
    .unwrap();

    // Process-wide, as an application sets it once at boot.
    honeybadger::context([("version", json!("1.4.2"))]);
    honeybadger::add_breadcrumb("booted", "custom", None);

    honeybadger::scope(async {
        honeybadger::request_id("req-facade");
        honeybadger::context([("user_id", json!(7))]);
        honeybadger::add_breadcrumb("query ran", "query", None);

        // The capture API through the public surface: a crumb recorded across a
        // thread boundary still belongs to this request.
        let scope = honeybadger::ScopeHandle::current();
        tokio::task::spawn_blocking(move || {
            scope.enter_sync(|| honeybadger::add_breadcrumb("blocking work", "custom", None))
        })
        .await
        .unwrap();

        honeybadger::notify(&std::io::Error::other("boom"));
        assert!(honeybadger::flush(Duration::from_secs(10)));
    })
    .await;

    mock.assert_async().await;
    let notices = bodies.lock().unwrap();
    assert_eq!(notices.len(), 1);
    let notice = &notices[0];

    let ctx = &notice["request"]["context"];
    assert_eq!(ctx["user_id"], json!(7), "the scope's own context");
    assert_eq!(
        ctx["version"],
        json!("1.4.2"),
        "with the process-wide base merged beneath it"
    );
    assert_eq!(
        notice["correlation_context"]["request_id"],
        json!("req-facade")
    );

    let crumbs = notice["breadcrumbs"]["trail"].as_array().unwrap();
    assert_eq!(
        crumbs.len(),
        2,
        "the scope's own trail alone — the process-wide `booted` crumb is not merged"
    );
    assert_eq!(crumbs[0]["message"], json!("query ran"));
    assert_eq!(
        crumbs[1]["message"],
        json!("blocking work"),
        "the crumb a re-entered ScopeHandle recorded off-thread"
    );
}
