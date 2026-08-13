//! Request-scoped context. Run with: cargo run --features tokio --example scoped_request
use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // `development` is in the default `exclude_envs`, so this runs the whole
    // assembly pipeline against the null transport and sends nothing.
    let config = honeybadger::Config::builder()
        .env("development")
        .api_key("example-key")
        .build()
        .expect("config");
    let _guard = honeybadger::init(config).expect("init");

    // Process-wide, set once at boot: every notice gets these.
    honeybadger::context([("version", json!("1.4.2"))]);

    let mut requests = Vec::new();
    for id in 0..3 {
        requests.push(tokio::spawn(honeybadger::scope(async move {
            honeybadger::request_id(format!("req-{id}"));
            honeybadger::context([("user_id", json!(id))]);
            honeybadger::add_breadcrumb("query ran", "query", None);

            // Crossing a thread boundary needs the scope carried explicitly.
            // `enter`/`enter_sync` consume the handle, so the spawned work owns
            // it; clone the handle when you need it more than once.
            let scope = honeybadger::ScopeHandle::current();
            tokio::task::spawn_blocking({
                let scope = scope.clone();
                move || {
                    scope.enter_sync(|| {
                        honeybadger::add_breadcrumb("blocking work", "custom", None);
                    })
                }
            })
            .await
            .expect("blocking task");

            // The async form goes straight to tokio::spawn — no wrapper needed.
            tokio::spawn(scope.enter(async {
                honeybadger::add_breadcrumb("cache warmed", "custom", None);
            }))
            .await
            .expect("spawned task");

            honeybadger::notify_notice(honeybadger::Notice::message(
                "Example",
                &format!("from request {id}"),
            ));
        })));
    }
    for r in requests {
        r.await.expect("request");
    }

    println!("three requests reported, each with only its own three breadcrumbs");
}
