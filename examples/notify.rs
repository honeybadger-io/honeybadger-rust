//! Reporting a handled error with context, breadcrumbs, and a custom notice.
use serde_json::json;

#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error("failed to load configuration")]
    ConfigLoad(#[from] std::io::Error),
}

fn main() {
    let _guard = honeybadger::init(
        honeybadger::Config::builder()
            .api_key("your-project-api-key")
            .env("production")
            .build()
            .expect("honeybadger config"),
    )
    .expect("honeybadger init");

    // Process-wide context: facts true of this whole process, not of one request.
    // In a concurrent server these are shared across every thread and task, so anything
    // request-shaped goes on the notice instead (below).
    honeybadger::context([("region", json!("us-east-1"))]);
    honeybadger::add_breadcrumb("loading config", "app", None);

    if let Err(e) = load_config() {
        // Request-scoped data travels with the notice, so a concurrent request cannot
        // overwrite it before delivery.
        honeybadger::notify_notice(
            honeybadger::Notice::from_error(&e)
                .context([("user_id", json!(123)), ("request_id", json!("req-9"))]),
        );
    }

    honeybadger::notify_notice(
        honeybadger::Notice::message("BillingAlert", "manual notice example")
            .tags(["billing"])
            .fingerprint("billing-alerts"),
    );
} // _guard drop flushes before exit

fn load_config() -> Result<(), AppError> {
    std::fs::read_to_string("/does/not/exist")?;
    Ok(())
}
