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

    honeybadger::context([("user_id", json!(123))]);
    honeybadger::add_breadcrumb("loading config", "app", None);

    if let Err(e) = load_config() {
        honeybadger::notify(&e);
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
