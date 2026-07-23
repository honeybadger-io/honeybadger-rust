//! Test fixture built with `panic = "abort"`: initializes Honeybadger against
//! HONEYBADGER_ENDPOINT and panics.
fn main() {
    let config = honeybadger::Config::builder()
        .env("fixture")
        .exclude_envs(Vec::<String>::new())
        .api_key("fixture-key")
        .build()
        .expect("config");
    let _guard = honeybadger::init(config).expect("init");
    honeybadger::add_breadcrumb("about to panic", "custom", None);
    panic!("abort fixture panicked on purpose");
}
