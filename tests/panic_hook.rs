//! End-to-end panic-hook tests against fixture processes and a mockito server.
use std::io::Read;
use std::process::Command;
use std::time::Duration;

fn spawn_fixture(cmd: &mut Command, endpoint: &str) -> std::process::Output {
    cmd.env("HONEYBADGER_ENDPOINT", endpoint)
        .env_remove("HONEYBADGER_ENV")
        .output()
        .expect("fixture ran")
}

/// Runs an `examples/` fixture, supplying the endpoint both as `HONEYBADGER_ENDPOINT`
/// and as argv[1] so fixtures can read it whichever way suits them.
fn run_fixture(name: &str, endpoint: &str) -> std::process::Output {
    spawn_fixture(
        Command::new(env!("CARGO")).args(["run", "--quiet", "--example", name, "--", endpoint]),
        endpoint,
    )
}

#[test]
fn test_unwind_panic_reports_before_exit() {
    let mut server = mockito::Server::new();
    let mock = server
        .mock("POST", "/v1/notices")
        .match_request(|req| {
            let mut body = String::new();
            flate2::read::ZlibDecoder::new(req.body().unwrap().as_slice())
                .read_to_string(&mut body)
                .unwrap();
            body.contains("\"class\":\"panic\"")
                && body.contains("fixture panicked on purpose")
                && body.contains("about to panic") // breadcrumb survived
        })
        .with_status(201)
        .expect(1)
        .create();

    let out = run_fixture("panic_fixture", &server.url());
    assert!(
        !out.status.success(),
        "fixture must exit nonzero after panic"
    );
    mock.assert();
}

#[test]
fn test_panicking_before_notify_hook_reports_no_panic_notice() {
    let mut server = mockito::Server::new();
    let mock = server
        .mock("POST", "/v1/notices")
        .with_status(201)
        .expect(1) // exactly one: the notice itself, never a panic notice
        .create();

    run_fixture("hook_panic_fixture", &server.url());

    mock.assert();
}

#[test]
fn test_abort_panic_reports_before_exit() {
    // `cargo package` strips tests/fixtures/abort_fixture: it carries its own Cargo.toml,
    // so cargo treats it as a separate package and leaves it out of the .crate archive.
    // Skip rather than fail when running against packaged sources.
    let manifest = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/abort_fixture/Cargo.toml"
    );
    if !std::path::Path::new(manifest).exists() {
        eprintln!("skipping: abort fixture absent (packaged sources)");
        return;
    }

    let mut server = mockito::Server::new();
    let mock = server
        .mock("POST", "/v1/notices")
        .match_request(|req| {
            let mut body = String::new();
            flate2::read::ZlibDecoder::new(req.body().unwrap().as_slice())
                .read_to_string(&mut body)
                .unwrap();
            body.contains("\"class\":\"panic\"")
        })
        .with_status(201)
        .expect(1)
        .create();

    let out = spawn_fixture(
        Command::new(env!("CARGO")).args(["run", "--quiet", "--manifest-path", manifest]),
        &server.url(),
    );
    assert!(!out.status.success());
    mock.assert();
}

#[test]
fn test_hook_recursion_and_chaining_do_not_abort_unwind_build() {
    // In-process: register via init, panic inside catch_unwind, assert we survive
    // and the previous hook still ran.
    use std::sync::atomic::{AtomicBool, Ordering};
    static PREV_RAN: AtomicBool = AtomicBool::new(false);
    std::panic::set_hook(Box::new(|_| PREV_RAN.store(true, Ordering::SeqCst)));

    let config = honeybadger::Config::builder()
        .env("fixture")
        .exclude_envs(Vec::<String>::new())
        .api_key("k")
        .endpoint("http://127.0.0.1:1") // urgent delivery fails fast; must not panic/loop
        .connect_timeout(Duration::from_millis(100))
        .request_timeout(Duration::from_millis(100))
        .build()
        .unwrap();
    let guard = honeybadger::init(config).unwrap();

    let result = std::panic::catch_unwind(|| panic!("in-process panic"));
    assert!(result.is_err());
    assert!(
        PREV_RAN.load(Ordering::SeqCst),
        "previous hook must still run"
    );
    drop(guard);
}
