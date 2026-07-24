//! End-to-end coverage for the events pipeline against a real HTTP server.
use serde_json::json;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[test]
fn test_events_post_ndjson_batches_to_the_events_endpoint() {
    let mut server = mockito::Server::new();
    let mock = server
        .mock("POST", "/v1/events")
        .match_header("content-type", "application/x-ndjson")
        .match_header("content-encoding", "deflate")
        .with_status(201)
        .expect(1)
        .create();

    let client = honeybadger::Client::new(
        honeybadger::Config::builder()
            .env_source(|_| None)
            .api_key("test-key")
            .env("production")
            .endpoint(server.url())
            .events_batch_size(1000)
            .build()
            .unwrap(),
    )
    .unwrap();

    for i in 0..25 {
        client.event("bulk.tick", json!({ "n": i }));
    }
    assert!(client.flush(Duration::from_secs(10)));
    client.shutdown(Duration::from_secs(10));

    mock.assert();
}

#[test]
fn test_batch_body_is_one_json_object_per_line() {
    // mockito 1.7 has no received_requests(), so capture the body as it arrives.
    let captured: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let sink = captured.clone();

    let mut server = mockito::Server::new();
    let mock = server
        .mock("POST", "/v1/events")
        .match_request(move |req| {
            *sink.lock().unwrap() = req.body().ok().cloned();
            true
        })
        .with_status(201)
        .expect(1)
        .create();

    let client = honeybadger::Client::new(
        honeybadger::Config::builder()
            .env_source(|_| None)
            .api_key("test-key")
            .env("production")
            .endpoint(server.url())
            .build()
            .unwrap(),
    )
    .unwrap();
    for i in 0..3 {
        client.event("line.check", json!({ "n": i }));
    }
    assert!(client.flush(Duration::from_secs(10)));
    client.shutdown(Duration::from_secs(10));

    mock.assert();
    // The captured body is deflated NDJSON: inflate it and parse every line.
    let body = captured.lock().unwrap().clone().expect("an events request");
    let mut ndjson = String::new();
    flate2::read::ZlibDecoder::new(&body[..])
        .read_to_string(&mut ndjson)
        .unwrap();
    let lines: Vec<&str> = ndjson.lines().collect();
    assert_eq!(lines.len(), 3);
    for line in lines {
        let value: serde_json::Value = serde_json::from_str(line).expect("each line parses");
        assert_eq!(value["event_type"], json!("line.check"));
        assert!(value["ts"].is_string());
    }
}
