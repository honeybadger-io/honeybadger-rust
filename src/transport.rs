//! Transport: the HTTP seam (spec "Transport"). Bodies arrive pre-compressed.
use flate2::Compression;
use flate2::write::ZlibEncoder;
use std::io::Write;
use std::sync::Mutex;
use std::time::Duration;

pub(crate) const NOTICES_PATH: &str = "/v1/notices";
pub(crate) const EVENTS_PATH: &str = "/v1/events";

/// Which Honeybadger API a request targets.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestKind {
    /// The error-reporting API, `POST /v1/notices`.
    Notices,
    /// The Insights events API, `POST /v1/events`.
    Events,
}

/// One outbound request. Construct via [`TransportRequest::notices`].
#[non_exhaustive]
pub struct TransportRequest<'a> {
    /// Which API this request targets.
    pub kind: RequestKind,
    /// Path to append to the configured endpoint.
    pub path: &'a str,
    /// MIME type of the body, before compression.
    pub content_type: &'a str,
    /// Request body, already zlib-deflated. Send it with `Content-Encoding: deflate`.
    pub body: &'a [u8],
    /// Set for panic notices, delivered synchronously while the process is dying.
    /// One shot: don't retry, and bound the wait — the caller is on its way out.
    pub urgent: bool,
}

impl<'a> TransportRequest<'a> {
    /// Builds a request against the notices API.
    pub fn notices(body: &'a [u8], urgent: bool) -> Self {
        TransportRequest {
            kind: RequestKind::Notices,
            path: NOTICES_PATH,
            content_type: "application/json",
            body,
            urgent,
        }
    }

    /// Builds a request against the Insights events API. The body is a batch of
    /// newline-delimited JSON objects, deflated like every other request.
    pub fn events(body: &'a [u8]) -> Self {
        TransportRequest {
            kind: RequestKind::Events,
            path: EVENTS_PATH,
            content_type: "application/x-ndjson",
            body,
            urgent: false,
        }
    }
}

/// Longest `Retry-After` we will honor. The real ceiling is the daily data
/// limit, which resets at most 24 hours out; anything beyond that is a bug or a
/// hostile proxy, and obeying it would park a pipeline indefinitely.
pub(crate) const MAX_RETRY_AFTER: Duration = Duration::from_secs(86_400);

/// Reads a `Retry-After` value, honoring only the delta-seconds form.
///
/// The HTTP-date form is equally legal, but the Honeybadger API does not send
/// it, and misreading a date as a second count would be worse than falling back
/// to the SDK's own backoff curve. Clamped to [`MAX_RETRY_AFTER`].
pub(crate) fn parse_retry_after(header: Option<&str>) -> Option<Duration> {
    let seconds: u64 = header?.trim().parse().ok()?;
    Some(MAX_RETRY_AFTER.min(Duration::from_secs(seconds)))
}

/// One response from the API.
///
/// Carries the status plus the parts of the response the delivery workers act
/// on. Build it with [`TransportResponse::new`], or `status.into()` when there
/// is nothing else to report.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransportResponse {
    /// HTTP status code.
    pub status: u16,
    /// The response's `Retry-After` as a duration, when it sent a parseable one.
    /// A worker backing off prefers this over its own curve, so a rate-limited
    /// endpoint is retried when it said to be rather than on our schedule.
    pub retry_after: Option<Duration>,
}

impl TransportResponse {
    /// A response carrying only a status.
    pub fn new(status: u16) -> Self {
        TransportResponse {
            status,
            retry_after: None,
        }
    }

    /// Attaches the `Retry-After` this response carried.
    pub fn retry_after(mut self, after: Option<Duration>) -> Self {
        self.retry_after = after;
        self
    }
}

impl From<u16> for TransportResponse {
    fn from(status: u16) -> Self {
        TransportResponse::new(status)
    }
}

/// A delivery failure: connection refused, timeout, TLS error, or a panicking
/// [`Transport`] impl. A non-2xx *response* is not an error — it comes back as
/// `Ok(response)` so the worker can pick the right backoff.
#[derive(Debug)]
pub struct TransportError(
    /// Human-readable description of what went wrong.
    pub String,
);

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "transport error: {}", self.0)
    }
}
impl std::error::Error for TransportError {}

/// The delivery seam. Implement this to intercept or fake HTTP delivery.
pub trait Transport: Send + Sync {
    /// Delivers one request, returning what the server answered.
    ///
    /// Return `Ok(response)` for *any* response the server produced, including 4xx and
    /// 5xx: the worker reads 401/402/403 as "suspend", 429/503 as "throttle", and
    /// anything else unexpected as a dropped notice. Reserve `Err` for requests that
    /// never got a response at all.
    ///
    /// A status alone converts with `Ok(201.into())`. Report `Retry-After` when the
    /// response carried one — see [`TransportResponse::retry_after`].
    ///
    /// Implementations must not block indefinitely — on the urgent path the caller is a
    /// process on its way out. A panicking implementation is caught and counted as a
    /// transport error rather than taking the worker down — though only in unwinding
    /// builds; under `panic = "abort"` no `catch_unwind` in any crate can help.
    fn deliver(&self, req: &TransportRequest) -> Result<TransportResponse, TransportError>;
}

pub(crate) fn compress(body: &[u8]) -> Vec<u8> {
    let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
    // Writing to a Vec cannot fail; fall back to uncompressed on the impossible path.
    if enc.write_all(body).is_err() {
        return body.to_vec();
    }
    enc.finish().unwrap_or_else(|_| body.to_vec())
}

pub(crate) fn user_agent() -> String {
    format!("Honeybadger Rust {}", env!("CARGO_PKG_VERSION"))
}

// ---------- Server ----------

pub(crate) struct ServerTransport {
    endpoint: String,
    api_key: String,
    agent: ureq::Agent,
    urgent_agent: ureq::Agent,
    user_agent: String,
}

/// Timeouts for the panic path, derived from the configured ones.
///
/// The urgent path gets one shot with no retry behind it, on the highest-value
/// notice a process sends, so it is not where to economize — by default it gets
/// the whole normal budget. Two bounds apply. It never waits longer than a normal
/// request was allowed, because a caller who tightened `request_timeout` did not
/// ask for a longer hang on panic than on a healthy send. And it never waits
/// longer than five seconds, because background delivery may take thirty and a
/// crash may not: the process is on its way out and something is likely waiting
/// to restart it.
///
/// Returns `(connect, total)`. Connect is clamped to the total it must fit inside.
fn urgent_budget(connect: Duration, request: Duration) -> (Duration, Duration) {
    let total = request.min(URGENT_MAX_TOTAL);
    (connect.min(total), total)
}

/// Ceiling on how long a panic may hang waiting for its notice to land.
const URGENT_MAX_TOTAL: Duration = Duration::from_secs(5);

fn build_agent(connect: Duration, total: Duration) -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_connect(Some(connect))
        .timeout_global(Some(total))
        .http_status_as_error(false) // every status returns Ok(response)
        .max_redirects(0)
        .build()
        .new_agent()
}

impl ServerTransport {
    pub(crate) fn new(
        endpoint: String,
        api_key: String,
        connect: Duration,
        request: Duration,
    ) -> Self {
        ServerTransport {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            api_key,
            agent: build_agent(connect, request),
            urgent_agent: {
                let (urgent_connect, urgent_total) = urgent_budget(connect, request);
                build_agent(urgent_connect, urgent_total)
            },
            user_agent: user_agent(),
        }
    }
}

impl Transport for ServerTransport {
    fn deliver(&self, req: &TransportRequest) -> Result<TransportResponse, TransportError> {
        let url = format!("{}{}", self.endpoint, req.path);
        let agent = if req.urgent {
            &self.urgent_agent
        } else {
            &self.agent
        };
        agent
            .post(&url)
            .header("X-API-Key", &self.api_key)
            .header("Content-Type", req.content_type)
            .header("Accept", "application/json")
            .header("Content-Encoding", "deflate")
            .header("User-Agent", &self.user_agent)
            .send(req.body)
            .map(|resp| {
                let retry_after = parse_retry_after(
                    resp.headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok()),
                );
                TransportResponse::new(resp.status().as_u16()).retry_after(retry_after)
            })
            .map_err(|e| TransportError(e.to_string()))
    }
}

// ---------- Null ----------

pub(crate) struct NullTransport;

impl Transport for NullTransport {
    fn deliver(&self, req: &TransportRequest) -> Result<TransportResponse, TransportError> {
        log::debug!(
            "honeybadger: reporting disabled; dropping {} bytes to {}",
            req.body.len(),
            req.path
        );
        Ok(201.into())
    }
}

// ---------- Test ----------

/// A request captured by [`TestTransport`].
#[non_exhaustive]
pub struct CapturedRequest {
    /// Which API the request targeted.
    pub kind: RequestKind,
    /// Path the request was sent to.
    pub path: String,
    /// Content type of the body.
    pub content_type: String,
    /// The compressed body as delivered; inflate it to inspect the JSON.
    pub body: Vec<u8>,
    /// Whether the request took the urgent (panic) path.
    pub urgent: bool,
}

/// An in-memory [`Transport`] for tests: records requests, replays programmed statuses.
#[derive(Default)]
pub struct TestTransport {
    requests: Mutex<Vec<CapturedRequest>>,
    responses: Mutex<Vec<TransportResponse>>,
}

impl TestTransport {
    /// Creates an empty test transport.
    pub fn new() -> Self {
        TestTransport::default()
    }

    /// Queue a status for the next delivery (FIFO). Unqueued deliveries return 201.
    pub fn respond_with(&self, status: u16) {
        self.respond_with_response(status.into());
    }

    /// Queue a status carrying a `Retry-After`, for exercising a worker's backoff.
    pub fn respond_with_retry_after(&self, status: u16, after: Duration) {
        self.respond_with_response(TransportResponse::new(status).retry_after(Some(after)));
    }

    /// Queue a fully built response for the next delivery (FIFO).
    pub fn respond_with_response(&self, response: TransportResponse) {
        self.responses
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(response);
    }

    /// Snapshot of every request delivered so far, oldest first.
    pub fn requests(&self) -> Vec<CapturedRequest> {
        let lock = self.requests.lock().unwrap_or_else(|e| e.into_inner());
        lock.iter()
            .map(|r| CapturedRequest {
                kind: r.kind,
                path: r.path.clone(),
                content_type: r.content_type.clone(),
                body: r.body.clone(),
                urgent: r.urgent,
            })
            .collect()
    }
}

impl Transport for TestTransport {
    fn deliver(&self, req: &TransportRequest) -> Result<TransportResponse, TransportError> {
        self.requests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(CapturedRequest {
                kind: req.kind,
                path: req.path.to_owned(),
                content_type: req.content_type.to_owned(),
                body: req.body.to_vec(),
                urgent: req.urgent,
            });
        let mut responses = self.responses.lock().unwrap_or_else(|e| e.into_inner());
        Ok(if responses.is_empty() {
            201.into()
        } else {
            responses.remove(0)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn inflate(body: &[u8]) -> String {
        let mut out = String::new();
        flate2::read::ZlibDecoder::new(body)
            .read_to_string(&mut out)
            .unwrap();
        out
    }

    #[test]
    fn test_compress_round_trip() {
        let compressed = compress(b"{\"a\":1}");
        assert_eq!(inflate(&compressed), "{\"a\":1}");
    }

    #[test]
    fn test_test_transport_captures_and_responds() {
        let t = TestTransport::new();
        t.respond_with(429);
        let body = compress(b"{}");
        let req = TransportRequest::notices(&body, false);
        assert_eq!(t.deliver(&req).unwrap().status, 429);
        assert_eq!(t.deliver(&req).unwrap().status, 201); // programmed consumed; default 201
        let captured = t.requests();
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].path, "/v1/notices");
        assert_eq!(inflate(&captured[0].body), "{}");
    }

    #[test]
    fn test_server_transport_headers_and_deflate() {
        let mut server = mockito::Server::new();
        let mock = server
            .mock("POST", "/v1/notices")
            .match_header("X-API-Key", "test-key")
            .match_header("Content-Type", "application/json")
            .match_header("Content-Encoding", "deflate")
            .match_header("User-Agent", user_agent().as_str())
            .with_status(201)
            .create();
        let t = ServerTransport::new(
            server.url(),
            "test-key".into(),
            std::time::Duration::from_secs(2),
            std::time::Duration::from_secs(5),
        );
        let body = compress(b"{\"api\":\"payload\"}");
        let status = t
            .deliver(&TransportRequest::notices(&body, false))
            .unwrap()
            .status;
        assert_eq!(status, 201);
        mock.assert();
    }

    #[test]
    fn test_server_transport_does_not_follow_redirects() {
        for urgent in [false, true] {
            for status in [301u16, 302, 303, 307, 308] {
                let mut origin = mockito::Server::new();
                let mut destination = mockito::Server::new();
                let observed = std::sync::Arc::new(Mutex::new(Vec::new()));
                let observed_by_get = std::sync::Arc::clone(&observed);
                let redirect = origin
                    .mock("POST", "/v1/notices")
                    .with_status(usize::from(status))
                    .with_header("Location", &format!("{}/stolen", destination.url()))
                    .create();
                let forwarded_get = destination
                    .mock("GET", "/stolen")
                    .match_body(mockito::Matcher::Any)
                    .match_request(move |request| {
                        observed_by_get.lock().unwrap().push((
                            request.method().to_owned(),
                            request
                                .header("X-API-Key")
                                .first()
                                .and_then(|value| value.to_str().ok())
                                .map(str::to_owned),
                            request.body().unwrap().clone(),
                        ));
                        true
                    })
                    .with_status(201)
                    .expect(0)
                    .create();
                let forwarded_post = destination
                    .mock("POST", "/stolen")
                    .match_body(mockito::Matcher::Any)
                    .with_status(201)
                    .expect(0)
                    .create();

                let t = ServerTransport::new(
                    origin.url(),
                    "test-key".into(),
                    Duration::from_secs(2),
                    Duration::from_secs(5),
                );
                let result = t.deliver(&TransportRequest::notices(&compress(b"{}"), urgent));

                assert_eq!(result.unwrap().status, status);
                assert!(observed.lock().unwrap().is_empty());
                forwarded_get.assert();
                forwarded_post.assert();
                redirect.assert();
            }
        }
    }

    #[test]
    fn test_server_transport_non_2xx_is_ok_status_not_err() {
        let mut server = mockito::Server::new();
        server.mock("POST", "/v1/notices").with_status(429).create();
        let t = ServerTransport::new(
            server.url(),
            "k".into(),
            std::time::Duration::from_secs(2),
            std::time::Duration::from_secs(5),
        );
        let body = compress(b"{}");
        assert_eq!(
            t.deliver(&TransportRequest::notices(&body, false))
                .unwrap()
                .status,
            429
        );
    }

    #[test]
    fn test_events_request_shape() {
        let body = compress(b"{\"a\":1}\n{\"b\":2}");
        let req = TransportRequest::events(&body);
        assert_eq!(req.kind, RequestKind::Events);
        assert_eq!(req.path, "/v1/events");
        assert_eq!(req.content_type, "application/x-ndjson");
        assert!(!req.urgent, "events are never delivered on the urgent path");
    }

    #[test]
    fn test_test_transport_records_kind() {
        let t = TestTransport::new();
        let body = compress(b"{}");
        t.deliver(&TransportRequest::notices(&body, false)).unwrap();
        t.deliver(&TransportRequest::events(&body)).unwrap();
        let kinds: Vec<RequestKind> = t.requests().iter().map(|r| r.kind).collect();
        assert_eq!(kinds, vec![RequestKind::Notices, RequestKind::Events]);
    }

    #[test]
    fn test_server_transport_posts_ndjson_to_events() {
        let mut server = mockito::Server::new();
        let mock = server
            .mock("POST", "/v1/events")
            .match_header("X-API-Key", "test-key")
            .match_header("Content-Type", "application/x-ndjson")
            .match_header("Content-Encoding", "deflate")
            .with_status(201)
            .create();
        let t = ServerTransport::new(
            server.url(),
            "test-key".into(),
            std::time::Duration::from_secs(2),
            std::time::Duration::from_secs(5),
        );
        let body = compress(b"{\"event_type\":\"x\"}");
        assert_eq!(
            t.deliver(&TransportRequest::events(&body)).unwrap().status,
            201
        );
        mock.assert();
    }

    #[test]
    fn test_urgent_budget_is_bounded_by_the_normal_request_and_by_five_seconds() {
        // Default config: the panic path gets the whole normal budget. It is the
        // highest-value notice a process sends and the only one with no retry
        // behind it, so it is not the place to economize.
        assert_eq!(
            urgent_budget(Duration::from_secs(4), Duration::from_secs(5)),
            (Duration::from_secs(4), Duration::from_secs(5))
        );
        // A loosened request timeout is capped: background delivery may take 30s,
        // a crash may not hang for 30s.
        assert_eq!(
            urgent_budget(Duration::from_secs(10), Duration::from_secs(30)),
            (Duration::from_secs(5), Duration::from_secs(5)),
            "connect is clamped to the total it has to fit inside"
        );
        // A deliberate tightening is deference, not something to second-guess
        // upward: a caller who allowed 500ms for a normal request did not ask for
        // a 5s hang on panic.
        assert_eq!(
            urgent_budget(Duration::from_millis(300), Duration::from_millis(500)),
            (Duration::from_millis(300), Duration::from_millis(500))
        );
    }

    #[test]
    fn test_a_tightened_request_timeout_shortens_the_urgent_path() {
        // The urgent budget was a hardcoded 2s, so a caller who tightened
        // request_timeout to 300ms still got a 2s hang on panic.
        //
        // A socket that accepts and then says nothing: the connect succeeds, so
        // what this measures is the global budget rather than the connect timeout.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            if let Ok((mut sock, _)) = listener.accept() {
                let _ = std::io::Read::read(&mut sock, &mut [0u8; 1024]);
                // Must outlast the old hardcoded 2s budget: if the socket closed
                // sooner, the client would fail on connection-closed rather than
                // on its timeout, and the test would pass either way.
                std::thread::sleep(Duration::from_secs(3));
            }
        });

        let t = ServerTransport::new(
            format!("http://{addr}"),
            "k".into(),
            Duration::from_millis(500),
            Duration::from_millis(300),
        );
        let body = compress(b"{}");
        let started = std::time::Instant::now();
        assert!(
            t.deliver(&TransportRequest::notices(&body, true)).is_err(),
            "a silent server must exhaust the urgent budget"
        );
        assert!(
            started.elapsed() < Duration::from_millis(1200),
            "the urgent path must honor the 300ms request timeout, not the old fixed 2s (took {:?})",
            started.elapsed()
        );
    }

    #[test]
    fn test_retry_after_parses_seconds_and_ignores_the_rest() {
        assert_eq!(
            parse_retry_after(Some("120")),
            Some(Duration::from_secs(120))
        );
        assert_eq!(
            parse_retry_after(Some(" 30 ")),
            Some(Duration::from_secs(30))
        );
        assert_eq!(parse_retry_after(None), None);
        // The HTTP-date form is legal but our API never sends it. Reading it as
        // seconds would be worse than falling back to our own curve.
        assert_eq!(
            parse_retry_after(Some("Wed, 21 Oct 2026 07:28:00 GMT")),
            None
        );
        assert_eq!(parse_retry_after(Some("-5")), None, "negative is nonsense");
        assert_eq!(
            parse_retry_after(Some("99999999")),
            Some(MAX_RETRY_AFTER),
            "a server that says 'wait a year' must not park the pipeline for one"
        );
    }

    #[test]
    fn test_server_transport_surfaces_retry_after() {
        // The events endpoint sends Retry-After with its 429 — seconds until the
        // daily limit resets. The worker cannot honor a header it never sees.
        let mut server = mockito::Server::new();
        server
            .mock("POST", "/v1/events")
            .with_status(429)
            .with_header("Retry-After", "120")
            .create();
        let t = ServerTransport::new(
            server.url(),
            "k".into(),
            Duration::from_secs(2),
            Duration::from_secs(5),
        );
        let body = compress(b"{}");
        let resp = t.deliver(&TransportRequest::events(&body)).unwrap();
        assert_eq!(resp.status, 429);
        assert_eq!(resp.retry_after, Some(Duration::from_secs(120)));
    }

    #[test]
    fn test_a_response_without_the_header_reports_none() {
        let mut server = mockito::Server::new();
        server.mock("POST", "/v1/events").with_status(201).create();
        let t = ServerTransport::new(
            server.url(),
            "k".into(),
            Duration::from_secs(2),
            Duration::from_secs(5),
        );
        let body = compress(b"{}");
        let resp = t.deliver(&TransportRequest::events(&body)).unwrap();
        assert_eq!(resp.status, 201);
        assert_eq!(resp.retry_after, None);
    }

    #[test]
    fn test_server_transport_connection_refused_is_err() {
        // Port 1 on localhost: nothing listens there.
        let t = ServerTransport::new(
            "http://127.0.0.1:1".into(),
            "k".into(),
            std::time::Duration::from_millis(200),
            std::time::Duration::from_millis(200),
        );
        let body = compress(b"{}");
        assert!(t.deliver(&TransportRequest::notices(&body, false)).is_err());
    }
}
