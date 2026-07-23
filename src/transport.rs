//! Transport: the HTTP seam (spec "Transport"). Bodies arrive pre-compressed.
use flate2::Compression;
use flate2::write::ZlibEncoder;
use std::io::Write;
use std::sync::Mutex;
use std::time::Duration;

pub(crate) const NOTICES_PATH: &str = "/v1/notices";

/// Which Honeybadger API a request targets.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestKind {
    Notices,
}

/// One outbound request. Construct via [`TransportRequest::notices`].
#[non_exhaustive]
pub struct TransportRequest<'a> {
    pub kind: RequestKind,
    pub path: &'a str,
    pub content_type: &'a str,
    pub body: &'a [u8],
    pub urgent: bool,
}

impl<'a> TransportRequest<'a> {
    pub fn notices(body: &'a [u8], urgent: bool) -> Self {
        TransportRequest {
            kind: RequestKind::Notices,
            path: NOTICES_PATH,
            content_type: "application/json",
            body,
            urgent,
        }
    }
}

#[derive(Debug)]
pub struct TransportError(pub String);

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "transport error: {}", self.0)
    }
}
impl std::error::Error for TransportError {}

/// The delivery seam. Implement this to intercept or fake HTTP delivery.
pub trait Transport: Send + Sync {
    fn deliver(&self, req: &TransportRequest) -> Result<u16, TransportError>;
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

fn build_agent(connect: Duration, total: Duration) -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_connect(Some(connect))
        .timeout_global(Some(total))
        .http_status_as_error(false) // every status returns Ok(response)
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
            urgent_agent: build_agent(Duration::from_secs(1), Duration::from_secs(2)),
            user_agent: user_agent(),
        }
    }
}

impl Transport for ServerTransport {
    fn deliver(&self, req: &TransportRequest) -> Result<u16, TransportError> {
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
            .map(|resp| resp.status().as_u16())
            .map_err(|e| TransportError(e.to_string()))
    }
}

// ---------- Null ----------

pub(crate) struct NullTransport;

impl Transport for NullTransport {
    fn deliver(&self, req: &TransportRequest) -> Result<u16, TransportError> {
        log::debug!(
            "honeybadger: reporting disabled; dropping {} bytes to {}",
            req.body.len(),
            req.path
        );
        Ok(201)
    }
}

// ---------- Test ----------

/// A request captured by [`TestTransport`].
pub struct CapturedRequest {
    pub path: String,
    pub content_type: String,
    pub body: Vec<u8>,
    pub urgent: bool,
}

/// An in-memory [`Transport`] for tests: records requests, replays programmed statuses.
#[derive(Default)]
pub struct TestTransport {
    requests: Mutex<Vec<CapturedRequest>>,
    responses: Mutex<Vec<u16>>,
}

impl TestTransport {
    pub fn new() -> Self {
        TestTransport::default()
    }

    /// Queue a status for the next delivery (FIFO). Unqueued deliveries return 201.
    pub fn respond_with(&self, status: u16) {
        self.responses
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(status);
    }

    pub fn requests(&self) -> Vec<CapturedRequest> {
        let lock = self.requests.lock().unwrap_or_else(|e| e.into_inner());
        lock.iter()
            .map(|r| CapturedRequest {
                path: r.path.clone(),
                content_type: r.content_type.clone(),
                body: r.body.clone(),
                urgent: r.urgent,
            })
            .collect()
    }
}

impl Transport for TestTransport {
    fn deliver(&self, req: &TransportRequest) -> Result<u16, TransportError> {
        self.requests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(CapturedRequest {
                path: req.path.to_owned(),
                content_type: req.content_type.to_owned(),
                body: req.body.to_vec(),
                urgent: req.urgent,
            });
        let mut responses = self.responses.lock().unwrap_or_else(|e| e.into_inner());
        Ok(if responses.is_empty() {
            201
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
        assert_eq!(t.deliver(&req).unwrap(), 429);
        assert_eq!(t.deliver(&req).unwrap(), 201); // programmed statuses consumed; default 201
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
        let status = t.deliver(&TransportRequest::notices(&body, false)).unwrap();
        assert_eq!(status, 201);
        mock.assert();
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
            t.deliver(&TransportRequest::notices(&body, false)).unwrap(),
            429
        );
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
