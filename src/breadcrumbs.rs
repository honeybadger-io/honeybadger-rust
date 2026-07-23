//! Breadcrumbs: a 40-entry ring buffer serialized into every notice.
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::VecDeque;

const CAPACITY: usize = 40;

/// A single breadcrumb: a timestamped note about what the app was doing.
#[derive(Clone, Serialize)]
pub struct Breadcrumb {
    pub(crate) message: String,
    pub(crate) category: String,
    pub(crate) metadata: Map<String, Value>,
    pub(crate) timestamp: String,
}

impl Breadcrumb {
    /// Builds a breadcrumb stamped with the current UTC time.
    ///
    /// `category` groups related crumbs in the Honeybadger UI (`"query"`, `"ui"`,
    /// `"custom"`, …). `metadata` is sanitized one level deep before delivery: nested
    /// objects and arrays are replaced with `"[DEPTH]"`, so keep it flat.
    pub fn new(message: &str, category: &str, metadata: Option<Map<String, Value>>) -> Self {
        Self::with_timestamp(message, category, metadata, now_iso8601_ms())
    }

    pub(crate) fn with_timestamp(
        message: &str,
        category: &str,
        metadata: Option<Map<String, Value>>,
        timestamp: String,
    ) -> Self {
        Breadcrumb {
            message: message.to_owned(),
            category: category.to_owned(),
            metadata: metadata.unwrap_or_default(),
            timestamp,
        }
    }
}

pub(crate) struct RingBuffer {
    buf: VecDeque<Breadcrumb>,
}

impl RingBuffer {
    pub(crate) fn new() -> Self {
        RingBuffer {
            buf: VecDeque::with_capacity(CAPACITY),
        }
    }

    pub(crate) fn push(&mut self, crumb: Breadcrumb) {
        if self.buf.len() == CAPACITY {
            self.buf.pop_front();
        }
        self.buf.push_back(crumb);
    }

    pub(crate) fn snapshot(&self) -> Vec<Breadcrumb> {
        self.buf.iter().cloned().collect()
    }

    pub(crate) fn clear(&mut self) {
        self.buf.clear();
    }
}

/// UTC timestamp like `2026-07-22T21:03:04.123Z` (ISO8601, millisecond precision).
pub(crate) fn now_iso8601_ms() -> String {
    let ts = jiff::Timestamp::now();
    ts.strftime("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_buffer_drops_oldest_beyond_capacity() {
        let mut buf = RingBuffer::new();
        for i in 0..45 {
            buf.push(Breadcrumb::new(&format!("crumb {i}"), "custom", None));
        }
        let snap = buf.snapshot();
        assert_eq!(snap.len(), 40);
        assert_eq!(snap.first().unwrap().message, "crumb 5");
        assert_eq!(snap.last().unwrap().message, "crumb 44");
    }

    #[test]
    fn test_breadcrumb_serialization_shape() {
        let mut meta = serde_json::Map::new();
        meta.insert("sql".into(), serde_json::json!("SELECT 1"));
        let crumb = Breadcrumb::with_timestamp(
            "query ran",
            "query",
            Some(meta),
            "2026-07-22T00:00:00.000Z".into(),
        );
        let v = serde_json::to_value(&crumb).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "message": "query ran",
                "category": "query",
                "metadata": {"sql": "SELECT 1"},
                "timestamp": "2026-07-22T00:00:00.000Z"
            })
        );
    }

    #[test]
    fn test_timestamp_is_iso8601_utc_ms() {
        let ts = now_iso8601_ms();
        // e.g. 2026-07-22T21:03:04.123Z
        assert!(ts.ends_with('Z'));
        assert_eq!(ts.len(), "2026-07-22T21:03:04.123Z".len());
    }
}
