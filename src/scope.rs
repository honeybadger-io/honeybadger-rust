//! Ambient state: the four stores that answer "what was happening?".
//!
//! Today there is exactly one set of them per client, process-wide. Task 3 adds
//! a per-request overlay in front; this module is where both live so the
//! resolution rule has one home.
use crate::breadcrumbs::RingBuffer;
use serde_json::{Map, Value};
use std::sync::Mutex;

/// A client's process-global ambient state.
pub(crate) struct Scope {
    pub(crate) context: Mutex<Map<String, Value>>,
    pub(crate) breadcrumbs: Mutex<RingBuffer>,
    pub(crate) event_context: Mutex<Map<String, Value>>,
    pub(crate) request_id: Mutex<Option<String>>,
}

impl Scope {
    pub(crate) fn new() -> Self {
        Scope {
            context: Mutex::new(Map::new()),
            breadcrumbs: Mutex::new(RingBuffer::new()),
            event_context: Mutex::new(Map::new()),
            request_id: Mutex::new(None),
        }
    }
}
