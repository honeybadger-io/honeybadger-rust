//! Structural sanitization: key redaction, depth capping, string truncation.
//! Runs LAST in the notify pipeline so hook-introduced data is covered (spec).
use serde_json::Value;

pub(crate) const FILTERED: &str = "[FILTERED]";
pub(crate) const DEPTH_MARKER: &str = "[DEPTH]";
pub(crate) const TRUNCATED: &str = "[TRUNCATED]";
pub(crate) const MAX_DEPTH: usize = 20;
pub(crate) const MAX_STRING_BYTES: usize = 65_536;

pub(crate) struct Sanitizer {
    filter_keys: Vec<String>, // lowercased
}

impl Sanitizer {
    pub(crate) fn new<I, S>(filter_keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Sanitizer {
            filter_keys: filter_keys
                .into_iter()
                .map(|k| k.as_ref().to_lowercase())
                .collect(),
        }
    }

    pub(crate) fn sanitize(&self, value: &mut Value) {
        self.walk(value, MAX_DEPTH, true);
    }

    pub(crate) fn sanitize_shallow(&self, value: &mut Value) {
        self.walk(value, 1, true);
    }

    /// Depth capping, string truncation, and UTF-8 boundary safety **without**
    /// key redaction — the events pipeline's rule (spec decision 5). Every field
    /// in an event was written by hand, so silently replacing a legitimately
    /// named one would corrupt analytics with no error to show for it.
    pub(crate) fn sanitize_structural(&self, value: &mut Value) {
        self.walk(value, MAX_DEPTH, false);
    }

    fn walk(&self, value: &mut Value, depth_left: usize, redact: bool) {
        match value {
            Value::String(s) => truncate_string(s),
            Value::Object(map) => {
                for (key, val) in map.iter_mut() {
                    if redact && self.filter_keys.iter().any(|f| key.to_lowercase() == *f) {
                        *val = Value::String(FILTERED.into());
                    } else if depth_left <= 1 && (val.is_object() || val.is_array()) {
                        *val = Value::String(DEPTH_MARKER.into());
                    } else {
                        self.walk(val, depth_left - 1, redact);
                    }
                }
            }
            Value::Array(items) => {
                for val in items.iter_mut() {
                    if depth_left <= 1 && (val.is_object() || val.is_array()) {
                        *val = Value::String(DEPTH_MARKER.into());
                    } else {
                        self.walk(val, depth_left - 1, redact);
                    }
                }
            }
            _ => {}
        }
    }
}

pub(crate) fn truncate_string(s: &mut String) {
    if s.len() <= MAX_STRING_BYTES {
        return;
    }
    let mut cut = MAX_STRING_BYTES;
    while !s.is_char_boundary(cut) {
        cut -= 1;
    }
    s.truncate(cut);
    s.push_str(TRUNCATED);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sanitizer() -> Sanitizer {
        Sanitizer::new(["password", "credit_card", "secret"])
    }

    #[test]
    fn test_filters_keys_case_insensitively() {
        let mut v = json!({"PassWord": "hunter2", "user": {"secret": "x", "name": "ok"}});
        sanitizer().sanitize(&mut v);
        assert_eq!(
            v,
            json!({"PassWord": "[FILTERED]", "user": {"secret": "[FILTERED]", "name": "ok"}})
        );
    }

    #[test]
    fn test_depth_cap() {
        // Build a value nested deeper than MAX_DEPTH.
        let mut v = json!("leaf");
        for _ in 0..(MAX_DEPTH + 2) {
            v = json!({ "k": v });
        }
        sanitizer().sanitize(&mut v);
        let s = serde_json::to_string(&v).unwrap();
        assert!(s.contains(DEPTH_MARKER));
        assert!(!s.contains("leaf"));
    }

    #[test]
    fn test_shallow_depth_for_breadcrumb_metadata() {
        let mut v = json!({"a": {"b": 1}, "c": "keep"});
        sanitizer().sanitize_shallow(&mut v);
        assert_eq!(v, json!({"a": "[DEPTH]", "c": "keep"}));
    }

    #[test]
    fn test_structural_keeps_filtered_keys_but_still_caps_and_truncates() {
        let mut v = json!({
            "password": "hunter2",
            "msg": "y".repeat(MAX_STRING_BYTES + 10),
        });
        sanitizer().sanitize_structural(&mut v);
        assert_eq!(
            v["password"],
            json!("hunter2"),
            "events are not key-redacted"
        );
        assert!(v["msg"].as_str().unwrap().ends_with(TRUNCATED));

        let mut deep = json!("leaf");
        for _ in 0..(MAX_DEPTH + 2) {
            deep = json!({ "k": deep });
        }
        sanitizer().sanitize_structural(&mut deep);
        let s = serde_json::to_string(&deep).unwrap();
        assert!(s.contains(DEPTH_MARKER));
        assert!(!s.contains("leaf"));
    }

    #[test]
    fn test_truncates_long_strings_on_char_boundary() {
        // 'é' is 2 bytes; an odd byte limit boundary must not split it.
        let long = "é".repeat(MAX_STRING_BYTES); // 2 × MAX bytes
        let mut v = json!({ "msg": long });
        sanitizer().sanitize(&mut v);
        let out = v["msg"].as_str().unwrap();
        assert!(out.ends_with(TRUNCATED));
        assert!(out.len() <= MAX_STRING_BYTES + TRUNCATED.len());
        assert!(out.is_char_boundary(out.len() - TRUNCATED.len()));
    }
}
