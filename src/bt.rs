//! Backtrace capture and frame processing (spec "Backtraces" section).
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;

pub(crate) const MAX_FRAMES: usize = 1_000;
const SOURCE_RADIUS: u32 = 2;
const PROJECT_ROOT: &str = "[PROJECT_ROOT]";

const INTERNAL_PREFIXES: &[&str] = &[
    "honeybadger::",
    "backtrace::",
    "std::rt::",
    "std::panicking::",
    "std::panic::",
    "core::panicking::",
    "std::sys::",
    "rust_begin_unwind",
    "__libc_start_main",
    "__rust_try",
    "core::ops::function::FnOnce::call_once",
];

#[derive(Clone, Serialize)]
pub(crate) struct Frame {
    pub(crate) number: Option<u32>,
    pub(crate) file: Option<String>,
    pub(crate) method: Option<String>,
    pub(crate) source: Option<BTreeMap<String, String>>,
}

pub(crate) fn capture(root: &str) -> Vec<Frame> {
    let bt = backtrace::Backtrace::new(); // captures + resolves symbols
    process_resolved(&bt, root)
}

pub(crate) fn process_resolved(bt: &backtrace::Backtrace, root: &str) -> Vec<Frame> {
    let mut frames = Vec::new();
    for frame in bt.frames() {
        for symbol in frame.symbols() {
            let name = symbol.name().map(|n| n.to_string());
            if let Some(f) = map_frame(name.as_deref(), symbol.filename(), symbol.lineno(), root) {
                frames.push(f);
                if frames.len() == MAX_FRAMES {
                    return frames;
                }
            }
        }
    }
    frames
}

pub(crate) fn map_frame(
    symbol_name: Option<&str>,
    file: Option<&Path>,
    line: Option<u32>,
    root: &str,
) -> Option<Frame> {
    if let Some(name) = symbol_name {
        // Strip the trailing hash (`::h0123abcd`) before matching and reporting.
        let clean = name
            .rsplit_once("::h")
            .map(|(head, _)| head)
            .unwrap_or(name);
        if INTERNAL_PREFIXES.iter().any(|p| clean.starts_with(p)) {
            return None;
        }
        let in_root = file
            .map(|f| f.starts_with(root) && !root.is_empty())
            .unwrap_or(false);
        let source = match (in_root, file, line) {
            (true, Some(f), Some(n)) => read_excerpt(f, n),
            _ => None,
        };
        let file_str = file.map(|f| {
            let s = f.to_string_lossy().into_owned();
            if in_root {
                s.replacen(root, PROJECT_ROOT, 1)
            } else {
                s
            }
        });
        return Some(Frame {
            number: line,
            file: file_str,
            method: Some(clean.to_owned()),
            source,
        });
    }
    // Unresolvable frames are kept (address-only) so gaps are visible.
    Some(Frame {
        number: line,
        file: file.map(|f| f.to_string_lossy().into_owned()),
        method: None,
        source: None,
    })
}

fn read_excerpt(file: &Path, lineno: u32) -> Option<BTreeMap<String, String>> {
    let content = std::fs::read_to_string(file).ok()?;
    let start = lineno.saturating_sub(SOURCE_RADIUS).max(1);
    let mut out = BTreeMap::new();
    for (idx, line) in content.lines().enumerate() {
        let n = (idx + 1) as u32;
        if n >= start && n <= lineno + SOURCE_RADIUS {
            out.insert(n.to_string(), line.to_owned());
        }
    }
    if out.is_empty() { None } else { Some(out) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_internal_frames_dropped() {
        for name in [
            "honeybadger::client::Client::notify",
            "backtrace::backtrace::trace",
            "std::rt::lang_start",
            "std::panicking::try",
            "core::panicking::panic_fmt",
            "__libc_start_main",
        ] {
            assert!(
                map_frame(Some(name), None, None, "/app").is_none(),
                "{name} should be dropped"
            );
        }
    }

    #[test]
    fn test_app_frame_mapped_with_project_root_substitution() {
        let f = map_frame(
            Some("my_app::checkout::charge"),
            Some(Path::new("/app/src/checkout.rs")),
            Some(42),
            "/app",
        )
        .unwrap();
        assert_eq!(f.method.as_deref(), Some("my_app::checkout::charge"));
        assert_eq!(f.file.as_deref(), Some("[PROJECT_ROOT]/src/checkout.rs"));
        assert_eq!(f.number, Some(42));
    }

    #[test]
    fn test_non_root_file_not_substituted_and_no_source() {
        let f = map_frame(
            Some("dep::thing"),
            Some(Path::new("/cargo/registry/dep/lib.rs")),
            Some(7),
            "/app",
        )
        .unwrap();
        assert_eq!(f.file.as_deref(), Some("/cargo/registry/dep/lib.rs"));
        assert!(f.source.is_none());
    }

    #[test]
    fn test_source_excerpt_only_under_root() {
        // Use this very repository as the "project root" and this very file as the frame file.
        let root = env!("CARGO_MANIFEST_DIR");
        let file = Path::new(root).join("src/bt.rs");
        let f = map_frame(Some("honeybadger_test::x"), Some(&file), Some(3), root).unwrap();
        let source = f.source.expect("source excerpt expected for in-root file");
        assert!(source.contains_key("3"));
        assert!(source.len() <= 5); // lineno ± 2
    }

    #[test]
    fn test_capture_returns_frames_capped() {
        let frames = capture(env!("CARGO_MANIFEST_DIR"));
        assert!(frames.len() <= MAX_FRAMES);
        // The capture helper itself must not appear (it's under honeybadger::).
        assert!(frames.iter().all(|f| {
            f.method
                .as_deref()
                .map(|m| !m.starts_with("honeybadger::bt"))
                .unwrap_or(true)
        }));
    }
}
