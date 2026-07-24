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
        let in_root = file.map(|f| lexically_under_root(f, root)).unwrap_or(false);
        let source = match (in_root, file, line) {
            // Lexical containment decides the display path, but reading a file needs the
            // stronger check: a symlink under the root can resolve outside it.
            (true, Some(f), Some(n)) if resolves_under_root(f, root) => read_excerpt(f, n),
            _ => None,
        };
        return Some(Frame {
            number: line,
            file: file.map(|f| display_path(f, root)),
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

/// Whether `file` sits under `root` by path components — separator style, redundant
/// separators, and trailing slashes are all normalized away by `Path`.
fn lexically_under_root(file: &Path, root: &str) -> bool {
    !root.is_empty() && file.starts_with(root)
}

/// The path as reported in a notice: `[PROJECT_ROOT]/…` for files under the project
/// root, absolute otherwise.
///
/// The root is stripped by *components*, never by byte prefix. A textual `replacen`
/// disagrees with the [`lexically_under_root`] check whenever the two paths spell the
/// same directory differently — `root = "/app/"` against `file = "/app/src/x.rs"`, or a
/// Windows `root = r"C:\app"` against a cargo-emitted `C:/app/src/x.rs` — and the
/// disagreement fails open, leaking the absolute build path into the payload.
///
/// The remainder is rejoined with `/` on every platform, so a fault's frames look the
/// same however the crate was built.
fn display_path(file: &Path, root: &str) -> String {
    if lexically_under_root(file, root)
        && let Ok(rest) = file.strip_prefix(root)
    {
        let rest: Vec<_> = rest
            .components()
            .map(|c| c.as_os_str().to_string_lossy())
            .collect();
        return if rest.is_empty() {
            PROJECT_ROOT.to_owned()
        } else {
            format!("{PROJECT_ROOT}/{}", rest.join("/"))
        };
    }
    file.to_string_lossy().into_owned()
}

/// Symlink-aware containment: both sides are canonicalized before comparison, so a link
/// living under `root` but pointing elsewhere cannot smuggle an out-of-tree file into a
/// notice. Only called for frames we are about to read, since it costs two `stat` walks.
///
/// If either path cannot be canonicalized (missing source on a deployed binary, for
/// instance) we report no containment — `read_excerpt` would fail on that path anyway.
fn resolves_under_root(file: &Path, root: &str) -> bool {
    match (std::fs::canonicalize(file), std::fs::canonicalize(root)) {
        (Ok(file), Ok(root)) => file.starts_with(root),
        _ => false,
    }
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

    /// The root substitution must agree with the containment check no matter how the two
    /// paths spell the same directory. A byte-prefix `replacen` did not: it either
    /// mangled the result or silently left the absolute build path in the payload.
    #[test]
    fn test_root_substitution_survives_separator_mismatch() {
        let cases: &[(&str, &str)] = &[
            // (root, file) — every one denotes /app + src/checkout.rs
            ("/app", "/app/src/checkout.rs"),
            ("/app/", "/app/src/checkout.rs"), // trailing slash on the root
            ("/app", "/app//src/checkout.rs"), // doubled separator in the file
            ("/app/", "/app//src/checkout.rs"),
        ];
        for (root, file) in cases {
            let f = map_frame(
                Some("my_app::checkout::charge"),
                Some(Path::new(file)),
                Some(42),
                root,
            )
            .unwrap();
            assert_eq!(
                f.file.as_deref(),
                Some("[PROJECT_ROOT]/src/checkout.rs"),
                "root {root:?} + file {file:?}"
            );
        }
    }

    /// Cargo emits forward slashes in debug info on Windows while `CARGO_MANIFEST_DIR`
    /// uses backslashes; `Path` treats both as separators, plain string matching does not.
    #[cfg(windows)]
    #[test]
    fn test_windows_mixed_separators_still_substituted() {
        let f = map_frame(
            Some("my_app::checkout::charge"),
            Some(Path::new(r"C:/app/src/checkout.rs")),
            Some(42),
            r"C:\app",
        )
        .unwrap();
        assert_eq!(f.file.as_deref(), Some("[PROJECT_ROOT]/src/checkout.rs"));
    }

    #[test]
    fn test_empty_root_never_substitutes() {
        let f = map_frame(
            Some("my_app::x"),
            Some(Path::new("/app/src/checkout.rs")),
            Some(1),
            "",
        )
        .unwrap();
        assert_eq!(f.file.as_deref(), Some("/app/src/checkout.rs"));
        assert!(f.source.is_none());
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

    #[cfg(unix)]
    #[test]
    fn test_symlink_out_of_root_yields_no_source_excerpt() {
        use std::io::Write;

        // A "project" whose src/leak.rs is a symlink to a file outside the project.
        let base = std::env::temp_dir().join(format!("hb-symlink-test-{}", std::process::id()));
        let root = base.join("project");
        let outside = base.join("outside");
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::create_dir_all(&outside).unwrap();

        let secret = outside.join("secret.rs");
        let mut f = std::fs::File::create(&secret).unwrap();
        writeln!(f, "const TOKEN: &str = \"do-not-leak\";").unwrap();
        writeln!(f, "// line two").unwrap();
        writeln!(f, "// line three").unwrap();
        drop(f);

        let link = root.join("src/leak.rs");
        let _ = std::fs::remove_file(&link);
        std::os::unix::fs::symlink(&secret, &link).unwrap();

        let root_str = root.to_string_lossy().into_owned();
        let frame = map_frame(Some("my_app::leak"), Some(&link), Some(2), &root_str).unwrap();

        assert!(
            frame.source.is_none(),
            "a symlink resolving outside the project root must not be excerpted"
        );
        // The path is still reported (and still root-substituted) — only reading is refused.
        assert_eq!(frame.file.as_deref(), Some("[PROJECT_ROOT]/src/leak.rs"));

        // A real file under the root is still excerpted.
        let honest = root.join("src/honest.rs");
        std::fs::write(&honest, "fn a() {}\nfn b() {}\nfn c() {}\n").unwrap();
        let frame = map_frame(Some("my_app::honest"), Some(&honest), Some(2), &root_str).unwrap();
        assert!(frame.source.is_some(), "in-root files are still excerpted");

        let _ = std::fs::remove_dir_all(&base);
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
