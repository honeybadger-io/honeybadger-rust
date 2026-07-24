//! Permanent panic dispatcher (spec "Panic hook"): installed once, never uninstalled;
//! Guard drop only deregisters the client.
use crate::bt::Frame;
use crate::client::Client;
use crate::notice::Notice;
use std::cell::Cell;
use std::panic::{AssertUnwindSafe, PanicHookInfo, catch_unwind};
use std::sync::{Once, RwLock};

static INSTALL: Once = Once::new();
static PANIC_CLIENT: RwLock<Option<Client>> = RwLock::new(None);

thread_local! {
    static IN_HOOK: Cell<bool> = const { Cell::new(false) };
}

thread_local! {
    /// Set while the SDK is deliberately running user code it expects to catch.
    static SUPPRESSED: Cell<bool> = const { Cell::new(false) };
}

/// RAII guard: while alive, our dispatcher does not report panics on this
/// thread. Restores the previous value on drop, so guards nest correctly.
pub(crate) struct Suppressed(bool);

pub(crate) fn suppress_reporting() -> Suppressed {
    SUPPRESSED.with(|s| {
        let previous = s.get();
        s.set(true);
        Suppressed(previous)
    })
}

impl Drop for Suppressed {
    fn drop(&mut self) {
        let previous = self.0;
        SUPPRESSED.with(|s| s.set(previous));
    }
}

pub(crate) fn is_suppressed() -> bool {
    SUPPRESSED.with(|s| s.get())
}

pub(crate) fn register(client: Client) {
    INSTALL.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| dispatch(info, previous.as_ref())));
    });
    *PANIC_CLIENT.write().unwrap_or_else(|e| e.into_inner()) = Some(client);
}

pub(crate) fn deregister() {
    *PANIC_CLIENT.write().unwrap_or_else(|e| e.into_inner()) = None;
}

fn dispatch(info: &PanicHookInfo<'_>, previous: &(dyn Fn(&PanicHookInfo<'_>) + Send + Sync)) {
    let reentered = IN_HOOK.with(|flag| flag.replace(true));
    if !reentered {
        // A panic inside user code we are about to catch is contained, not
        // reported: reporting it would pay the urgent HTTP timeout on the
        // caller's thread and file a notice for a panic that never escaped.
        if !is_suppressed() {
            let client = PANIC_CLIENT
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if let Some(client) = client {
                let _ = catch_unwind(AssertUnwindSafe(|| report(&client, info)));
            }
        }
        IN_HOOK.with(|flag| flag.set(false));
    }
    // Always chain: non-Honeybadger panic handling must be unaffected.
    previous(info);
}

fn report(client: &Client, info: &PanicHookInfo<'_>) {
    let message = if let Some(s) = info.payload().downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = info.payload().downcast_ref::<String>() {
        s.clone()
    } else {
        "Box<dyn Any>".to_owned()
    };

    let mut notice = Notice::message("panic", &message);
    // Location as the top frame, then the captured backtrace below it.
    let mut frames = Vec::new();
    if let Some(location) = info.location() {
        frames.push(Frame {
            number: Some(location.line()),
            file: Some(location.file().to_owned()),
            method: Some("panic".to_owned()),
            source: None,
        });
    }
    frames.extend(client.capture_frames());
    notice.frames = Some(frames);
    client.deliver_now(notice);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_suppression_guard_nests_and_restores() {
        assert!(!is_suppressed());
        {
            let _outer = suppress_reporting();
            assert!(is_suppressed());
            {
                let _inner = suppress_reporting();
                assert!(is_suppressed());
            }
            assert!(
                is_suppressed(),
                "an inner guard dropping must not clear the outer one"
            );
        }
        assert!(!is_suppressed(), "outermost drop restores reporting");
    }
}
