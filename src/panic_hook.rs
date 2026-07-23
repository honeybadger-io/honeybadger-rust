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
        let client = PANIC_CLIENT
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(client) = client {
            let _ = catch_unwind(AssertUnwindSafe(|| report(&client, info)));
        }
        IN_HOOK.with(|flag| flag.set(false));
    }
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
