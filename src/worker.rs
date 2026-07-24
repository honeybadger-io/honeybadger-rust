//! The delivery worker: dedicated OS thread, bounded notice channel + unbounded
//! control channel, throttle/suspend semantics (spec "Delivery architecture").
use crate::drops::DropCounter;
use crate::transport::{Transport, TransportError, TransportRequest};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TrySendError, bounded, unbounded};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const MAX_THROTTLE_EXP: i32 = 150;
const MAX_THROTTLE_INTERVAL: Duration = Duration::from_secs(300);
const SUSPEND_INTERVAL: Duration = Duration::from_secs(3600);

pub(crate) enum Control {
    Flush(Sender<bool>),
    Shutdown(Sender<()>),
}

pub(crate) struct WorkerHandle {
    notices: Sender<Vec<u8>>,
    control: Sender<Control>,
    join: Mutex<Option<JoinHandle<()>>>,
}

pub(crate) fn throttle_interval(n: u32) -> Duration {
    if n == 0 {
        return Duration::ZERO;
    }
    let exp = (n as i64).min(MAX_THROTTLE_EXP as i64) as i32;
    let secs = 1.05f64.powi(exp) - 1.0;
    if !secs.is_finite() || secs <= 0.0 {
        return Duration::ZERO;
    }
    MAX_THROTTLE_INTERVAL.min(Duration::from_secs_f64(secs.min(300.0)))
}

pub(crate) fn spawn(
    transport: Arc<dyn Transport>,
    queue_size: usize,
    drops: Arc<DropCounter>,
) -> std::io::Result<WorkerHandle> {
    spawn_with_intervals(transport, queue_size, SUSPEND_INTERVAL, drops)
}

pub(crate) fn spawn_with_intervals(
    transport: Arc<dyn Transport>,
    queue_size: usize,
    suspend_interval: Duration,
    drops: Arc<DropCounter>,
) -> std::io::Result<WorkerHandle> {
    let (notice_tx, notice_rx) = bounded(queue_size);
    let (control_tx, control_rx) = unbounded();
    let join = std::thread::Builder::new()
        .name("honeybadger-worker".into())
        .spawn(move || {
            Worker {
                transport,
                notices: notice_rx,
                control: control_rx,
                throttle: 0,
                suspend_interval,
                drops,
            }
            .run()
        })?;
    Ok(WorkerHandle {
        notices: notice_tx,
        control: control_tx,
        join: Mutex::new(Some(join)),
    })
}

impl WorkerHandle {
    /// Returns false if the payload was dropped (queue full or worker gone).
    pub(crate) fn try_enqueue(&self, payload: Vec<u8>) -> bool {
        match self.notices.try_send(payload) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => false,
        }
    }

    /// Starts a flush and returns its acknowledgement channel, so a caller can
    /// begin flushes on both pipelines before waiting on either.
    pub(crate) fn flush_begin(&self) -> Option<Receiver<bool>> {
        let (ack_tx, ack_rx) = bounded(1);
        self.control.send(Control::Flush(ack_tx)).ok()?;
        Some(ack_rx)
    }

    pub(crate) fn shutdown(&self, timeout: Duration) {
        let (ack_tx, ack_rx) = bounded(1);
        if self.control.send(Control::Shutdown(ack_tx)).is_err() {
            return;
        }
        // Worker acks right before exiting; bounded wait, then join (instant) or detach.
        if ack_rx.recv_timeout(timeout).is_ok() {
            if let Some(handle) = self.join.lock().unwrap_or_else(|e| e.into_inner()).take() {
                let _ = handle.join();
            }
        } else {
            log::warn!("honeybadger: worker did not stop within {timeout:?}; detaching");
            // Dropping the JoinHandle detaches; the thread exits after its current send.
            self.join.lock().unwrap_or_else(|e| e.into_inner()).take();
        }
    }
}

enum SendOutcome {
    Continue,
    Suspend,
}

struct Worker {
    transport: Arc<dyn Transport>,
    notices: Receiver<Vec<u8>>,
    control: Receiver<Control>,
    throttle: u32,
    suspend_interval: Duration,
    drops: Arc<DropCounter>,
}

impl Worker {
    fn run(mut self) {
        loop {
            crossbeam_channel::select! {
                recv(self.control) -> msg => match msg {
                    Ok(control) => if self.handle_control(control) { return; },
                    Err(_) => return, // all handles dropped
                },
                recv(self.notices) -> msg => match msg {
                    Ok(payload) => {
                        match self.send_one(&payload) {
                            SendOutcome::Suspend => if self.suspended_wait() { return; },
                            SendOutcome::Continue => if self.throttle_pause() { return; },
                        }
                    }
                    Err(_) => return,
                },
            }
        }
    }

    /// Returns true when the worker should exit.
    fn handle_control(&mut self, control: Control) -> bool {
        match control {
            Control::Flush(ack) => {
                self.drain_and_send();
                let _ = ack.send(true);
                false
            }
            Control::Shutdown(ack) => {
                if self.throttle == 0 {
                    self.drain_and_send();
                } else {
                    let dropped = self.drain_and_drop();
                    if dropped > 0 {
                        self.drops.record_many(dropped as u64);
                        log::warn!(
                            "honeybadger: dropping {dropped} queued notices at shutdown (throttled)"
                        );
                    }
                }
                self.drops.report_final();
                let _ = ack.send(());
                true
            }
        }
    }

    /// Barrier semantics: everything already in the notice channel is processed.
    fn drain_and_send(&mut self) {
        while let Ok(payload) = self.notices.try_recv() {
            if matches!(self.send_one(&payload), SendOutcome::Suspend) {
                let dropped = self.drain_and_drop();
                if dropped > 0 {
                    self.drops.record_many(dropped as u64);
                }
                log::warn!("honeybadger: suspended during flush; dropped {dropped} queued notices");
                return;
            }
        }
    }

    /// Drains and *counts* the queue. `drain_and_drop` alone loses the tally,
    /// which is the whole point of the drop counter.
    fn discard_queued(&mut self) {
        let dropped = self.drain_and_drop();
        if dropped > 0 {
            self.drops.record_many(dropped as u64);
        }
    }

    fn drain_and_drop(&mut self) -> usize {
        let mut count = 0;
        while self.notices.try_recv().is_ok() {
            count += 1;
        }
        count
    }

    fn send_one(&mut self, payload: &[u8]) -> SendOutcome {
        let req = TransportRequest::notices(payload, false);
        // `Transport` is user-implementable: a panicking impl must not kill the worker.
        // The guard also stops our own panic hook from reporting it — that would
        // re-enter this same transport from inside the hook, and a panic there
        // aborts the process.
        let result = {
            let _suppressed = crate::panic_hook::suppress_reporting();
            catch_unwind(AssertUnwindSafe(|| self.transport.deliver(&req)))
                .unwrap_or_else(|_| Err(TransportError("transport panicked".into())))
        };
        match result {
            Ok(status) if (200..300).contains(&status) => {
                self.throttle = self.throttle.saturating_sub(1);
                self.drops.report();
                SendOutcome::Continue
            }
            Ok(429) | Ok(503) => {
                self.throttle = self.throttle.saturating_add(1);
                log::debug!("honeybadger: throttled (n={})", self.throttle);
                SendOutcome::Continue
            }
            Ok(402) => {
                log::warn!(
                    "honeybadger: payment required; suspending delivery for {:?}",
                    self.suspend_interval
                );
                SendOutcome::Suspend
            }
            // A malformed key is 401, not 403, and it is not going to fix itself
            // between one notice and the next. Falling through to the
            // "unexpected status" arm below would drop notices one at a time,
            // forever, under a message that names no cause.
            Ok(401) => {
                log::warn!(
                    "honeybadger: unauthorized (malformed API key); suspending delivery for {:?}",
                    self.suspend_interval
                );
                SendOutcome::Suspend
            }
            Ok(403) => {
                log::warn!(
                    "honeybadger: unauthorized (bad API key or inactive account); suspending delivery for {:?}",
                    self.suspend_interval
                );
                SendOutcome::Suspend
            }
            Ok(413) => {
                log::warn!("honeybadger: payload too large; notice dropped");
                SendOutcome::Continue
            }
            Ok(status) => {
                log::warn!("honeybadger: unexpected API status {status}; notice dropped");
                SendOutcome::Continue
            }
            Err(e) => {
                log::warn!("honeybadger: delivery failed: {e}");
                SendOutcome::Continue
            }
        }
    }

    /// Interruptible throttle pause. Returns true when the worker should exit.
    fn throttle_pause(&mut self) -> bool {
        let pause = throttle_interval(self.throttle);
        if pause.is_zero() {
            return false;
        }
        match self.control.recv_timeout(pause) {
            Ok(control) => self.handle_control(control),
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => true,
        }
    }

    /// Suspension: drop the queue, wait out the interval servicing control.
    /// Returns true when the worker should exit.
    fn suspended_wait(&mut self) -> bool {
        let dropped = self.drain_and_drop();
        if dropped > 0 {
            self.drops.record_many(dropped as u64);
            log::warn!("honeybadger: dropped {dropped} queued notices (suspended)");
        }
        let deadline = Instant::now() + self.suspend_interval;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                self.throttle = 0; // reset on resume
                self.discard_queued(); // anything accepted while suspended is stale
                return false;
            }
            match self.control.recv_timeout(remaining) {
                Ok(Control::Flush(ack)) => {
                    self.discard_queued();
                    let _ = ack.send(true); // queue empty by definition while suspended
                }
                Ok(Control::Shutdown(ack)) => {
                    self.discard_queued();
                    // Shutting down while suspended still owes the operator a
                    // summary of everything the storm cost.
                    self.drops.report_final();
                    let _ = ack.send(());
                    return true;
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return true,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{TestTransport, compress};
    use std::sync::Arc;
    use std::time::Duration;

    fn payload() -> Vec<u8> {
        compress(b"{}")
    }

    fn drops() -> Arc<DropCounter> {
        Arc::new(DropCounter::new("notices"))
    }

    /// The blocking form of `flush_begin`. Production code starts both
    /// pipelines' flushes before waiting on either, so only tests want this.
    fn flush(w: &WorkerHandle, timeout: Duration) -> bool {
        match w.flush_begin() {
            Some(rx) => rx.recv_timeout(timeout).unwrap_or(false),
            None => false,
        }
    }

    #[test]
    fn test_throttle_interval_shape_and_saturation() {
        assert_eq!(throttle_interval(0), Duration::ZERO);
        assert!(throttle_interval(10) > Duration::ZERO);
        assert!(throttle_interval(10) < throttle_interval(50));
        assert_eq!(throttle_interval(10_000), Duration::from_secs(300)); // saturated, no panic
        assert_eq!(throttle_interval(u32::MAX), Duration::from_secs(300));
    }

    #[test]
    fn test_delivers_and_flush_barrier() {
        let transport = Arc::new(TestTransport::new());
        let w = spawn(transport.clone(), 10, drops()).unwrap();
        for _ in 0..3 {
            assert!(w.try_enqueue(payload()));
        }
        assert!(flush(&w, Duration::from_secs(5)));
        assert_eq!(transport.requests().len(), 3);
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_suspend_on_401_stops_delivery() {
        // The shared API-key middleware answers a malformed key with 401, not
        // 403. Treating it as merely "unexpected" drops one notice per send for
        // the life of the process, each with a message that names no cause.
        let transport = Arc::new(TestTransport::new());
        transport.respond_with(401);
        let w =
            spawn_with_intervals(transport.clone(), 10, Duration::from_secs(30), drops()).unwrap();
        assert!(w.try_enqueue(payload()));
        std::thread::sleep(Duration::from_millis(200));
        assert!(w.try_enqueue(payload()));
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            transport.requests().len(),
            1,
            "a 401 must suspend delivery rather than dropping notices one by one"
        );
        w.shutdown(Duration::from_secs(2));
    }

    #[test]
    fn test_queue_overflow_drops() {
        let transport = Arc::new(TestTransport::new());
        // Suspend delivery by pre-programming a 402 on the FIRST send, so the worker
        // enters suspension and stops consuming; then overfill the queue.
        transport.respond_with(402);
        let w =
            spawn_with_intervals(transport.clone(), 2, Duration::from_secs(30), drops()).unwrap();
        assert!(w.try_enqueue(payload())); // consumed, triggers suspension
        std::thread::sleep(Duration::from_millis(200)); // let suspension start
        let mut accepted = 0;
        for _ in 0..10 {
            if w.try_enqueue(payload()) {
                accepted += 1;
            }
        }
        assert!(
            accepted <= 2,
            "bounded queue must reject overflow (accepted {accepted})"
        );
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_suspend_on_403_drops_queue_but_flush_and_shutdown_work() {
        let transport = Arc::new(TestTransport::new());
        transport.respond_with(403);
        let w =
            spawn_with_intervals(transport.clone(), 10, Duration::from_secs(30), drops()).unwrap();
        w.try_enqueue(payload()); // 403 → suspended
        std::thread::sleep(Duration::from_millis(200));
        w.try_enqueue(payload()); // lands in queue, will be dropped by suspension drain
        assert!(
            flush(&w, Duration::from_secs(2)),
            "flush must ack while suspended"
        );
        assert_eq!(transport.requests().len(), 1, "no delivery while suspended");
        w.shutdown(Duration::from_secs(2)); // must return promptly despite 30s suspension
    }

    #[test]
    fn test_throttle_429_slows_but_continues_and_recovers() {
        let transport = Arc::new(TestTransport::new());
        transport.respond_with(429);
        let w = spawn(transport.clone(), 10, drops()).unwrap();
        w.try_enqueue(payload()); // 429 → throttle n=1 (~0.05s pause)
        w.try_enqueue(payload()); // 201 → n back to 0
        w.try_enqueue(payload());
        assert!(flush(&w, Duration::from_secs(10)));
        assert_eq!(transport.requests().len(), 3);
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_panicking_transport_does_not_kill_the_worker() {
        use crate::transport::{Transport, TransportError, TransportRequest};
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct PanicOnFirst {
            calls: AtomicUsize,
        }
        impl Transport for PanicOnFirst {
            fn deliver(&self, _req: &TransportRequest) -> Result<u16, TransportError> {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("transport blew up");
                }
                Ok(201)
            }
        }

        let transport = Arc::new(PanicOnFirst {
            calls: AtomicUsize::new(0),
        });
        let w = spawn(transport.clone(), 10, drops()).unwrap();
        assert!(w.try_enqueue(payload())); // panics inside deliver
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            w.try_enqueue(payload()),
            "worker must still accept notices after a transport panic"
        );
        assert!(
            flush(&w, Duration::from_secs(5)),
            "worker must still service flush after a transport panic"
        );
        assert_eq!(
            transport.calls.load(Ordering::SeqCst),
            2,
            "the second notice must still have been delivered"
        );
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_shutting_down_while_suspended_still_reports_its_drops() {
        // Regression: the suspended shutdown path acknowledged without calling
        // report_final, so every drop accumulated during the storm that caused
        // the suspension was silently discarded instead of summarised.
        let transport = Arc::new(TestTransport::new());
        transport.respond_with(402);
        let drops = drops();
        let w = spawn_with_intervals(transport.clone(), 2, Duration::from_secs(30), drops.clone())
            .unwrap();
        w.try_enqueue(payload()); // consumed -> suspends
        std::thread::sleep(Duration::from_millis(200));
        for _ in 0..10 {
            w.try_enqueue(payload()); // queued behind the suspension, or refused
        }
        w.shutdown(Duration::from_secs(5));
        assert!(
            drops.reported() > 0,
            "drops accumulated under suspension must be summarised at shutdown"
        );
        assert_eq!(drops.pending(), 0, "nothing may be left unreported");
    }

    #[test]
    fn test_transport_delivery_runs_with_panic_reporting_suppressed() {
        // See the events worker's twin of this test: without the guard a
        // panicking transport is re-entered from inside our own panic hook,
        // which aborts the process rather than containing the panic.
        use crate::transport::{Transport, TransportError, TransportRequest};
        use std::sync::atomic::{AtomicBool, Ordering};

        struct Spy {
            suppressed: AtomicBool,
        }
        impl Transport for Spy {
            fn deliver(&self, _req: &TransportRequest) -> Result<u16, TransportError> {
                self.suppressed
                    .store(crate::panic_hook::is_suppressed(), Ordering::SeqCst);
                Ok(201)
            }
        }

        let transport = Arc::new(Spy {
            suppressed: AtomicBool::new(false),
        });
        let w = spawn(transport.clone(), 10, drops()).unwrap();
        w.try_enqueue(payload());
        assert!(flush(&w, Duration::from_secs(5)));
        assert!(
            transport.suppressed.load(Ordering::SeqCst),
            "delivery must run under the panic-suppression guard"
        );
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_enqueue_after_shutdown_returns_false() {
        let transport = Arc::new(TestTransport::new());
        let w = spawn(transport, 10, drops()).unwrap();
        w.shutdown(Duration::from_secs(5));
        assert!(!w.try_enqueue(payload()));
    }
}
