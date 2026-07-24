//! The events delivery worker: dedicated OS thread, bounded event channel plus
//! an unbounded control channel, batching on count, bytes, or time.
use crate::drops::DropCounter;
use crate::transport::{Transport, TransportRequest, compress};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TrySendError, bounded, unbounded};
use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Cut a batch at this many uncompressed bytes, leaving margin under the API's
/// documented 5 MB request ceiling. Without it, `batch_size` events at the
/// 100 kB per-event limit would be a 100 MB request.
pub(crate) const BATCH_BYTE_LIMIT: usize = 4_500_000;

/// How long the worker parks when it has nothing pending at all.
const IDLE_PARK: Duration = Duration::from_secs(3600);

pub(crate) struct EventsConfig {
    pub(crate) batch_size: usize,
    pub(crate) flush_interval: Duration,
    pub(crate) queue_size: usize,
    pub(crate) max_retries: u32,
    pub(crate) suspend_interval: Duration,
}

pub(crate) enum Control {
    Flush(Sender<bool>),
    Shutdown(Sender<()>),
}

pub(crate) struct EventsWorkerHandle {
    events: Sender<String>,
    control: Sender<Control>,
    join: Mutex<Option<JoinHandle<()>>>,
    /// Recorded at spawn so a forked child can detect it has no worker thread.
    pub(crate) pid: u32,
}

pub(crate) fn spawn(
    transport: Arc<dyn Transport>,
    cfg: EventsConfig,
    drops: Arc<DropCounter>,
) -> std::io::Result<EventsWorkerHandle> {
    let (event_tx, event_rx) = bounded(cfg.queue_size);
    let (control_tx, control_rx) = unbounded();
    let join = std::thread::Builder::new()
        .name("honeybadger-events".into())
        .spawn(move || {
            EventsWorker {
                transport,
                events: event_rx,
                control: control_rx,
                drops,
                cfg,
                current: Vec::new(),
                current_bytes: 0,
                deadline: None,
                retry_at: None,
                retries: VecDeque::new(),
                outstanding: 0,
                throttle: 0,
                stopped: false,
            }
            .run()
        })?;
    Ok(EventsWorkerHandle {
        events: event_tx,
        control: control_tx,
        join: Mutex::new(Some(join)),
        pid: std::process::id(),
    })
}

impl EventsWorkerHandle {
    /// Returns false if the line was dropped (queue full or worker gone).
    pub(crate) fn try_enqueue(&self, line: String) -> bool {
        match self.events.try_send(line) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => false,
        }
    }

    /// Starts a flush and returns the acknowledgement channel, so a caller can
    /// begin flushes on several pipelines before waiting on any of them.
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
        if ack_rx.recv_timeout(timeout).is_ok() {
            if let Some(handle) = self.join.lock().unwrap_or_else(|e| e.into_inner()).take() {
                let _ = handle.join();
            }
        } else {
            log::warn!("honeybadger: events worker did not stop within {timeout:?}; detaching");
            self.join.lock().unwrap_or_else(|e| e.into_inner()).take();
        }
    }
}

struct Batch {
    body: Vec<u8>,
    attempts: u32,
    events: usize,
}

/// What a delivery attempt means for the batch at the head of the queue.
enum Outcome {
    Success,
    /// Retry later, spending one attempt.
    Retry,
    /// Unrecoverable for this batch: never retry. Carries the status when it
    /// fell outside every documented range, so the log can name it.
    Drop {
        unexpected: Option<u16>,
    },
    /// Back off. `burn` distinguishes 503 (a server error) from 429 (an
    /// instruction to wait, which must not age a batch out).
    Throttle {
        burn: bool,
    },
    Suspend,
}

fn classify(status: u16) -> Outcome {
    match status {
        200..=299 => Outcome::Success,
        402 | 403 => Outcome::Suspend,
        429 => Outcome::Throttle { burn: false },
        503 => Outcome::Throttle { burn: true },
        400..=499 => Outcome::Drop { unexpected: None },
        500..=599 => Outcome::Retry,
        // 1xx, 3xx, and anything out of range: undefined by the API, so the
        // status itself is the only useful thing we can report.
        status => Outcome::Drop {
            unexpected: Some(status),
        },
    }
}

struct EventsWorker {
    transport: Arc<dyn Transport>,
    events: Receiver<String>,
    control: Receiver<Control>,
    drops: Arc<DropCounter>,
    cfg: EventsConfig,
    current: Vec<String>,
    current_bytes: usize,
    deadline: Option<Instant>,
    retry_at: Option<Instant>,
    retries: VecDeque<Batch>,
    /// Events held in the current batch plus the retry queue. The channel's
    /// occupancy is added to this for budget checks — see `accept`.
    outstanding: usize,
    throttle: u32,
    stopped: bool,
}

impl EventsWorker {
    fn run(mut self) {
        while !self.stopped {
            let wake = self.next_wake();
            crossbeam_channel::select! {
                recv(self.control) -> msg => match msg {
                    Ok(control) => self.handle_control(control),
                    Err(_) => return,
                },
                recv(self.events) -> msg => match msg {
                    Ok(line) => self.accept(line),
                    Err(_) => return,
                },
                default(wake) => {
                    if self.deadline.is_some_and(|d| d <= Instant::now()) {
                        self.cut();
                    }
                    if self.ready_to_send() {
                        self.send_pending();
                    }
                }
            }
        }
    }

    /// False while a backoff is still pending. Flush and shutdown ignore this:
    /// they are explicit barriers, not opportunistic sends.
    fn ready_to_send(&self) -> bool {
        self.retry_at.is_none_or(|at| at <= Instant::now())
    }

    fn next_wake(&self) -> Duration {
        let now = Instant::now();
        let mut wake = IDLE_PARK;
        if let Some(deadline) = self.deadline {
            wake = wake.min(deadline.saturating_duration_since(now));
        }
        if let Some(retry_at) = self.retry_at {
            wake = wake.min(retry_at.saturating_duration_since(now));
        }
        wake
    }

    fn accept(&mut self, line: String) {
        // One budget covers everything outstanding: the channel's backlog, the
        // accumulating batch, and every retained batch. Counting only what the
        // worker holds would let the channel hold a second full queue's worth.
        //
        // Shed the stalest data first so a stalled head batch cannot pin the
        // pipeline; only refuse the new event once nothing older is left.
        while self.outstanding + self.events.len() >= self.cfg.queue_size {
            match self.retries.pop_front() {
                Some(batch) => {
                    self.outstanding -= batch.events;
                    self.drops.record_many(batch.events as u64);
                    log::warn!(
                        "honeybadger: dropped a retained batch of {} events to stay within events_queue_size",
                        batch.events
                    );
                }
                None => {
                    self.drops.record();
                    return;
                }
            }
        }

        if self.current.is_empty() {
            // An unrepresentable interval yields None, i.e. no time trigger —
            // the count and byte triggers and any flush still cut the batch.
            self.deadline = Instant::now().checked_add(self.cfg.flush_interval);
        }
        self.current_bytes += line.len() + 1; // the joining newline
        self.current.push(line);
        self.outstanding += 1;

        if self.current.len() >= self.cfg.batch_size || self.current_bytes >= BATCH_BYTE_LIMIT {
            self.cut();
            // Filling a batch must not bypass the retry schedule: an endpoint
            // that just asked us to slow down would otherwise be hit again on
            // every trigger, at exactly the rate that provoked the 429.
            if self.ready_to_send() {
                self.send_pending();
            }
        }
    }

    /// Moves the accumulating batch into the retry queue as compressed bytes.
    fn cut(&mut self) {
        if self.current.is_empty() {
            return;
        }
        let lines = std::mem::take(&mut self.current);
        let events = lines.len();
        self.current_bytes = 0;
        self.deadline = None;
        self.retries.push_back(Batch {
            body: compress(lines.join("\n").as_bytes()),
            attempts: 0,
            events,
        });
    }

    fn send_pending(&mut self) {
        while !self.retries.is_empty() {
            let outcome = {
                let body = &self.retries.front().expect("non-empty").body;
                let req = TransportRequest::events(body);
                // `Transport` is user-implementable: a panicking impl must not
                // take the worker down. The guard also stops our own panic hook
                // from reporting it — that would re-enter this same transport
                // from inside the hook, and a panic there aborts the process.
                let _suppressed = crate::panic_hook::suppress_reporting();
                match catch_unwind(AssertUnwindSafe(|| self.transport.deliver(&req))) {
                    Ok(Ok(status)) => classify(status),
                    Ok(Err(e)) => {
                        log::warn!("honeybadger: events delivery failed: {e}");
                        Outcome::Retry
                    }
                    Err(_) => {
                        log::warn!("honeybadger: events transport panicked");
                        Outcome::Retry
                    }
                }
            };

            match outcome {
                Outcome::Success => {
                    self.pop_head();
                    self.throttle = self.throttle.saturating_sub(1);
                    self.drops.report();
                }
                Outcome::Drop { unexpected } => {
                    let batch = self.pop_head();
                    match unexpected {
                        Some(status) => log::warn!(
                            "honeybadger: unexpected API status {status}; dropped a batch of {} events",
                            batch.events
                        ),
                        None => log::warn!(
                            "honeybadger: dropped a batch of {} events the API rejected",
                            batch.events
                        ),
                    }
                    self.drops.record_many(batch.events as u64);
                }
                Outcome::Retry => {
                    if self.charge_attempt() {
                        continue; // aged out; try the next batch
                    }
                    self.schedule_retry();
                    return;
                }
                Outcome::Throttle { burn } => {
                    self.throttle = self.throttle.saturating_add(1);
                    log::debug!("honeybadger: events throttled (n={})", self.throttle);
                    if burn && self.charge_attempt() {
                        continue;
                    }
                    self.schedule_retry();
                    return;
                }
                Outcome::Suspend => {
                    self.suspend();
                    return;
                }
            }
        }
        self.retry_at = None;
    }

    fn pop_head(&mut self) -> Batch {
        let batch = self.retries.pop_front().expect("non-empty");
        self.outstanding -= batch.events;
        batch
    }

    /// Spends one attempt on the head batch. Returns true if it aged out and
    /// was dropped.
    fn charge_attempt(&mut self) -> bool {
        let head = self.retries.front_mut().expect("non-empty");
        head.attempts += 1;
        if head.attempts <= self.cfg.max_retries {
            return false;
        }
        let batch = self.pop_head();
        log::warn!(
            "honeybadger: dropped a batch of {} events after {} attempts",
            batch.events,
            batch.attempts
        );
        self.drops.record_many(batch.events as u64);
        true
    }

    fn schedule_retry(&mut self) {
        // Reuse the shipped notices curve, floored at one flush interval so a
        // batch retry never becomes a hot loop.
        let backoff = crate::worker::throttle_interval(self.throttle).max(self.cfg.flush_interval);
        self.retry_at = Instant::now().checked_add(backoff);
    }

    /// 402/403: nothing will change until a human acts, so discard everything
    /// outstanding and wait out the interval, still servicing control.
    fn suspend(&mut self) {
        let dropped = self.outstanding;
        self.retries.clear();
        self.current.clear();
        self.current_bytes = 0;
        self.deadline = None;
        self.retry_at = None;
        self.outstanding = 0;
        if dropped > 0 {
            self.drops.record_many(dropped as u64);
        }
        log::warn!(
            "honeybadger: events delivery suspended for {:?}",
            self.cfg.suspend_interval
        );

        // None means the interval is unrepresentable; park in bounded chunks
        // rather than panicking on the addition.
        let until = Instant::now().checked_add(self.cfg.suspend_interval);
        loop {
            let remaining =
                until.map_or(IDLE_PARK, |t| t.saturating_duration_since(Instant::now()));
            if remaining.is_zero() {
                self.throttle = 0;
                self.discard_queued();
                return;
            }
            match self.control.recv_timeout(remaining) {
                Ok(Control::Flush(ack)) => {
                    self.discard_queued();
                    let _ = ack.send(true); // nothing is pending by definition
                }
                Ok(Control::Shutdown(ack)) => {
                    self.discard_queued();
                    self.drops.report_final();
                    let _ = ack.send(());
                    self.stopped = true;
                    return;
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    self.stopped = true;
                    return;
                }
            }
        }
    }

    /// Counts everything still held as dropped. For shutdown, where anything
    /// undelivered is about to be destroyed with the worker.
    fn abandon_outstanding(&mut self) {
        if self.outstanding > 0 {
            log::warn!(
                "honeybadger: dropping {} undelivered events at shutdown",
                self.outstanding
            );
            self.drops.record_many(self.outstanding as u64);
        }
        self.retries.clear();
        self.current.clear();
        self.current_bytes = 0;
        self.outstanding = 0;
    }

    fn discard_queued(&mut self) {
        let mut n = 0u64;
        while self.events.try_recv().is_ok() {
            n += 1;
        }
        if n > 0 {
            self.drops.record_many(n);
        }
    }

    fn handle_control(&mut self, control: Control) {
        match control {
            Control::Flush(ack) => {
                self.drain_channel();
                self.cut();
                self.send_pending();
                // Head-of-line blocking means a retained batch stops everything
                // behind it, so the barrier only completed if nothing is left.
                let _ = ack.send(self.retries.is_empty());
            }
            Control::Shutdown(ack) => {
                self.drain_channel();
                self.cut(); // force-cut: a partial batch must not be lost
                self.send_pending();
                // Whatever delivery could not place dies with the worker, so it
                // is counted rather than vanishing silently.
                self.abandon_outstanding();
                self.drops.report_final();
                let _ = ack.send(());
                self.stopped = true;
            }
        }
    }

    /// Pulls everything already queued into the current batch, so a flush is a
    /// barrier over every event enqueued before the flush call.
    fn drain_channel(&mut self) {
        while let Ok(line) = self.events.try_recv() {
            self.accept(line);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::drops::DropCounter;
    use crate::transport::TestTransport;
    use std::io::Read;

    fn cfg() -> EventsConfig {
        EventsConfig {
            batch_size: 3,
            flush_interval: Duration::from_millis(200),
            queue_size: 100,
            max_retries: 3,
            suspend_interval: Duration::from_secs(30),
        }
    }

    fn lines(transport: &TestTransport) -> Vec<Vec<String>> {
        transport
            .requests()
            .iter()
            .map(|r| {
                let mut s = String::new();
                flate2::read::ZlibDecoder::new(&r.body[..])
                    .read_to_string(&mut s)
                    .unwrap();
                s.lines().map(str::to_owned).collect()
            })
            .collect()
    }

    fn worker(transport: Arc<TestTransport>, cfg: EventsConfig) -> EventsWorkerHandle {
        spawn(transport, cfg, Arc::new(DropCounter::new("events"))).unwrap()
    }

    #[test]
    fn test_count_trigger_cuts_a_batch() {
        let transport = Arc::new(TestTransport::new());
        let w = worker(transport.clone(), cfg());
        for i in 0..3 {
            assert!(w.try_enqueue(format!("{{\"n\":{i}}}")));
        }
        // The third event fills the batch, so delivery happens without a flush.
        std::thread::sleep(Duration::from_millis(300));
        let batches = lines(&transport);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 3, "one batch of exactly batch_size");
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_time_trigger_starts_with_the_batch_not_the_last_send() {
        let transport = Arc::new(TestTransport::new());
        let w = worker(transport.clone(), cfg());
        // Idle well past the interval, then send a single event. If the
        // deadline ran "since the last send" it would already have expired and
        // this would ship immediately as a batch of one.
        std::thread::sleep(Duration::from_millis(500));
        assert!(w.try_enqueue("{\"n\":1}".into()));
        std::thread::sleep(Duration::from_millis(80));
        assert!(
            transport.requests().is_empty(),
            "a fresh batch must get a full interval, not a stale deadline"
        );
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(lines(&transport).len(), 1);
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_byte_trigger_cuts_before_the_count_trigger() {
        let transport = Arc::new(TestTransport::new());
        let w = worker(
            transport.clone(),
            EventsConfig {
                batch_size: 100_000,
                ..cfg()
            },
        );
        // Each line is ~50 KB; enough of them exceed BATCH_BYTE_LIMIT long
        // before the count trigger could fire.
        let big = format!("{{\"blob\":\"{}\"}}", "y".repeat(50_000));
        for _ in 0..100 {
            w.try_enqueue(big.clone());
        }
        std::thread::sleep(Duration::from_millis(500));
        assert!(
            !transport.requests().is_empty(),
            "the byte ceiling must cut a batch on its own"
        );
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_flush_is_a_barrier_for_a_partial_batch() {
        let transport = Arc::new(TestTransport::new());
        let w = worker(
            transport.clone(),
            EventsConfig {
                flush_interval: Duration::from_secs(3600),
                ..cfg()
            },
        );
        w.try_enqueue("{\"n\":1}".into());
        let rx = w.flush_begin().unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)), Ok(true));
        assert_eq!(lines(&transport), vec![vec!["{\"n\":1}".to_string()]]);
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_shutdown_force_cuts_a_partial_batch() {
        let transport = Arc::new(TestTransport::new());
        let w = worker(
            transport.clone(),
            EventsConfig {
                batch_size: 1000,
                flush_interval: Duration::from_secs(3600),
                ..cfg()
            },
        );
        for i in 0..5 {
            w.try_enqueue(format!("{{\"n\":{i}}}"));
        }
        w.shutdown(Duration::from_secs(5));
        assert_eq!(
            lines(&transport).concat().len(),
            5,
            "shutdown must not silently discard an uncut batch"
        );
    }

    fn attempts_for(status: u16) -> usize {
        let transport = Arc::new(TestTransport::new());
        for _ in 0..10 {
            transport.respond_with(status);
        }
        let w = worker(
            transport.clone(),
            EventsConfig {
                batch_size: 1,
                flush_interval: Duration::from_millis(50),
                max_retries: 2, // 3 attempts in total
                ..cfg()
            },
        );
        w.try_enqueue("{\"n\":1}".into());
        std::thread::sleep(Duration::from_millis(600));
        let n = transport.requests().len();
        w.shutdown(Duration::from_secs(5));
        n
    }

    #[test]
    fn test_retryable_failures_are_retried_to_the_limit() {
        assert_eq!(attempts_for(500), 3, "5xx retries up to max_retries + 1");
    }

    #[test]
    fn test_poison_pill_statuses_are_dropped_on_the_first_response() {
        // The Go client burns its whole retry budget on batches the server has
        // already called unprocessable. We must not.
        assert_eq!(attempts_for(413), 1, "413 is never retried");
        assert_eq!(attempts_for(422), 1, "422 is never retried");
        assert_eq!(attempts_for(400), 1, "other 4xx are never retried");
        assert_eq!(attempts_for(301), 1, "unexpected statuses drop, not hang");
    }

    #[test]
    fn test_429_burns_no_retry_budget_but_503_does() {
        // A rate limit is an instruction to wait, not a failure, so it must not
        // age a batch out. A 503 carries no such instruction.
        assert!(
            attempts_for(429) > 3,
            "429 keeps retrying past max_retries + 1"
        );
        assert_eq!(attempts_for(503), 3, "503 burns the retry budget");
    }

    #[test]
    fn test_suspend_on_402_drops_everything_and_still_serves_control() {
        let transport = Arc::new(TestTransport::new());
        transport.respond_with(402);
        let drops = Arc::new(DropCounter::new("events"));
        let w = spawn(
            transport.clone(),
            EventsConfig {
                batch_size: 1,
                suspend_interval: Duration::from_secs(30),
                ..cfg()
            },
            drops.clone(),
        )
        .unwrap();
        w.try_enqueue("{\"n\":1}".into());
        std::thread::sleep(Duration::from_millis(300));
        w.try_enqueue("{\"n\":2}".into());

        let rx = w.flush_begin().unwrap();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(2)),
            Ok(true),
            "flush must acknowledge while suspended"
        );
        assert_eq!(
            transport.requests().len(),
            1,
            "nothing is delivered while suspended"
        );
        // Must return promptly despite the 30s suspension.
        w.shutdown(Duration::from_secs(2));
    }

    #[test]
    fn test_budget_drops_the_oldest_retained_batch_first() {
        // A permanently throttled endpoint retains its head batch forever; the
        // budget must still let newer events in by shedding the stalest.
        let transport = Arc::new(TestTransport::new());
        for _ in 0..50 {
            transport.respond_with(429);
        }
        let drops = Arc::new(DropCounter::new("events"));
        let w = spawn(
            transport.clone(),
            EventsConfig {
                batch_size: 2,
                queue_size: 4,
                flush_interval: Duration::from_millis(50),
                ..cfg()
            },
            drops.clone(),
        )
        .unwrap();
        for i in 0..40 {
            w.try_enqueue(format!("{{\"n\":{i}}}"));
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            drops.pending() > 0,
            "the budget must shed data rather than grow without bound"
        );
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
        let w = spawn(
            transport.clone(),
            EventsConfig {
                batch_size: 1,
                ..cfg()
            },
            Arc::new(DropCounter::new("events")),
        )
        .unwrap();
        w.try_enqueue("{\"n\":1}".into());
        std::thread::sleep(Duration::from_millis(200));
        assert!(w.try_enqueue("{\"n\":2}".into()));
        let rx = w.flush_begin().unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)), Ok(true));
        assert!(transport.calls.load(Ordering::SeqCst) >= 2);
        w.shutdown(Duration::from_secs(5));
    }

    /// Builds a worker without spawning it, so a test can drive `accept` and
    /// inspect the state directly. The run loop is never entered.
    fn direct_worker(queue_size: usize) -> (EventsWorker, Sender<String>, Arc<DropCounter>) {
        let (tx, rx) = bounded(queue_size);
        let (_control_tx, control_rx) = unbounded();
        let drops = Arc::new(DropCounter::new("events"));
        let worker = EventsWorker {
            transport: Arc::new(TestTransport::new()),
            events: rx,
            control: control_rx,
            drops: drops.clone(),
            cfg: EventsConfig {
                queue_size,
                batch_size: 1000,
                flush_interval: Duration::from_secs(3600),
                max_retries: 3,
                suspend_interval: Duration::from_secs(30),
            },
            current: Vec::new(),
            current_bytes: 0,
            deadline: None,
            retry_at: None,
            retries: VecDeque::new(),
            outstanding: 0,
            throttle: 0,
            stopped: false,
        };
        (worker, tx, drops)
    }

    #[test]
    fn test_the_channel_backlog_counts_against_the_budget() {
        // Regression: the budget counted only the current batch and the retry
        // queue, so the bounded channel could hold a second full queue's worth
        // and the real ceiling was 2 x events_queue_size. Delivered counts
        // cannot see this — the excess is transient memory — so drive `accept`
        // directly with a full channel and assert the event is refused.
        let (mut worker, tx, drops) = direct_worker(4);
        for i in 0..4 {
            tx.try_send(format!("{{\"n\":{i}}}")).unwrap();
        }
        assert_eq!(worker.outstanding, 0, "the worker itself holds nothing yet");

        worker.accept("{\"n\":\"extra\"}".into());

        assert_eq!(
            worker.current.len(),
            0,
            "the pipeline already owns events_queue_size events in the channel"
        );
        assert_eq!(drops.pending(), 1, "the refused event must be counted");
    }

    #[test]
    fn test_a_full_batch_does_not_bypass_an_active_backoff() {
        // Regression: every count/byte trigger called send_pending directly, so
        // a throttled endpoint was retried at exactly the rate that provoked
        // the 429 instead of waiting out the backoff.
        let transport = Arc::new(TestTransport::new());
        for _ in 0..50 {
            transport.respond_with(429);
        }
        let w = worker(
            transport.clone(),
            EventsConfig {
                batch_size: 1, // every event cuts a batch
                flush_interval: Duration::from_millis(400),
                ..cfg()
            },
        );
        for i in 0..10 {
            w.try_enqueue(format!("{{\"n\":{i}}}"));
        }
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(
            transport.requests().len(),
            1,
            "the backoff must hold until retry_at, however many batches pile up"
        );
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_flush_reports_false_while_a_batch_is_still_retained() {
        // Head-of-line blocking means a retained batch stops everything behind
        // it, so acknowledging `true` would claim a barrier that did not happen.
        let transport = Arc::new(TestTransport::new());
        for _ in 0..10 {
            transport.respond_with(500);
        }
        let w = worker(
            transport.clone(),
            EventsConfig {
                batch_size: 1,
                ..cfg()
            },
        );
        w.try_enqueue("{\"n\":1}".into());
        let rx = w.flush_begin().unwrap();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(5)),
            Ok(false),
            "an undelivered batch must not be reported as flushed"
        );
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_shutdown_counts_what_it_could_not_deliver() {
        // Regression: batches still retained when the worker stopped were
        // destroyed with it and never counted, so the shutdown summary
        // understated the loss.
        let transport = Arc::new(TestTransport::new());
        for _ in 0..50 {
            transport.respond_with(500);
        }
        let drops = Arc::new(DropCounter::new("events"));
        let w = spawn(
            transport.clone(),
            EventsConfig {
                batch_size: 1,
                flush_interval: Duration::from_secs(3600),
                ..cfg()
            },
            drops.clone(),
        )
        .unwrap();
        for i in 0..3 {
            w.try_enqueue(format!("{{\"n\":{i}}}"));
        }
        w.shutdown(Duration::from_secs(5));
        assert_eq!(
            drops.reported(),
            3,
            "every undelivered event must be counted before the worker exits"
        );
    }

    #[test]
    fn test_an_unrepresentable_flush_interval_does_not_panic() {
        // `Instant::now() + Duration::MAX` panics; config validation accepts a
        // huge interval, so the worker has to cope with one.
        let transport = Arc::new(TestTransport::new());
        let w = worker(
            transport.clone(),
            EventsConfig {
                batch_size: 1000,
                flush_interval: Duration::MAX,
                ..cfg()
            },
        );
        assert!(w.try_enqueue("{\"n\":1}".into()));
        let rx = w.flush_begin().unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)), Ok(true));
        assert_eq!(lines(&transport).concat().len(), 1);
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_transport_delivery_runs_with_panic_reporting_suppressed() {
        // Without the guard, a panicking transport trips our own panic hook,
        // which reports the panic by calling that same transport again — a
        // panic inside the panic hook, which aborts the process. The scenario
        // needs the global hook installed, so assert the guard directly.
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
        let w = spawn(
            transport.clone(),
            EventsConfig {
                batch_size: 1,
                ..cfg()
            },
            Arc::new(DropCounter::new("events")),
        )
        .unwrap();
        w.try_enqueue("{\"n\":1}".into());
        let rx = w.flush_begin().unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)), Ok(true));
        assert!(
            transport.suppressed.load(Ordering::SeqCst),
            "delivery must run under the panic-suppression guard"
        );
        w.shutdown(Duration::from_secs(5));
    }

    #[test]
    fn test_enqueue_after_shutdown_returns_false() {
        let transport = Arc::new(TestTransport::new());
        let w = worker(transport, cfg());
        w.shutdown(Duration::from_secs(5));
        assert!(!w.try_enqueue("{\"n\":1}".into()));
    }
}
