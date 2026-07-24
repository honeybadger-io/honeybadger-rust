//! Consolidated drop accounting shared by both delivery pipelines.
//!
//! A queue only fills during a storm, which is precisely when one log line per
//! dropped item turns a bad situation into an unreadable one. Counts accumulate
//! here and are summarised at most once a minute, on the next successful
//! delivery and again at shutdown.
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const MIN_LOG_INTERVAL: Duration = Duration::from_secs(60);

pub(crate) struct DropCounter {
    label: &'static str,
    dropped: AtomicU64,
    last_log: Mutex<Option<Instant>>,
    /// Running total of everything ever summarised. Tests assert on this
    /// because `report`/`report_final` clear `dropped` as a side effect, so a
    /// zero `pending` cannot distinguish "counted and reported" from "lost".
    #[cfg(test)]
    reported: AtomicU64,
}

impl DropCounter {
    pub(crate) const fn new(label: &'static str) -> Self {
        DropCounter {
            label,
            dropped: AtomicU64::new(0),
            last_log: Mutex::new(None),
            #[cfg(test)]
            reported: AtomicU64::new(0),
        }
    }

    pub(crate) fn record(&self) {
        self.record_many(1);
    }

    pub(crate) fn record_many(&self, n: u64) {
        self.dropped.fetch_add(n, Ordering::Relaxed);
    }

    /// Emits a summary if anything is pending and the rate limit allows.
    /// A suppressed report leaves the count intact for the next one.
    pub(crate) fn report(&self) -> Option<u64> {
        if self.dropped.load(Ordering::Relaxed) == 0 {
            return None;
        }
        let mut last = self.last_log.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        if let Some(prev) = *last
            && now.duration_since(prev) < MIN_LOG_INTERVAL
        {
            return None;
        }
        *last = Some(now);
        self.emit()
    }

    /// Emits whatever is pending regardless of the rate limit. For shutdown.
    pub(crate) fn report_final(&self) -> Option<u64> {
        let mut last = self.last_log.lock().unwrap_or_else(|e| e.into_inner());
        let emitted = self.emit();
        if emitted.is_some() {
            *last = Some(Instant::now());
        }
        emitted
    }

    fn emit(&self) -> Option<u64> {
        let n = self.dropped.swap(0, Ordering::Relaxed);
        if n == 0 {
            return None;
        }
        #[cfg(test)]
        self.reported.fetch_add(n, Ordering::Relaxed);
        // Deliberately cause-neutral: this counter also records batches the API
        // rejected, batches that aged out of their retries, and data shed by a
        // suspension. Naming one cause would misdiagnose the other three.
        log::warn!("honeybadger: dropped {n} {}", self.label);
        Some(n)
    }

    #[cfg(test)]
    pub(crate) fn pending(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Everything summarised so far, across every report.
    #[cfg(test)]
    pub(crate) fn reported(&self) -> u64 {
        self.reported.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accumulates_and_reports_once_then_rate_limits() {
        let c = DropCounter::new("notices");
        assert_eq!(c.report(), None, "nothing pending, nothing logged");

        c.record();
        c.record_many(4);
        assert_eq!(c.pending(), 5);

        assert_eq!(c.report(), Some(5), "first report emits the running total");
        assert_eq!(c.pending(), 0, "reporting clears the counter");

        // Within the rate-limit window a second report is suppressed, and
        // crucially the count is RETAINED rather than discarded.
        c.record();
        assert_eq!(c.report(), None, "rate limited");
        assert_eq!(c.pending(), 1, "suppressed reports must not lose the count");

        // Shutdown ignores the rate limit so nothing is silently lost.
        assert_eq!(c.report_final(), Some(1));
        assert_eq!(c.pending(), 0);
        assert_eq!(c.report_final(), None, "nothing left to report");
        assert_eq!(c.reported(), 6, "every reported drop is accounted for");
    }
}
