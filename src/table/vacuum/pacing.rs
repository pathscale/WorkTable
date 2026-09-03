//! Deciding whether now is a good time to vacuum.
//!
//! Vacuum takes the registry's write side for as long as it runs, and while it
//! holds it every insert asking for reclaimable space is turned away and
//! allocates a fresh page instead. Measured on a table at 60% fragmentation
//! that costs 25-49% of insert throughput and doubles median insert latency,
//! for the whole duration of the sweep. At 25% fragmentation the same sweep
//! costs between nothing and 10%.
//!
//! So the cost is not a constant to be scheduled around, it is a function of
//! how much of the table vacuum holds at once and of how busy the table is
//! while it does. Both are addressed here: the sweep is cut into batches that
//! release the exclusion between them, and before every batch vacuum asks the
//! mutation stripes whether foreground work is active or has completed since
//! the preceding check. Every insert, delete and upsert passes through those
//! stripes, including mutations that never ask for reclaimable space.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use smart_default::SmartDefault;

/// A bit a caller flips to hold vacuum off entirely.
///
/// Separate from the automatic backoff below: that one reacts to live mutation
/// activity, this one is for a caller who knows something the table cannot see
/// — a bulk load about to start, a latency-sensitive window, a benchmark.
#[derive(Debug, Default)]
pub struct VacuumGate {
    paused: AtomicBool,
    /// Bumped every time vacuum stood down, so a test can prove the gate is
    /// doing something rather than merely being set.
    stand_downs: AtomicU64,
}

impl VacuumGate {
    /// Hold vacuum off. Takes effect at the next batch boundary; it does not
    /// interrupt a batch in flight, so no move is left half-applied.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Release);
    }

    /// Let vacuum resume.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Release);
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    /// How many batch boundaries vacuum has stood down at.
    pub fn stand_downs(&self) -> u64 {
        self.stand_downs.load(Ordering::Relaxed)
    }

    pub(crate) fn note_stand_down(&self) {
        self.stand_downs.fetch_add(1, Ordering::Relaxed);
    }
}

/// How vacuum paces itself against foreground work.
#[derive(Debug, Clone, SmartDefault)]
pub struct VacuumPacing {
    /// Source pages processed before releasing the exclusion.
    ///
    /// This is the knob that decides how long inserts go without free-space
    /// reuse. Small enough that a foreground burst waits microseconds rather
    /// than the length of a whole-table sweep, large enough that the
    /// re-acquisition is not the dominant cost.
    #[default = 8]
    pub batch_pages: usize,

    /// How long to stand down for when the table is busy. Doubles on each
    /// consecutive stand-down, up to [`Self::max_backoff`], so a table under
    /// sustained load is polled cheaply rather than every couple of
    /// milliseconds.
    #[default(Duration::from_millis(2))]
    pub backoff: Duration,

    /// Ceiling for the doubling.
    #[default(Duration::from_millis(128))]
    pub max_backoff: Duration,

    /// Consecutive samples that must all find the table idle before a sweep
    /// takes the exclusion.
    ///
    /// One sample is not enough: under a heavy write load the stripes are free
    /// for most of any given instant, so a single look finds a gap almost
    /// immediately and the sweep goes in on top of the workload anyway. A short
    /// run of quiet samples distinguishes a gap between two writes from a table
    /// that has actually stopped.
    #[default = 3]
    pub quiet_samples: u32,
}

/// What a sweep asks before taking the exclusion.
///
/// Deliberately a live question rather than a budget. A sweep that defers for
/// a fixed span and then forces itself in is not waiting its turn, it is
/// queueing, and it takes its cut from every burst that outlasts the span:
/// measured against a null arm, 3.8 to 12.5% of foreground throughput. Asking
/// the table what it is doing has no such span to run out.
pub trait ForegroundActivity {
    /// Writers currently inside a mutation gate or queued for one.
    fn mutations_in_flight(&self) -> usize;

    /// Aggregate completion counter for the mutation gates.
    ///
    /// If this changes between polls, foreground work happened even when both
    /// instantaneous reads happened to land between operations.
    fn mutation_epoch(&self) -> u64;
}

impl<LockType, PrimaryKey> ForegroundActivity for crate::lock::LockMap<LockType, PrimaryKey>
where
    PrimaryKey: Clone + std::fmt::Debug + Eq + std::hash::Hash,
{
    fn mutations_in_flight(&self) -> usize {
        crate::lock::LockMap::mutations_in_flight(self)
    }

    fn mutation_epoch(&self) -> u64 {
        crate::lock::LockMap::mutation_epoch(self)
    }
}

impl VacuumPacing {
    /// Called at a batch boundary, with the exclusion already released.
    ///
    /// Returns once vacuum should take the exclusion again. Yields at least
    /// once even on an idle table, so a waiting insert gets the registry
    /// before vacuum asks for it back.
    pub(crate) async fn wait_until_quiet(&self, activity: &impl ForegroundActivity, gate: &VacuumGate) {
        tokio::task::yield_now().await;

        let mut backoff = self.backoff;
        let mut quiet = 0;
        let mut observed_epoch = activity.mutation_epoch();
        loop {
            let current_epoch = activity.mutation_epoch();
            if gate.is_paused() || activity.mutations_in_flight() > 0 || current_epoch != observed_epoch {
                gate.note_stand_down();
                quiet = 0;
                observed_epoch = current_epoch;
                tokio::time::sleep(backoff).await;
                // Doubling, so a table busy for a long time is asked about
                // cheaply rather than every couple of milliseconds.
                backoff = backoff.saturating_mul(2).min(self.max_backoff);
                continue;
            }

            quiet += 1;
            if quiet >= self.quiet_samples {
                return;
            }
            // Idle once is a gap between two writes. Look again, close
            // together, before believing it.
            tokio::time::sleep(self.backoff).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use super::*;

    #[derive(Default)]
    struct ActivityBetweenChecks {
        active: AtomicBool,
        epoch: AtomicU64,
    }

    impl ForegroundActivity for ActivityBetweenChecks {
        fn mutations_in_flight(&self) -> usize {
            0
        }

        fn mutation_epoch(&self) -> u64 {
            if self.active.load(Ordering::Acquire) {
                self.epoch.fetch_add(1, Ordering::AcqRel) + 1
            } else {
                self.epoch.load(Ordering::Acquire)
            }
        }
    }

    #[tokio::test]
    async fn completed_work_between_checks_resets_the_quiet_buffer() {
        let activity = Arc::new(ActivityBetweenChecks::default());
        activity.active.store(true, Ordering::Release);
        let gate = Arc::new(VacuumGate::default());
        let pacing = VacuumPacing {
            backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            quiet_samples: 3,
            ..Default::default()
        };

        let waiting = {
            let activity = Arc::clone(&activity);
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                pacing.wait_until_quiet(activity.as_ref(), gate.as_ref()).await;
            })
        };

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !waiting.is_finished(),
            "activity between snapshots must keep vacuum out"
        );

        activity.active.store(false, Ordering::Release);
        tokio::time::timeout(Duration::from_millis(20), waiting)
            .await
            .expect("vacuum should enter after the recheck buffer stays quiet")
            .unwrap();
    }
}
