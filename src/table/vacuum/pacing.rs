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
//! release the exclusion between them, and between batches vacuum looks at how
//! much foreground demand it turned away and stands down when the answer is
//! "a lot".

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use smart_default::SmartDefault;

use crate::in_memory::EmptyLinkRegistry;

/// A bit a caller flips to hold vacuum off entirely.
///
/// Separate from the automatic backoff below: that one reacts to measured
/// demand, this one is for a caller who knows something the table cannot see —
/// a bulk load about to start, a latency-sensitive window, a benchmark.
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

    /// Foreground space requests per millisecond above which vacuum stands
    /// down instead of taking the exclusion again.
    ///
    /// An idle table reads zero. A table under the insert load in
    /// `wt-benchmarks` reads in the hundreds, so the threshold does not need
    /// to be delicate to separate the two.
    #[default = 50]
    pub busy_demand_per_ms: u64,

    /// How long to stand down for when the table is busy.
    #[default(Duration::from_millis(2))]
    pub backoff: Duration,

    /// Ceiling for the stand-down, which doubles each consecutive time.
    ///
    /// A fixed backoff is the wrong shape for sustained load. Sixteen 2ms
    /// stand-downs is a 32ms pause and then a batch regardless, so a table
    /// under a writer doing hundreds of inserts per millisecond still gets a
    /// steady grind of row moves and pays for them. Doubling turns sustained
    /// pressure into a low duty cycle instead: a burst costs a few
    /// milliseconds, an hour of load costs a sweep every fraction of a second,
    /// and the bound on consecutive stand-downs still guarantees the sweep
    /// eventually proceeds.
    #[default(Duration::from_millis(128))]
    pub max_backoff: Duration,

    /// How many times in a row to stand down before proceeding anyway.
    ///
    /// Without this a permanently busy table would never be vacuumed, which
    /// trades a bounded slowdown for an unbounded one: fragmentation that is
    /// never reclaimed makes every later insert allocate.
    #[default = 16]
    pub max_consecutive_backoffs: u32,
}

/// Demand measured across one batch.
///
/// Sampling is free: the counter is read at both ends of work that was
/// happening anyway, so deciding costs no added latency. A separate
/// observation window would have added its own delay to every batch.
pub(crate) struct BatchDemand {
    started: Instant,
    attempts_at_start: u64,
}

impl BatchDemand {
    pub(crate) fn start<const N: usize>(registry: &EmptyLinkRegistry<N>) -> Self {
        Self {
            started: Instant::now(),
            attempts_at_start: registry.pop_attempts(),
        }
    }

    /// Foreground space requests per millisecond over the batch.
    pub(crate) fn per_ms<const N: usize>(&self, registry: &EmptyLinkRegistry<N>) -> u64 {
        let attempts = registry.pop_attempts().saturating_sub(self.attempts_at_start);
        let elapsed_ms = self.started.elapsed().as_secs_f64() * 1_000.0;
        if elapsed_ms <= 0.0 {
            return 0;
        }
        (attempts as f64 / elapsed_ms) as u64
    }
}

impl VacuumPacing {
    /// The nth consecutive stand-down, doubling and capped.
    ///
    /// Saturating rather than shifting by `n`, so a long stand-down streak
    /// cannot overflow the duration into something absurd.
    fn backoff_for(&self, consecutive: u32) -> Duration {
        let doublings = consecutive.saturating_sub(1).min(20);
        self.backoff.saturating_mul(1u32 << doublings).min(self.max_backoff)
    }

    /// Called at a batch boundary, with the exclusion already released.
    ///
    /// Returns once vacuum should take the exclusion again. Yields at least
    /// once even on an idle table, so a waiting insert gets the registry
    /// before vacuum asks for it back.
    pub(crate) async fn wait_until_quiet<const N: usize>(
        &self,
        registry: &EmptyLinkRegistry<N>,
        gate: &VacuumGate,
        demand_per_ms: u64,
    ) {
        tokio::task::yield_now().await;

        let mut busy = demand_per_ms >= self.busy_demand_per_ms;
        let mut stood_down = 0;
        while (busy || gate.is_paused()) && stood_down < self.max_consecutive_backoffs {
            gate.note_stand_down();
            stood_down += 1;

            // The backoff doubles as the observation window, so re-measuring
            // costs nothing beyond the wait already being taken. Trusting the
            // reading that sent us here instead would keep standing down long
            // after a burst had passed.
            let sample = BatchDemand::start(registry);
            tokio::time::sleep(self.backoff_for(stood_down)).await;
            busy = sample.per_ms(registry) >= self.busy_demand_per_ms;
        }
    }
}
