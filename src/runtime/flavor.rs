//! The WorkTable runtime registry: every pool a table can dispatch to.
//!
//! # One table, one byte, one spelling
//!
//! [`Flavor`] is the definition the `runtime:` DSL key, the
//! `WT_DEFAULT_RUNTIME` environment variable, the results tables and the docs
//! all refer to. Adding a flavor means adding a row to the enum, an arm to
//! [`Flavor::tuning`] and an arm to [`Flavor::from_name`], and nothing else.
//!
//! # Why a byte, and not a type parameter
//!
//! The hot path reads it. `spawn` resolves a flavor to a pool on every call,
//! so the representation of a flavor is a cost the engine pays per spawned
//! task. One `#[repr(u8)]` discriminant makes the comparison a single `cmp`
//! and the pool lookup an array index; see [`crate::runtime::NagoyaRt`] for
//! what it replaced, which was a process-wide mutex and a linear scan over
//! `Tuning` structs compared field by field.
//!
//! A type parameter cannot do the job on its own regardless.
//! `WT_DEFAULT_RUNTIME` is read at run time, so the selection has to exist as
//! a value; a generic on top of that would be a second mechanism for the same
//! thing. The type parameter stays because a schema names its flavor at
//! compile time and `F::FLAVOR` then folds to a constant, but the value is
//! what everything downstream carries.
//!
//! # The discriminants are stable
//!
//! They appear in results tables and in `WT_DEFAULT_RUNTIME`. Renumbering
//! them silently rewrites history, so they are written out rather than left
//! to the compiler.

use crate::runtime::Tuning;

/// Every runtime WorkTable can dispatch to.
///
/// One byte, `Copy`, so a comparison is a single `cmp` and a lookup is an
/// array index.
///
/// | # | flavor | spelling | what it does |
/// |---|---|---|---|
/// | 0 | [`Locality`](Flavor::Locality) | `nagoya(locality)` | keeps a woken task on the worker that woke it |
/// | 1 | [`Spread`](Flavor::Spread) | `nagoya(spread)` | forwards every wake to the injector |
/// | 2 | [`Throughput`](Flavor::Throughput) | `nagoya(throughput)` | spread, plus a fatter injector trip |
/// | 3 | [`LowLatency`](Flavor::LowLatency) | `nagoya(low_latency)` | looks again eight times sooner |
/// | 4 | [`WideInjector`](Flavor::WideInjector) | `nagoya(wide_injector)` | one long intake trip, for chunky submissions |
/// | 5 | [`SharedSlot`](Flavor::SharedSlot) | `nagoya(shared_slot)` | locality, but at most one task stays private |
///
/// Discriminants 6 to 9 are reserved for the flavors that need a scheduler
/// mechanism ps-st3 does not expose yet, so that adding one later does not
/// renumber the five above. See [`RESERVED`].
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub enum Flavor {
    /// Keep a woken task on the worker that woke it.
    ///
    /// `local_wakes: true`, `injector_batch: 1`. The default, and what
    /// `nagoya::runtime::background()` already runs with. For work whose wakes
    /// are a chain: an update path handing a row lock to its successor wants
    /// the lines the releasing worker just touched.
    #[default]
    Locality = 0,
    /// Send every wake to the injector, where any worker can take it.
    ///
    /// `local_wakes: false`. For work whose wakes are independent, which is
    /// what read-mostly and insert-mostly tables look like.
    Spread = 1,
    /// Fewer, larger trips to the injector.
    ///
    /// `local_wakes: false`, `injector_batch: 8`. For a firehose of short
    /// independent operations submitted from outside the pool, where the trip
    /// to the shared queue is the cost.
    Throughput = 2,
    /// Locality, but a worker waits an eighth as long between empty looks.
    ///
    /// `backoff_spins: 128` rather than the 1024 default. Buys wake latency
    /// and spends CPU: a worker that looks eight times as often takes the
    /// cache lines the producer is trying to fill, so this is the flavor whose
    /// `cpu_x` has to be reported next to its throughput or the number means
    /// nothing.
    LowLatency = 3,
    /// One long trip to the injector, for work submitted in chunks.
    ///
    /// `injector_batch: 32`. The opposite trade to [`Throughput`](Flavor::Throughput)'s
    /// eight: a batch is a job's exposure to whatever its worker takes private
    /// and then sits on, so this wins only where submissions are already
    /// chunky and uniform.
    WideInjector = 4,
    /// Locality's routing, but at most one task stays private to a worker.
    ///
    /// A job displaced from a worker's LIFO slot goes to the injector rather
    /// than to a private inbox behind it, so it is reachable by any worker.
    ///
    /// This is the flavor for read-mostly work with independent tasks. Under
    /// [`Locality`](Flavor::Locality) such work can pile several self-waking
    /// tasks onto one worker and keep them there, because neither the slot nor
    /// the inbox is stealable and the heartbeat that would share them is
    /// starved by the slot itself. Measured on sixteen workers with eight
    /// read-only client tasks: four runs in eight collapsed to a single active
    /// worker at exactly the one-thread rate.
    ///
    /// It is not a strict improvement, which is why it is a flavor and not a
    /// fix. Every displacement costs an injector push and a wake, and on work
    /// that contends on row locks there is no parallelism to win and the churn
    /// is pure cost: a 50% update workload fell from 943,606 to 651,554 while
    /// cores busy went from 1.13 to 13.16.
    SharedSlot = 5,
}

/// How many flavors there are, and the length of the executor table.
pub const FLAVOR_COUNT: usize = 6;

/// Discriminants held back for the flavors that need ps-st3 to grow a
/// mechanism first, recorded so a later release does not renumber the ones
/// above.
///
/// | # | name | needs |
/// |---|---|---|
/// | 6 | `stealable_deque` | local wakes onto the stealable deque, not a private slot |
/// | 7 | `self_wake` | only a task's *own* wake stays local |
/// | 8 | `idle_gated` | stay local only when no worker is idle |
/// | 9 | `bounded_steal` | a cap on how many workers sweep for work at once |
///
/// All five are value-level policy in principle and scheduler code in
/// practice, so each one is a ps-st3 release rather than a `Tuning` field this
/// crate can set.
pub const RESERVED: &[(u8, &str)] = &[
    (6, "stealable_deque"),
    (7, "self_wake"),
    (8, "idle_gated"),
    (9, "bounded_steal"),
];

impl Flavor {
    /// Every flavor, in discriminant order.
    ///
    /// The one place that enumerates them, so a `match` that gains an arm and
    /// a loop that does not cannot disagree.
    pub const ALL: [Flavor; FLAVOR_COUNT] = [
        Flavor::Locality,
        Flavor::Spread,
        Flavor::Throughput,
        Flavor::LowLatency,
        Flavor::WideInjector,
        Flavor::SharedSlot,
    ];

    /// The spelling that selects this flavor.
    ///
    /// The same word in `runtime: nagoya(spread)`, in
    /// `WT_DEFAULT_RUNTIME=nagoya(spread)` and in a results row, so the three
    /// cannot drift apart.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Flavor::Locality => "locality",
            Flavor::Spread => "spread",
            Flavor::Throughput => "throughput",
            Flavor::LowLatency => "low_latency",
            Flavor::WideInjector => "wide_injector",
            Flavor::SharedSlot => "shared_slot",
        }
    }

    /// The flavor a spelling selects.
    ///
    /// A name held back in [`RESERVED`] is rejected with what it is waiting
    /// for rather than as an unknown word, because those are different
    /// mistakes and want different next steps from the reader.
    ///
    /// # Errors
    ///
    /// The unrecognised name, and what was expected.
    pub fn from_name(name: &str) -> Result<Self, alloc::string::String> {
        use alloc::string::ToString as _;

        for flavor in Flavor::ALL {
            if flavor.name() == name {
                return Ok(flavor);
            }
        }
        for (discriminant, reserved) in RESERVED {
            if *reserved == name {
                return Err(alloc::format!(
                    "nagoya flavor `{name}` is reserved as discriminant {discriminant} but not implemented: it \
                     needs a scheduler mechanism ps-st3 does not expose yet"
                ));
            }
        }
        let known = Flavor::ALL.map(Flavor::name).join("`, `");
        Err(alloc::format!("unknown nagoya flavor `{name}`; expected one of `{known}`").to_string())
    }

    /// The idle policy the pool for this flavor runs with, after any
    /// environment overrides.
    ///
    /// See [`tuning_overrides`] for the four knobs and why they exist.
    #[cfg(feature = "std")]
    #[must_use]
    pub fn tuned(self) -> Tuning {
        tuning_overrides(self.tuning())
    }

    /// The idle policy the pool for this flavor runs with.
    ///
    /// Built from a preset and then overridden rather than written as a
    /// literal: [`Tuning`] is `#[non_exhaustive]` from ps-st3 0.6, so a field
    /// it gains later is not a breaking change and this function does not have
    /// to be edited again.
    #[must_use]
    pub fn tuning(self) -> Tuning {
        match self {
            Flavor::Locality => Tuning::locality(),
            Flavor::Spread => Tuning::spread(),
            Flavor::Throughput => Tuning::throughput(),
            // Locality's wake routing, with the *shape* of the idle policy
            // changed and its total length held constant.
            //
            // `backoff_spins` alone was wrong and the failure was not subtle.
            // A worker parks after `rounds_before_park` empty rounds of
            // `backoff_spins` each, so dropping the spins from 1024 to 128
            // does not only make a worker look more often, it makes it park
            // **eight times sooner in wall-clock time**. Workers were then
            // asleep during the window when client tasks arrive, the tasks
            // concentrated onto whichever worker was awake, and a private LIFO
            // slot is not stealable, so they stayed there. Measured: YCSB C
            // fell to 2,665,801 at exactly one core busy, against 13,049,077
            // for locality.
            //
            // 512 rounds of 128 spins is the same 65,536 spins before parking
            // as 64 rounds of 1024. The worker looks eight times as often,
            // which is the whole point, and sleeps no sooner, which was never
            // the point.
            Flavor::LowLatency => Tuning::locality().with_backoff_spins(128).with_rounds_before_park(512),
            // Spread's wake routing, because a wide intake is pointless if a
            // wake never reaches the injector to be batched with anything.
            Flavor::WideInjector => Tuning::spread().with_injector_batch(32),
            // Locality's routing exactly, with only the overflow policy
            // changed. Changing the wake routing too would make it a second
            // spelling of `spread` rather than a third point between them.
            Flavor::SharedSlot => Tuning::locality().with_share_displaced(true).with_lifo_run_limit(4),
        }
    }
}

impl core::fmt::Display for Flavor {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(formatter, "nagoya({})", self.name())
    }
}

/// The process-level flavor override, or `None` if `WT_DEFAULT_RUNTIME` is
/// unset.
///
/// Read and parsed **once**, on the first call, and never again:
/// `std::env::var` allocates and must not appear below the setup path. Every
/// later call is one acquire load and a branch.
///
/// The variable takes the same spelling as the DSL key, with the backend
/// optional: `nagoya(spread)` and `spread` both select
/// [`Flavor::Spread`](Flavor::Spread).
///
/// # Resolution order
///
/// 1. a flavor passed at construction
/// 2. `WT_DEFAULT_RUNTIME`
/// 3. the table's declared `runtime:`
/// 4. [`Flavor::Locality`]
///
/// An env override outranks the declared flavor deliberately: it is what lets
/// one benchmark binary sweep every flavor with no rebuild, which is the only
/// way to interleave arms inside a single process. A silent override is a
/// debugging trap, so anything that resolves a flavor should print what it
/// resolved.
///
/// # Panics
///
/// On an unparseable value. A benchmark arm that silently fell back to the
/// default is the easiest possible way to publish a wrong table, and it has
/// happened on this project already, so a typo fails loudly at startup rather
/// than quietly at the top of a results column.
#[cfg(feature = "std")]
#[inline]
pub fn env_override() -> Option<Flavor> {
    static SELECTED: std::sync::OnceLock<Option<Flavor>> = std::sync::OnceLock::new();
    *SELECTED.get_or_init(|| {
        let raw = std::env::var("WT_DEFAULT_RUNTIME").ok()?;
        Some(parse_selection(raw.trim()).unwrap_or_else(|error| {
            panic!("WT_DEFAULT_RUNTIME={raw:?} is not a runtime selection: {error}");
        }))
    })
}

/// `nagoya(spread)`, or the bare `spread`.
///
/// # Errors
///
/// What was wrong with the spelling.
#[cfg(feature = "std")]
pub fn parse_selection(source: &str) -> Result<Flavor, alloc::string::String> {
    let inner = match source.split_once('(') {
        Some((backend, rest)) => {
            let backend = backend.trim();
            if backend != "nagoya" {
                return Err(alloc::format!(
                    "`{backend}` is not a flavored backend; only `nagoya` takes a flavor"
                ));
            }
            rest.strip_suffix(')')
                .ok_or_else(|| alloc::string::String::from("missing the closing parenthesis"))?
        }
        None => source,
    };
    Flavor::from_name(inner.trim())
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::{FLAVOR_COUNT, Flavor, RESERVED, parse_selection};

    #[test]
    fn discriminants_are_the_ones_written_down() {
        // These appear in results tables. Renumbering them rewrites history,
        // so the values are asserted rather than left to the compiler.
        assert_eq!(Flavor::Locality as u8, 0);
        assert_eq!(Flavor::Spread as u8, 1);
        assert_eq!(Flavor::Throughput as u8, 2);
        assert_eq!(Flavor::LowLatency as u8, 3);
        assert_eq!(Flavor::WideInjector as u8, 4);
        assert_eq!(Flavor::SharedSlot as u8, 5);
    }

    #[test]
    fn a_flavor_is_one_byte() {
        assert_eq!(size_of::<Flavor>(), 1);
        assert_eq!(
            size_of::<Option<Flavor>>(),
            1,
            "the niche is worth having on the hot path"
        );
    }

    #[test]
    fn all_is_every_variant_in_discriminant_order() {
        assert_eq!(Flavor::ALL.len(), FLAVOR_COUNT);
        for (index, flavor) in Flavor::ALL.into_iter().enumerate() {
            assert_eq!(
                flavor as usize, index,
                "{flavor} is out of order, so the array lookup would miss"
            );
        }
    }

    #[test]
    fn every_name_round_trips() {
        for flavor in Flavor::ALL {
            assert_eq!(Flavor::from_name(flavor.name()).unwrap(), flavor);
        }
    }

    #[test]
    fn names_are_distinct() {
        let mut names = Flavor::ALL.map(Flavor::name).to_vec();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(names.len(), before, "two flavors share a spelling");
    }

    #[test]
    fn a_reserved_name_says_what_it_is_waiting_for() {
        for (_, reserved) in RESERVED {
            let error = Flavor::from_name(reserved).unwrap_err();
            assert!(error.contains("reserved"), "{error}");
            assert!(error.contains("ps-st3"), "{error}");
        }
    }

    #[test]
    fn a_reserved_discriminant_is_not_in_use() {
        for (discriminant, _) in RESERVED {
            assert!(
                Flavor::ALL.iter().all(|flavor| *flavor as u8 != *discriminant),
                "discriminant {discriminant} is both reserved and in use"
            );
        }
    }

    #[test]
    fn an_unknown_name_lists_what_would_have_worked() {
        let error = Flavor::from_name("banana").unwrap_err();
        for flavor in Flavor::ALL {
            assert!(error.contains(flavor.name()), "{} missing from: {error}", flavor.name());
        }
    }

    #[test]
    fn the_env_spelling_is_the_dsl_spelling() {
        for flavor in Flavor::ALL {
            assert_eq!(
                parse_selection(&alloc::format!("nagoya({})", flavor.name())).unwrap(),
                flavor
            );
            assert_eq!(parse_selection(flavor.name()).unwrap(), flavor);
        }
    }

    #[test]
    fn the_env_spelling_tolerates_whitespace() {
        assert_eq!(parse_selection("  nagoya( spread ) ".trim()).unwrap(), Flavor::Spread);
    }

    #[test]
    fn a_non_nagoya_backend_is_rejected_as_one() {
        let error = parse_selection("tokio(spread)").unwrap_err();
        assert!(error.contains("only `nagoya` takes a flavor"), "{error}");
    }

    #[test]
    fn an_unclosed_parenthesis_says_so() {
        let error = parse_selection("nagoya(spread").unwrap_err();
        assert!(error.contains("closing parenthesis"), "{error}");
    }

    #[test]
    fn the_default_is_locality() {
        assert_eq!(Flavor::default(), Flavor::Locality);
    }

    #[test]
    fn display_is_the_dsl_form() {
        assert_eq!(Flavor::Spread.to_string(), "nagoya(spread)");
    }

    /// Each flavor has to select a genuinely different pool, or the sweep is
    /// measuring the same executor under several names. This is the check
    /// that would have caught an arm that fell through to a preset.
    #[test]
    fn no_two_flavors_share_a_tuning() {
        for (index, flavor) in Flavor::ALL.into_iter().enumerate() {
            for other in Flavor::ALL.into_iter().skip(index + 1) {
                assert_ne!(
                    flavor.tuning(),
                    other.tuning(),
                    "{flavor} and {other} are the same pool under two names"
                );
            }
        }
    }

    /// The idle policy changes shape, not length.
    ///
    /// A worker parks after `rounds_before_park * backoff_spins` spins, so
    /// cutting the spins without raising the rounds parks it that much sooner.
    /// That is a different change from the one `low_latency` is asking for,
    /// and it cost YCSB C 79% of its throughput by putting workers to sleep
    /// during the window when client tasks arrive.
    #[test]
    fn low_latency_looks_more_often_without_sleeping_sooner() {
        let base = Flavor::Locality.tuning();
        let fast = Flavor::LowLatency.tuning();
        assert_eq!(fast.backoff_spins, 128);
        assert!(fast.backoff_spins < base.backoff_spins, "it has to look more often");
        assert_eq!(
            u64::from(fast.rounds_before_park) * u64::from(fast.backoff_spins),
            u64::from(base.rounds_before_park) * u64::from(base.backoff_spins),
            "the budget before parking has to be the same, or this is a park-sooner flavor wearing a \
             look-sooner name"
        );
        assert_eq!(fast.local_wakes, base.local_wakes);
        assert_eq!(fast.injector_batch, base.injector_batch);
    }

    /// Both of `shared_slot`'s changes are about the same thing: letting go of
    /// work a worker cannot run soon. Neither touches the wake routing, which
    /// is what would make it a second spelling of `spread`.
    #[test]
    fn shared_slot_changes_only_how_a_worker_lets_go() {
        let base = Flavor::Locality.tuning();
        let shared = Flavor::SharedSlot.tuning();
        assert!(shared.share_displaced);
        assert!(!base.share_displaced, "locality keeps its overflow private");
        assert!(
            shared.lifo_run_limit < base.lifo_run_limit,
            "it has to reach its own queue sooner, or the work it let go of is never promoted"
        );
        assert_eq!(shared.local_wakes, base.local_wakes);
        assert_eq!(shared.backoff_spins, base.backoff_spins);
        assert_eq!(shared.injector_batch, base.injector_batch);
    }

    #[test]
    fn wide_injector_changes_only_the_intake() {
        let base = Flavor::Spread.tuning();
        let wide = Flavor::WideInjector.tuning();
        assert_eq!(wide.injector_batch, 32);
        assert_eq!(wide.local_wakes, base.local_wakes);
        assert_eq!(wide.backoff_spins, base.backoff_spins);
    }
}

/// The four free parameters, overridden per process.
///
/// # Why these are knobs and not flavors
///
/// A flavor is a named point somebody has measured and can recommend. These
/// are the axes those points sit on, and sweeping an axis is how a new point
/// gets found. Turning every value of every axis into a flavor would be four
/// nested loops of names nobody chose.
///
/// So they exist for the sweep, they are read once each, and a run that sets
/// one is not running a flavor any more: it is running an unnamed tuning that
/// happens to start from one. Anything reporting a number from such a run has
/// to say so, which is why [`describe_tuning`] exists.
///
/// | variable | field | default |
/// |---|---|---|
/// | `WT_ROUNDS` | `rounds_before_park` | 64 |
/// | `WT_BACKOFF` | `backoff_spins` | 1024, or 128 under `low_latency` |
/// | `WT_PROMOTE` | `promote_every` | 64 |
/// | `WT_BATCH` | `injector_batch` | per flavor |
/// | `WT_LIFO` | `lifo_run_limit` | 32 |
///
/// The names match the `ROUNDS` / `BACKOFF` / `PROMOTE` / `BATCH` variables
/// the `perf-benchmarks` examples already take, prefixed so they cannot
/// collide with a host's own environment.
#[cfg(feature = "std")]
#[must_use]
pub fn tuning_overrides(base: Tuning) -> Tuning {
    fn read<T: core::str::FromStr>(name: &str) -> Option<T> {
        std::env::var(name).ok()?.trim().parse().ok()
    }
    static OVERRIDES: std::sync::OnceLock<(Option<u32>, Option<u32>, Option<u64>, Option<usize>, Option<u32>)> =
        std::sync::OnceLock::new();
    let (rounds, backoff, promote, batch, lifo) = *OVERRIDES.get_or_init(|| {
        (
            read("WT_ROUNDS"),
            read("WT_BACKOFF"),
            read("WT_PROMOTE"),
            read("WT_BATCH"),
            read("WT_LIFO"),
        )
    });

    let mut tuning = base;
    if let Some(rounds) = rounds {
        tuning = tuning.with_rounds_before_park(rounds);
    }
    if let Some(backoff) = backoff {
        tuning = tuning.with_backoff_spins(backoff);
    }
    if let Some(promote) = promote {
        tuning = tuning.with_promote_every(promote);
    }
    if let Some(batch) = batch {
        tuning = tuning.with_injector_batch(batch);
    }
    if let Some(lifo) = lifo {
        tuning = tuning.with_lifo_run_limit(lifo);
    }
    tuning
}

/// One line naming the pool a run actually used.
///
/// A results row that says only `spread` when `WT_BACKOFF` was set describes a
/// tuning nobody can reproduce from the flavor name, so this prints the fields
/// rather than the label.
#[cfg(feature = "std")]
#[must_use]
pub fn describe_tuning(flavor: Flavor) -> alloc::string::String {
    let tuning = flavor.tuned();
    alloc::format!(
        "nagoya({}) rounds={} backoff={} promote={} batch={} lifo={} local_wakes={} share_displaced={}",
        flavor.name(),
        tuning.rounds_before_park,
        tuning.backoff_spins,
        tuning.promote_every,
        tuning.injector_batch,
        tuning.lifo_run_limit,
        tuning.local_wakes,
        tuning.share_displaced,
    )
}
