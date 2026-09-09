//! Where the table's own background work runs.
//!
//! # Why the table owns this rather than the caller
//!
//! The vacuum sweep is not the caller's work. A table fragments because of how
//! it is used, it has to be swept whether or not anyone is watching, and a
//! caller should not have to hand over a thread for a job it does not know
//! exists. That is why `run_vacuum_task` used to call `tokio::spawn`: tokio has
//! an implicit global runtime, so the engine could spawn without saying so.
//!
//! It said so anyway, in the dependency graph. `cargo tree
//! --no-default-features -e normal -i tokio` showed tokio linked with every
//! feature off, because that spawn was never gated. The crate's own source was
//! `std`-free and its closure was not, which is the difference between
//! `cargo check --no-default-features` passing and a `no_std` build being real.
//!
//! So the runtime is explicit now, and gated. With `std` the table starts two
//! threads of its own, once, the first time something needs them. Without
//! `std` this module does not exist and neither does the background sweep: a
//! build with no threads cannot have one, and saying that is better than
//! linking a runtime to pretend otherwise.

use nagoya::runtime::Runtime;

/// Threads for the table's background work.
///
/// Two: the vacuum manager's loop and the persistence engine's task. They are
/// long-lived and mostly asleep, so this is a floor rather than a tuning
/// choice, and a table that wants parallel sweeps should say so rather than
/// have it inferred from a constant here.
const BACKGROUND_THREADS: usize = 2;

/// The table's background runtime, started on first use.
///
/// Started lazily because most tables never vacuum and never persist, and a
/// process that opens one should not pay two threads for the possibility.
pub(crate) fn background() -> &'static Runtime {
    static RUNTIME: std::sync::OnceLock<Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| Runtime::new(BACKGROUND_THREADS))
}
