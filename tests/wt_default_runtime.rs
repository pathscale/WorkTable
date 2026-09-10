//! `WT_DEFAULT_RUNTIME`, end to end.
//!
//! **One test in this file, deliberately.** The selection is cached in a
//! `OnceLock` so that reading it costs one acquire load rather than an
//! allocating `std::env::var` on every spawn, which means the first caller in
//! the process fixes the answer for all of them. A second test here would
//! either race for that slot or silently observe the first one's value, and
//! either way it would be testing the cache rather than the resolution.
//!
//! An integration test is its own binary, so this one owns its process.

use worktable::prelude::{Flavor, engine_flavor, env_override, parse_selection};

#[test]
fn the_environment_selects_the_pool_the_engine_spawns_on() {
    // Before anything has read it. Set here rather than through the test
    // harness so the test is self-contained and the value is visible in the
    // source that asserts on it.
    //
    // SAFETY: this is the first statement of the only test in this binary, so
    // no other thread of this process exists yet to observe the environment
    // concurrently.
    unsafe {
        std::env::set_var("WT_DEFAULT_RUNTIME", "nagoya(spread)");
    }

    assert_eq!(env_override(), Some(Flavor::Spread));

    // The engine's own background work follows the process-level selection,
    // not the locality default it used to be pinned to. A benchmark that
    // moved only its client tasks would otherwise leave the flush loop and
    // the vacuum sweep on a different scheduler, and the arm would describe a
    // stack nobody would ship.
    assert_eq!(engine_flavor(), Flavor::Spread);

    // Reading it again cannot change the answer, which is what makes it safe
    // to call from the hot path.
    assert_eq!(env_override(), Some(Flavor::Spread));

    // And the value really is cached rather than re-read: changing the
    // variable now must not move the selection, or a benchmark could have its
    // arm changed underneath it mid-run.
    //
    // SAFETY: as above, still single-threaded with respect to the environment.
    unsafe {
        std::env::set_var("WT_DEFAULT_RUNTIME", "nagoya(throughput)");
    }
    assert_eq!(env_override(), Some(Flavor::Spread), "the selection is read once");

    // The parser itself is pure and can be exercised freely.
    assert_eq!(parse_selection("nagoya(throughput)").unwrap(), Flavor::Throughput);
    assert_eq!(parse_selection("low_latency").unwrap(), Flavor::LowLatency);
    assert!(parse_selection("nagoya(banana)").is_err());
}
