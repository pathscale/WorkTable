//! Small repeatable probe for ART point-operation regressions.
//!
//! This is deliberately separate from publication benchmarks. Run it several
//! times on the same quiet machine and compare revisions with identical build
//! flags:
//!
//! `cargo run --release --example art_backend_probe`

use std::hint::black_box;
use std::time::{Duration, Instant};

use worktable::prelude::{ArcticIndex, CongeeIndex, UniqueIndex};

const POPULATION: u64 = 65_536;
const READ_OPERATIONS: u64 = 4_000_000;
const MUTATION_PAIRS: u64 = 250_000;
const TRIALS: usize = 10;

fn measure(operation: impl Fn()) -> Duration {
    let started = Instant::now();
    operation();
    started.elapsed()
}

fn nanoseconds_per(duration: Duration, operations: u64) -> f64 {
    duration.as_secs_f64() * 1_000_000_000.0 / operations as f64
}

fn summarize(name: &str, mut values: Vec<f64>) {
    values.sort_by(f64::total_cmp);
    let median = (values[TRIALS / 2 - 1] + values[TRIALS / 2]) / 2.0;
    println!("{name:24} median {median:8.3} ns/op  trials {values:?}");
}

fn probe_congee() {
    let index = CongeeIndex::<u64, u64>::default();
    for key in 0..POPULATION {
        index.insert_value(key, key ^ 0x5a5a_5a5a);
    }

    // Warm instruction/data caches before collecting whole-run averages.
    for key in 0..POPULATION {
        black_box(index.get_value(black_box(&key)));
    }

    let reads = (0..TRIALS)
        .map(|trial| {
            nanoseconds_per(
                measure(|| {
                    for operation in 0..READ_OPERATIONS {
                        let key = operation.wrapping_mul(1_103_515_245).wrapping_add(trial as u64) & (POPULATION - 1);
                        black_box(index.get_value(black_box(&key)));
                    }
                }),
                READ_OPERATIONS,
            )
        })
        .collect();
    summarize("congee point read", reads);

    let mutations = (0..TRIALS)
        .map(|trial| {
            nanoseconds_per(
                measure(|| {
                    for operation in 0..MUTATION_PAIRS {
                        let key = POPULATION + operation + trial as u64 * MUTATION_PAIRS;
                        black_box(index.insert_value(black_box(key), black_box(key)));
                        black_box(index.remove_value(black_box(&key)));
                    }
                }),
                MUTATION_PAIRS * 2,
            )
        })
        .collect();
    summarize("congee insert/remove", mutations);
}

fn probe_arctic() {
    let index = ArcticIndex::<u64, u64>::default();
    for key in 0..POPULATION {
        index.insert_value(key, key ^ 0x5a5a_5a5a);
    }

    for key in 0..POPULATION {
        black_box(index.get_value(black_box(&key)));
    }

    let reads = (0..TRIALS)
        .map(|trial| {
            nanoseconds_per(
                measure(|| {
                    for operation in 0..READ_OPERATIONS {
                        let key = operation.wrapping_mul(1_103_515_245).wrapping_add(trial as u64) & (POPULATION - 1);
                        black_box(index.get_value(black_box(&key)));
                    }
                }),
                READ_OPERATIONS,
            )
        })
        .collect();
    summarize("arctic point read", reads);

    let mutations = (0..TRIALS)
        .map(|trial| {
            nanoseconds_per(
                measure(|| {
                    for operation in 0..MUTATION_PAIRS {
                        let key = POPULATION + operation + trial as u64 * MUTATION_PAIRS;
                        black_box(index.insert_value(black_box(key), black_box(key)));
                        black_box(index.remove_value(black_box(&key)));
                    }
                }),
                MUTATION_PAIRS * 2,
            )
        })
        .collect();
    summarize("arctic insert/remove", mutations);
}

fn main() {
    probe_congee();
    probe_arctic();
}
