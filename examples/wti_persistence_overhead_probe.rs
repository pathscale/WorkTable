//! Measures WorkTablesIndex's synchronous structural-CDC mutation cost.
//!
//! This is the WTI control for the ART persistence probe. It excludes the
//! asynchronous data/index file writes:
//! `cargo run --release --example wti_persistence_overhead_probe`.

use std::hint::black_box;
use std::time::{Duration, Instant};

use worktable::prelude::{IndexMap, Link, OffsetEqLink, TableIndexCdc, UniqueIndex};

const POPULATION: u64 = 65_536;
const MUTATION_PAIRS: u64 = 250_000;
const TRIALS: usize = 10;
const DATA_LENGTH: usize = 4_096;

fn link(operation: u64) -> Link {
    Link {
        page_id: ((operation / DATA_LENGTH as u64) as u32).into(),
        offset: (operation % DATA_LENGTH as u64) as u32,
        length: 8,
    }
}

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
    println!("{name:30} median {median:8.3} ns/op  trials {values:?}");
}

fn direct_trial(index: &IndexMap<u64, OffsetEqLink<DATA_LENGTH>>, trial: usize) -> f64 {
    nanoseconds_per(
        measure(|| {
            for operation in 0..MUTATION_PAIRS {
                let key = POPULATION + operation + trial as u64 * MUTATION_PAIRS;
                let value = OffsetEqLink(link(operation));
                black_box(index.insert_value(black_box(key), black_box(value)));
                black_box(index.remove_value(black_box(&key)));
            }
        }),
        MUTATION_PAIRS * 2,
    )
}

fn cdc_trial(index: &IndexMap<u64, OffsetEqLink<DATA_LENGTH>>, trial: usize) -> f64 {
    nanoseconds_per(
        measure(|| {
            for operation in 0..MUTATION_PAIRS {
                let key = POPULATION + operation + trial as u64 * MUTATION_PAIRS;
                let value = link(operation);
                black_box(TableIndexCdc::insert_cdc(index, black_box(key), black_box(value)));
                black_box(TableIndexCdc::remove_cdc(index, black_box(key), black_box(value)));
            }
        }),
        MUTATION_PAIRS * 2,
    )
}

fn main() {
    let direct = IndexMap::<u64, OffsetEqLink<DATA_LENGTH>>::default();
    let cdc = IndexMap::<u64, OffsetEqLink<DATA_LENGTH>>::default();
    for key in 0..POPULATION {
        let value = OffsetEqLink(link(key));
        direct.insert_value(key, value);
        cdc.insert_value(key, value);
    }

    let mut direct_results = Vec::with_capacity(TRIALS);
    let mut cdc_results = Vec::with_capacity(TRIALS);
    for trial in 0..TRIALS {
        if trial % 2 == 0 {
            direct_results.push(direct_trial(&direct, trial));
            cdc_results.push(cdc_trial(&cdc, trial));
        } else {
            cdc_results.push(cdc_trial(&cdc, trial));
            direct_results.push(direct_trial(&direct, trial));
        }
    }

    summarize("WTI direct mutation", direct_results);
    summarize("WTI structural CDC mutation", cdc_results);
}
