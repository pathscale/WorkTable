//! Measures the in-process sequencing/event cost of persisted ART indexes.
//!
//! Disk encoding and I/O happen in WorkTable's persistence task and are not
//! included here. This probe isolates the synchronous mutation-front-end cost:
//! `cargo run --release --example art_persistence_overhead_probe`.

use std::hint::black_box;
use std::time::{Duration, Instant};

use worktable::prelude::{
    ArcticIndex, CongeeIndex, Link, OffsetEqLink, PersistentArcticIndex, PersistentCongeeIndex, TableIndexCdc,
    UniqueIndex,
};

const POPULATION: u64 = 65_536;
const READ_OPERATIONS: u64 = 4_000_000;
const MUTATION_PAIRS: u64 = 250_000;
const TRIALS: usize = 10;
const DATA_LENGTH: usize = 4_096;

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

fn link(operation: u64) -> Link {
    Link {
        page_id: ((operation / DATA_LENGTH as u64) as u32).into(),
        offset: (operation % DATA_LENGTH as u64) as u32,
        length: 8,
    }
}

fn point_read_trial<I>(index: &I, trial: usize) -> f64
where
    I: UniqueIndex<u64, OffsetEqLink<DATA_LENGTH>>,
{
    nanoseconds_per(
        measure(|| {
            for operation in 0..READ_OPERATIONS {
                let key = operation.wrapping_mul(1_103_515_245).wrapping_add(trial as u64) & (POPULATION - 1);
                black_box(index.get_value(black_box(&key)));
            }
        }),
        READ_OPERATIONS,
    )
}

fn cdc_mutation_trial<I>(index: &I, trial: usize) -> f64
where
    I: TableIndexCdc<u64>,
{
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

fn paired_reads<I, P>(native: &I, persisted: &P) -> (Vec<f64>, Vec<f64>)
where
    I: UniqueIndex<u64, OffsetEqLink<DATA_LENGTH>>,
    P: UniqueIndex<u64, OffsetEqLink<DATA_LENGTH>>,
{
    let mut native_results = Vec::with_capacity(TRIALS);
    let mut persisted_results = Vec::with_capacity(TRIALS);
    for trial in 0..TRIALS {
        if trial % 2 == 0 {
            native_results.push(point_read_trial(native, trial));
            persisted_results.push(point_read_trial(persisted, trial));
        } else {
            persisted_results.push(point_read_trial(persisted, trial));
            native_results.push(point_read_trial(native, trial));
        }
    }
    (native_results, persisted_results)
}

fn paired_mutations<I, P>(native: &I, persisted: &P) -> (Vec<f64>, Vec<f64>)
where
    I: TableIndexCdc<u64>,
    P: TableIndexCdc<u64>,
{
    let mut native_results = Vec::with_capacity(TRIALS);
    let mut persisted_results = Vec::with_capacity(TRIALS);
    for trial in 0..TRIALS {
        if trial % 2 == 0 {
            native_results.push(cdc_mutation_trial(native, trial));
            persisted_results.push(cdc_mutation_trial(persisted, trial));
        } else {
            persisted_results.push(cdc_mutation_trial(persisted, trial));
            native_results.push(cdc_mutation_trial(native, trial));
        }
    }
    (native_results, persisted_results)
}

fn congee() {
    let native = CongeeIndex::<u64, OffsetEqLink<DATA_LENGTH>>::default();
    let persisted = PersistentCongeeIndex::<u64, OffsetEqLink<DATA_LENGTH>>::default();
    for key in 0..POPULATION {
        let value = OffsetEqLink(link(key));
        native.insert_value(key, value);
        persisted.insert_value(key, value);
    }

    let (native_reads, persisted_reads) = paired_reads(&native, &persisted);
    summarize("congee native point read", native_reads);
    summarize("congee persisted point read", persisted_reads);

    let (native_mutations, persisted_mutations) = paired_mutations(&native, &persisted);
    summarize("congee native CDC mutation", native_mutations);
    summarize("congee persisted CDC mutation", persisted_mutations);
}

fn arctic() {
    let native = ArcticIndex::<u64, OffsetEqLink<DATA_LENGTH>>::default();
    let persisted = PersistentArcticIndex::<u64, OffsetEqLink<DATA_LENGTH>>::default();
    for key in 0..POPULATION {
        let value = OffsetEqLink(link(key));
        native.insert_value(key, value);
        persisted.insert_value(key, value);
    }

    let (native_reads, persisted_reads) = paired_reads(&native, &persisted);
    summarize("arctic native point read", native_reads);
    summarize("arctic persisted point read", persisted_reads);

    let (native_mutations, persisted_mutations) = paired_mutations(&native, &persisted);
    summarize("arctic native CDC mutation", native_mutations);
    summarize("arctic persisted CDC mutation", persisted_mutations);
}

fn main() {
    congee();
    arctic();
}
