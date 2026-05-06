mod cases;
mod common;

use criterion::criterion_main;

criterion_main!(
    cases::simple::simple_benchmarks,
    cases::unique_index::unique_index_benchmarks,
    cases::non_unique_index::non_unique_index_benchmarks,
    cases::full_featured::full_featured_benchmarks,
    cases::update_contention::update_contention_benchmarks,
);
