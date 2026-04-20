use criterion::Criterion;
use std::time::Duration;

pub fn configure_criterion() -> Criterion {
    Criterion::default()
        .sample_size(300)
        .measurement_time(Duration::from_secs(10))
        .warm_up_time(Duration::from_secs(5))
}