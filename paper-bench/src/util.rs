use std::time::{Duration, Instant};

/// Tiny xorshift RNG — no external dep, deterministic across runs.
pub struct Rng(u64);
impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }
    #[inline]
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    #[inline]
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

pub fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

pub fn env_secs(key: &str, default: u64) -> Duration {
    Duration::from_secs(env_u64(key, default))
}

/// Run `f` repeatedly for `reps`, return median ops/sec given per-rep op count.
pub fn median_ops_per_sec<F: FnMut() -> u64>(reps: usize, mut f: F) -> f64 {
    let mut rates: Vec<f64> = (0..reps)
        .map(|_| {
            let t = Instant::now();
            let ops = f();
            ops as f64 / t.elapsed().as_secs_f64()
        })
        .collect();
    rates.sort_by(|a, b| a.partial_cmp(b).unwrap());
    rates[rates.len() / 2]
}
