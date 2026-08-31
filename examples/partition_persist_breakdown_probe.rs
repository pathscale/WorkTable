//! Where the 6.1 ms of persisted-table construction goes.
//!
//! A partitioned store with per-partition persistence pays this cost every
//! time a partition is materialised. At eight tables per partition that is
//! 49 ms per partition group, which makes lazy reload after eviction
//! unaffordable on a query path. Whether that is fixable depends entirely on
//! which phase the time is in: filesystem work can be restructured, engine
//! work cannot.
//!
//! `cargo run --release --example partition_persist_breakdown_probe`

use std::time::{Duration, Instant};

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: Probe,
    persist: true,
    columns: { id: u64 primary_key autoincrement, v: u64, w: u64 },
    indexes: { v_idx: v unique using arctic },
    config: { page_size: 1024 }
);

const N: usize = 200;

fn ms(d: Duration) -> f64 {
    d.as_nanos() as f64 / 1e6
}

fn count_tree(p: &std::path::Path) -> (usize, u64) {
    let (mut files, mut bytes) = (0, 0);
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            let path = e.path();
            if path.is_dir() {
                let (f, b) = count_tree(&path);
                files += f;
                bytes += b;
            } else {
                files += 1;
                bytes += e.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }
    }
    (files, bytes)
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let root = std::env::temp_dir().join(format!("wt-persist-breakdown-{}", std::process::id()));
    std::fs::create_dir_all(&root)?;

    // Warm: first construction pays for lazy statics and a cold page cache.
    {
        let dir = root.join("warm");
        std::fs::create_dir_all(&dir)?;
        let cfg = DiskConfig::new_with_table_name(dir.display().to_string(), "t", 1);
        let engine = ProbePersistenceEngine::new(cfg).await?;
        let _t = ProbeWorkTable::new(engine).await?;
    }

    let (mut mkdir, mut config, mut engine_new, mut table_new) =
        (Duration::ZERO, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let mut held = Vec::with_capacity(N);
    let overall = Instant::now();

    for i in 0..N {
        let dir = root.join(format!("p{i:05}"));

        let t = Instant::now();
        std::fs::create_dir_all(&dir)?;
        mkdir += t.elapsed();

        let t = Instant::now();
        let cfg = DiskConfig::new_with_table_name(dir.display().to_string(), "t", 1);
        config += t.elapsed();

        let t = Instant::now();
        let engine = ProbePersistenceEngine::new(cfg).await?;
        engine_new += t.elapsed();

        let t = Instant::now();
        held.push(ProbeWorkTable::new(engine).await?);
        table_new += t.elapsed();
    }
    let total = overall.elapsed();

    let per = |d: Duration| ms(d) / N as f64;
    println!("Persisted table construction, {N} instances, one directory each\n");
    println!("  {:<32} {:>10}   {:>7}", "phase", "ms each", "share");
    println!("  {}", "-".repeat(54));
    for (label, d) in [
        ("create_dir_all", mkdir),
        ("DiskConfig::new_with_table_name", config),
        ("PersistenceEngine::new", engine_new),
        ("PersistedWorkTable::new", table_new),
    ] {
        println!("  {label:<32} {:>10.3}   {:>6.1}%", per(d), 100.0 * ms(d) / ms(total));
    }
    println!("  {:<32} {:>10.3}", "total", per(total));

    let (files, bytes) = count_tree(&root);
    println!("\n  on disk after {N} partitions");
    println!("    files                          {files:>10}");
    println!("    bytes                          {bytes:>10}");
    println!(
        "    files per partition            {:>10.1}",
        files as f64 / (N + 1) as f64
    );
    println!(
        "    bytes per partition            {:>10.0}",
        bytes as f64 / (N + 1) as f64
    );

    // Does it degrade as the parent directory fills, or is it flat?
    println!("\n  is the cost flat across the run, or does the filesystem degrade?");
    let mut marks = Vec::new();
    for chunk in 0..4 {
        let t = Instant::now();
        for i in 0..25 {
            let idx = 10_000 + chunk * 25 + i;
            let dir = root.join(format!("q{idx:05}"));
            std::fs::create_dir_all(&dir)?;
            let cfg = DiskConfig::new_with_table_name(dir.display().to_string(), "t", 1);
            let engine = ProbePersistenceEngine::new(cfg).await?;
            held.push(ProbeWorkTable::new(engine).await?);
        }
        marks.push(ms(t.elapsed()) / 25.0);
    }
    for (i, m) in marks.iter().enumerate() {
        println!(
            "    partitions {:>4} to {:>4}          {m:>10.3} ms each",
            200 + i * 25,
            225 + i * 25
        );
    }

    drop(held);
    std::fs::remove_dir_all(&root).ok();
    Ok(())
}
