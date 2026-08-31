//! Fixed cost of one WorkTable instance, decomposed.
//!
//! A partitioned table is N instances of one table type, so the per-instance
//! fixed cost decides how many partitions are affordable. An earlier
//! measurement put an empty instance at roughly 32 KB, which is 81 GB at
//! 800,000 instances and 650 MB at the few hundred a residency budget would
//! keep live. That difference decides whether a fine-grained partition axis is
//! buildable, so this probe breaks the number into its parts.
//!
//! Bytes are live heap, measured with a counting global allocator, minus the
//! storage of the vector holding the instances. Run with:
//! `cargo run --release --example partition_overhead_probe`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use worktable::prelude::*;
use worktable::worktable;

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            LIVE.fetch_add(l.size(), Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        unsafe { System.dealloc(p, l) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn live() -> usize {
    LIVE.load(Ordering::Relaxed)
}

const N: usize = 1_000;

/// Build N instances, report total bytes and construction time per instance.
macro_rules! probe {
    ($label:literal, $table:ty, $make:expr) => {{
        // Warm: first construction touches lazy statics we do not want counted.
        {
            let _w = $make();
        }
        let before = live();
        let start = Instant::now();
        let held: Vec<$table> = (0..N).map(|_| $make()).collect();
        let ns = start.elapsed().as_nanos() as f64 / N as f64;
        let vec_bytes = std::mem::size_of::<$table>() * N;
        let per = (live().saturating_sub(before).saturating_sub(vec_bytes)) as f64 / N as f64
            + std::mem::size_of::<$table>() as f64;
        drop(held);
        ($label, per, ns)
    }};
}

// V1: the floor. One primary key, one payload column, no secondary index.
worktable!(name: V1Minimal, columns: { id: u64 primary_key, v: u64 });

// V2, V3: same shape, different page sizes. Isolates the data page.
worktable!(name: V2Page1k, columns: { id: u64 primary_key, v: u64 }, config: { page_size: 1024 });
worktable!(name: V3Page8k, columns: { id: u64 primary_key, v: u64 }, config: { page_size: 8192 });

// V4, V5: one and two secondary indexes. The delta is the per-index cost.
worktable!(
    name: V4OneIdx,
    columns: { id: u64 primary_key, v: u64, w: u64 },
    indexes: { v_idx: v unique }
);
worktable!(
    name: V5TwoIdx,
    columns: { id: u64 primary_key, v: u64, w: u64 },
    indexes: { v_idx: v unique, w_idx: w unique }
);

// V6: twelve columns, no extra index. Isolates per-column cost.
worktable!(
    name: V6Wide,
    columns: {
        id: u64 primary_key, c1: u64, c2: u64, c3: u64, c4: u64, c5: u64,
        c6: f64, c7: f64, c8: f64, c9: i64, c10: i64, c11: u32
    }
);

// V7: a String column and a String index, the shape a text index actually has.
worktable!(
    name: V7String,
    columns: { id: u64 primary_key, name: String, v: u64 },
    indexes: { name_idx: name unique }
);

// V8 to V10: index backend comparison at a fixed shape. AgentCode indexes on
// arctic, so whether a backend changes the per-instance cost matters directly
// to how many partitions fit.
worktable!(
    name: V8Wti,
    persist: false,
    columns: { id: u64 primary_key, v: u64, w: u64 },
    indexes: { v_idx: v unique using worktables_index }
);
worktable!(
    name: V9Congee,
    persist: false,
    columns: { id: u64 primary_key, v: u64, w: u64 },
    indexes: { v_idx: v unique using congee }
);
worktable!(
    name: V10Arctic,
    persist: false,
    columns: { id: u64 primary_key, v: u64, w: u64 },
    indexes: { v_idx: v unique using arctic }
);

// V11 to V14: how low can the floor go, and what does persistence add?
worktable!(name: V11Page256, columns: { id: u64 primary_key, v: u64 }, config: { page_size: 256 });
worktable!(
    name: V12Cheapest,
    persist: false,
    columns: { id: u64 primary_key, v: u64, w: u64 },
    indexes: { v_idx: v unique using congee },
    config: { page_size: 1024 }
);
worktable!(
    name: V13ArcticSmallPage,
    persist: false,
    columns: { id: u64 primary_key, v: u64, w: u64 },
    indexes: { v_idx: v unique using arctic },
    config: { page_size: 1024 }
);
worktable!(
    name: V14Persisted,
    persist: true,
    columns: { id: u64 primary_key autoincrement, v: u64, w: u64 },
    indexes: { v_idx: v unique using arctic },
    config: { page_size: 1024 }
);

fn row(label: &str, per: f64, ns: f64, base: Option<f64>) {
    let delta = match base {
        Some(b) => format!("{:+9.0}", per - b),
        None => "         ".to_string(),
    };
    println!("  {label:<28} {per:>9.0} B {delta}   {ns:>9.0} ns");
}

/// Persisted instances, measured separately: they need an async engine and a
/// directory each, so this runs at a smaller N and includes real disk I/O.
/// AgentCode persists every partition, so this is the number that actually
/// applies to them rather than the in-memory floor above.
async fn persisted_cost() -> eyre::Result<f64> {
    const PN: usize = 200;
    let root = std::env::temp_dir().join(format!("wt-partition-probe-{}", std::process::id()));
    std::fs::create_dir_all(&root)?;

    // Warm, so lazy statics and the first directory creation are not counted.
    {
        let cfg = DiskConfig::new_with_table_name(root.join("warm").display().to_string(), "t", 1);
        let engine = V14PersistedPersistenceEngine::new(cfg).await?;
        let _t = V14PersistedWorkTable::new(engine).await?;
    }

    let before = live();
    let start = Instant::now();
    let mut held = Vec::with_capacity(PN);
    for i in 0..PN {
        let dir = root.join(format!("p{i:05}"));
        let cfg = DiskConfig::new_with_table_name(dir.display().to_string(), "t", 1);
        let engine = V14PersistedPersistenceEngine::new(cfg).await?;
        held.push(V14PersistedWorkTable::new(engine).await?);
    }
    let ns = start.elapsed().as_nanos() as f64 / PN as f64;
    let vec_bytes = std::mem::size_of::<V14PersistedWorkTable>() * PN;
    let per = (live().saturating_sub(before).saturating_sub(vec_bytes)) as f64 / PN as f64
        + std::mem::size_of::<V14PersistedWorkTable>() as f64;
    drop(held);
    println!("\n  persisted, arctic index, page_size 1024, {PN} instances");
    println!("  {}", "-".repeat(66));
    println!("  {:<28} {per:>9.0} B             {:>9.0} ns", "V14 persist: true", ns);
    std::fs::remove_dir_all(&root).ok();
    Ok(per)
}

fn main() {
    println!("Per-instance fixed cost, {N} instances each, empty tables\n");
    println!(
        "  {:<28} {:>9}   {:>9}   {:>12}",
        "variant", "bytes", "vs floor", "construct"
    );
    println!("  {}", "-".repeat(66));

    let (_, v1, v1ns) = probe!("V1 minimal, no index", V1MinimalWorkTable, V1MinimalWorkTable::default);
    row("V1 minimal, no index", v1, v1ns, None);

    let (l, p, ns) = probe!("V2 page_size 1024", V2Page1kWorkTable, V2Page1kWorkTable::default);
    row(l, p, ns, Some(v1));
    let (l, p, ns) = probe!("V3 page_size 8192", V3Page8kWorkTable, V3Page8kWorkTable::default);
    row(l, p, ns, Some(v1));
    let (l, p, ns) = probe!("V4 one secondary index", V4OneIdxWorkTable, V4OneIdxWorkTable::default);
    row(l, p, ns, Some(v1));
    let (l, p, ns) = probe!(
        "V5 two secondary indexes",
        V5TwoIdxWorkTable,
        V5TwoIdxWorkTable::default
    );
    row(l, p, ns, Some(v1));
    let (l, p, ns) = probe!("V6 twelve columns", V6WideWorkTable, V6WideWorkTable::default);
    row(l, p, ns, Some(v1));
    let (l, p, ns) = probe!(
        "V7 String column + index",
        V7StringWorkTable,
        V7StringWorkTable::default
    );
    row(l, p, ns, Some(v1));

    println!("\n  index backend, same shape as V4");
    println!("  {}", "-".repeat(66));
    let (l, p, ns) = probe!("V8 worktables_index", V8WtiWorkTable, V8WtiWorkTable::default);
    row(l, p, ns, Some(v1));
    let (l, p, ns) = probe!("V9 congee", V9CongeeWorkTable, V9CongeeWorkTable::default);
    row(l, p, ns, Some(v1));
    let (l, p, ns) = probe!("V10 arctic", V10ArcticWorkTable, V10ArcticWorkTable::default);
    row(l, p, ns, Some(v1));

    println!("\n  how low can it go");
    println!("  {}", "-".repeat(66));
    let (l, p, ns) = probe!(
        "V11 no index, page_size 256",
        V11Page256WorkTable,
        V11Page256WorkTable::default
    );
    row(l, p, ns, Some(v1));
    let (l, cheap, ns) = probe!(
        "V12 congee + page 1024",
        V12CheapestWorkTable,
        V12CheapestWorkTable::default
    );
    row(l, cheap, ns, Some(v1));
    let (l, p, ns) = probe!(
        "V13 arctic + page 1024",
        V13ArcticSmallPageWorkTable,
        V13ArcticSmallPageWorkTable::default
    );
    row(l, p, ns, Some(v1));

    println!("\n  eight tables per partition, the AgentCode shape");
    println!("  {}", "-".repeat(66));
    for (label, per) in [
        ("as measured today, arctic + default page", 48_296.0),
        ("congee + page 1024", cheap),
    ] {
        println!("    {label}");
        for n in [800usize, 10_000, 100_000] {
            let mb = per * 8.0 * n as f64 / 1e6;
            let unit = if mb > 1000.0 {
                format!("{:.1} GB", mb / 1000.0)
            } else {
                format!("{mb:.0} MB")
            };
            println!("      {n:>7} partitions x8 tables  {unit:>10}");
        }
    }

    let persisted = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(persisted_cost());
    match persisted {
        Ok(per) => {
            println!("\n  eight persisted tables per partition, what AgentCode would actually pay");
            for n in [800usize, 10_000, 100_000] {
                let mb = per * 8.0 * n as f64 / 1e6;
                let unit = if mb > 1000.0 {
                    format!("{:.1} GB", mb / 1000.0)
                } else {
                    format!("{mb:.0} MB")
                };
                println!("      {n:>7} partitions x8 tables  {unit:>10}");
            }
        }
        Err(e) => println!("\n  persisted probe failed: {e}"),
    }

    println!("\n  What the floor costs at partition scale, V1 with no index:");
    for n in [1_000usize, 10_000, 100_000, 800_000] {
        println!("    {n:>9} instances  {:>10.2} MB", v1 * n as f64 / 1e6);
    }
}
