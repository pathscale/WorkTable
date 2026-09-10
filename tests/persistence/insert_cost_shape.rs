//! Where the time in an insert goes, since it is not the disk.
//!
//! WorkTable's bulk load runs at about 200 MB/s while the write path under it
//! does gigabytes, and removing persistence entirely changes nothing. So the
//! cost is in the insert. This asks the first question that splits the
//! candidates: does it scale with the size of the row, or is it a fixed price
//! per row?

use worktable::prelude::*;
use worktable_codegen::worktable;

worktable!(
    name: InsertShape,
    columns: {
        id: u64 primary_key,
        payload: String,
    }
);

#[test]
#[ignore = "a measurement, not an assertion"]
fn does_insert_cost_scale_with_row_size() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        const ROWS: u64 = 25_000;
        println!("  payload      rows/s     us/row      MB/s");
        for size in [8usize, 64, 256, 1024, 2048, 4096, 8192] {
            let payload = "x".repeat(size);
            // Warm: allocator and the table's first growth are not the subject.
            {
                let warm = InsertShapeWorkTable::default();
                for id in 0..1_000u64 {
                    warm.insert(InsertShapeRow {
                        id,
                        payload: payload.clone(),
                    })
                    .await
                    .unwrap();
                }
            }
            let table = InsertShapeWorkTable::default();
            let at = std::time::Instant::now();
            for id in 0..ROWS {
                table
                    .insert(InsertShapeRow {
                        id,
                        payload: payload.clone(),
                    })
                    .await
                    .unwrap();
            }
            let elapsed = at.elapsed().as_secs_f64();
            println!(
                "  {size:>7}   {:>9.0}   {:>8.2}   {:>7.0}",
                ROWS as f64 / elapsed,
                elapsed * 1e6 / ROWS as f64,
                (ROWS as usize * size) as f64 / 1e6 / elapsed,
            );
        }
    });
}

/// The same rows, with the table taken out of it: what the loop costs when all
/// it does is build the row and drop it. Anything the insert arm spends beyond
/// this is the table.
#[test]
#[ignore = "a measurement, not an assertion"]
fn what_the_loop_costs_without_the_table() {
    const ROWS: u64 = 25_000;
    println!("  payload      rows/s     us/row");
    for size in [8usize, 4096] {
        let payload = "x".repeat(size);
        let at = std::time::Instant::now();
        let mut sink = 0usize;
        for id in 0..ROWS {
            let row = InsertShapeRow {
                id,
                payload: payload.clone(),
            };
            sink = sink.wrapping_add(row.payload.len());
            std::hint::black_box(&row);
        }
        let elapsed = at.elapsed().as_secs_f64();
        std::hint::black_box(sink);
        println!(
            "  {size:>7}   {:>9.0}   {:>8.3}",
            ROWS as f64 / elapsed,
            elapsed * 1e6 / ROWS as f64,
        );
    }
}

/// A long run of the expensive case, so a sampling profiler has something to
/// look at. Not a measurement in itself.
#[test]
#[ignore = "for profiling only"]
fn keep_inserting_four_kilobyte_rows() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let payload = "x".repeat(4096);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            let table = InsertShapeWorkTable::default();
            for id in 0..20_000u64 {
                table
                    .insert(InsertShapeRow {
                        id,
                        payload: payload.clone(),
                    })
                    .await
                    .unwrap();
            }
        }
    });
}

/// Does an insert get slower as the table fills?
///
/// A cost that scales with rows already present is O(n) per insert and O(n^2)
/// overall, which is what a super-linear response to row size would look like
/// if bigger rows simply reach any given page count sooner.
#[test]
#[ignore = "a measurement, not an assertion"]
fn does_insert_slow_down_as_the_table_fills() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        for size in [256usize, 4096] {
            let payload = "x".repeat(size);
            let table = InsertShapeWorkTable::default();
            const BLOCK: u64 = 2_000;
            const BLOCKS: u64 = 10;
            println!("  payload {size}: us/row by block of {BLOCK}");
            let mut line = String::new();
            for block in 0..BLOCKS {
                let at = std::time::Instant::now();
                for n in 0..BLOCK {
                    let id = block * BLOCK + n;
                    table
                        .insert(InsertShapeRow {
                            id,
                            payload: payload.clone(),
                        })
                        .await
                        .unwrap();
                }
                let per_row = at.elapsed().as_secs_f64() * 1e6 / BLOCK as f64;
                line.push_str(&format!("{per_row:>8.2}"));
            }
            println!("   {line}");
        }
    });
}

/// The per-row cost with the page-list clone nearly absent, which is the floor
/// a fix for it would approach.
///
/// The clone is O(pages), so a table that has barely any pages barely pays it.
/// Timing small tables gives the cost of everything else: the row clone, the
/// rkyv serialize, and the copy into the page.
#[test]
#[ignore = "a measurement, not an assertion"]
fn the_floor_with_almost_no_pages() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        println!("  payload   rows   pages    us/row      MB/s");
        for size in [256usize, 1024, 4096] {
            let payload = "x".repeat(size);
            let per_page = (16356 / size).max(1);
            for rows in [30u64, 120, 480] {
                // Median of several fresh tables: a single small run is noise.
                let mut samples = Vec::new();
                for _ in 0..25 {
                    let table = InsertShapeWorkTable::default();
                    let at = std::time::Instant::now();
                    for id in 0..rows {
                        table
                            .insert(InsertShapeRow {
                                id,
                                payload: payload.clone(),
                            })
                            .await
                            .unwrap();
                    }
                    samples.push(at.elapsed().as_secs_f64() / rows as f64);
                }
                samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let per_row = samples[samples.len() / 2];
                println!(
                    "  {size:>7}  {rows:>5}   {:>5}   {:>7.3}   {:>7.0}",
                    rows as usize / per_page,
                    per_row * 1e6,
                    size as f64 / 1e6 / per_row,
                );
            }
        }
    });
}
