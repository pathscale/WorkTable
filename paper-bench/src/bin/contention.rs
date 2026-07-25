//! Lock-granularity contention matrix (paper §5, C2 claim).
//!
//! One hot row; N tasks update it continuously for DURATION_SECS.
//! Modes:
//!   disjoint — half the tasks update column `b`, half update column `e`
//!              (disjoint write sets => field-granular locks don't collide)
//!   overlap  — every task updates {b,e} (identical write sets => serialize)
//!   mutex    — like disjoint, but behind one external tokio::Mutex
//!              (emulates a single-lock table)
//!   inplace  — every task runs the in_place closure increment on `b`
//!
//! CSV to stdout: bench,mode,tasks,ops_per_sec

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use wt_paper_bench::util::*;
use wt_paper_bench::*;

async fn run(mode: &'static str, tasks: usize) -> f64 {
    let table = Arc::new(BenchWorkTable::default());
    let pk = table.insert(mk_row(&table, 1)).unwrap();
    let pk_val: u64 = pk.into();

    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let big_lock = Arc::new(tokio::sync::Mutex::new(()));

    let mut handles = Vec::new();
    for i in 0..tasks {
        let table = table.clone();
        let stop = stop.clone();
        let total = total.clone();
        let big_lock = big_lock.clone();
        handles.push(tokio::spawn(async move {
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                match mode {
                    "disjoint" => {
                        if i % 2 == 0 {
                            table.update_upd_b(UpdBQuery { b: n }, pk_val).await.unwrap();
                        } else {
                            table.update_upd_e(UpdEQuery { e: n }, pk_val).await.unwrap();
                        }
                    }
                    "overlap" => {
                        table.update_upd_be(UpdBEQuery { b: n, e: n }, pk_val).await.unwrap();
                    }
                    "mutex" => {
                        let _g = big_lock.lock().await;
                        if i % 2 == 0 {
                            table.update_upd_b(UpdBQuery { b: n }, pk_val).await.unwrap();
                        } else {
                            table.update_upd_e(UpdEQuery { e: n }, pk_val).await.unwrap();
                        }
                    }
                    "inplace" => {
                        table.update_inc_b_in_place(|b| *b += 1, pk_val).await.unwrap();
                    }
                    _ => unreachable!(),
                }
                n += 1;
            }
            total.fetch_add(n, Ordering::Relaxed);
        }));
    }

    let dur = env_secs("DURATION_SECS", 5);
    let t0 = Instant::now();
    tokio::time::sleep(dur).await;
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.await.unwrap();
    }
    total.load(Ordering::Relaxed) as f64 / t0.elapsed().as_secs_f64()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    println!("bench,mode,tasks,ops_per_sec");
    let task_counts = [1usize, 2, 4, 8, 16];
    for mode in ["disjoint", "overlap", "mutex", "inplace"] {
        for &t in &task_counts {
            // warmup
            let _ = run(mode, t).await;
            let ops = run(mode, t).await;
            println!("contention,{mode},{t},{ops:.0}");
        }
    }
}
