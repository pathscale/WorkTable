//! Hand-rolled Rust baselines (paper Table 2 rows 1-2): the structures a Rust
//! engineer reaches for before adopting an engine. Bounds the price of the
//! engine itself.
//!
//! CSV to stdout: bench,engine,op,ops_per_sec
//! External DB baselines (sled/redb/LMDB/SQLite) are a separate follow-up
//! crate so their heavy deps don't gate this one.

use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::HashMap;
use wt_paper_bench::util::*;

#[derive(Clone, Debug)]
struct Row {
    #[allow(dead_code)]
    id: u64,
    a: u64,
    b: u64,
    e: u64,
    c: f64,
    d: String,
}

fn mk(id: u64) -> Row {
    Row { id, a: id, b: id, e: id, c: id as f64, d: "payloadpayload".into() }
}

fn main() {
    let rows = env_u64("ROWS", 1_000_000);
    let reps = env_u64("REPS", 5) as usize;
    println!("bench,engine,op,ops_per_sec");

    // Vec<T> — the absolute floor (pk == index)
    let ins = median_ops_per_sec(reps, || {
        let mut v: Vec<Row> = Vec::new();
        for i in 0..rows {
            v.push(mk(i));
        }
        std::hint::black_box(&v);
        rows
    });
    println!("baseline,vec,insert,{ins:.0}");

    let v: Vec<Row> = (0..rows).map(mk).collect();
    let rd = median_ops_per_sec(reps, || {
        let mut rng = Rng::new(42);
        for _ in 0..rows {
            let i = rng.below(rows) as usize;
            std::hint::black_box(&v[i]);
        }
        rows
    });
    println!("baseline,vec,point_read,{rd:.0}");

    // RwLock<HashMap> — simplest shared map
    let m: RwLock<HashMap<u64, Row>> = RwLock::new(HashMap::new());
    let ins = median_ops_per_sec(reps, || {
        let mut g = m.write();
        g.clear();
        for i in 0..rows {
            g.insert(i, mk(i));
        }
        rows
    });
    println!("baseline,rwlock_hashmap,insert,{ins:.0}");
    let rd = median_ops_per_sec(reps, || {
        let mut rng = Rng::new(42);
        let g = m.read();
        for _ in 0..rows {
            std::hint::black_box(g.get(&rng.below(rows)));
        }
        rows
    });
    println!("baseline,rwlock_hashmap,point_read,{rd:.0}");
    let upd = median_ops_per_sec(reps, || {
        let mut rng = Rng::new(7);
        let n = rows / 10;
        for _ in 0..n {
            let pk = rng.below(rows);
            if let Some(r) = m.write().get_mut(&pk) {
                r.b = pk;
            }
        }
        n
    });
    println!("baseline,rwlock_hashmap,update_field,{upd:.0}");

    // DashMap — sharded concurrent map
    let dm: DashMap<u64, Row> = DashMap::new();
    let ins = median_ops_per_sec(reps, || {
        dm.clear();
        for i in 0..rows {
            dm.insert(i, mk(i));
        }
        rows
    });
    println!("baseline,dashmap,insert,{ins:.0}");
    let rd = median_ops_per_sec(reps, || {
        let mut rng = Rng::new(42);
        for _ in 0..rows {
            std::hint::black_box(dm.get(&rng.below(rows)));
        }
        rows
    });
    println!("baseline,dashmap,point_read,{rd:.0}");
    let upd = median_ops_per_sec(reps, || {
        let mut rng = Rng::new(7);
        let n = rows / 10;
        for _ in 0..n {
            let pk = rng.below(rows);
            if let Some(mut r) = dm.get_mut(&pk) {
                r.b = pk;
            }
        }
        n
    });
    println!("baseline,dashmap,update_field,{upd:.0}");
}
