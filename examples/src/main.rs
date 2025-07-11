use std::path::Path;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use rand::Rng;
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: S5Trace,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        symbol: String,
        timestamp: i64,
        event_id: u64,
        current_spread: f64,
        target_position: f64,
        max_unhedged: f64,
        error: String,

        left_best_bid_price: f64,
        left_best_ask_price: f64,
        left_best_bid_volume: f64,
        left_best_ask_volume: f64,
        left_bid_right_ask_spread: f64,
        left_next_funding_timestamp: u64,
        left_funding_rate: f64,
        left_exchange_average_funding: f64,
        left_bid_right_ask_median_spread: f64,

        right_best_bid_price: f64,
        right_best_ask_price: f64,
        right_best_bid_volume: f64,
        right_best_ask_volume: f64,
        right_bid_left_ask_spread: f64,
        right_next_funding_timestamp: u64,
        right_funding_rate: f64,
        right_exchange_average_funding: f64,
        right_bid_left_ask_median_spread: f64,

        extrapolated_funding: f64,
        difference_exchange_funding: f64,

    }
    indexes: {
        event_id_idx: event_id,
    },
    config: {
        row_derives: Default,
    }
);

impl S5TraceWorkTable {
    pub fn record(&self, row: &S5TraceRow) -> Result<S5TracePrimaryKey, WorkTableError> {
        let mut new_row = row.clone();
        new_row.id = self.get_next_pk().into();
        self.insert(new_row)
    }
}

use std::sync::{Arc, Mutex};
use tokio::task;

#[tokio::main(worker_threads = 8)]
async fn main() {
    // Init Worktable
    let config = PersistenceConfig::new("data", "data");
    let my_table = Arc::new(S5TraceWorkTable::new(config).await.unwrap());

    let total: u64 = 1_000_000;
    let tasks = 8;
    let chunk = total / tasks;

    let counter = Arc::new(AtomicI64::new(0));

    let mut handles = Vec::with_capacity(tasks as usize);
    for t in 0..tasks {
        let start_id = t * chunk;
        let end_id = if t == tasks - 1 {
            total
        } else {
            (t + 1) * chunk
        };

        let table = my_table.clone();
        let c = counter.clone();
        handles.push(task::spawn(async move {
            // Each task creates its own RNG
            let symbol = String::from("12321312312");

            for id in start_id..end_id {
                let now = c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let error = "dasdasdsada".to_string();

                table.record(&S5TraceRow {
                    id,
                    symbol: symbol.clone(),
                    timestamp: now,
                    event_id: now as u64,
                    current_spread: 0.0,
                    target_position: 0.0,
                    max_unhedged: 0.0,
                    error,

                    left_best_bid_price: 0.0,
                    left_best_ask_price: 0.0,
                    left_best_bid_volume: 0.0,
                    left_best_ask_volume: 0.0,
                    left_bid_right_ask_spread: 0.0,
                    left_next_funding_timestamp: now as u64,
                    left_funding_rate: 0.0,
                    left_exchange_average_funding: 0.0,
                    left_bid_right_ask_median_spread: 0.0,

                    right_best_bid_price: 0.0,
                    right_best_ask_price: 0.0,
                    right_best_bid_volume: 0.0,
                    right_best_ask_volume: 0.0,
                    right_bid_left_ask_spread: 0.0,
                    right_next_funding_timestamp: now as u64,
                    right_funding_rate: 0.0,
                    right_exchange_average_funding: 0.0,
                    right_bid_left_ask_median_spread: 0.0,

                    extrapolated_funding: 0.0,
                    difference_exchange_funding: 0.0,
                });
            }

            // Simulate some async work
            tokio::time::sleep(Duration::from_millis(50)).await;
        }));
    }

    // Await all tasks
    for h in handles {
        let _ = h.await;
    }
}
